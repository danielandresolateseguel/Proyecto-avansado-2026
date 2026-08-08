from flask import Blueprint, send_from_directory, current_app, jsonify, make_response, request
from app.database import get_db
from app.utils import get_cached_tenant_config
import html
import os
import re

bp = Blueprint('public', __name__)
PUBLIC_MENU_BASE_FILES = ('public-menu-base.html', 'gastronomia-local1.html')

def _no_store(resp):
    try:
        resp.headers['Cache-Control'] = 'no-store, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
    except Exception:
        pass
    return resp

def _title_from_slug(slug):
    try:
        return re.sub(r'\s+', ' ', str(slug or '').replace('-', ' ').replace('_', ' ')).strip().title()
    except Exception:
        return ''

def _humanize_label(value):
    text = re.sub(r'[-_]+', ' ', str(value or '').strip())
    text = re.sub(r'\s+', ' ', text).strip()
    if not text:
        return ''
    return text[0].upper() + text[1:]

def _normalize_summary_text(value, max_len=160):
    text = re.sub(r'\s+', ' ', str(value or '').strip())
    if not text:
        return ''
    if len(text) <= max_len:
        return text
    clipped = text[:max_len].rsplit(' ', 1)[0].strip(' ,.;:-')
    return (clipped or text[:max_len].strip()) + '...'

def _collect_share_categories(cfg):
    labels = []
    seen = set()
    main_menu_categories = cfg.get('main_menu_categories')
    if isinstance(main_menu_categories, list):
        for item in main_menu_categories:
            if isinstance(item, dict):
                label = item.get('label') or item.get('name') or item.get('title') or item.get('id')
            else:
                label = item
            normalized = _humanize_label(label)
            key = normalized.lower()
            if normalized and key not in seen and key != 'todos':
                labels.append(normalized)
                seen.add(key)
    filters = cfg.get('filters')
    if isinstance(filters, dict):
        raw_categories = filters.get('categories')
        if isinstance(raw_categories, list):
            for item in raw_categories:
                normalized = _humanize_label(item)
                key = normalized.lower()
                if normalized and key not in seen and key != 'todos':
                    labels.append(normalized)
                    seen.add(key)
    return labels[:3]

def _join_labels(labels):
    parts = [str(label or '').strip() for label in labels if str(label or '').strip()]
    if not parts:
        return ''
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f'{parts[0]} y {parts[1]}'
    return f'{parts[0]}, {parts[1]} y {parts[2]}'

def _preferred_public_url():
    try:
        current_url = str(request.url or '').strip()
    except Exception:
        current_url = ''
    if current_url:
        return re.sub(r'^http://', 'https://', current_url, count=1, flags=re.IGNORECASE)
    try:
        fallback = str(request.url_root or '').rstrip('/') + '/' + str(request.path or '').lstrip('/')
    except Exception:
        fallback = ''
    return re.sub(r'^http://', 'https://', fallback, count=1, flags=re.IGNORECASE)

def _absolutize_public_asset(url):
    raw = str(url or '').strip()
    if not raw:
        return ''
    if re.match(r'^https?://', raw, flags=re.IGNORECASE):
        return re.sub(r'^http://', 'https://', raw, count=1, flags=re.IGNORECASE)
    if raw.startswith('//'):
        return 'https:' + raw
    try:
        base = re.sub(r'^http://', 'https://', str(request.url_root or '').rstrip('/'), count=1, flags=re.IGNORECASE)
    except Exception:
        base = ''
    normalized_path = re.sub(r'^\.?/{1,}', '', raw)
    if not normalized_path:
        return ''
    path_lower = normalized_path.lower()
    known_folders = (
        'imagenes/',
        'css/',
        'js/',
        'config/',
        'theme-loader.js',
        'media-loader.js',
        'image-loader.js',
        'lazy-phase1.js',
        'carousel.js',
        'admin.html',
        'adminmaster.html',
        'index.html',
        'public-menu-base.html',
        'gastronomia-local',
        'gastronomia-independiente',
        'gastronomia.html',
        'gastronomia-local1.html',
        'gastronomia-local2.html',
        'gastronomia-local3.html',
        'gastronomia-local4.html',
        'gastronomia-local5.html',
        'gastronomia-independiente.html',
        'comercio',
    )
    absolute_path = normalized_path
    if not normalized_path.startswith('api/'):
        if any(path_lower.startswith(prefix) for prefix in known_folders):
            absolute_path = '/' + normalized_path
        else:
            # Si no es conocido pero termina en .html, asumir root; si es asset img con nombre conocido, también root.
            if normalized_path.endswith('.html'):
                absolute_path = '/' + normalized_path
            elif any(path_lower.endswith(ext) for ext in ('.png', '.jpg', '.jpeg', '.webp', '.svg', '.avif', '.gif', '.jfif')):
                absolute_path = '/' + normalized_path
    if base and absolute_path.startswith('/'):
        out = base + absolute_path
    elif base:
        out = base + '/' + absolute_path
    else:
        out = absolute_path if absolute_path.startswith('/') else '/' + absolute_path
    # Remover slashes duplicados después del esquema para no romper routes
    try:
        if '://' in out:
            head, tail = out.split('://', 1)
            tail = re.sub(r'/{2,}', '/', tail)
            out = f'{head}://{tail}'
        else:
            out = re.sub(r'/{2,}', '/', out)
    except Exception:
        pass
    return out

def _resolve_tenant_display_name(slug):
    cfg = get_cached_tenant_config(slug) or {}
    meta_branding = cfg.get('meta', {}).get('branding', {})
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT name FROM tenants WHERE tenant_slug = ?", (slug,))
        row = cur.fetchone()
        tenant_name = str(row[0] or '').strip() if row and row[0] else ''
    except Exception:
        tenant_name = ''
    return (
        str(cfg.get('name') or '').strip()
        or tenant_name
        or str(meta_branding.get('name') or '').strip()
        or _title_from_slug(slug)
        or 'Carta Online'
    )

def _build_share_title(slug):
    cfg = get_cached_tenant_config(slug) or {}
    configured = _normalize_summary_text(cfg.get('share_title') or '', max_len=80)
    if configured:
        return configured
    name = _resolve_tenant_display_name(slug)
    return f'{name} | Menu y pedidos online'

def _build_share_description(slug):
    cfg = get_cached_tenant_config(slug) or {}
    configured = _normalize_summary_text(cfg.get('share_description') or '', max_len=160)
    if configured:
        return configured
    name = _resolve_tenant_display_name(slug)
    categories = _collect_share_categories(cfg)
    tagline = (
        cfg.get('footer_tagline')
        or cfg.get('announcement_text')
        or cfg.get('meta', {}).get('branding', {}).get('tagline')
        or ''
    )
    cleaned_tagline = _normalize_summary_text(tagline, max_len=110)
    if cleaned_tagline:
        return _normalize_summary_text(f'{name}. {cleaned_tagline}', max_len=160)
    if categories:
        return _normalize_summary_text(
            f'Descubri la carta online de {name}: {_join_labels(categories)}. Mira el menu y pedi directo con el local.',
            max_len=160
        )
    return _normalize_summary_text(
        f'Descubri la carta online de {name}, con especialidades de la casa, bebidas y pedidos directos desde el local.',
        max_len=160
    )

def _build_share_image(slug, fallback_url=''):
    cfg = get_cached_tenant_config(slug) or {}
    meta_branding = cfg.get('meta', {}).get('branding', {})
    configured = (
        cfg.get('logo_url')
        or cfg.get('share_image_url')
        or meta_branding.get('logo_url')
        or fallback_url
    )
    return _absolutize_public_asset(configured)

def _render_public_shell(slug):
    title = _build_share_title(slug)
    description = _build_share_description(slug)
    safe_title = html.escape(title, quote=False)
    safe_description = html.escape(description, quote=True)
    public_url = _preferred_public_url()
    shell_path = None
    for candidate in PUBLIC_MENU_BASE_FILES:
        path = os.path.join(current_app.static_folder, candidate)
        if os.path.exists(path):
            shell_path = path
            break
    if not shell_path:
        raise FileNotFoundError('No se encontró la plantilla base pública')
    with open(shell_path, 'r', encoding='utf-8') as f:
        content = f.read()
    content = re.sub(r'<title>.*?</title>', f'<title>{safe_title}</title>', content, count=1, flags=re.IGNORECASE | re.DOTALL)
    content = re.sub(r'(<meta\s+name="description"\s+content=")([^"]*)(")', rf'\g<1>{safe_description}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<meta\s+property="og:title"\s+content=")([^"]*)(")', rf'\g<1>{safe_title}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<meta\s+property="og:description"\s+content=")([^"]*)(")', rf'\g<1>{safe_description}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<meta\s+name="twitter:title"\s+content=")([^"]*)(")', rf'\g<1>{safe_title}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<meta\s+name="twitter:description"\s+content=")([^"]*)(")', rf'\g<1>{safe_description}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<link\s+rel="canonical"\s+href=")([^"]*)(")', rf'\g<1>{html.escape(public_url, quote=True)}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<meta\s+property="og:url"\s+content=")([^"]*)(")', rf'\g<1>{html.escape(public_url, quote=True)}\g<3>', content, count=1, flags=re.IGNORECASE)
    og_image_match = re.search(r'<meta\s+property="og:image"\s+content="([^"]*)"', content, flags=re.IGNORECASE)
    og_image_url = _build_share_image(slug, og_image_match.group(1) if og_image_match else '')
    if og_image_url:
        safe_og_image_url = html.escape(og_image_url, quote=True)
        content = re.sub(r'(<meta\s+property="og:image"\s+content=")([^"]*)(")', rf'\g<1>{safe_og_image_url}\g<3>', content, count=1, flags=re.IGNORECASE)
        content = re.sub(r'(<meta\s+name="twitter:image"\s+content=")([^"]*)(")', rf'\g<1>{safe_og_image_url}\g<3>', content, count=1, flags=re.IGNORECASE)
    resp = make_response(content)
    resp.headers['Content-Type'] = 'text/html; charset=utf-8'
    return _no_store(resp)

@bp.route('/Imagenes/<path:filename>')
def serve_images(filename):
    # Construct absolute path to Imagenes directory in project root
    # app.root_path points to '.../app'
    project_root = os.path.dirname(current_app.root_path)
    images_dir = os.path.join(project_root, 'Imagenes')
    return send_from_directory(images_dir, filename)

@bp.route('/')
def index():
    resp = send_from_directory(current_app.static_folder, 'index.html')
    return _no_store(resp)

@bp.route('/api/ping')
def ping():
    return jsonify({'pong': True})

@bp.route('/api/routes')
def routes_list():
    return jsonify({'routes': [{'rule': r.rule, 'methods': list(r.methods)} for r in current_app.url_map.iter_rules()]})

def _is_valid_logo(value):
    raw = str(value or '').strip()
    if not raw:
        return False
    if raw.startswith('data:image/gif;base64'):
        return False
    lowered = raw.lower()
    if lowered.endswith('.png') or lowered.endswith('.jpg') or lowered.endswith('.jpeg') or lowered.endswith('.webp') or lowered.endswith('.svg') or lowered.endswith('.avif'):
        return True
    if raw.startswith('http://') or raw.startswith('https://') or raw.startswith('/'):
        return True
    if raw.startswith('data:image/'):
        return True
    return False

def _collect_showcase_slugs():
    slugs = []
    seen = set()

    def add(slug):
        key = str(slug or '').strip()
        if not key or key in seen:
            return
        seen.add(key)
        slugs.append(key)

    try:
        conn = get_db()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT tenant_slug, COALESCE(status, 'active') AS status FROM tenants ORDER BY created_at DESC"
            )
            rows = cur.fetchall()
            for r in rows:
                status = str(r[1] if len(r) > 1 else 'active').strip().lower() or 'active'
                if status != 'suspended':
                    add(r[0])
        except Exception:
            try:
                cur.execute("SELECT DISTINCT tenant_slug FROM tenants")
                for r in cur.fetchall():
                    add(r[0])
            except Exception:
                pass

        try:
            cur.execute("SELECT DISTINCT tenant_slug FROM tenant_config")
            for r in cur.fetchall():
                add(r[0])
        except Exception:
            pass
    except Exception:
        pass

    try:
        project_root = os.path.dirname(current_app.root_path)
        config_dir = os.path.join(project_root, 'config')
        if os.path.isdir(config_dir):
            for fname in sorted(os.listdir(config_dir)):
                if not fname.endswith('.json'):
                    continue
                slug_candidate = fname[:-5].strip()
                if slug_candidate:
                    add(slug_candidate)
    except Exception:
        pass

    return slugs

@bp.route('/api/public/showcase/logos')
def public_showcase_logos():
    resp = jsonify({'items': []})
    resp.headers['Cache-Control'] = 'public, max-age=60, s-maxage=300'
    try:
        slugs = _collect_showcase_slugs()
        items = []
        for slug in slugs:
            try:
                cfg = get_cached_tenant_config(slug) or {}
                meta = cfg.get('meta') if isinstance(cfg.get('meta'), dict) else {}
                branding = meta.get('branding') if isinstance(meta.get('branding'), dict) else {}
                logo_raw = (
                    cfg.get('logo_url')
                    or branding.get('logo_url')
                    or ''
                )
                if not _is_valid_logo(logo_raw):
                    continue
                display_name = _resolve_tenant_display_name(slug)
                logo_url = _absolutize_public_asset(logo_raw)
                menu_url = _absolutize_public_asset(f'{slug}.html')
                items.append({
                    'slug': slug,
                    'name': display_name,
                    'logo': logo_url,
                    'menu_url': menu_url,
                })
            except Exception:
                continue
        resp = jsonify({'items': items})
        resp.headers['Cache-Control'] = 'public, max-age=60, s-maxage=300'
        return resp
    except Exception:
        return resp

@bp.route('/<path:path>')
def static_proxy(path):
    # Evitar capturar prefijos de API que no hayan sido manejados por otros blueprints
    if path.startswith('api/'):
        return jsonify({'error': 'Ruta de API no válida'}), 404
    try:
        resp = send_from_directory(current_app.static_folder, path)
        if path.endswith('.html'):
            resp = _no_store(resp)
        return resp
    except Exception:
        # Si piden un HTML que no existe, devolvemos la carta base principal.
        # El JS interno calculará el tenant_slug a partir del nombre del archivo solicitado.
        if path.endswith('.html') and re.match(r'^[a-z0-9\-_]+\.html$', path):
            try:
                slug = path[:-5]
                return _render_public_shell(slug)
            except Exception:
                return jsonify({'error': 'Página no disponible'}), 404
        # Para otros assets inexistentes devolvemos 404 controlado
        return jsonify({'error': 'Recurso no encontrado'}), 404
