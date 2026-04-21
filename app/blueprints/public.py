from flask import Blueprint, send_from_directory, current_app, jsonify, make_response, request
from app.database import get_db
from app.utils import get_cached_tenant_config
import html
import os
import re

bp = Blueprint('public', __name__)

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

def _render_public_shell(slug):
    title = _resolve_tenant_display_name(slug)
    safe_title = html.escape(title, quote=False)
    public_url = request.url_root.rstrip('/') + '/' + slug + '.html'
    shell_path = os.path.join(current_app.static_folder, 'gastronomia-local1.html')
    with open(shell_path, 'r', encoding='utf-8') as f:
        content = f.read()
    content = re.sub(r'<title>.*?</title>', f'<title>{safe_title}</title>', content, count=1, flags=re.IGNORECASE | re.DOTALL)
    content = re.sub(r'(<meta\s+property="og:title"\s+content=")([^"]*)(")', rf'\g<1>{safe_title}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<meta\s+name="twitter:title"\s+content=")([^"]*)(")', rf'\g<1>{safe_title}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<link\s+rel="canonical"\s+href=")([^"]*)(")', rf'\g<1>{html.escape(public_url, quote=True)}\g<3>', content, count=1, flags=re.IGNORECASE)
    content = re.sub(r'(<meta\s+property="og:url"\s+content=")([^"]*)(")', rf'\g<1>{html.escape(public_url, quote=True)}\g<3>', content, count=1, flags=re.IGNORECASE)
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
