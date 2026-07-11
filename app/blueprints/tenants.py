from flask import Blueprint, request, jsonify, session, current_app
from app.database import get_db, is_postgres
from app.utils import is_authed, check_csrf, get_cached_tenant_config, invalidate_tenant_config
import os
import json
import unicodedata
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash

# Force reload check
bp = Blueprint('tenants', __name__, url_prefix='/api')

_SLUG_ALLOWED = set("abcdefghijklmnopqrstuvwxyz0123456789-_")

def _norm_slug(v):
    return str(v or '').strip().lower()

def _is_valid_slug(v, max_len=64):
    slug = _norm_slug(v)
    if not slug or len(slug) > max_len:
        return False
    return all(ch in _SLUG_ALLOWED for ch in slug)

def _bounded_int(value, default, min_value, max_value):
    try:
        n = int(value)
    except Exception:
        n = int(default)
    if n < min_value or n > max_value:
        raise ValueError()
    return n

def _parse_perms_json(s):
    if not s:
        return {}
    try:
        v = json.loads(s)
        if isinstance(v, dict):
            return {str(k): bool(v[k]) for k in v.keys()}
        if isinstance(v, list):
            out = {}
            for it in v:
                k = str(it or '').strip()
                if k:
                    out[k] = True
            return out
    except Exception:
        return {}
    return {}

def _can_manage_tenant_slug(slug, require_owner_or_admin=True):
    session_tenant = str(session.get('tenant_slug') or '').strip()
    role = str(session.get('admin_role') or '').strip().lower()
    owner = bool(session.get('admin_owner'))
    perms = _parse_perms_json(session.get('admin_perms') or '')
    if session_tenant and slug and session_tenant != slug:
        return False
    if not require_owner_or_admin:
        return owner or role == 'admin' or bool(perms)
    return owner or role == 'admin'

def _can_view_tenant_slug(slug, required_perm=None):
    session_tenant = str(session.get('tenant_slug') or '').strip()
    role = str(session.get('admin_role') or '').strip().lower()
    owner = bool(session.get('admin_owner'))
    perms = _parse_perms_json(session.get('admin_perms') or '')
    if session_tenant and slug and session_tenant != slug:
        return False
    if owner or role == 'admin':
        return True
    if required_perm:
        return bool(perms.get(required_perm))
    return bool(perms)

def _slugify_menu_category(value):
    text = str(value or '').strip().lower()
    if not text:
        return ''
    text = unicodedata.normalize('NFD', text)
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn')
    out = []
    prev_dash = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append('-')
                prev_dash = True
    return ''.join(out).strip('-')

def _normalize_main_menu_categories(value):
    if not isinstance(value, list):
        return []
    out = []
    seen = set()
    for idx, raw in enumerate(value, start=1):
        label = ''
        cat_id = ''
        position = idx
        if isinstance(raw, dict):
            label = str(raw.get('label') or raw.get('name') or raw.get('title') or '').strip()
            cat_id = str(raw.get('id') or raw.get('value') or raw.get('slug') or '').strip()
            try:
                position = int(raw.get('position') or idx)
            except Exception:
                position = idx
        elif isinstance(raw, str):
            label = str(raw).strip()
        if not label and cat_id:
            label = cat_id.replace('-', ' ').strip().title()
        if not cat_id and label:
            cat_id = _slugify_menu_category(label)
        norm_id = _slugify_menu_category(cat_id)
        if not label or not norm_id or norm_id == 'todos' or norm_id in seen:
            continue
        seen.add(norm_id)
        out.append({
            'id': norm_id,
            'label': label,
            'position': max(1, position)
        })
    out.sort(key=lambda item: (int(item.get('position') or 0), str(item.get('label') or '').lower(), str(item.get('id') or '').lower()))
    for idx, item in enumerate(out, start=1):
        item['position'] = idx
    return out

def ensure_tenants_status_message_column(conn, cur):
    if is_postgres():
        try:
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS status_message TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        return
    try:
        cur.execute("PRAGMA table_info(tenants)")
        cols = [r[1] for r in cur.fetchall()]
        if 'status_message' not in cols:
            cur.execute("ALTER TABLE tenants ADD COLUMN status_message TEXT DEFAULT ''")
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def ensure_tenants_plan_columns(conn, cur):
    if is_postgres():
        try:
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'standard'")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        try:
            cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_users INTEGER NOT NULL DEFAULT 3")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        return
    try:
        cur.execute("PRAGMA table_info(tenants)")
        cols = [r[1] for r in cur.fetchall()]
        changed = False
        if 'plan' not in cols:
            cur.execute("ALTER TABLE tenants ADD COLUMN plan TEXT NOT NULL DEFAULT 'standard'")
            changed = True
        if 'max_users' not in cols:
            cur.execute("ALTER TABLE tenants ADD COLUMN max_users INTEGER NOT NULL DEFAULT 3")
            changed = True
        if changed:
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def ensure_admin_users_rbac_columns(conn, cur):
    if is_postgres():
        try:
            cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'admin'")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        try:
            cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS permissions_json TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        try:
            cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS is_owner INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        return
    try:
        cur.execute("PRAGMA table_info(admin_users)")
        cols = [r[1] for r in cur.fetchall()]
        changed = False
        if 'role' not in cols:
            cur.execute("ALTER TABLE admin_users ADD COLUMN role TEXT NOT NULL DEFAULT 'admin'")
            changed = True
        if 'permissions_json' not in cols:
            cur.execute("ALTER TABLE admin_users ADD COLUMN permissions_json TEXT DEFAULT ''")
            changed = True
        if 'is_owner' not in cols:
            cur.execute("ALTER TABLE admin_users ADD COLUMN is_owner INTEGER NOT NULL DEFAULT 0")
            changed = True
        if changed:
            conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def calculate_average_times(conn, slug):
    """Calcula tiempos promedio de entrega/servicio basados en historial reciente (últimos 7 días)."""
    avgs = {}
    try:
        cur = conn.cursor()
        # Mapeo config key -> (order_type, target_status)
        metrics = [
            ('time_mesa', 'mesa', 'listo'),
            ('time_espera', 'espera', 'listo'),
            ('time_delivery', 'direccion', 'entregado')
        ]
        
        limit_date = (datetime.utcnow() - timedelta(days=7)).isoformat()
        
        for cfg_key, otype, target_status in metrics:
            # Buscamos pedidos completados recientemente
            cur.execute(f"""
                SELECT o.created_at, h.changed_at 
                FROM orders o
                JOIN order_status_history h ON o.id = h.order_id
                WHERE o.tenant_slug = ? 
                  AND o.order_type = ? 
                  AND h.status = ?
                  AND o.created_at >= ?
            """, (slug, otype, target_status, limit_date))
            
            rows = cur.fetchall()
            if not rows:
                continue
                
            total_minutes = 0
            count = 0
            for r in rows:
                try:
                    start = datetime.fromisoformat(r[0])
                    end = datetime.fromisoformat(r[1])
                    diff = (end - start).total_seconds() / 60
                    if 0 < diff < 180: # Filtrar anomalías (>3h)
                        total_minutes += diff
                        count += 1
                except:
                    pass
            
            if count > 0:
                avgs[cfg_key] = round(total_minutes / count)
                
    except Exception as e:
        print(f"Error calculating metrics: {e}")
        
    return avgs

@bp.route('/tenant_header', methods=['GET', 'PATCH'])
def get_tenant_header():
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    
    if request.method == 'PATCH':
        if not is_authed():
            return jsonify({'error': 'no autorizado'}), 401
        if not check_csrf():
            return jsonify({'error': 'csrf inválido'}), 403
        session_tenant = str(session.get('tenant_slug') or '').strip()
        role = str(session.get('admin_role') or '').strip().lower()
        owner = bool(session.get('admin_owner'))
        if session_tenant and slug and session_tenant != slug:
            return jsonify({'error': 'tenant inválido'}), 403
        if not (owner or role == 'admin'):
            return jsonify({'error': 'no autorizado'}), 403
            
        payload = request.get_json(silent=True) or {}
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
        row = cur.fetchone()
        
        current_cfg = {}
        if row and row[0]:
            try:
                current_cfg = json.loads(row[0])
            except:
                pass
                
        fields = ['name', 'whatsapp', 'instagram', 'instagram_label', 'location_label', 'location_url', 'opening_hours', 'opening_hours_label', 'contact_email', 'footer_title', 'footer_tagline', 'footer_contact_title', 'footer_location_title', 'footer_bottom', 'timezone', 'currency_code', 'currency_locale', 'logo_url', 'announcement_text', 'announcement_active', 'theme_color', 'header_bg_color', 'featured_bg_color', 'menu_bg_color', 'interest_bg_color', 'main_menu_compact_view']
        for f in fields:
            if f in payload:
                current_cfg[f] = payload[f]
        if 'promotions' in payload and isinstance(payload.get('promotions'), dict):
            current_cfg['promotions'] = payload.get('promotions')
        if 'main_menu_categories' in payload:
            current_cfg['main_menu_categories'] = _normalize_main_menu_categories(payload.get('main_menu_categories'))
        
        # Special case for location/location_label compatibility
        if 'location_label' in payload:
            current_cfg['location'] = payload['location_label']
            
        try:
            cur.execute("INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)", 
                       (slug, json.dumps(current_cfg, ensure_ascii=False)))
            conn.commit()
            invalidate_tenant_config(slug)
            return jsonify({'ok': True})
        except Exception as e:
            print(f"Error saving header: {e}")
            return jsonify({'error': 'error al guardar'}), 500

    cfg = get_cached_tenant_config(slug)
    
    # Fallback for nested config (legacy format support)
    meta_branding = cfg.get('meta', {}).get('branding', {})
    meta_contact = meta_branding.get('contact', {})

    tenant_name = ''
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT name FROM tenants WHERE tenant_slug = ?", (slug,))
        row = cur.fetchone()
        if row and row[0]:
            tenant_name = str(row[0] or '').strip()
    except Exception:
        tenant_name = ''
    
    oh = cfg.get('opening_hours') or meta_contact.get('opening_hours') or {}
    def _parse_intervals(v):
        out = []
        if isinstance(v, list):
            for it in v:
                if isinstance(it, list) and len(it) >= 2 and it[0] and it[1]:
                    out.append([str(it[0]), str(it[1])])
                elif isinstance(it, str) and '-' in it:
                    parts = [p.strip() for p in it.split('-') if p.strip()]
                    if len(parts) >= 2:
                        out.append([parts[0], parts[1]])
        elif isinstance(v, str):
            segs = [s.strip() for s in v.split(',') if s.strip()]
            for seg in segs:
                if '-' in seg:
                    a, b = [p.strip() for p in seg.split('-', 1)]
                    if a and b:
                        out.append([a, b])
        return out
    def _normalize_hours(obj):
        days_map = {
            'mon': 'mon','monday':'mon','lunes':'mon','lun':'mon',
            'tue': 'tue','tuesday':'tue','martes':'tue','mar':'tue',
            'wed': 'wed','wednesday':'wed','miercoles':'wed','miércoles':'wed','mie':'wed','mié':'wed',
            'thu': 'thu','thursday':'thu','jueves':'thu','jue':'thu',
            'fri': 'fri','friday':'fri','viernes':'fri','vie':'fri',
            'sat': 'sat','saturday':'sat','sabado':'sat','sábado':'sat','sab':'sat','sáb':'sat',
            'sun': 'sun','sunday':'sun','domingo':'sun','dom':'sun'
        }
        if isinstance(obj, dict):
            res = {}
            for k, v in obj.items():
                key = days_map.get(str(k).strip().lower())
                if not key:
                    continue
                parsed = _parse_intervals(v)
                if parsed:
                    res[key] = parsed
            return res
        if isinstance(obj, str):
            parsed = _parse_intervals(obj)
            if parsed:
                return {'mon': parsed, 'tue': parsed, 'wed': parsed, 'thu': parsed, 'fri': parsed, 'sat': parsed, 'sun': parsed}
        return {}
    opening_hours = _normalize_hours(oh)
    timezone = str(cfg.get('timezone') or meta_contact.get('timezone') or 'America/Argentina/Mendoza').strip()
    currency_code = str(cfg.get('currency_code') or meta_contact.get('currency_code') or 'ARS').strip().upper()
    currency_locale = str(cfg.get('currency_locale') or meta_contact.get('currency_locale') or 'es-AR').strip()
    promotions = cfg.get('promotions') if isinstance(cfg.get('promotions'), dict) else {}
    promotion_banner = promotions.get('banner') if isinstance(promotions.get('banner'), dict) else {}
    promotion_entry = promotions.get('entry_modal') if isinstance(promotions.get('entry_modal'), dict) else {}
    announcement_active = bool(cfg.get('announcement_active', promotion_banner.get('active', False)))
    announcement_text = cfg.get('announcement_text') or promotion_banner.get('text') or meta_branding.get('announcement_text', '')

    return jsonify({
        'name': cfg.get('name') or tenant_name or meta_branding.get('name', ''),
        'whatsapp': cfg.get('whatsapp') or meta_contact.get('whatsapp', ''),
        'instagram': cfg.get('instagram') or meta_contact.get('instagram', ''),
        'instagram_label': cfg.get('instagram_label') or meta_contact.get('instagram_label', ''),
        'location': cfg.get('location') or meta_contact.get('location', ''),
        'location_label': cfg.get('location_label') or cfg.get('location') or meta_contact.get('location_label') or meta_contact.get('location', ''),
        'location_url': cfg.get('location_url') or meta_contact.get('location_url', ''),
        'opening_hours': opening_hours,
        'opening_hours_label': cfg.get('opening_hours_label') or meta_contact.get('opening_hours_label', ''),
        'contact_email': cfg.get('contact_email') or meta_contact.get('email', ''),
        'footer_title': cfg.get('footer_title') or meta_branding.get('footer_title', ''),
        'footer_tagline': cfg.get('footer_tagline') or meta_branding.get('footer_tagline', ''),
        'footer_contact_title': cfg.get('footer_contact_title') or meta_branding.get('footer_contact_title', ''),
        'footer_location_title': cfg.get('footer_location_title') or meta_branding.get('footer_location_title', ''),
        'footer_bottom': cfg.get('footer_bottom') or meta_branding.get('footer_bottom', ''),
        'timezone': timezone,
        'currency_code': currency_code,
        'currency_locale': currency_locale,
        'logo_url': cfg.get('logo_url') or meta_branding.get('logo_url', ''),
        'announcement_active': announcement_active,
        'announcement_text': announcement_text,
        'promotions': {
            'banner': {
                'active': announcement_active,
                'text': announcement_text
            },
            'entry_modal': promotion_entry
        },
        'theme_color': cfg.get('theme_color', '#ff6a00'),
        'header_bg_color': cfg.get('header_bg_color', '#2c1e36'),
        'featured_bg_color': cfg.get('featured_bg_color', '#0c0c0c'),
        'menu_bg_color': cfg.get('menu_bg_color', '#0f0f0f'),
        'interest_bg_color': cfg.get('interest_bg_color', '#121212'),
        'main_menu_compact_view': bool(cfg.get('main_menu_compact_view', False)),
        'main_menu_categories': _normalize_main_menu_categories(cfg.get('main_menu_categories'))
    })

@bp.route('/tenant_checkout', methods=['GET', 'PATCH'])
def tenant_checkout():
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'

    if request.method == 'GET':
        if not is_authed():
            return jsonify({'error': 'no autorizado'}), 401
        if not _can_manage_tenant_slug(slug):
            return jsonify({'error': 'no autorizado'}), 403

    if request.method == 'PATCH':
        if not is_authed():
            return jsonify({'error': 'no autorizado'}), 401
        if not check_csrf():
            return jsonify({'error': 'csrf inválido'}), 403
        if not _can_manage_tenant_slug(slug):
            return jsonify({'error': 'no autorizado'}), 403

        payload = request.get_json(silent=True) or {}
        val_enabled = payload.get('whatsapp_enabled', None)
        val_number = payload.get('whatsapp_number', None)
        val_template = payload.get('whatsapp_template', None)

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
        row = cur.fetchone()

        current_cfg = {}
        if row and row[0]:
            try:
                current_cfg = json.loads(row[0])
            except Exception:
                current_cfg = {}

        checkout = current_cfg.get('checkout')
        if not isinstance(checkout, dict):
            checkout = {}

        if val_enabled is not None:
            checkout['whatsappEnabled'] = bool(val_enabled)
        if val_number is not None:
            checkout['whatsappNumber'] = str(val_number or '').strip()
        if val_template is not None:
            checkout['whatsappTemplate'] = str(val_template or '')

        current_cfg['checkout'] = checkout

        try:
            cur.execute(
                "INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)",
                (slug, json.dumps(current_cfg, ensure_ascii=False))
            )
            conn.commit()
            invalidate_tenant_config(slug)
            return jsonify({'ok': True, 'checkout': checkout})
        except Exception as e:
            print(f"Error saving tenant checkout: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            return jsonify({'error': 'error al guardar'}), 500

    cfg = get_cached_tenant_config(slug)
    checkout = cfg.get('checkout')
    if not isinstance(checkout, dict):
        checkout = {}
    return jsonify({
        'whatsappEnabled': bool(checkout.get('whatsappEnabled', True)),
        'whatsappNumber': str(checkout.get('whatsappNumber', '') or ''),
        'whatsappTemplate': str(checkout.get('whatsappTemplate', '') or '')
    })

@bp.route('/tenants', methods=['GET'])
def get_tenants():
    """Returns a list of available tenants. Currently returns hardcoded list based on config or DB."""
    # En el futuro esto podría venir de una tabla 'tenants' real.
    # Por ahora devolvemos el tenant por defecto y los que encontremos en config.
    tenants_list = [
        {'slug': 'gastronomia-local1', 'name': 'Gastronomía Local 1'}
    ]
    if not session.get('master_auth'):
        s = str(session.get('tenant_slug') or '').strip()
        if s and s != 'gastronomia-local1':
            tenants_list.append({'slug': s, 'name': s.replace('-', ' ').title()})
        return jsonify(tenants_list)
    # Intentar leer más tenants de la DB si existen (opcional)
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT tenant_slug FROM tenant_config")
        rows = cur.fetchall()
        seen = {'gastronomia-local1'}
        for r in rows:
            slug = r[0]
            if slug and slug not in seen:
                tenants_list.append({'slug': slug, 'name': slug.replace('-', ' ').title()})
                seen.add(slug)
    except Exception:
        pass
        
    return jsonify(tenants_list)

@bp.route('/master/tenants', methods=['GET', 'PATCH'])
def master_get_tenants():
    if not session.get('master_auth'):
        return jsonify({'error': 'no autorizado'}), 401
    if request.method == 'PATCH':
        if not check_csrf():
            return jsonify({'error': 'csrf inválido'}), 403
        payload = request.get_json(silent=True) or {}
        slug = _norm_slug(payload.get('tenant_slug') or payload.get('slug'))
        status = str(payload.get('status') or '').strip().lower()
        status_message = str(payload.get('status_message') or payload.get('message') or '').strip()
        plan = str(payload.get('plan') or '').strip().lower()
        max_users = payload.get('max_users')
        if not slug:
            return jsonify({'error': 'tenant_slug requerido'}), 400
        if not _is_valid_slug(slug):
            return jsonify({'error': 'tenant_slug inválido. Usa letras, números, - y _.'}), 400
        if status not in ('active', 'warning', 'suspended'):
            return jsonify({'error': 'estado inválido'}), 400
        if len(status_message) > 300:
            return jsonify({'error': 'mensaje demasiado largo'}), 400
        if plan and plan not in ('standard', 'pro'):
            return jsonify({'error': 'plan inválido'}), 400
        if max_users is not None:
            try:
                max_users = int(max_users)
            except Exception:
                return jsonify({'error': 'max_users inválido'}), 400
            if max_users < 1 or max_users > 50:
                return jsonify({'error': 'max_users fuera de rango'}), 400
        now = datetime.utcnow().isoformat()
        conn = get_db()
        cur = conn.cursor()
        try:
            ensure_tenants_status_message_column(conn, cur)
        except Exception:
            pass
        try:
            ensure_tenants_plan_columns(conn, cur)
        except Exception:
            pass
        try:
            cur.execute("SELECT id FROM tenants WHERE tenant_slug = ?", (slug,))
            row = cur.fetchone()
            if not row:
                name = slug.replace('-', ' ').replace('_', ' ').title()
                cur.execute(
                    "INSERT INTO tenants (tenant_slug, name, contact_email, contact_phone, status, status_message, plan, max_users, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (slug, name, None, None, status, status_message, plan or 'standard', int(max_users) if max_users is not None else 3, now)
                )
            else:
                if plan:
                    cur.execute("UPDATE tenants SET plan = ? WHERE tenant_slug = ?", (plan, slug))
                if max_users is not None:
                    cur.execute("UPDATE tenants SET max_users = ? WHERE tenant_slug = ?", (int(max_users), slug))
                cur.execute("UPDATE tenants SET status = ?, status_message = ? WHERE tenant_slug = ?", (status, status_message, slug))
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return jsonify({'error': 'no se pudo actualizar el comercio'}), 500
        return jsonify({'ok': True, 'tenant_slug': slug, 'status': status, 'status_message': status_message, 'plan': plan or None, 'max_users': max_users})

    tenants_list = []
    try:
        conn = get_db()
        cur = conn.cursor()
        try:
            ensure_tenants_status_message_column(conn, cur)
        except Exception:
            pass
        try:
            ensure_tenants_plan_columns(conn, cur)
        except Exception:
            pass
        cur.execute("SELECT tenant_slug, name, status, COALESCE(status_message, '') AS status_message, COALESCE(plan, 'standard') AS plan, COALESCE(max_users, 3) AS max_users FROM tenants ORDER BY created_at DESC")
        rows = cur.fetchall()
        for r in rows:
            tenants_list.append({'slug': r[0], 'name': r[1] or r[0], 'status': r[2] or 'active', 'status_message': r[3] or '', 'plan': r[4] or 'standard', 'max_users': int(r[5] or 3)})
    except Exception:
        tenants_list = []

    if not tenants_list:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT tenant_slug FROM tenant_config")
            rows = cur.fetchall()
            seen = set()
            for r in rows:
                slug = r[0]
                if slug and slug not in seen:
                    tenants_list.append({'slug': slug, 'name': slug.replace('-', ' ').title(), 'status': 'active', 'status_message': '', 'plan': 'standard', 'max_users': 3})
                    seen.add(slug)
        except Exception:
            pass

    return jsonify({'tenants': tenants_list})

@bp.route('/tenants/create_demo', methods=['POST'])
def create_demo_tenant():
    if not session.get('master_auth'):
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    
    payload = request.get_json(silent=True) or {}
    slug = _norm_slug(payload.get('tenant_slug') or payload.get('slug'))
    name = str(payload.get('name') or '').strip()
    contact_email = str(payload.get('contact_email') or '').strip() or None
    contact_phone = str(payload.get('contact_phone') or '').strip() or None
    admin_username = str(payload.get('admin_username') or '').strip()
    admin_password = str(payload.get('admin_password') or '')
    
    if not slug:
        return jsonify({'error': 'tenant_slug requerido'}), 400
    if not admin_username or not admin_password:
        return jsonify({'error': 'usuario y clave requeridos'}), 400
    if not _is_valid_slug(slug):
        return jsonify({'error': 'tenant_slug inválido. Usa letras, números, - y _.'}), 400
    if len(name) > 120:
        return jsonify({'error': 'nombre demasiado largo'}), 400
    if contact_email and len(contact_email) > 160:
        return jsonify({'error': 'email demasiado largo'}), 400
    if contact_phone and len(contact_phone) > 40:
        return jsonify({'error': 'teléfono demasiado largo'}), 400
    if len(admin_username) > 64:
        return jsonify({'error': 'usuario demasiado largo'}), 400
    if len(admin_password) < 6 or len(admin_password) > 256:
        return jsonify({'error': 'clave inválida'}), 400
    if not name:
        name = slug.replace('-', ' ').replace('_', ' ').title()
    try:
        shipping_cost = _bounded_int(payload.get('shipping_cost', 0), 0, 0, 1000000)
        time_mesa = _bounded_int(payload.get('time_mesa', 0), 0, 0, 300)
        time_espera = _bounded_int(payload.get('time_espera', 0), 0, 0, 300)
        time_delivery = _bounded_int(payload.get('time_delivery', 0), 0, 0, 300)
        warning_minutes = _bounded_int(payload.get('warning_minutes', 15), 15, 1, 240)
        critical_minutes = _bounded_int(payload.get('critical_minutes', 30), 30, 2, 480)
    except ValueError:
        return jsonify({'error': 'valores numéricos inválidos'}), 400
    if critical_minutes <= warning_minutes:
        return jsonify({'error': 'sla inválido'}), 400
    
    now = datetime.utcnow().isoformat()
    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_tenants_plan_columns(conn, cur)
    except Exception:
        pass
    cur.execute("SELECT id FROM tenants WHERE tenant_slug = ?", (slug,))
    row = cur.fetchone()
    if row:
        return jsonify({'error': 'tenant ya existe', 'tenant_slug': slug}), 409
    
    cur.execute(
        "INSERT INTO tenants (tenant_slug, name, contact_email, contact_phone, status, plan, max_users, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (slug, name, contact_email, contact_phone, 'active', 'standard', 3, now)
    )
    
    default_cfg = {
        'shipping_cost': shipping_cost,
        'require_order_approval': True,
        'time_mesa': time_mesa,
        'time_espera': time_espera,
        'time_delivery': time_delivery,
        'time_auto': bool(payload.get('time_auto') or False),
        'sla': {
            'warning_minutes': warning_minutes,
            'critical_minutes': critical_minutes
        }
    }
    cur.execute(
        "INSERT OR IGNORE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)",
        (slug, json.dumps(default_cfg, ensure_ascii=False))
    )

    ph = generate_password_hash(admin_password)
    try:
        cur.execute(
            "INSERT INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)",
            (slug, admin_username, ph)
        )
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({'error': 'no se pudo crear el usuario principal (puede existir)'}), 409
    try:
        try:
            ensure_admin_users_rbac_columns(conn, cur)
        except Exception:
            pass
        admin_defaults = {
            'orders_view': True,
            'orders_update_status': True,
            'orders_cancel': True,
            'orders_create': True,
            'tables_manage': True,
            'cash_view': True,
            'cash_manage': True,
            'products_manage': True,
            'carousel_manage': True,
            'reports_view': True,
            'users_manage': True
        }
        cur.execute(
            "UPDATE admin_users SET role = ?, is_owner = ?, permissions_json = ? WHERE tenant_slug = ? AND lower(username) = lower(?)",
            ('admin', 1, json.dumps(admin_defaults, ensure_ascii=False), slug, admin_username)
        )
    except Exception:
        pass
    conn.commit()
    invalidate_tenant_config(slug)
    
    return jsonify({'ok': True, 'tenant_slug': slug, 'name': name, 'admin_username': admin_username})

@bp.route('/tenant_tables', methods=['GET'])
def get_tenant_tables():
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not _can_view_tenant_slug(slug, required_perm='tables_manage'):
        return jsonify({'error': 'no autorizado'}), 403
    j = get_cached_tenant_config(slug)
    tables = []
    if j:
        tables = j.get('tables') or []
        # Support legacy structure if needed, or default structure
        if not tables:
             tables = {'zones': [{'id': 1, 'name': 'Salón Principal', 'tables': []}]}
    
    # Ensure it returns the full object structure expected by frontend
    if isinstance(tables, list):
         # Convert legacy flat list to zones structure
         tables = {'zones': [{'id': 1, 'name': 'Salón Principal', 'tables': tables}]}
         
    return jsonify(tables)

@bp.route('/tenant_tables', methods=['POST'])
def update_tenant_tables():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    payload = request.get_json(silent=True) or {}
    session_tenant = str(session.get('tenant_slug') or '').strip()
    role = str(session.get('admin_role') or '').strip().lower()
    owner = bool(session.get('admin_owner'))
    perms = _parse_perms_json(session.get('admin_perms') or '')
    if session_tenant and slug and session_tenant != slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not (owner or role == 'admin' or perms.get('tables_manage')):
        return jsonify({'error': 'sin permisos'}), 403
    
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
    row = cur.fetchone()
    
    current_cfg = {}
    if row and row[0]:
        try:
            current_cfg = json.loads(row[0])
        except:
            pass
            
    # Validate payload structure slightly?
    # payload should be the 'data' object from frontend: { zones: [...] }
    if not isinstance(payload, dict) or 'zones' not in payload:
         return jsonify({'error': 'formato inválido'}), 400
         
    current_cfg['tables'] = payload
    
    try:
        cur.execute("INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)", (slug, json.dumps(current_cfg, ensure_ascii=False)))
        conn.commit()
        invalidate_tenant_config(slug)
    except Exception as e:
        print(f"Error saving tables: {e}")
        return jsonify({'error': 'error al guardar'}), 500
        
    return jsonify({'ok': True})

@bp.route('/tenant_sla', methods=['GET'])
def get_tenant_sla():
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not _can_view_tenant_slug(slug, required_perm='reports_view'):
        return jsonify({'error': 'no autorizado'}), 403
    
    # 1. Get Configured SLA
    j = get_cached_tenant_config(slug)
    sla_config = {}
    if j:
        sla_config = j.get('sla') or {}
        
    # 2. Calculate Actual Averages (Metrics)
    conn = get_db()
    metrics = calculate_average_times(conn, slug)
    
    return jsonify({
        'config': sla_config,
        'metrics': metrics
    })

@bp.route('/tenant_prefs', methods=['GET'])
def get_tenant_prefs():
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    role = str(session.get('admin_role') or '').strip().lower()
    owner = bool(session.get('admin_owner'))
    perms = _parse_perms_json(session.get('admin_perms') or '')
    session_tenant = str(session.get('tenant_slug') or '').strip()
    if session_tenant and slug and session_tenant != slug:
        return jsonify({'error': 'no autorizado'}), 403
    is_super = bool(owner) or role == 'admin'
    can_view = bool(perms) and any(bool(perms.get(k)) for k in ('cash_view', 'cash_manage', 'delivery_manage', 'orders_view', 'reports_view'))
    if not (is_super or can_view):
        return jsonify({'error': 'no autorizado'}), 403
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
    row = cur.fetchone()
    if row and row[0]:
        try:
            cfg = json.loads(row[0])
            if is_super:
                resp = jsonify(cfg)
                resp.headers['Cache-Control'] = 'no-store, max-age=0'
                resp.headers['Pragma'] = 'no-cache'
                resp.headers['Expires'] = '0'
                return resp
            safe = {}
            for key in ('tip_percent', 'tip_default_enabled', 'ticket_format', 'payment_methods'):
                if key in cfg:
                    safe[key] = cfg.get(key)
            resp = jsonify(safe)
            resp.headers['Cache-Control'] = 'no-store, max-age=0'
            resp.headers['Pragma'] = 'no-cache'
            resp.headers['Expires'] = '0'
            return resp
        except:
            pass
    resp = jsonify({})
    resp.headers['Cache-Control'] = 'no-store, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

@bp.route('/tenant_prefs', methods=['POST'])
def update_tenant_prefs():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
        
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    payload = request.get_json(silent=True) or {}
    section = payload.get('section')
    data = payload.get('data')
    if not _can_manage_tenant_slug(slug):
        return jsonify({'error': 'no autorizado'}), 403
    
    if not section or data is None:
        return jsonify({'error': 'datos incompletos'}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
    row = cur.fetchone()
    
    current_cfg = {}
    if row and row[0]:
        try:
            current_cfg = json.loads(row[0])
        except:
            pass
            
    # Update specific section
    current_cfg[section] = data
    
    try:
        cur.execute("INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)", (slug, json.dumps(current_cfg, ensure_ascii=False)))
        conn.commit()
        invalidate_tenant_config(slug)
    except Exception as e:
        print(f"Error saving prefs: {e}")
        return jsonify({'error': 'error al guardar'}), 500
        
    return jsonify({'ok': True})
