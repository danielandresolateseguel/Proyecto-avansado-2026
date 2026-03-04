from flask import Blueprint, request, jsonify, session, current_app
from app.database import get_db
from app.utils import is_authed, check_csrf, get_cached_tenant_config, invalidate_tenant_config
import os
import json
from datetime import datetime, timedelta

print("DEBUG: Cargando modulo tenants...")

bp = Blueprint('tenants', __name__, url_prefix='/api')

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
                
        # Update header fields
        fields = ['whatsapp', 'instagram', 'instagram_label', 'location_label', 'location_url', 'opening_hours', 'logo_url', 'announcement_text', 'announcement_active', 'theme_color']
        for f in fields:
            if f in payload:
                current_cfg[f] = payload[f]
        
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
    return jsonify({
        'whatsapp': cfg.get('whatsapp', ''),
        'instagram': cfg.get('instagram', ''),
        'instagram_label': cfg.get('instagram_label', ''),
        'location': cfg.get('location', ''),
        'location_label': cfg.get('location_label', '') or cfg.get('location', ''),
        'location_url': cfg.get('location_url', ''),
        'opening_hours': cfg.get('opening_hours', ''),
        'logo_url': cfg.get('logo_url', ''),
        'announcement_active': cfg.get('announcement_active', False),
        'announcement_text': cfg.get('announcement_text', ''),
        'theme_color': cfg.get('theme_color', '#ff6a00')
    })

@bp.route('/tenants', methods=['GET'])
def get_tenants():
    """Returns a list of available tenants. Currently returns hardcoded list based on config or DB."""
    # En el futuro esto podría venir de una tabla 'tenants' real.
    # Por ahora devolvemos el tenant por defecto y los que encontremos en config.
    tenants_list = [
        {'slug': 'gastronomia-local1', 'name': 'Gastronomía Local 1'}
    ]
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

@bp.route('/tenants/create_demo', methods=['POST'])
def create_demo_tenant():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    
    payload = request.get_json(silent=True) or {}
    slug = str(payload.get('tenant_slug') or payload.get('slug') or '').strip()
    name = str(payload.get('name') or '').strip()
    contact_email = str(payload.get('contact_email') or '').strip() or None
    contact_phone = str(payload.get('contact_phone') or '').strip() or None
    
    if not slug:
        return jsonify({'error': 'tenant_slug requerido'}), 400
    slug = slug.lower()
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-_"
    if any(ch not in allowed for ch in slug):
        return jsonify({'error': 'tenant_slug inválido. Usa letras, números, - y _.'}), 400
    if not name:
        name = slug.replace('-', ' ').replace('_', ' ').title()
    
    now = datetime.utcnow().isoformat()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM tenants WHERE tenant_slug = ?", (slug,))
    row = cur.fetchone()
    if row:
        return jsonify({'error': 'tenant ya existe', 'tenant_slug': slug}), 409
    
    cur.execute(
        "INSERT INTO tenants (tenant_slug, name, contact_email, contact_phone, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (slug, name, contact_email, contact_phone, 'active', now)
    )
    
    default_cfg = {
        'shipping_cost': int(payload.get('shipping_cost') or 0),
        'time_mesa': int(payload.get('time_mesa') or 0),
        'time_espera': int(payload.get('time_espera') or 0),
        'time_delivery': int(payload.get('time_delivery') or 0),
        'time_auto': bool(payload.get('time_auto') or False),
        'sla': {
            'warning_minutes': int(payload.get('warning_minutes') or 15),
            'critical_minutes': int(payload.get('critical_minutes') or 30)
        }
    }
    cur.execute(
        "INSERT OR IGNORE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)",
        (slug, json.dumps(default_cfg, ensure_ascii=False))
    )
    conn.commit()
    invalidate_tenant_config(slug)
    
    return jsonify({'ok': True, 'tenant_slug': slug, 'name': name})

@bp.route('/tenant_tables', methods=['GET'])
def get_tenant_tables():
    slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
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
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
    row = cur.fetchone()
    if row and row[0]:
        try:
            return jsonify(json.loads(row[0]))
        except:
            pass
    return jsonify({})

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
