import os
import json
import io
import re
from datetime import datetime
from werkzeug.utils import secure_filename
from flask import Blueprint, request, jsonify, session, current_app, send_file
from app.database import get_db
from app.utils import is_authed, check_csrf, read_xlsx_sheets, slugify_simple, get_cached_tenant_config, invalidate_tenant_config, create_xlsx_bytes
import cloudinary
import cloudinary.uploader

bp = Blueprint('products', __name__, url_prefix='/api')

def _safe_str(v):
    return str(v or '').strip()

_ID_INTLIKE_RE = re.compile(r'^(\d+)\.0+$')
_ID_DIGITS_RE = re.compile(r'^\d+$')

def _normalize_product_id_input(value):
    if value is None:
        return ''
    if isinstance(value, bool):
        return ''
    if isinstance(value, float):
        try:
            if value == value and float(value).is_integer():
                return str(int(value))
        except Exception:
            pass
    s = str(value).strip()
    if not s:
        return ''
    m = _ID_INTLIKE_RE.match(s)
    if m:
        return m.group(1)
    return s

def _product_id_canonical_key(pid):
    s = _normalize_product_id_input(pid)
    if not s:
        return ''
    if _ID_DIGITS_RE.match(s):
        try:
            return str(int(s))
        except Exception:
            return s.lstrip('0') or '0'
    return s.lower()

def _resolve_existing_product_id(pid_input, existing_ids_set):
    pid_input = _normalize_product_id_input(pid_input)
    if not pid_input:
        return ''
    if pid_input in existing_ids_set:
        return pid_input
    canon = _product_id_canonical_key(pid_input)
    if not canon:
        return pid_input
    candidates = [eid for eid in existing_ids_set if _product_id_canonical_key(eid) == canon]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise ValueError(f'id ambiguo: "{pid_input}" coincide con {", ".join(sorted(candidates)[:5])}')
    return pid_input

def _cell_present(v):
    if v is None:
        return False
    if v is False:
        return True
    s = str(v).strip()
    return s != ''

def _parse_bool(v):
    if isinstance(v, bool):
        return v
    s = str(v or '').strip().lower()
    if s in ('1', 'true', 'si', 'sí', 'yes', 'y'):
        return True
    if s in ('0', 'false', 'no', 'n'):
        return False
    return None

def _parse_int(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if not (v == v):
            return None
        return int(round(v))
    s = str(v or '').strip()
    if not s:
        return None
    s = s.replace('\u00a0', ' ').strip()
    s = s.replace(' ', '')
    s = s.replace('$', '').replace('ars', '').replace('clp', '').replace('usd', '').replace('eur', '')
    s = s.strip()
    if not s:
        return None
    try:
        if ',' in s and '.' in s:
            s2 = s.replace('.', '').replace(',', '.')
            return int(round(float(s2)))
        if ',' in s and '.' not in s:
            parts = s.split(',')
            if len(parts) >= 2 and len(parts[-1]) in (1, 2):
                s2 = s.replace(',', '.')
                return int(round(float(s2)))
            return int(s.replace(',', ''))
        return int(round(float(s)))
    except Exception:
        return None

def _split_categories(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        parts = [str(x).strip() for x in raw]
    else:
        parts = [p.strip() for p in str(raw).split(',')]
    out = []
    seen = set()
    for p in parts:
        if not p:
            continue
        cid = slugify_simple(p)
        if not cid or cid == 'todos' or cid in seen:
            continue
        seen.add(cid)
        out.append(cid)
    return out

def _find_sheet_by_name(sheets, candidates):
    lower_map = {str(k or '').strip().lower(): k for k in (sheets or {}).keys()}
    for name in candidates:
        k = lower_map.get(str(name).strip().lower())
        if k:
            return k
    if candidates:
        return lower_map.get(str(candidates[0]).strip().lower())
    return None

def _rows_to_dicts(rows):
    if not rows:
        return []
    header_row = rows[0] if rows else []
    headers = []
    for h in header_row:
        key = slugify_simple(h).replace('-', '_')
        headers.append(key)
    alias = {
        'codigo': 'id',
        'código': 'id',
        'sku': 'id',
        'producto': 'nombre',
        'nombre_producto': 'nombre',
        'price': 'precio',
        'cantidad': 'stock',
        'detalle': 'descripcion',
        'detalles': 'descripcion',
        'descripcion': 'descripcion',
        'descripción': 'descripcion',
        'categorias': 'food_categories',
        'categorías': 'food_categories',
        'categoria': 'food_categories',
        'categoría': 'food_categories',
        'foodcategories': 'food_categories',
        'food_categories': 'food_categories',
        'seccion': 'seccion',
        'sección': 'seccion',
        'section': 'seccion',
        'interest': 'interest_tag',
        'tag': 'interest_tag',
        'interes': 'interest_tag',
        'interés': 'interest_tag',
        'position': 'posicion',
        'posicion': 'posicion',
        'posición': 'posicion',
        'orden': 'posicion',
        'active': 'activo'
    }
    norm_headers = [alias.get(h, h) for h in headers]
    out = []
    for idx in range(1, len(rows)):
        r = rows[idx]
        if not r or all((v is None or str(v).strip() == '') for v in r):
            continue
        item = {'__row_index': idx + 1}
        for col_idx, key in enumerate(norm_headers):
            if not key:
                continue
            val = r[col_idx] if col_idx < len(r) else None
            item[key] = val
        out.append(item)
    return out

def _next_id_generator(existing_ids):
    used = set(str(x).strip() for x in (existing_ids or []) if str(x).strip())
    max_n = 0
    width = 0
    for pid in used:
        if pid.isdigit():
            width = max(width, len(pid))
            try:
                n = int(pid)
            except Exception:
                continue
            if n > max_n:
                max_n = n
    next_n = max_n + 1
    while True:
        candidate = str(next_n)
        if width > 1 and len(candidate) < width:
            candidate = candidate.zfill(width)
        while candidate in used:
            next_n += 1
            candidate = str(next_n)
            if width > 1 and len(candidate) < width:
                candidate = candidate.zfill(width)
        used.add(candidate)
        yield candidate
        next_n += 1

def _merge_main_menu_categories(tenant_slug, categories_in):
    cfg = get_cached_tenant_config(tenant_slug) or {}
    existing = cfg.get('main_menu_categories')
    normalized = []
    try:
        from app.blueprints.tenants import _normalize_main_menu_categories as _norm_menu_cats
        normalized = _norm_menu_cats(existing)
    except Exception:
        normalized = []
    by_id = {str(c.get('id') or '').strip(): dict(c) for c in (normalized or []) if isinstance(c, dict) and c.get('id')}
    ordered = [c for c in (normalized or []) if isinstance(c, dict) and c.get('id')]
    created = []
    updated_labels = 0
    for cat in (categories_in or []):
        if not isinstance(cat, dict):
            continue
        cid = slugify_simple(cat.get('id') or cat.get('label') or '')
        label = _safe_str(cat.get('label') or '')
        if not cid or cid == 'todos':
            continue
        if cid in by_id:
            if label and label != str(by_id[cid].get('label') or '').strip():
                by_id[cid]['label'] = label
                updated_labels += 1
            continue
        new_item = {'id': cid, 'label': label or cid.replace('-', ' ').title(), 'position': len(ordered) + 1}
        by_id[cid] = new_item
        ordered.append(new_item)
        created.append({'id': new_item['id'], 'label': new_item['label']})
    try:
        from app.blueprints.tenants import _normalize_main_menu_categories as _norm_menu_cats
        final_list = _norm_menu_cats(ordered)
    except Exception:
        final_list = ordered
        for idx, item in enumerate(final_list, start=1):
            item['position'] = idx
    cfg['main_menu_categories'] = final_list
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (tenant_slug,))
    row = cur.fetchone()
    current_cfg = {}
    if row and row[0]:
        try:
            current_cfg = json.loads(row[0])
        except Exception:
            current_cfg = {}
    if not isinstance(current_cfg, dict):
        current_cfg = {}
    current_cfg['main_menu_categories'] = final_list
    cur.execute(
        "INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)",
        (tenant_slug, json.dumps(current_cfg, ensure_ascii=False))
    )
    conn.commit()
    invalidate_tenant_config(tenant_slug)
    return {
        'created': created,
        'created_count': len(created),
        'updated_labels_count': updated_labels,
        'final_count': len(final_list)
    }

def _build_import_plan(tenant_slug, products_rows, assigned_ids_map=None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT product_id, name, price, stock, COALESCE(details,''), COALESCE(variants_json,''), active, COALESCE(image_url,''), COALESCE(position, 0) "
        "FROM products WHERE tenant_slug = ?",
        (tenant_slug,)
    )
    existing_rows = cur.fetchall() or []
    existing = {}
    existing_ids = []
    for r in existing_rows:
        pid = str(r[0] or '').strip()
        if not pid:
            continue
        existing_ids.append(pid)
        existing[pid] = {
            'id': pid,
            'name': r[1],
            'price': r[2],
            'stock': r[3],
            'details': r[4],
            'variants_json': r[5],
            'active': bool(r[6]),
            'image_url': r[7],
            'position': int(r[8] or 0)
        }
    existing_ids_set = set(existing.keys())
    id_gen = _next_id_generator(existing_ids)
    assigned_ids = {}
    actions = []
    categories_detected = []
    categories_seen = set()
    to_create = 0
    to_update = 0
    errors_count = 0
    for row in (products_rows or []):
        row_num = int(row.get('__row_index') or 0) or 0
        raw_id = row.get('id')
        pid_input = _normalize_product_id_input(raw_id)
        pid = pid_input
        if not pid:
            if assigned_ids_map and str(row_num) in assigned_ids_map:
                pid = _normalize_product_id_input(assigned_ids_map.get(str(row_num)))
            if not pid:
                pid = next(id_gen)
            assigned_ids[str(row_num)] = pid
        else:
            try:
                pid = _resolve_existing_product_id(pid, existing_ids_set)
            except Exception as e:
                pid = pid_input
                row['__id_error'] = str(e)
        name_val = row.get('nombre') if 'nombre' in row else row.get('name')
        price_val = row.get('precio') if 'precio' in row else row.get('price')
        stock_val = row.get('stock')
        details_val = row.get('descripcion') if 'descripcion' in row else row.get('details')
        section_val = row.get('seccion') if 'seccion' in row else row.get('section')
        interest_val = row.get('interest_tag')
        cats_val = row.get('food_categories') if 'food_categories' in row else row.get('categorias')
        active_val = row.get('activo') if 'activo' in row else row.get('active')
        position_val = row.get('posicion') if 'posicion' in row else row.get('position')
        entry_errors = []
        is_update = pid in existing
        if row.get('__id_error'):
            entry_errors.append(str(row.get('__id_error')))
        has_name = _cell_present(name_val)
        has_price = _cell_present(price_val)
        if not is_update:
            if not has_name:
                entry_errors.append('nombre requerido para crear')
            if not has_price:
                entry_errors.append('precio requerido para crear')
        if has_name and not _safe_str(name_val):
            entry_errors.append('nombre vacío')
        price_n = _parse_int(price_val) if has_price else None
        if has_price and price_n is None:
            entry_errors.append('precio inválido')
        stock_n = _parse_int(stock_val) if _cell_present(stock_val) else None
        if _cell_present(stock_val) and stock_n is None:
            entry_errors.append('stock inválido')
        pos_n = _parse_int(position_val) if _cell_present(position_val) else None
        if _cell_present(position_val) and pos_n is None:
            entry_errors.append('posición inválida')
        sec = _safe_str(section_val).lower()
        if _cell_present(section_val) and sec and sec not in ('main', 'featured', 'interest'):
            entry_errors.append('sección inválida (main/featured/interest)')
        cats_list = _split_categories(cats_val) if _cell_present(cats_val) else None
        if cats_list is not None:
            for cid in cats_list:
                if cid not in categories_seen:
                    categories_seen.add(cid)
                    categories_detected.append({'id': cid, 'label': cid.replace('-', ' ').title()})
        active_b = _parse_bool(active_val) if _cell_present(active_val) else None
        if _cell_present(active_val) and active_b is None:
            entry_errors.append('activo inválido (true/false/1/0)')
        has_any_change = (
            _cell_present(name_val) or
            _cell_present(price_val) or
            _cell_present(stock_val) or
            _cell_present(details_val) or
            _cell_present(section_val) or
            _cell_present(interest_val) or
            _cell_present(cats_val) or
            _cell_present(active_val) or
            _cell_present(position_val)
        )
        action = 'update' if is_update else 'create'
        if entry_errors:
            action = 'error'
            errors_count += 1
        elif action == 'update' and not has_any_change:
            action = 'skip'
        elif action == 'create':
            to_create += 1
        elif action == 'update':
            to_update += 1
        actions.append({
            'row': row_num,
            'action': action,
            'id': pid,
            'name': _safe_str(name_val) if has_name else (existing.get(pid, {}).get('name') if is_update else ''),
            'errors': entry_errors
        })
    return {
        'assigned_ids': assigned_ids,
        'actions': actions,
        'products_to_create': to_create,
        'products_to_update': to_update,
        'errors_count': errors_count,
        'categories_detected': categories_detected,
        'existing_products': existing
    }

def _session_tenant_matches(slug):
    session_tenant = str(session.get('tenant_slug') or '').strip()
    slug = str(slug or '').strip()
    return (not session_tenant) or (not slug) or session_tenant == slug

def _safe_json_loads(raw, fallback=None):
    try:
        if isinstance(raw, dict):
            return raw
        text = str(raw or '').strip()
        if not text:
            return {} if fallback is None else fallback
        return json.loads(text)
    except Exception:
        return {} if fallback is None else fallback

def _normalize_food_categories(value):
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(',') if part.strip()]
    return []

def _product_scope_from_parts(section='', interest_tag='', food_categories=None):
    sec = str(section or '').strip().lower()
    cats = _normalize_food_categories(food_categories)
    if cats:
        return f"main:{cats[0].lower()}"
    if sec == 'interest':
        tag = str(interest_tag or '').strip().lower()
        return f"interest:{tag or '__all__'}"
    if sec == 'featured':
        return 'featured:__all__'
    if sec == 'main':
        return 'main:__all__'
    return 'unassigned:__all__'

def _product_scope_from_variants(raw_variants):
    variants = _safe_json_loads(raw_variants, {})
    return _product_scope_from_parts(
        variants.get('section') or '',
        variants.get('interest_tag') or '',
        variants.get('food_categories') or []
    )

def _next_product_position(cur, tenant_slug):
    cur.execute("SELECT COALESCE(MAX(position), 0) FROM products WHERE tenant_slug = ?", (tenant_slug,))
    row = cur.fetchone()
    try:
        return int((row[0] if row else 0) or 0) + 1
    except Exception:
        return 1

def _resequence_scope(cur, tenant_slug, target_product_id, scope_key, desired_position=None):
    cur.execute(
        """
        SELECT product_id, name, COALESCE(position, 0) AS position, COALESCE(variants_json, '') AS variants_json
        FROM products
        WHERE tenant_slug = ?
        """,
        (tenant_slug,)
    )
    rows = cur.fetchall() or []
    scoped = []
    target = None
    for row in rows:
        pid = str(row[0] or '').strip()
        if not pid:
            continue
        row_scope = _product_scope_from_variants(row[3] or '')
        if row_scope != scope_key:
            continue
        item = {
            'product_id': pid,
            'name': str(row[1] or ''),
            'position': int(row[2] or 0),
        }
        scoped.append(item)
        if pid == str(target_product_id or '').strip():
            target = item
    if not scoped or target is None:
        return
    scoped.sort(key=lambda item: (int(item['position'] or 0), str(item['name'] or '').lower(), item['product_id']))
    scoped = [item for item in scoped if item['product_id'] != target['product_id']]
    insert_at = len(scoped)
    if desired_position is not None:
        try:
            requested = int(desired_position)
        except Exception:
            requested = len(scoped) + 1
        requested = max(1, requested)
        insert_at = min(len(scoped), requested - 1)
    scoped.insert(insert_at, target)
    for idx, item in enumerate(scoped, start=1):
        if int(item['position'] or 0) == idx:
            continue
        cur.execute(
            "UPDATE products SET position = ?, last_modified = ? WHERE tenant_slug = ? AND product_id = ?",
            (idx, datetime.utcnow().isoformat(), tenant_slug, item['product_id'])
        )

@bp.route('/products', methods=['GET'])
def list_products():
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    include_inactive_requested = request.args.get('include_inactive') == 'true'
    include_inactive = bool(include_inactive_requested and is_authed())
    conn = get_db()
    cur = conn.cursor()
    
    query = "SELECT product_id, name, price, stock, COALESCE(position, 0) as position, active, COALESCE(details,'') as details, COALESCE(variants_json,'') as variants_json, COALESCE(last_modified, '') as last_modified, COALESCE(image_url, '') as image_url FROM products WHERE tenant_slug = ?"
    params = [tenant_slug]
    
    if not include_inactive:
        query += " AND active = 1"
        
    query += " ORDER BY CASE WHEN COALESCE(position, 0) <= 0 THEN 1 ELSE 0 END ASC, COALESCE(position, 0) ASC, name ASC"
    
    cur.execute(query, params)
    rows = cur.fetchall()
    
    # Deduplicate by product_id
    seen_ids = set()
    items = []
    for r in rows:
        pid = r[0]
        if pid not in seen_ids:
            seen_ids.add(pid)
            items.append({
                'id': pid, 
                'name': r[1], 
                'price': int(r[2] or 0), 
                'stock': int(r[3] or 0), 
                'position': int(r[4] or 0),
                'active': bool(r[5]), 
                'details': r[6] or '', 
                'variants': r[7] or '', 
                'last_modified': r[8] or '', 
                'image_url': r[9] or ''
            })
            
    return jsonify({'products': items, 'tenant_slug': tenant_slug})

@bp.route('/products', methods=['POST'])
def create_product():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    
    data = request.get_json(silent=True) or {}
    tenant_slug = data.get('tenant_slug')
    # En entorno multi-tenant, si la sesión tiene tenant_slug, debe coincidir
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    product_id = data.get('id')
    name = data.get('name')
    
    try:
        price = int(data.get('price'))
    except:
        return jsonify({'error': 'Precio inválido'}), 400
        
    stock = int(data.get('stock', 0))
    position = data.get('position')
    details = data.get('details', '')
    image_url = data.get('image_url', '')
    section = data.get('section', '')
    interest_tag = data.get('interest_tag', '')
    food_categories = data.get('food_categories') or []
    
    if (not section) and food_categories:
        section = 'main'
    
    if isinstance(product_id, str):
        product_id = product_id.strip()
    if not tenant_slug or not name:
        return jsonify({'error': 'Faltan campos obligatorios (tenant, name)'}), 400

    variants = {}
    if section: variants['section'] = section
    if interest_tag: variants['interest_tag'] = interest_tag
    if isinstance(food_categories, list):
        if food_categories: variants['food_categories'] = food_categories
    elif isinstance(food_categories, str):
        cats = [c.strip() for c in food_categories.split(',') if c.strip()]
        if cats: variants['food_categories'] = cats
    extra_variants = data.get('variants')
    if isinstance(extra_variants, str):
        raw = extra_variants.strip()
        if raw:
            try:
                extra_variants = json.loads(raw)
            except Exception:
                extra_variants = None
        else:
            extra_variants = None
    if isinstance(extra_variants, dict):
        for k, v in extra_variants.items():
            variants[k] = v
    variants_json = json.dumps(variants)
    scope_key = _product_scope_from_parts(section, interest_tag, food_categories)
    desired_position = None
    if position not in (None, ''):
        try:
            desired_position = max(1, int(position))
        except Exception:
            return jsonify({'error': 'Posición inválida'}), 400
    
    try:
        conn = get_db()
        cur = conn.cursor()

        if not product_id:
            cur.execute("SELECT product_id FROM products WHERE tenant_slug = ?", (tenant_slug,))
            rows = cur.fetchall() or []
            max_n = 0
            width = 0
            for r in rows:
                raw = str(r[0] or '').strip()
                if not raw.isdigit():
                    continue
                width = max(width, len(raw))
                try:
                    n = int(raw)
                except Exception:
                    continue
                if n > max_n:
                    max_n = n
            next_n = max_n + 1
            candidate = str(next_n)
            if width > 1 and len(candidate) < width:
                candidate = candidate.zfill(width)
            while True:
                cur.execute("SELECT 1 FROM products WHERE tenant_slug = ? AND product_id = ? LIMIT 1", (tenant_slug, candidate))
                exists = cur.fetchone()
                if not exists:
                    break
                next_n += 1
                candidate = str(next_n)
                if width > 1 and len(candidate) < width:
                    candidate = candidate.zfill(width)
            product_id = candidate

        cur.execute("SELECT active FROM products WHERE tenant_slug=? AND product_id=?", (tenant_slug, product_id))
        row = cur.fetchone()
        if row:
            cur.execute("""
                UPDATE products 
                SET name=?, price=?, stock=?, active=1, details=?, variants_json=?, image_url=?, last_modified=?
                WHERE tenant_slug=? AND product_id=?
            """, (name, price, stock, details, variants_json, image_url, datetime.utcnow().isoformat(), tenant_slug, product_id))
            if desired_position is not None:
                _resequence_scope(cur, tenant_slug, product_id, scope_key, desired_position)
            conn.commit()
            return jsonify({'ok': True, 'id': product_id, 'updated': True})
        
        initial_position = _next_product_position(cur, tenant_slug)
        cur.execute("""
            INSERT INTO products (tenant_slug, product_id, name, price, stock, position, active, details, variants_json, image_url, last_modified)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        """, (tenant_slug, product_id, name, price, stock, initial_position, details, variants_json, image_url, datetime.utcnow().isoformat()))
        _resequence_scope(cur, tenant_slug, product_id, scope_key, desired_position)
        conn.commit()
        return jsonify({'ok': True, 'id': product_id, 'created': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@bp.route('/products/<product_id>', methods=['PATCH'])
def update_product(product_id):
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if session.get('tenant_slug') and session.get('tenant_slug') != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    payload = request.get_json(silent=True) or {}
    fields = []
    params = []
    if 'stock' in payload:
        try:
            s = int(payload.get('stock'))
            fields.append('stock = ?')
            params.append(max(0, s))
        except: return jsonify({'error': 'stock inválido'}), 400
    if 'price' in payload:
        try:
            pr = int(payload.get('price'))
            fields.append('price = ?')
            params.append(max(0, pr))
        except: return jsonify({'error': 'price inválido'}), 400
    if 'active' in payload:
        try:
            ac = 1 if bool(payload.get('active')) else 0
            fields.append('active = ?')
            params.append(ac)
        except: return jsonify({'error': 'active inválido'}), 400
    if 'name' in payload:
        nm = str(payload.get('name') or '').strip()
        if not nm: return jsonify({'error': 'name requerido'}), 400
        fields.append('name = ?')
        params.append(nm)
    if 'details' in payload:
        dt = str(payload.get('details') or '').strip()
        fields.append('details = ?')
        params.append(dt)
    if 'image_url' in payload:
        img = str(payload.get('image_url') or '').strip()
        fields.append('image_url = ?')
        params.append(img)
    if 'variants' in payload:
        try:
            v = payload.get('variants')
            if isinstance(v, str):
                json.loads(v)
                fields.append('variants_json = ?')
                params.append(v)
            else:
                fields.append('variants_json = ?')
                params.append(json.dumps(v or {}))
        except: return jsonify({'error': 'variants inválido'}), 400
    desired_position = None
    if 'position' in payload:
        try:
            desired_position = max(1, int(payload.get('position')))
        except Exception:
            return jsonify({'error': 'position inválida'}), 400
        
    if not fields and desired_position is None: return jsonify({'error': 'sin cambios'}), 400
    fields.append('last_modified = ?')
    params.append(datetime.utcnow().isoformat())
    params.extend([tenant_slug, product_id])
    
    conn = get_db()
    cur = conn.cursor()
    cur.execute(f"UPDATE products SET {', '.join(fields)} WHERE tenant_slug = ? AND product_id = ?", params)
    if desired_position is not None:
        cur.execute("SELECT COALESCE(variants_json, '') FROM products WHERE tenant_slug = ? AND product_id = ?", (tenant_slug, product_id))
        row = cur.fetchone()
        scope_key = _product_scope_from_variants(row[0] if row else '')
        _resequence_scope(cur, tenant_slug, product_id, scope_key, desired_position)
    conn.commit()
    return jsonify({'ok': True, 'product_id': product_id, 'last_modified': params[len(fields)-1]})

@bp.route('/products/<product_id>', methods=['DELETE'])
def delete_product(product_id):
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    tenant_slug = request.args.get('tenant_slug')
    if not tenant_slug: return jsonify({'error': 'Falta tenant_slug'}), 400
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE products 
            SET active = 0, last_modified = ? 
            WHERE tenant_slug = ? AND product_id = ?
        """, (datetime.utcnow().isoformat(), tenant_slug, product_id))
        if cur.rowcount == 0: return jsonify({'error': 'Producto no encontrado'}), 404
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@bp.route('/upload', methods=['POST'])
def upload_file():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    if 'file' not in request.files: return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '': return jsonify({'error': 'No selected file'}), 400
    
    if file:
        # Check for Cloudinary configuration
        cloud_name = os.getenv('CLOUDINARY_CLOUD_NAME')
        api_key = os.getenv('CLOUDINARY_API_KEY')
        api_secret = os.getenv('CLOUDINARY_API_SECRET')
        
        if cloud_name and api_key and api_secret:
            try:
                cloudinary.config(
                    cloud_name=cloud_name,
                    api_key=api_key,
                    api_secret=api_secret,
                    secure=True
                )
                
                # Upload to Cloudinary with optimizations
                upload_result = cloudinary.uploader.upload(
                    file,
                    quality="auto",
                    fetch_format="auto",
                    width=1200,
                    crop="limit"
                )
                return jsonify({'url': upload_result['secure_url']})
            except Exception as e:
                return jsonify({'error': f'Cloudinary upload failed: {str(e)}'}), 500

        # Fallback to local storage if Cloudinary is not configured
        filename = secure_filename(file.filename)
        ts = int(datetime.utcnow().timestamp())
        filename = f"{ts}_{filename}"
        
        # Determine upload dir relative to app root (one level up from app package)
        # Assuming app/ is where this blueprint is, and we want uploads in project_root/Imagenes/uploads
        project_root = os.path.dirname(current_app.root_path) 
        base_upload_dir = os.path.join(project_root, 'Imagenes', 'uploads')

        # Check for tenant_slug to organize images by tenant
        tenant_slug = request.args.get('tenant_slug') or request.form.get('tenant_slug')
        if not _session_tenant_matches(tenant_slug):
            return jsonify({'error': 'acceso denegado al tenant'}), 403
        if tenant_slug:
            # Sanitize slug (alphanumeric + hyphens/underscores)
            safe_slug = "".join([c for c in tenant_slug if c.isalnum() or c in ('-','_')])
            upload_dir = os.path.join(base_upload_dir, safe_slug)
            url_prefix = f'Imagenes/uploads/{safe_slug}'
        else:
            upload_dir = base_upload_dir
            url_prefix = 'Imagenes/uploads'

        os.makedirs(upload_dir, exist_ok=True)
        
        filepath = os.path.join(upload_dir, filename)
        file.save(filepath)
        return jsonify({'url': f'{url_prefix}/{filename}'})
    
    return jsonify({'error': 'Upload failed'}), 500

@bp.route('/delete_file', methods=['DELETE'])
def delete_file():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    path = request.args.get('path')
    if not path:
        payload = request.get_json(silent=True) or {}
        path = payload.get('path')
    if not path: return jsonify({'error': 'path requerido'}), 400
    
    path = str(path).strip()
    if '..' in path or not path.replace('\\', '/').startswith('Imagenes/uploads/'):
        return jsonify({'error': 'ruta inválida o prohibida'}), 400
    normalized = path.replace('\\', '/')
    prefix = 'Imagenes/uploads/'
    relative_path = normalized[len(prefix):] if normalized.startswith(prefix) else ''
    tenant_part = relative_path.split('/', 1)[0].strip() if relative_path else ''
    session_tenant = str(session.get('tenant_slug') or '').strip()
    if session_tenant and tenant_part and tenant_part != session_tenant:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    
    project_root = os.path.dirname(current_app.root_path)
    full_path = os.path.join(project_root, path)
    
    if os.path.exists(full_path) and os.path.isfile(full_path):
        try:
            os.remove(full_path)
            return jsonify({'ok': True})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'archivo no encontrado'}), 404

@bp.route('/products_import_preview', methods=['POST'])
def products_import_preview():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    tenant_slug = request.args.get('tenant_slug') or request.form.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if 'file' not in request.files:
        return jsonify({'error': 'archivo requerido'}), 400
    f = request.files['file']
    filename = str(getattr(f, 'filename', '') or '')
    if not filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'solo se permite .xlsx'}), 400
    try:
        raw = f.read()
        sheets = read_xlsx_sheets(raw)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    sheet_key = _find_sheet_by_name(sheets, ['Productos', 'products', 'productos'])
    if not sheet_key:
        return jsonify({'error': 'no se encontró la hoja "Productos"'}), 400
    products_rows = _rows_to_dicts(sheets.get(sheet_key) or [])
    assigned_ids_map = None
    try:
        assigned_raw = request.form.get('assigned_ids_json')
        if assigned_raw:
            assigned_ids_map = json.loads(assigned_raw) if isinstance(assigned_raw, str) else None
    except Exception:
        assigned_ids_map = None
    plan = _build_import_plan(tenant_slug, products_rows, assigned_ids_map)
    cfg = get_cached_tenant_config(tenant_slug) or {}
    existing_cats = cfg.get('main_menu_categories') or []
    existing_ids = set()
    for c in (existing_cats or []):
        if isinstance(c, dict) and c.get('id'):
            existing_ids.add(str(c.get('id')).strip())
    new_cats = [c for c in plan.get('categories_detected') or [] if c.get('id') and c.get('id') not in existing_ids]
    return jsonify({
        'ok': True,
        'tenant_slug': tenant_slug,
        'sheet': sheet_key,
        'products': {
            'to_create': plan['products_to_create'],
            'to_update': plan['products_to_update'],
            'errors': plan['errors_count'],
            'total_rows': len(plan['actions'])
        },
        'categories': {
            'detected': len(plan.get('categories_detected') or []),
            'to_create': len(new_cats)
        },
        'assigned_ids': plan.get('assigned_ids') or {},
        'actions': plan.get('actions') or [],
        'new_categories': new_cats[:200]
    })

@bp.route('/products_import_apply', methods=['POST'])
def products_import_apply():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    tenant_slug = request.args.get('tenant_slug') or request.form.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if 'file' not in request.files:
        return jsonify({'error': 'archivo requerido'}), 400
    f = request.files['file']
    filename = str(getattr(f, 'filename', '') or '')
    if not filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'solo se permite .xlsx'}), 400
    assigned_ids_map = None
    try:
        assigned_raw = request.form.get('assigned_ids_json')
        if assigned_raw:
            assigned_ids_map = json.loads(assigned_raw) if isinstance(assigned_raw, str) else None
    except Exception:
        assigned_ids_map = None
    try:
        raw = f.read()
        sheets = read_xlsx_sheets(raw)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    sheet_key = _find_sheet_by_name(sheets, ['Productos', 'products', 'productos'])
    if not sheet_key:
        return jsonify({'error': 'no se encontró la hoja "Productos"'}), 400
    products_rows = _rows_to_dicts(sheets.get(sheet_key) or [])
    plan = _build_import_plan(tenant_slug, products_rows, assigned_ids_map)
    if plan.get('errors_count'):
        return jsonify({'error': 'hay errores en el archivo', 'actions': plan.get('actions') or []}), 400
    categories_merge = _merge_main_menu_categories(tenant_slug, plan.get('categories_detected') or [])
    conn = get_db()
    cur = conn.cursor()
    created = 0
    updated = 0
    try:
        for act in (plan.get('actions') or []):
            if act.get('action') not in ('create', 'update'):
                continue
            pid = str(act.get('id') or '').strip()
            row_num = int(act.get('row') or 0) or 0
            src = None
            for r in products_rows:
                if int(r.get('__row_index') or 0) == row_num:
                    src = r
                    break
            if not src:
                continue
            name_val = src.get('nombre') if 'nombre' in src else src.get('name')
            price_val = src.get('precio') if 'precio' in src else src.get('price')
            stock_val = src.get('stock')
            details_val = src.get('descripcion') if 'descripcion' in src else src.get('details')
            section_val = src.get('seccion') if 'seccion' in src else src.get('section')
            interest_val = src.get('interest_tag')
            cats_val = src.get('food_categories') if 'food_categories' in src else src.get('categorias')
            active_val = src.get('activo') if 'activo' in src else src.get('active')
            position_val = src.get('posicion') if 'posicion' in src else src.get('position')

            existing = plan.get('existing_products', {}).get(pid)
            variants = {}
            if existing and existing.get('variants_json'):
                try:
                    variants = json.loads(existing.get('variants_json') or '{}') or {}
                except Exception:
                    variants = {}
            if not isinstance(variants, dict):
                variants = {}

            desired_position = None
            if _cell_present(position_val):
                desired_position = max(1, int(_parse_int(position_val) or 1))

            if _cell_present(section_val):
                sec = _safe_str(section_val).lower()
                if sec:
                    variants['section'] = sec
            if _cell_present(interest_val):
                it = _safe_str(interest_val)
                if it:
                    variants['interest_tag'] = it
            if _cell_present(cats_val):
                cats_list = _split_categories(cats_val)
                if cats_list:
                    variants['food_categories'] = cats_list
                    if not _cell_present(section_val) and not _safe_str(variants.get('section')):
                        variants['section'] = 'main'

            variants_json = json.dumps(variants, ensure_ascii=False)
            scope_key = _product_scope_from_parts(variants.get('section') or '', variants.get('interest_tag') or '', variants.get('food_categories') or [])

            if act.get('action') == 'create':
                nm = _safe_str(name_val)
                pr = int(_parse_int(price_val) or 0)
                st = int(_parse_int(stock_val) or 0) if _cell_present(stock_val) else 0
                dt = _safe_str(details_val) if _cell_present(details_val) else ''
                ac = _parse_bool(active_val) if _cell_present(active_val) else True
                initial_position = _next_product_position(cur, tenant_slug)
                cur.execute(
                    """
                    INSERT INTO products (tenant_slug, product_id, name, price, stock, position, active, details, variants_json, image_url, last_modified)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (tenant_slug, pid, nm, pr, st, initial_position, 1 if ac else 0, dt, variants_json, '', datetime.utcnow().isoformat())
                )
                _resequence_scope(cur, tenant_slug, pid, scope_key, desired_position)
                created += 1
            else:
                fields = []
                params = []
                if _cell_present(name_val):
                    nm = _safe_str(name_val)
                    if nm:
                        fields.append('name = ?')
                        params.append(nm)
                if _cell_present(price_val):
                    pr = _parse_int(price_val)
                    if pr is not None:
                        fields.append('price = ?')
                        params.append(int(pr))
                if _cell_present(stock_val):
                    st = _parse_int(stock_val)
                    if st is not None:
                        fields.append('stock = ?')
                        params.append(max(0, int(st)))
                if _cell_present(details_val):
                    fields.append('details = ?')
                    params.append(_safe_str(details_val))
                if _cell_present(active_val):
                    ab = _parse_bool(active_val)
                    if ab is not None:
                        fields.append('active = ?')
                        params.append(1 if ab else 0)
                if _cell_present(section_val) or _cell_present(interest_val) or _cell_present(cats_val):
                    fields.append('variants_json = ?')
                    params.append(variants_json)
                fields.append('last_modified = ?')
                params.append(datetime.utcnow().isoformat())
                params.extend([tenant_slug, pid])
                if fields:
                    cur.execute(
                        f"UPDATE products SET {', '.join(fields)} WHERE tenant_slug = ? AND product_id = ?",
                        params
                    )
                if desired_position is not None:
                    _resequence_scope(cur, tenant_slug, pid, scope_key, desired_position)
                updated += 1
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({'error': str(e)}), 500
    return jsonify({
        'ok': True,
        'tenant_slug': tenant_slug,
        'categories': categories_merge,
        'products': {'created': created, 'updated': updated}
    })

@bp.route('/products_import_template', methods=['GET'])
def products_import_template():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    mode = str(request.args.get('mode') or '').strip().lower() or 'blank'
    cfg = get_cached_tenant_config(tenant_slug) or {}
    cats = cfg.get('main_menu_categories') or []
    categories_rows = [['id', 'label', 'position']]
    for c in (cats or []):
        if not isinstance(c, dict):
            continue
        cid = str(c.get('id') or '').strip()
        if not cid:
            continue
        categories_rows.append([
            cid,
            str(c.get('label') or '').strip(),
            str(c.get('position') or '')
        ])
    products_rows = [[
        'id',
        'nombre',
        'precio',
        'stock',
        'descripcion',
        'food_categories',
        'seccion',
        'interest_tag',
        'activo',
        'posicion'
    ]]
    if mode == 'export':
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT product_id, name, price, stock, COALESCE(details,''), COALESCE(variants_json,''), active, COALESCE(position, 0) "
            "FROM products WHERE tenant_slug = ? "
            "ORDER BY CASE WHEN COALESCE(position, 0) <= 0 THEN 1 ELSE 0 END ASC, COALESCE(position, 0) ASC, name ASC",
            (tenant_slug,)
        )
        rows = cur.fetchall() or []
        detected = {}
        existing_ids = set()
        for c in (cats or []):
            if isinstance(c, dict) and c.get('id'):
                existing_ids.add(str(c.get('id') or '').strip())
        for r in rows:
            pid = str(r[0] or '').strip()
            if not pid:
                continue
            vraw = r[5] or ''
            variants = {}
            try:
                variants = json.loads(vraw) if isinstance(vraw, str) else (vraw or {})
            except Exception:
                variants = {}
            if not isinstance(variants, dict):
                variants = {}
            sec = str(variants.get('section') or '').strip()
            it = str(variants.get('interest_tag') or '').strip()
            fc = variants.get('food_categories')
            fc_str = ''
            if isinstance(fc, list):
                fc_norm = []
                for x in fc:
                    cid = slugify_simple(x)
                    if cid:
                        fc_norm.append(cid)
                        if cid not in existing_ids and cid not in detected:
                            detected[cid] = cid.replace('-', ' ').title()
                fc_str = ', '.join(fc_norm)
            elif isinstance(fc, str):
                fc_norm = _split_categories(fc)
                for cid in fc_norm:
                    if cid not in existing_ids and cid not in detected:
                        detected[cid] = cid.replace('-', ' ').title()
                fc_str = ', '.join(fc_norm)
            products_rows.append([
                pid,
                str(r[1] or '').strip(),
                str(int(r[2] or 0)),
                str(int(r[3] or 0)),
                str(r[4] or ''),
                fc_str,
                sec,
                it,
                '1' if bool(r[6]) else '0',
                str(int(r[7] or 0)) if int(r[7] or 0) > 0 else ''
            ])
        for cid, label in detected.items():
            categories_rows.append([cid, label, ''])
    xlsx_bytes = create_xlsx_bytes([
        {'name': 'Productos', 'rows': products_rows},
        {'name': 'Categorias', 'rows': categories_rows}
    ])
    fname = f'plantilla_productos_{tenant_slug}.xlsx' if mode != 'export' else f'inventario_{tenant_slug}.xlsx'
    return send_file(
        io.BytesIO(xlsx_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=fname
    )
