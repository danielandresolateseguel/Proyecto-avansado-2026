import os
import json
import re
import unicodedata
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, session, current_app, send_file
from app.database import get_db
from app.utils import is_authed, check_csrf, read_xlsx_sheets, create_xlsx_bytes, normalize_recipe_breakdown, apply_recipe_to_product_fields

bp = Blueprint('costs', __name__, url_prefix='/api')


def _session_tenant_matches(tenant_slug):
    session_tenant = str(session.get('tenant_slug') or '').strip()
    if not session_tenant:
        return True
    if not tenant_slug:
        return False
    return session_tenant == str(tenant_slug).strip()


def _norm_date(s, end=False):
    try:
        if s and len(str(s).strip()) == 10:
            return str(s).strip() + ('T23:59:59' if end else 'T00:00:00')
    except Exception:
        pass
    return s


def _parse_iso_dt(value):
    try:
        if not value:
            return None
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        raw = str(value).strip()
        if not raw:
            return None
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d'):
            try:
                return datetime.strptime(raw, fmt)
            except Exception:
                continue
        return None
    except Exception:
        return None


def _local_tz_offset_minutes():
    try:
        cfg = None
        try:
            from app.utils import get_cached_tenant_config
            ts = session.get('tenant_slug') or request.args.get('tenant_slug') or request.args.get('slug') or ''
            cfg = get_cached_tenant_config(ts) if ts else {}
        except Exception:
            cfg = {}
        tz_raw = None
        if isinstance(cfg, dict):
            tz_raw = cfg.get('timezone_offset_min') or cfg.get('tz_minutes') or cfg.get('tz_offset_min')
        if isinstance(tz_raw, int):
            return tz_raw
        raw_env = os.getenv('TZ_OFFSET_MINUTES') or ''
        if raw_env:
            try:
                return int(raw_env)
            except Exception:
                pass
        return -180
    except Exception:
        return -180


def _utc_naive_to_local(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    from datetime import timedelta
    offset = _local_tz_offset_minutes()
    return (dt.astimezone(timezone.utc) + timedelta(minutes=offset)).replace(tzinfo=None)


def _local_date_boundary_to_utc_naive(value, end=False):
    try:
        dt_local = datetime.strptime(str(value or '').strip(), '%Y-%m-%d')
        dt_local = dt_local.replace(
            hour=23 if end else 0,
            minute=59 if end else 0,
            second=59 if end else 0,
            microsecond=0
        )
        from datetime import timedelta
        offset = _local_tz_offset_minutes()
        dt_utc_naive = (dt_local - timedelta(minutes=offset))
        return dt_utc_naive
    except Exception:
        return None


def _resolve_sales_range(from_raw, to_raw):
    now_dt = datetime.utcnow().replace(microsecond=0)
    now_local = _utc_naive_to_local(now_dt)
    from datetime import timedelta
    default_from = now_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=-_local_tz_offset_minutes())
    from_raw_str = str(from_raw or '').strip()
    to_raw_str = str(to_raw or '').strip()
    from_dt = _local_date_boundary_to_utc_naive(from_raw_str, end=False) if len(from_raw_str) == 10 else _parse_iso_dt(_norm_date(from_raw, end=False))
    to_dt = _local_date_boundary_to_utc_naive(to_raw_str, end=True) if len(to_raw_str) == 10 else _parse_iso_dt(_norm_date(to_raw, end=True))
    if from_dt is None and to_dt is None:
        from_dt = default_from
        to_dt = now_dt
    elif from_dt is None:
        if to_dt:
            local_to = _utc_naive_to_local(to_dt)
            from_dt = local_to.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=-_local_tz_offset_minutes())
        else:
            from_dt = default_from
    elif to_dt is None:
        to_dt = now_dt
    if to_dt < from_dt:
        to_dt = from_dt
    truncated_to_now = False
    if to_dt > now_dt:
        to_dt = now_dt
        truncated_to_now = True
    return from_dt, to_dt, from_dt.isoformat(), to_dt.isoformat(), truncated_to_now


def _previous_period(from_dt, to_dt, truncated_to_now):
    duration_s = max(1, int((to_dt - from_dt).total_seconds()))
    from datetime import timedelta
    prev_to = from_dt - timedelta(seconds=1)
    prev_from = prev_to - timedelta(seconds=duration_s - 1)
    if truncated_to_now:
        now_local = _utc_naive_to_local(datetime.utcnow().replace(microsecond=0))
        prev_to_local = _utc_naive_to_local(prev_to)
        if prev_to_local is not None and now_local is not None:
            from datetime import time as _dtime
            target_time = now_local.time()
            prev_to_local = datetime.combine(prev_to_local.date(), target_time)
            prev_to = prev_to_local + timedelta(minutes=-_local_tz_offset_minutes())
    return prev_from, prev_to, prev_from.isoformat(), prev_to.isoformat()


def _norm_channel(order_type, source=None):
    value = str(order_type or '').strip().lower()
    src = str(source or '').strip().lower()
    if src == 'pedidos_ya':
        if value in ('direccion', 'delivery'):
            return 'PedidosYa · Delivery'
        if value == 'retiro':
            return 'PedidosYa · Retiro'
        return 'PedidosYa'
    if src == 'rappi':
        if value in ('direccion', 'delivery'):
            return 'Rappi · Delivery'
        if value == 'retiro':
            return 'Rappi · Retiro'
        return 'Rappi'
    if value == 'mesa':
        return 'Mesa'
    if value in ('direccion', 'delivery'):
        return 'Delivery'
    if value == 'retiro':
        return 'Retiro'
    if value == 'espera':
        return 'Espera'
    return 'Otros'


def _percent(part, total):
    try:
        part_val = float(part or 0)
        total_val = float(total or 0)
        if total_val == 0:
            return 0
        return round((part_val / total_val) * 100, 2)
    except Exception:
        return 0


def _pp_diff(current_pct, previous_pct):
    try:
        c = float(current_pct or 0)
        p = float(previous_pct or 0)
        return round(c - p, 2)
    except Exception:
        return 0.0


def _delta_val(current, previous):
    try:
        return int(round(float(current or 0) - float(previous or 0)))
    except Exception:
        return 0


def _delta_percent(current, previous):
    try:
        c = float(current or 0)
        p = float(previous or 0)
        if p == 0:
            return None
        return round(((c - p) / abs(p)) * 100, 2)
    except Exception:
        return None


def _safe_str(v):
    return str(v or '').strip()


def _parse_int(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        try:
            if v == v and float(v).is_integer():
                return int(v)
        except Exception:
            pass
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace('.', '').replace(',', '')
    try:
        return int(s)
    except Exception:
        return None


def _cell_present(v):
    if v is None:
        return False
    if v is False:
        return False
    if isinstance(v, float) and v != v:
        return False
    s = str(v).strip()
    if not s:
        return False
    if s.lower() in ('none', 'null', '-', '/'):
        return False
    return True


def _rows_to_dicts(rows):
    if not rows:
        return []
    out = []
    if len(rows) < 2:
        return []
    header = rows[0]
    data = rows[1:]
    norm_headers = []
    idx = 0
    for h in header:
        raw = _safe_str(h).lower()
        if raw:
            norm_headers.append((idx, raw))
        idx += 1
    row_idx = 2
    for r in data:
        if r is None:
            row_idx += 1
            continue
        is_empty = True
        obj = {}
        for ci, key in norm_headers:
            try:
                val = r[ci]
            except Exception:
                val = None
            if _cell_present(val):
                is_empty = False
                obj[key] = val
            else:
                obj[key] = None
        obj['__row_index'] = row_idx
        if not is_empty:
            out.append(obj)
        row_idx += 1
    return out


def _find_sheet_by_name(sheets, candidates):
    if not sheets:
        return None
    for c in candidates:
        key = _safe_str(c).lower()
        for s in sheets.keys():
            if _safe_str(s).lower() == key:
                return s
    for s in sheets.keys():
        return s
    return None


def _fetch_existing_products(cur, tenant_slug):
    cur.execute(
        """
        SELECT product_id, name, price, COALESCE(cost_price, 0),
               COALESCE(cost_type, 'fixed'), COALESCE(margin_percent, 0),
               COALESCE(variants_json, ''), active
        FROM products WHERE tenant_slug = ?
        """,
        (tenant_slug,)
    )
    rows = cur.fetchall() or []
    existing = {}
    for r in rows:
        pid = _safe_str(r[0])
        if not pid:
            continue
        variants = {}
        try:
            variants = json.loads(r[6] or '{}') or {} if r[6] else {}
        except Exception:
            variants = {}
        existing[pid] = {
            'id': pid,
            'name': _safe_str(r[1]),
            'price': int(r[2] or 0),
            'cost_price': int(r[3] or 0),
            'cost_type': _safe_str(r[4]) or 'fixed',
            'margin_percent': int(r[5] or 0),
            'variants_json': r[6] or '',
            'variants': variants,
            'active': bool(r[7])
        }
    return existing


def _normalize_cost_type(v):
    s = _safe_str(v).lower()
    if s in ('fijo', 'fija', 'fix', 'fixed'):
        return 'fixed'
    if s in ('porcentaje', 'percentage', 'percent', '%'):
        return 'percentage'
    if s in ('receta', 'recipe', 'formula', 'formula'):
        return 'recipe'
    return 'fixed'


def _build_cost_import_plan(tenant_slug, rows):
    conn = get_db()
    cur = conn.cursor()
    existing = _fetch_existing_products(cur, tenant_slug)
    actions = []
    errors_count = 0
    to_update = 0
    to_create = 0
    without_match = 0
    matched_ids = set()
    for r in rows:
        row_idx = int(r.get('__row_index') or 0) or 0
        id_raw = r.get('id') if 'id' in r else r.get('product_id') if 'product_id' in r else r.get('codigo') if 'codigo' in r else r.get('código')
        name_raw = r.get('nombre') if 'nombre' in r else r.get('name')
        cost_price_raw = r.get('costo_unitario') if 'costo_unitario' in r else r.get('cost_price') if 'cost_price' in r else r.get('costo') if 'costo' in r else r.get('unit_cost')
        cost_type_raw = r.get('tipo_costo') if 'tipo_costo' in r else r.get('cost_type')
        margin_percent_raw = r.get('margen_sugerido') if 'margen_sugerido' in r else r.get('margin_percent') if 'margin_percent' in r else r.get('margen') if 'margen' in r else None
        price_raw = r.get('precio') if 'precio' in r else r.get('price')

        pid = _safe_str(id_raw)
        name = _safe_str(name_raw)
        if not pid and not name:
            actions.append({'row': row_idx, 'action': 'error',
                            'error': 'falta id o nombre del producto'})
            errors_count += 1
            continue

        target_pid = ''
        if pid and pid in existing:
            target_pid = pid
        elif name:
            for eid, info in existing.items():
                if _safe_str(info.get('name')).lower() == name.lower():
                    target_pid = eid
                    break
        if not target_pid:
            to_create_name = name or f'producto_fila_{row_idx}'
            to_create_pid = pid or ''
            to_create += 1
            action_obj = {
                'row': row_idx,
                'action': 'create',
                'id': to_create_pid or to_create_name,
                'name': to_create_name,
                'price': int(_parse_int(price_raw) or 0),
                'cost_price': int(_parse_int(cost_price_raw) or 0),
                'cost_type': _normalize_cost_type(cost_type_raw),
                'margin_percent': int(_parse_int(margin_percent_raw) or 0),
                'note': 'Nuevo producto creado desde plantilla de costos'
            }
            if not to_create_pid:
                action_obj['note'] += ' · ID vacío, se generará ID auto-numerico'
            actions.append(action_obj)
            without_match += 1
            continue

        matched_ids.add(target_pid)
        info = existing[target_pid]
        new_cost = int(_parse_int(cost_price_raw) or 0) if _cell_present(cost_price_raw) else info.get('cost_price', 0)
        new_ct = _normalize_cost_type(cost_type_raw) if _cell_present(cost_type_raw) else info.get('cost_type', 'fixed')
        new_mp = int(_parse_int(margin_percent_raw) or 0) if _cell_present(margin_percent_raw) else info.get('margin_percent', 0)
        new_price = int(_parse_int(price_raw) or 0) if _cell_present(price_raw) else info.get('price', 0)

        changed = (
            new_cost != info.get('cost_price', 0)
            or new_ct != info.get('cost_type', 'fixed')
            or new_mp != info.get('margin_percent', 0)
            or (new_price and new_price != info.get('price', 0))
        )
        if not changed:
            actions.append({'row': row_idx, 'action': 'skip', 'id': target_pid, 'name': info.get('name'),
                            'note': 'sin cambios'})
            continue
        to_update += 1
        actions.append({
            'row': row_idx,
            'action': 'update',
            'id': target_pid,
            'name': info.get('name'),
            'from': {
                'cost_price': info.get('cost_price'),
                'cost_type': info.get('cost_type'),
                'margin_percent': info.get('margin_percent'),
                'price': info.get('price')
            },
            'to': {
                'cost_price': new_cost,
                'cost_type': new_ct,
                'margin_percent': new_mp,
                'price': new_price
            }
        })

    unchanged_products = [
        {'id': eid, 'name': info.get('name'), 'cost_price': info.get('cost_price')}
        for eid, info in existing.items()
        if eid not in matched_ids and (info.get('cost_price') or 0) == 0
    ]
    return {
        'actions': actions,
        'errors_count': errors_count,
        'to_create': to_create,
        'to_update': to_update,
        'without_match': without_match,
        'total_rows': len(rows),
        'products_without_cost_unchanged': unchanged_products[:500],
        'count_without_cost': len(unchanged_products)
    }


@bp.route('/products/<product_id>/cost', methods=['PATCH'])
def update_product_cost_only(product_id):
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    payload = request.get_json(silent=True) or {}
    fields = []
    params = []
    if 'cost_price' in payload:
        try:
            cp = int(payload.get('cost_price'))
            if cp < 0:
                return jsonify({'error': 'cost_price negativo'}), 400
            fields.append('cost_price = ?')
            params.append(cp)
        except Exception:
            return jsonify({'error': 'cost_price inválido'}), 400
    if 'cost_type' in payload:
        ct = _normalize_cost_type(payload.get('cost_type'))
        fields.append('cost_type = ?')
        params.append(ct)
    if 'margin_percent' in payload:
        try:
            mp = int(payload.get('margin_percent'))
            if mp < 0:
                mp = 0
            if mp > 1000:
                mp = 1000
            fields.append('margin_percent = ?')
            params.append(mp)
        except Exception:
            return jsonify({'error': 'margin_percent inválido'}), 400
    if not fields:
        return jsonify({'error': 'sin cambios'}), 400
    fields.append('last_modified = ?')
    params.append(datetime.utcnow().isoformat())
    params.extend([tenant_slug, _safe_str(product_id)])
    conn = get_db()
    cur = conn.cursor()
    cur.execute(f"UPDATE products SET {', '.join(fields)} WHERE tenant_slug = ? AND product_id = ?", params)
    if cur.rowcount <= 0:
        conn.rollback()
        return jsonify({'error': 'producto no encontrado'}), 404
    conn.commit()
    cur.execute(
        "SELECT product_id, name, price, COALESCE(cost_price, 0), COALESCE(cost_type, 'fixed'), COALESCE(margin_percent, 0), last_modified "
        "FROM products WHERE tenant_slug = ? AND product_id = ?",
        (tenant_slug, _safe_str(product_id))
    )
    r = cur.fetchone()
    out = {'ok': True}
    if r:
        out['product'] = {
            'id': r[0],
            'name': r[1],
            'price': int(r[2] or 0),
            'cost_price': int(r[3] or 0),
            'cost_type': r[4] or 'fixed',
            'margin_percent': int(r[5] or 0),
            'last_modified': r[6] or ''
        }
    return jsonify(out)


@bp.route('/products/<product_id>/recipe', methods=['PATCH'])
def update_product_recipe_only(product_id):
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    payload = request.get_json(silent=True) or {}
    mode = str(payload.get('mode') or 'replace').strip().lower()
    if mode not in ('replace', 'merge', 'clear'):
        mode = 'replace'
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT product_id, name, COALESCE(price, 0), COALESCE(variants_json, ''), "
        "COALESCE(cost_price, 0), COALESCE(cost_type, 'fixed'), COALESCE(margin_percent, 0) "
        "FROM products WHERE tenant_slug = ? AND product_id = ?",
        (tenant_slug, _safe_str(product_id))
    )
    row = cur.fetchone()
    if not row:
        return jsonify({'error': 'producto no encontrado'}), 404
    current_price = int(row[2] or 0)
    variants_raw = row[3] or ''
    current_cost_price = int(row[4] or 0)
    current_cost_type = _safe_str(row[5]) or 'fixed'
    current_margin = int(row[6] or 0)
    variants = {}
    try:
        variants = json.loads(variants_raw or '{}') or {} if variants_raw else {}
    except Exception:
        variants = {}
    if not isinstance(variants, dict):
        variants = {}
    if mode == 'clear':
        variants.pop('recipe_cost_breakdown', None)
        final_recipe = None
        cost_fields = {}
        if current_cost_type == 'recipe':
            new_type = 'fixed'
            new_fields = ['cost_type = ?', 'variants_json = ?', 'last_modified = ?']
            new_params = [new_type, json.dumps(variants, ensure_ascii=False), datetime.utcnow().isoformat(), tenant_slug, _safe_str(product_id)]
            cur.execute(f"UPDATE products SET {', '.join(new_fields)} WHERE tenant_slug = ? AND product_id = ?", new_params)
            conn.commit()
            return jsonify({'ok': True, 'recipe': None, 'cost_fields': {'cost_type': new_type, 'cost_price': current_cost_price}})
        cur.execute("UPDATE products SET variants_json = ?, last_modified = ? WHERE tenant_slug = ? AND product_id = ?",
                    (json.dumps(variants, ensure_ascii=False), datetime.utcnow().isoformat(), tenant_slug, _safe_str(product_id)))
        conn.commit()
        return jsonify({'ok': True, 'recipe': None, 'cost_fields': {'cost_price': current_cost_price, 'cost_type': current_cost_type}})
    raw_recipe = payload.get('recipe_cost_breakdown') or payload.get('recipe') or payload.get('escandallo')
    if raw_recipe is None and 'ingredients' not in payload:
        return jsonify({'error': 'falta recipe_cost_breakdown, ingredients o mode=clear'}), 400
    existing = variants.get('recipe_cost_breakdown') if isinstance(variants, dict) else None
    if mode == 'merge' and isinstance(existing, dict):
        if isinstance(raw_recipe, dict):
            merged = dict(existing)
            for k, v in raw_recipe.items():
                merged[k] = v
            if 'ingredients' in payload and isinstance(payload.get('ingredients'), list):
                merged['ingredients'] = list(payload.get('ingredients') or [])
            if 'other_variables' in payload and isinstance(payload.get('other_variables'), list):
                merged['other_variables'] = list(payload.get('other_variables') or [])
            final_recipe = normalize_recipe_breakdown(merged)
        else:
            if isinstance(existing.get('ingredients'), list):
                existing['ingredients'].extend(payload.get('ingredients') or [])
            final_recipe = normalize_recipe_breakdown(existing)
    else:
        if raw_recipe is None and isinstance(payload.get('ingredients'), list):
            obj = {}
            for k in ('yield_servings', 'currency', 'prep_minutes', 'labor_cost_per_minute',
                      'auto_sync_to_product_cost', 'other_variables'):
                if k in payload:
                    obj[k] = payload.get(k)
            obj['ingredients'] = payload.get('ingredients') or []
            final_recipe = normalize_recipe_breakdown(obj)
        else:
            final_recipe = normalize_recipe_breakdown(raw_recipe)
    variants['recipe_cost_breakdown'] = final_recipe
    cost_fields = apply_recipe_to_product_fields(final_recipe, {'price': current_price})
    fields_to_set = ['variants_json = ?']
    params_list = [json.dumps(variants, ensure_ascii=False)]
    if cost_fields:
        if 'cost_price' in cost_fields:
            fields_to_set.append('cost_price = ?')
            params_list.append(int(cost_fields['cost_price']))
        if 'cost_type' in cost_fields:
            fields_to_set.append('cost_type = ?')
            params_list.append(str(cost_fields['cost_type']))
        if 'margin_percent' in cost_fields:
            fields_to_set.append('margin_percent = ?')
            params_list.append(int(cost_fields['margin_percent']))
    else:
        if 'cost_price' in payload or 'unit_cost' in payload:
            cp = payload.get('cost_price') if 'cost_price' in payload else payload.get('unit_cost')
            try:
                cp_int = max(0, int(cp))
                fields_to_set.append('cost_price = ?')
                params_list.append(cp_int)
            except Exception:
                pass
    fields_to_set.append('last_modified = ?')
    params_list.append(datetime.utcnow().isoformat())
    params_list.extend([tenant_slug, _safe_str(product_id)])
    cur.execute(f"UPDATE products SET {', '.join(fields_to_set)} WHERE tenant_slug = ? AND product_id = ?", params_list)
    conn.commit()
    resp = {'ok': True, 'recipe': final_recipe}
    if cost_fields:
        resp['cost_fields'] = cost_fields
    else:
        resp['cost_fields'] = {'cost_price': current_cost_price, 'cost_type': current_cost_type, 'margin_percent': current_margin}
    return jsonify(resp)


@bp.route('/products/<product_id>/recipe', methods=['GET'])
def get_product_recipe(product_id):
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT product_id, name, COALESCE(price, 0), COALESCE(variants_json, ''), "
        "COALESCE(cost_price, 0), COALESCE(cost_type, 'fixed'), COALESCE(margin_percent, 0) "
        "FROM products WHERE tenant_slug = ? AND product_id = ?",
        (tenant_slug, _safe_str(product_id))
    )
    row = cur.fetchone()
    if not row:
        return jsonify({'error': 'producto no encontrado'}), 404
    variants_raw = row[3] or ''
    variants = {}
    try:
        variants = json.loads(variants_raw or '{}') or {} if variants_raw else {}
    except Exception:
        variants = {}
    recipe = variants.get('recipe_cost_breakdown') if isinstance(variants, dict) else None
    return jsonify({
        'ok': True,
        'product': {
            'id': row[0],
            'name': row[1],
            'price': int(row[2] or 0),
            'cost_price': int(row[4] or 0),
            'cost_type': row[5] or 'fixed',
            'margin_percent': int(row[6] or 0)
        },
        'recipe': recipe
    })


@bp.route('/costs/recipe_preview', methods=['POST'])
def costs_recipe_preview():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    price = 0
    if 'price' in payload:
        try:
            price = max(0, int(payload.get('price') or 0))
        except Exception:
            price = 0
    raw_recipe = payload.get('recipe_cost_breakdown') or payload.get('recipe') or payload.get('escandallo')
    if raw_recipe is None and isinstance(payload.get('ingredients'), list):
        obj = {}
        for k in ('yield_servings', 'currency', 'prep_minutes', 'labor_cost_per_minute',
                  'auto_sync_to_product_cost', 'other_variables'):
            if k in payload:
                obj[k] = payload.get(k)
        obj['ingredients'] = payload.get('ingredients') or []
        raw_recipe = obj
    try:
        final_recipe = normalize_recipe_breakdown(raw_recipe)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    cost_fields = apply_recipe_to_product_fields(final_recipe, {'price': price})
    return jsonify({
        'ok': True,
        'price': int(price),
        'recipe': final_recipe,
        'cost_fields': cost_fields,
        'summary': {
            'ingredient_lines': len(final_recipe.get('ingredients') or []),
            'ingredients_subtotal': int(final_recipe.get('ingredients_subtotal') or 0),
            'other_variables_subtotal': int(final_recipe.get('other_vars_subtotal') or 0),
            'labor_subtotal': int(final_recipe.get('labor_subtotal') or 0),
            'calculated_total_cost': int(final_recipe.get('calculated_total_cost') or 0),
            'yield_servings': int(final_recipe.get('yield_servings') or 1),
            'unit_serving_cost': int(final_recipe.get('unit_serving_cost') or 0),
            'suggested_cost_price': int(cost_fields.get('cost_price', final_recipe.get('unit_serving_cost') or 0)),
            'suggested_margin_percent': int(cost_fields.get('margin_percent', 0)),
            'gross_profit_per_unit': (int(price) - int(cost_fields.get('cost_price', final_recipe.get('unit_serving_cost') or 0))) if price > 0 else None
        }
    })


def _compute_costs_payload(tenant_slug, from_dt, to_dt, truncated_to_now, category_filter=None, channel_filter=None, source_filter=None):
    conn = get_db()
    cur = conn.cursor()
    status_filter_orders = ""
    source_filter_sql = ""
    params = [tenant_slug]
    if channel_filter:
        cf = _safe_str(channel_filter).lower()
        if cf in ('mesa', 'delivery', 'retiro', 'espera'):
            status_filter_orders += " AND LOWER(COALESCE(o.order_type,'')) = ? "
            params.append(cf)
    if source_filter:
        sf = _safe_str(source_filter).lower()
        if sf in ('ambos', 'all', '', 'none'):
            source_filter_sql = ""
        elif sf == 'local':
            source_filter_sql += " AND (COALESCE(o.source,'') = '' OR LOWER(COALESCE(o.source,'')) IN ('local','','propio','mesa')) "
        elif sf in ('pedidosya', 'pedidos_ya'):
            source_filter_sql += " AND LOWER(COALESCE(o.source,'')) = 'pedidos_ya' "
        elif sf == 'rappi':
            source_filter_sql += " AND LOWER(COALESCE(o.source,'')) = 'rappi' "
        elif sf in ('ifood', 'mercadopago', 'mp'):
            source_filter_sql += " AND LOWER(COALESCE(o.source,'')) = ? "
            params.append(sf.lower())
    params.extend([from_dt.isoformat(), to_dt.isoformat()])
    from datetime import timedelta as _td_buff
    _buffer_h = 36
    from_buffered = (from_dt - _td_buff(hours=_buffer_h)).isoformat()
    to_buffered = (to_dt + _td_buff(hours=_buffer_h)).isoformat()
    params_buffered = list(params[:-2]) + [from_buffered, to_buffered]
    order_ids_sql = (
        "SELECT DISTINCT o.id FROM orders o "
        "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history "
        "      WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
        "WHERE o.tenant_slug = ? AND o.status = 'entregado' "
        + status_filter_orders + source_filter_sql +
        "  AND h.last_change BETWEEN ? AND ?"
    )
    base_sql = (
        "SELECT "
        "  COALESCE(oi.product_id,'') AS pid, "
        "  COALESCE(NULLIF(TRIM(COALESCE(oi.name,'')),''), p.name) AS pname, "
        "  COALESCE(p.variants_json,'') AS variants, "
        "  COALESCE(o.order_type,'') AS order_type, "
        "  COALESCE(o.source,'') AS order_source, "
        "  SUM(COALESCE(oi.qty,0)) AS qty, "
        "  SUM(COALESCE(oi.qty,0) * COALESCE(oi.unit_price,0)) AS revenue, "
        "  SUM(COALESCE(oi.qty,0) * COALESCE(oi.unit_cost,0)) AS cogs, "
        "  SUM(CASE WHEN COALESCE(oi.unit_cost,0)=0 THEN COALESCE(oi.qty,0) * COALESCE(oi.unit_price,0) ELSE 0 END) AS blind_revenue, "
        "  SUM(CASE WHEN COALESCE(oi.unit_cost,0)=0 THEN 1 ELSE 0 END) AS blind_lines "
        "FROM order_items oi "
        "JOIN orders o ON o.id = oi.order_id "
        "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history "
        "      WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
        "LEFT JOIN products p ON p.tenant_slug = o.tenant_slug AND p.product_id = oi.product_id "
        "WHERE o.tenant_slug = ? AND o.status = 'entregado' "
        + status_filter_orders + source_filter_sql +
        "  AND h.last_change BETWEEN ? AND ? "
        "GROUP BY pid, pname, variants, order_type, order_source "
        "ORDER BY revenue DESC"
    )
    cur.execute(base_sql, params_buffered)
    rows = cur.fetchall() or []
    product_map = {}
    channel_map = {}
    category_map = {}
    net_sales = 0
    total_cogs = 0
    blind_revenue = 0
    blind_lines = 0
    distinct_products_without_cost = set()
    product_counts = {}
    product_missing_lines = {}
    for r in rows:
        pid = _safe_str(r[0])
        name = _safe_str(r[1]) or '(Sin nombre)'
        variants_raw = r[2] or ''
        ot = r[3]
        osrc = r[4]
        channel_key = _norm_channel(ot, source=osrc)
        qty = int(r[5] or 0)
        revenue = int(r[6] or 0)
        cogs = int(r[7] or 0)
        line_blind_rev = int(r[8] or 0)
        line_blind_lines = int(r[9] or 0)
        net_sales += revenue
        total_cogs += cogs
        blind_revenue += line_blind_rev
        if line_blind_lines > 0:
            blind_lines += line_blind_lines
            if pid:
                distinct_products_without_cost.add(pid)
            product_missing_lines[pid] = int(product_missing_lines.get(pid, 0) or 0) + int(line_blind_lines or 0)
        gross = revenue - cogs
        product_counts[pid] = product_counts.get(pid, 0) + 1
        if pid not in product_map:
            product_map[pid] = {
                'id': pid,
                'name': name,
                'qty': 0,
                'revenue': 0,
                'total_cost': 0,
                'gross_profit': 0,
                'category': '',
                'section': ''
            }
        e = product_map[pid]
        e['qty'] += qty
        e['revenue'] += revenue
        e['total_cost'] += cogs
        e['gross_profit'] += gross
        if variants_raw:
            try:
                vj = json.loads(variants_raw) or {}
                if isinstance(vj, dict):
                    cats = vj.get('food_categories')
                    if isinstance(cats, list) and cats:
                        e['category'] = _safe_str(cats[0]) or e['category']
                    elif isinstance(cats, str) and _safe_str(cats):
                        e['category'] = _safe_str(cats)
                    sec = _safe_str(vj.get('section'))
                    if sec:
                        e['section'] = sec
            except Exception:
                pass
        if channel_key not in channel_map:
            channel_map[channel_key] = {'channel': channel_key, 'qty': 0, 'revenue': 0, 'total_cost': 0, 'gross_profit': 0, 'external_fee': 0}
        ce = channel_map[channel_key]
        ce['qty'] += qty
        ce['revenue'] += revenue
        ce['total_cost'] += cogs
        ce['gross_profit'] += gross
        cat_key = (product_map[pid].get('category') or 'Sin categoría').strip() or 'Sin categoría'
        if category_filter and _safe_str(category_filter).lower() != cat_key.lower():
            pass
        if cat_key not in category_map:
            category_map[cat_key] = {'category': cat_key, 'qty': 0, 'revenue': 0, 'total_cost': 0, 'gross_profit': 0, 'external_fee': 0}
        ca = category_map[cat_key]
        ca['qty'] += qty
        ca['revenue'] += revenue
        ca['total_cost'] += cogs
        ca['gross_profit'] += gross
    # Fase 5: external_fee_total (comisión PedidosYa/Rappi) por orden entregada en rango+canal+source
    gross_sales_on_orders = 0
    try:
        fee_total_sql = (
            "SELECT COALESCE(SUM(COALESCE(o.external_fee_amount,0)),0), "
            "       COALESCE(SUM(COALESCE(o.total,0)),0) "
            "FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history "
            "      WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ? AND o.status = 'entregado' "
            + status_filter_orders + source_filter_sql +
            "  AND h.last_change BETWEEN ? AND ?"
        )
        cur.execute(fee_total_sql, params_buffered)
        fr = cur.fetchone()
        external_fee_total = int(fr[0] or 0) if fr else 0
        gross_sales_on_orders = int(fr[1] or 0) if fr else 0
    except Exception:
        external_fee_total = 0
        gross_sales_on_orders = 0
    # Fase 5: external fee por canal (desglose por source+order_type)
    try:
        fee_by_src_sql = (
            "SELECT COALESCE(o.order_type,''), COALESCE(o.source,''), COALESCE(SUM(COALESCE(o.external_fee_amount,0)),0) "
            "FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history "
            "      WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ? AND o.status = 'entregado' "
            + status_filter_orders + source_filter_sql +
            "  AND h.last_change BETWEEN ? AND ? "
            "GROUP BY COALESCE(o.order_type,''), COALESCE(o.source,'')"
        )
        cur.execute(fee_by_src_sql, params_buffered)
        fee_rows = cur.fetchall() or []
        for frow in fee_rows:
            ot = frow[0]
            osrc = frow[1]
            f = int(frow[2] or 0)
            if f <= 0:
                continue
            ck = _norm_channel(ot, source=osrc)
            if ck in channel_map:
                channel_map[ck]['external_fee'] = int(channel_map[ck].get('external_fee') or 0) + f
    except Exception:
        pass
    gross_profit = net_sales - total_cogs
    external_fee_total = int(external_fee_total or 0)
    gross_sales_on_orders = int(gross_sales_on_orders or 0)
    real_profit = gross_profit - external_fee_total
    gross_margin_pct = _percent(gross_profit, net_sales)
    real_margin_percent = _percent(real_profit, net_sales)
    external_fee_pct_effective = _percent(external_fee_total, net_sales) if net_sales > 0 else 0.0
    external_fee_pct_contractual = _percent(external_fee_total, gross_sales_on_orders) if gross_sales_on_orders > 0 else 0.0
    blind_costs_warning = blind_revenue > 0 or total_cogs == 0
    by_product_list = []
    for pid, p in product_map.items():
        if p['revenue'] <= 0 and p['qty'] <= 0:
            continue
        margin = _percent(p['gross_profit'], p['revenue'])
        unit_margin = int(round(p['gross_profit'] / p['qty'])) if p['qty'] > 0 else 0
        missing_count = int(product_missing_lines.get(pid, 0) or 0)
        by_product_list.append({
            'id': p['id'],
            'product_id': p['id'],
            'name': p['name'],
            'product_name': p['name'],
            'category': p['category'],
            'qty': p['qty'],
            'revenue': p['revenue'],
            'total_cost': p['total_cost'],
            'gross_profit': p['gross_profit'],
            'margin_percent': margin,
            'gross_margin_percent': margin,
            'unit_margin': unit_margin,
            'missing_cost_count': missing_count,
            'share_profit_percent': _percent(p['gross_profit'], gross_profit)
        })
    by_product_list.sort(key=lambda x: x['gross_profit'], reverse=True)
    by_category_list = sorted(list(category_map.values()), key=lambda x: x['gross_profit'], reverse=True)
    for c in by_category_list:
        m = _percent(c['gross_profit'], c['revenue'])
        c['margin_percent'] = m
        c['gross_margin_percent'] = m
        fee = int(c.get('external_fee') or 0)
        c['external_fee'] = fee
        c['real_profit'] = int(c['gross_profit']) - fee
        c['real_margin_percent'] = _percent(c['real_profit'], c['revenue'])
        c['share_profit_percent'] = _percent(c['gross_profit'], gross_profit)
    by_channel_list = sorted(list(channel_map.values()), key=lambda x: x['gross_profit'], reverse=True)
    for c in by_channel_list:
        m = _percent(c['gross_profit'], c['revenue'])
        c['margin_percent'] = m
        c['gross_margin_percent'] = m
        fee = int(c.get('external_fee') or 0)
        c['external_fee'] = fee
        c['real_profit'] = int(c['gross_profit']) - fee
        c['real_margin_percent'] = _percent(c['real_profit'], c['revenue'])
        c['share_profit_percent'] = _percent(c['gross_profit'], gross_profit)
    by_product = [x for x in by_product_list if not category_filter or (_safe_str(x.get('category')).lower() == _safe_str(category_filter).lower())]
    most_profitable = by_product[0] if by_product else None
    worst_margin = None
    candidates_worst = [x for x in by_product if x['revenue'] > 0 and x['qty'] >= 1]
    if candidates_worst:
        worst_margin = sorted(candidates_worst, key=lambda x: x['margin_percent'])[0]
    best_category = by_category_list[0] if by_category_list else None
    leaders = {
        'best_profit': most_profitable,
        'worst_margin': worst_margin,
        'best_category': best_category,
        'most_profitable_product': (most_profitable.get('name') if most_profitable else '') or '',
        'most_profitable_product_id': (most_profitable.get('id') if most_profitable else '') or '',
        'most_profitable_product_profit': int(most_profitable.get('gross_profit') or 0) if most_profitable else 0,
        'worst_margin_product': (worst_margin.get('name') if worst_margin else '') or '',
        'worst_margin_product_id': (worst_margin.get('id') if worst_margin else '') or '',
        'worst_margin_percent': float(worst_margin.get('gross_margin_percent') or 0) if worst_margin else 0.0,
        'best_category_margin_name': (best_category.get('category') if best_category else '') or '',
        'best_category_margin_percent': float(_percent(best_category.get('gross_profit'), best_category.get('revenue')) if best_category else 0.0)
    }
    summary = {
        'net_sales': int(net_sales),
        'gross_sales_on_orders': int(gross_sales_on_orders),
        'total_cost_of_goods': int(total_cogs),
        'gross_profit': int(gross_profit),
        'gross_margin_percent': float(gross_margin_pct),
        'external_fee_total': int(external_fee_total),
        'external_fee_pct_effective': float(external_fee_pct_effective),
        'external_fee_pct_contractual': float(external_fee_pct_contractual),
        'real_profit': int(real_profit),
        'real_margin_percent': float(real_margin_percent),
        'blind_costs_warning': bool(blind_costs_warning),
        'products_without_cost': len(distinct_products_without_cost),
        'exposed_revenue_without_cost': int(blind_revenue),
        'blind_revenue': int(blind_revenue),
        'blind_order_lines': int(blind_lines),
        'distinct_products_sold': len([x for x in by_product_list if x['qty'] > 0]),
        'order_count': 0
    }
    payload = {
        'truncated_to_now': bool(truncated_to_now),
        'summary': summary,
        'by_product': by_product,
        'by_category': by_category_list,
        'by_channel': by_channel_list,
        'leaders': leaders,
        '_raw_order_count_sql_id': None
    }
    cur.execute("SELECT COUNT(DISTINCT o.id) FROM orders o "
                "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history "
                "      WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
                "WHERE o.tenant_slug = ? AND o.status = 'entregado' "
                + status_filter_orders + source_filter_sql + " AND h.last_change BETWEEN ? AND ?",
                params_buffered)
    cr = cur.fetchone()
    order_count_val = int(cr[0] or 0) if cr else 0
    payload['order_count'] = order_count_val
    payload['summary']['order_count'] = order_count_val
    return payload


@bp.route('/costs/analytics', methods=['GET'])
def costs_analytics():
    try:
        if not is_authed():
            return jsonify({'error': 'no autorizado'}), 401
        tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
        if not _session_tenant_matches(tenant_slug):
            return jsonify({'error': 'acceso denegado al tenant'}), 403
        from_raw = request.args.get('from') or request.args.get('from_date') or request.args.get('dateFrom') or request.args.get('desde')
        to_raw = request.args.get('to') or request.args.get('to_date') or request.args.get('dateTo') or request.args.get('hasta')
        category_filter = request.args.get('category') or request.args.get('categoria')
        channel_filter = request.args.get('channel') or request.args.get('order_type') or request.args.get('canal')
        source_filter = request.args.get('source') or request.args.get('canal_filtro') or request.args.get('fuente')
        from_dt, to_dt, from_iso, to_iso, truncated_to_now = _resolve_sales_range(from_raw, to_raw)
        prev_from_dt, prev_to_dt, prev_from_iso, prev_to_iso = _previous_period(from_dt, to_dt, truncated_to_now)
        current = _compute_costs_payload(tenant_slug, from_dt, to_dt, truncated_to_now, category_filter=category_filter, channel_filter=channel_filter, source_filter=source_filter)
        previous = _compute_costs_payload(tenant_slug, prev_from_dt, prev_to_dt, truncated_to_now=False, category_filter=category_filter, channel_filter=channel_filter, source_filter=source_filter)
        curr_sum = current['summary']
        prev_sum = previous['summary']
        current_gm = float(curr_sum.get('gross_margin_percent') or 0)
        prev_gm = float(prev_sum.get('gross_margin_percent') or 0)
        cur_order_count = int(current.get('order_count') or 0)
        prev_order_count = int(previous.get('order_count') or 0)
        current_block = {
            'net_sales': int(curr_sum.get('net_sales') or 0),
            'total_cost_of_goods': int(curr_sum.get('total_cost_of_goods') or 0),
            'gross_profit': int(curr_sum.get('gross_profit') or 0),
            'gross_margin_percent': float(curr_sum.get('gross_margin_percent') or 0),
            'external_fee_total': int(curr_sum.get('external_fee_total') or 0),
            'real_profit': int(curr_sum.get('real_profit') or 0),
            'real_margin_percent': float(curr_sum.get('real_margin_percent') or 0),
            'products_without_cost': int(curr_sum.get('products_without_cost') or 0),
            'blind_revenue': int(curr_sum.get('blind_revenue') or curr_sum.get('exposed_revenue_without_cost') or 0),
            'order_count': cur_order_count
        }
        previous_block = {
            'net_sales': int(prev_sum.get('net_sales') or 0),
            'total_cost_of_goods': int(prev_sum.get('total_cost_of_goods') or 0),
            'gross_profit': int(prev_sum.get('gross_profit') or 0),
            'gross_margin_percent': float(prev_sum.get('gross_margin_percent') or 0),
            'external_fee_total': int(prev_sum.get('external_fee_total') or 0),
            'real_profit': int(prev_sum.get('real_profit') or 0),
            'real_margin_percent': float(prev_sum.get('real_margin_percent') or 0),
            'products_without_cost': int(prev_sum.get('products_without_cost') or 0),
            'blind_revenue': int(prev_sum.get('blind_revenue') or prev_sum.get('exposed_revenue_without_cost') or 0),
            'order_count': prev_order_count
        }
        current_rm = float(curr_sum.get('real_margin_percent') or 0)
        prev_rm = float(prev_sum.get('real_margin_percent') or 0)
        comparison = {
            'previous_range': {'from': prev_from_iso, 'to': prev_to_iso},
            'current': current_block,
            'previous': previous_block,
            'previous_net_sales': int(prev_sum.get('net_sales') or 0),
            'delta_net_sales': _delta_val(curr_sum.get('net_sales'), prev_sum.get('net_sales')),
            'delta_net_sales_percent': _delta_percent(curr_sum.get('net_sales'), prev_sum.get('net_sales')),
            'previous_total_cost': int(prev_sum.get('total_cost_of_goods') or 0),
            'delta_cost': _delta_val(curr_sum.get('total_cost_of_goods'), prev_sum.get('total_cost_of_goods')),
            'delta_cost_percent': _delta_percent(curr_sum.get('total_cost_of_goods'), prev_sum.get('total_cost_of_goods')),
            'previous_gross_profit': int(prev_sum.get('gross_profit') or 0),
            'delta_gross_profit': _delta_val(curr_sum.get('gross_profit'), prev_sum.get('gross_profit')),
            'delta_gross_profit_percent': _delta_percent(curr_sum.get('gross_profit'), prev_sum.get('gross_profit')),
            'previous_gross_margin': float(prev_gm),
            'delta_gross_margin_pp': float(_pp_diff(current_gm, prev_gm)),
            'previous_external_fee': int(prev_sum.get('external_fee_total') or 0),
            'delta_external_fee': _delta_val(curr_sum.get('external_fee_total'), prev_sum.get('external_fee_total')),
            'delta_external_fee_percent': _delta_percent(curr_sum.get('external_fee_total'), prev_sum.get('external_fee_total')),
            'previous_real_profit': int(prev_sum.get('real_profit') or 0),
            'delta_real_profit': _delta_val(curr_sum.get('real_profit'), prev_sum.get('real_profit')),
            'delta_real_profit_percent': _delta_percent(curr_sum.get('real_profit'), prev_sum.get('real_profit')),
            'previous_real_margin': float(prev_rm),
            'delta_real_margin_pp': float(_pp_diff(current_rm, prev_rm)),
            'previous_products_without_cost': int(prev_sum.get('products_without_cost') or 0)
        }
        out = {
            'ok': True,
            'range': {
                'from': from_iso,
                'to': to_iso,
                'truncated_to_now': bool(truncated_to_now)
            },
            'summary': curr_sum,
            'comparison': comparison,
            'by_product': current.get('by_product') or [],
            'by_category': current.get('by_category') or [],
            'by_channel': current.get('by_channel') or [],
            'order_count': int(current.get('order_count') or 0),
            'previous_order_count': int(previous.get('order_count') or 0),
            'leaders': current.get('leaders') or {}
        }
        top_profit = out['by_product'][:15] if out['by_product'] else []
        bottom_margin = sorted(
            [x for x in (out['by_product'] or []) if x.get('revenue', 0) > 0 and x.get('qty', 0) >= 1],
            key=lambda x: x.get('gross_margin_percent', x.get('margin_percent', 0))
        )[:10]
        out['top_by_profit'] = top_profit
        out['top15_by_profit'] = top_profit
        out['bottom_by_margin'] = bottom_margin
        out['bottom10_by_margin'] = bottom_margin
        return jsonify(out)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/costs/kpis_small', methods=['GET'])
def costs_kpis_small():
    try:
        if not is_authed():
            return jsonify({'error': 'no autorizado'}), 401
        tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
        if not _session_tenant_matches(tenant_slug):
            return jsonify({'error': 'acceso denegado al tenant'}), 403
        from_raw = request.args.get('from')
        to_raw = request.args.get('to')
        from_dt, to_dt, from_iso, to_iso, truncated_to_now = _resolve_sales_range(from_raw, to_raw)
        payload = _compute_costs_payload(tenant_slug, from_dt, to_dt, truncated_to_now)
        s = payload['summary']
        return jsonify({
            'ok': True,
            'range': {'from': from_iso, 'to': to_iso, 'truncated_to_now': bool(truncated_to_now)},
            'kpis': {
                'net_sales': int(s.get('net_sales') or 0),
                'total_cost_of_goods': int(s.get('total_cost_of_goods') or 0),
                'gross_profit': int(s.get('gross_profit') or 0),
                'gross_margin_percent': float(s.get('gross_margin_percent') or 0.0),
                'external_fee_total': int(s.get('external_fee_total') or 0),
                'real_profit': int(s.get('real_profit') or 0),
                'real_margin_percent': float(s.get('real_margin_percent') or 0.0),
                'products_without_cost': int(s.get('products_without_cost') or 0),
                'order_count': int(payload.get('order_count') or 0)
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/costs/products_template', methods=['GET'])
def costs_products_template():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    if not _session_tenant_matches(tenant_slug):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT product_id, name, price, COALESCE(cost_price, 0),
               COALESCE(cost_type, 'fixed'), COALESCE(margin_percent, 0),
               COALESCE(variants_json, '')
        FROM products WHERE tenant_slug = ?
        ORDER BY CASE WHEN COALESCE(position, 0) <= 0 THEN 1 ELSE 0 END ASC,
                 COALESCE(position, 0) ASC, name ASC
        """,
        (tenant_slug,)
    )
    rows = cur.fetchall() or []
    data_rows = []
    for r in rows:
        variants = {}
        try:
            variants = json.loads(r[6] or '{}') or {} if r[6] else {}
        except Exception:
            variants = {}
        cats = variants.get('food_categories') or []
        if isinstance(cats, list):
            cat_txt = ', '.join([str(x).strip() for x in cats if str(x).strip()])
        else:
            cat_txt = _safe_str(cats)
        data_rows.append({
            'id': _safe_str(r[0]),
            'nombre': _safe_str(r[1]),
            'precio': int(r[2] or 0),
            'costo_unitario': int(r[3] or 0),
            'tipo_costo': _safe_str(r[4]) or 'fixed',
            'margen_sugerido': int(r[5] or 0),
            'categoria': cat_txt
        })
    if not data_rows:
        data_rows = [{
            'id': 'EJ1',
            'nombre': 'Ejemplo Hamburguesa Completa',
            'precio': 6000,
            'costo_unitario': 2400,
            'tipo_costo': 'fixed',
            'margen_sugerido': 60,
            'categoria': 'hamburguesas'
        }]
    headers_order = ['id', 'nombre', 'precio', 'costo_unitario', 'tipo_costo', 'margen_sugerido', 'categoria']
    pretty_headers = {
        'id': 'ID Producto',
        'nombre': 'Nombre',
        'precio': 'Precio Venta',
        'costo_unitario': 'Costo Unitario',
        'tipo_costo': 'Tipo Costo (fixed/percentage/recipe)',
        'margen_sugerido': 'Margen Sugerido %',
        'categoria': 'Categoria'
    }
    xlsx_bytes = create_xlsx_bytes({
        'Costos Productos': {
            'headers': headers_order,
            'rows': data_rows,
            'pretty_headers': pretty_headers,
            'bold_headers': True
        }
    })
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M')
    filename = f'plantilla_costos_{tenant_slug}_{ts}.xlsx'
    return send_file(
        __import__('io').BytesIO(xlsx_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )


@bp.route('/costs/import_preview', methods=['POST'])
def costs_import_preview():
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
    sheet_key = _find_sheet_by_name(sheets, ['Costos Productos', 'Costos', 'Costos_Productos', 'costos', 'costos productos', 'Productos', 'products', 'productos'])
    if not sheet_key:
        return jsonify({'error': 'no se encontró la hoja "Costos Productos" o "Productos"'}), 400
    rows = _rows_to_dicts(sheets.get(sheet_key) or [])
    plan = _build_cost_import_plan(tenant_slug, rows)
    return jsonify({
        'ok': True,
        'tenant_slug': tenant_slug,
        'sheet': sheet_key,
        'summary': {
            'to_create': plan['to_create'],
            'to_update': plan['to_update'],
            'without_match': plan['without_match'],
            'errors': plan['errors_count'],
            'total_rows': plan['total_rows'],
            'products_without_cost_in_db': plan['count_without_cost']
        },
        'actions': plan.get('actions') or [],
        'products_without_cost_unchanged': plan.get('products_without_cost_unchanged') or []
    })


@bp.route('/costs/import_apply', methods=['POST'])
def costs_import_apply():
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
    sheet_key = _find_sheet_by_name(sheets, ['Costos Productos', 'Costos', 'Costos_Productos', 'costos', 'costos productos', 'Productos', 'products', 'productos'])
    if not sheet_key:
        return jsonify({'error': 'no se encontró la hoja "Costos Productos" o "Productos"'}), 400
    rows = _rows_to_dicts(sheets.get(sheet_key) or [])
    plan = _build_cost_import_plan(tenant_slug, rows)
    if plan.get('errors_count'):
        return jsonify({'error': 'hay errores en el archivo', 'actions': plan.get('actions') or []}), 400

    conn = get_db()
    cur = conn.cursor()
    existing = _fetch_existing_products(cur, tenant_slug)
    created = 0
    updated = 0
    skipped = 0
    try:
        for act in (plan.get('actions') or []):
            row_idx = int(act.get('row') or 0) or 0
            if act.get('action') not in ('create', 'update'):
                skipped += 1
                continue
            pid = _safe_str(act.get('id'))
            src = None
            for r in rows:
                if int(r.get('__row_index') or 0) == row_idx:
                    src = r
                    break
            if src is None:
                src = act
            id_raw = src.get('id') if 'id' in src else src.get('product_id') if 'product_id' in src else src.get('codigo') if 'codigo' in src else src.get('código')
            name_raw = src.get('nombre') if 'nombre' in src else src.get('name')
            cost_price_raw = src.get('costo_unitario') if 'costo_unitario' in src else src.get('cost_price') if 'cost_price' in src else src.get('costo') if 'costo' in src else src.get('unit_cost')
            cost_type_raw = src.get('tipo_costo') if 'tipo_costo' in src else src.get('cost_type')
            margin_percent_raw = src.get('margen_sugerido') if 'margen_sugerido' in src else src.get('margin_percent') if 'margin_percent' in src else src.get('margen') if 'margen' in src else None
            price_raw = src.get('precio') if 'precio' in src else src.get('price')

            nm = _safe_str(name_raw)
            final_pid = _safe_str(pid) or _safe_str(id_raw)
            if not final_pid and not nm:
                continue
            cp = int(_parse_int(cost_price_raw) or 0) if _cell_present(cost_price_raw) else 0
            ct = _normalize_cost_type(cost_type_raw) if _cell_present(cost_type_raw) else 'fixed'
            mp = int(_parse_int(margin_percent_raw) or 0) if _cell_present(margin_percent_raw) else 0
            pr = int(_parse_int(price_raw) or 0) if _cell_present(price_raw) else 0

            if act.get('action') == 'create':
                if not final_pid:
                    cur.execute("SELECT product_id FROM products WHERE tenant_slug = ?", (tenant_slug,))
                    existing_rows = cur.fetchall() or []
                    max_n = 0
                    width = 0
                    for ex_r in existing_rows:
                        raw_s = str(ex_r[0] or '').strip()
                        if not raw_s.isdigit():
                            continue
                        width = max(width, len(raw_s))
                        try:
                            n = int(raw_s)
                        except Exception:
                            continue
                        if n > max_n:
                            max_n = n
                    next_n = max_n + 1
                    candidate = str(next_n)
                    if width > 1 and len(candidate) < width:
                        candidate = candidate.zfill(width)
                    while True:
                        cur.execute(
                            "SELECT 1 FROM products WHERE tenant_slug = ? AND product_id = ? LIMIT 1",
                            (tenant_slug, candidate)
                        )
                        if not cur.fetchone():
                            break
                        next_n += 1
                        candidate = str(next_n)
                        if width > 1 and len(candidate) < width:
                            candidate = candidate.zfill(width)
                    final_pid = candidate
                cur.execute(
                    """
                    INSERT INTO products (tenant_slug, product_id, name, price, cost_price, cost_type, margin_percent, stock, position, active, details, variants_json, last_modified)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 1, '', ?, ?)
                    """,
                    (tenant_slug, final_pid, nm or final_pid, pr, cp, ct, mp, json.dumps({}, ensure_ascii=False), datetime.utcnow().isoformat())
                )
                created += 1
            else:
                target_pid = _safe_str(act.get('id'))
                info = existing.get(target_pid)
                if not info:
                    skipped += 1
                    continue
                fields = []
                params_list = []
                if _cell_present(price_raw) and pr > 0:
                    fields.append('price = ?')
                    params_list.append(pr)
                if _cell_present(cost_price_raw):
                    fields.append('cost_price = ?')
                    params_list.append(cp)
                if _cell_present(cost_type_raw):
                    fields.append('cost_type = ?')
                    params_list.append(ct)
                if _cell_present(margin_percent_raw):
                    fields.append('margin_percent = ?')
                    params_list.append(mp)
                if not fields:
                    skipped += 1
                    continue
                fields.append('last_modified = ?')
                params_list.append(datetime.utcnow().isoformat())
                params_list.extend([tenant_slug, target_pid])
                cur.execute(
                    f"UPDATE products SET {', '.join(fields)} WHERE tenant_slug = ? AND product_id = ?",
                    params_list
                )
                updated += 1
        conn.commit()
        remaining_without_cost = 0
        try:
            cur.execute(
                "SELECT COUNT(*) FROM products WHERE tenant_slug = ? AND active = 1 AND (COALESCE(cost_price, 0) = 0)",
                (tenant_slug,)
            )
            rw = cur.fetchone()
            remaining_without_cost = int(rw[0] if rw else 0)
        except Exception:
            remaining_without_cost = 0
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({'error': str(e)}), 500
    return jsonify({
        'ok': True,
        'tenant_slug': tenant_slug,
        'sheet': sheet_key,
        'summary': {
            'created': created,
            'updated': updated,
            'skipped': skipped,
            'unchanged': skipped,
            'remaining_without_cost': remaining_without_cost,
            'errors': plan.get('errors_count') or 0
        }
    })
