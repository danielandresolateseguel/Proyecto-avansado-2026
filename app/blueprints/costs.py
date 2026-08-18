import os
import json
from datetime import datetime
from flask import Blueprint, request, jsonify, session, current_app, send_file
from app.database import get_db
from app.utils import is_authed, check_csrf, read_xlsx_sheets, create_xlsx_bytes

bp = Blueprint('costs', __name__, url_prefix='/api')


def _session_tenant_matches(tenant_slug):
    session_tenant = str(session.get('tenant_slug') or '').strip()
    if not session_tenant:
        return True
    if not tenant_slug:
        return False
    return session_tenant == str(tenant_slug).strip()


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
            'errors': plan.get('errors_count') or 0
        }
    })
