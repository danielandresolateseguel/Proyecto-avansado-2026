from flask import Blueprint, request, jsonify, session, Response
from app.database import get_db
from app.utils import is_authed, check_csrf, get_cached_tenant_config
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import re
import unicodedata
import io
import csv
import json

bp = Blueprint('archive', __name__, url_prefix='/api')
try:
    ANALYTICS_TZ = ZoneInfo('America/Argentina/Buenos_Aires')
except Exception:
    ANALYTICS_TZ = timezone(timedelta(hours=-3))

def _parse_perms_json(raw):
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _norm_date(s, end=False):
    try:
        if s and len(s) == 10:
            return s + ('T23:59:59' if end else 'T00:00:00')
    except Exception:
        pass
    return s

def _series_letters_to_index(series):
    letters = re.sub(r'[^A-Z]', '', str(series or '').upper())
    if not letters:
        return 0
    idx = 0
    for ch in letters:
        idx = (idx * 26) + (ord(ch) - 64)
    return max(0, idx - 1)

def _parse_visible_order_number(value):
    raw = str(value or '').strip().upper()
    if not raw:
        return None
    raw = re.sub(r'^\s*PEDIDO\s*#?\s*', '', raw, flags=re.IGNORECASE)
    raw = raw.replace(' ', '').replace('-', '')
    m = re.fullmatch(r'([A-Z]*)(\d+)', raw)
    if not m:
        return None
    series = str(m.group(1) or '').strip().upper()
    try:
        number = int(m.group(2) or '0')
    except Exception:
        return None
    if number <= 0:
        return None
    if not series:
        return number
    if number > 9999:
        return None
    return 10000 + (_series_letters_to_index(series) * 9999) + (number - 1)

def _parse_iso_dt(value):
    try:
        if not value:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None

def _utc_naive_to_local(dt):
    try:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.astimezone(ANALYTICS_TZ)
    except Exception:
        return None

def _local_date_boundary_to_utc_naive(value, end=False):
    try:
        dt_local = datetime.strptime(str(value or '').strip(), '%Y-%m-%d')
        dt_local = dt_local.replace(
            hour=23 if end else 0,
            minute=59 if end else 0,
            second=59 if end else 0,
            microsecond=0,
            tzinfo=ANALYTICS_TZ,
        )
        return dt_local.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None

def _format_dt_for_client(dt):
    try:
        return dt.isoformat() if dt is not None else ''
    except Exception:
        return ''

def _money(value):
    try:
        return int(round(float(value or 0)))
    except Exception:
        return 0

def _apply_archive_filters(base_sql, params, *, a_type=None, from_date=None, to_date=None, order_type=None, q=None, date_field='archived'):
    sql = str(base_sql or '')
    out_params = list(params or [])
    if a_type:
        sql += " AND a.type = ?"
        out_params.append(a_type)
    date_col = 'a.archived_at' if date_field == 'archived' else 'o.created_at'
    if from_date:
        sql += f" AND {date_col} >= ?"
        out_params.append(from_date)
    if to_date:
        sql += f" AND {date_col} <= ?"
        out_params.append(to_date)
    if order_type:
        sql += " AND o.order_type = ?"
        out_params.append(order_type)
    if q:
        visible_num = _parse_visible_order_number(q)
        if visible_num is not None:
            try:
                qid = int(str(q).strip())
                sql += " AND (o.id = ? OR o.tenant_order_number = ?)"
                out_params.extend([qid, visible_num])
            except Exception:
                sql += " AND o.tenant_order_number = ?"
                out_params.append(visible_num)
        else:
            nq = re.sub(r"^(destino|direccion|dir|cliente|tel|telefono)\s*:\s*", "", str(q), flags=re.IGNORECASE).strip()
            like = f"%{nq.lower()}%"
            sql += (
                " AND (LOWER(COALESCE(o.address_json,'')) LIKE ?"
                " OR LOWER(COALESCE(o.table_number,'')) LIKE ?"
                " OR LOWER(COALESCE(o.customer_name,'')) LIKE ?"
                " OR LOWER(COALESCE(o.customer_phone,'')) LIKE ?)"
            )
            out_params.extend([like, like, like, like])
    return sql, out_params

def _round_metric(value, digits=1):
    try:
        return round(float(value or 0), digits)
    except Exception:
        return 0.0

def _resolve_sales_range(from_raw, to_raw):
    now_dt = datetime.utcnow().replace(microsecond=0)
    now_local = _utc_naive_to_local(now_dt)
    default_from = now_local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).replace(tzinfo=None)
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
            from_dt = local_to.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).replace(tzinfo=None)
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

def _has_reports_access():
    role = str(session.get('admin_role') or '').strip().lower()
    if bool(session.get('admin_owner')) or role == 'admin':
        return True
    perms = _parse_perms_json(session.get('admin_perms') or '')
    return bool(perms.get('reports_view'))

def _ctx():
    tenant = str(session.get('tenant_slug') or '').strip()
    role = str(session.get('admin_role') or '').strip().lower()
    owner = bool(session.get('admin_owner'))
    perms = _parse_perms_json(session.get('admin_perms') or '')
    return tenant, role, perms, owner

def _has_perm(perms, owner, role, key):
    if owner or role == 'admin':
        return True
    return bool((perms or {}).get(key))

def _norm_channel(order_type):
    value = str(order_type or '').strip().lower()
    if value == 'mesa':
        return 'mesa', 'Mesa'
    if value in ('direccion', 'delivery'):
        return 'delivery', 'Delivery'
    if value == 'retiro':
        return 'retiro', 'Retiro'
    if value == 'espera':
        return 'espera', 'Espera'
    return 'otros', 'Otros'

def _norm_payment_method(payment_method):
    raw_value = str(payment_method or '').strip().lower()
    value = ''.join(
        c for c in unicodedata.normalize('NFKD', raw_value)
        if not unicodedata.combining(c)
    )
    value = re.sub(r'[\s_\-]+', ' ', value).strip()
    if value in ('efectivo', 'cash', 'contado', 'efvo') or 'efectivo' in value or value.startswith('contado'):
        return 'efectivo', 'Efectivo'
    if value in ('pos', 'pos/qr', 'qr', 'tarjeta', 'card', 'debito', 'credito', 'mercado pago', 'mercadopago') or 'pos' in value or 'qr' in value or 'tarjeta' in value:
        return 'pos', 'POS/QR'
    if value in ('transferencia', 'transfer', 'trans', 'transferencia bancaria') or 'transferencia' in value or value == 'trans' or value.startswith('transfer'):
        return 'transferencia', 'Transferencia'
    if value in ('mixed', 'mixto', 'pago mixto') or 'mixto' in value or 'mixed' in value:
        return 'mixto', 'Mixto'
    return 'otros', 'Otros'

def _percent(part, total):
    try:
        part_val = float(part or 0)
        total_val = float(total or 0)
        if total_val <= 0:
            return 0.0
        return round((part_val / total_val) * 100.0, 2)
    except Exception:
        return 0.0

def _delta_percent(current, previous):
    try:
        current_val = float(current or 0)
        previous_val = float(previous or 0)
        if previous_val <= 0:
            return None if current_val > 0 else 0.0
        return round(((current_val - previous_val) / previous_val) * 100.0, 2)
    except Exception:
        return None

def _base_payment_label(base):
    k = str(base or '').strip().lower()
    if k == 'contado':
        return 'Efectivo'
    if k == 'pos':
        return 'POS'
    if k == 'qr':
        return 'QR'
    if k == 'transferencia':
        return 'Transferencia'
    return k or '-'

def _load_payment_methods_config(tenant_slug):
    cfg = get_cached_tenant_config(tenant_slug) or {}
    pm_cfg = cfg.get('payment_methods') or {}
    items = []
    if isinstance(pm_cfg, dict) and isinstance(pm_cfg.get('methods'), list):
        items = pm_cfg.get('methods') or []
    elif isinstance(pm_cfg, list):
        items = pm_cfg
    base_allowed = ('contado', 'pos', 'qr', 'transferencia')
    out = {
        'contado': {'id': 'contado', 'base': 'contado', 'label': 'Efectivo', 'locked': True},
        'pos': {'id': 'pos', 'base': 'pos', 'label': 'POS', 'locked': True},
        'qr': {'id': 'qr', 'base': 'qr', 'label': 'QR', 'locked': True},
        'transferencia': {'id': 'transferencia', 'base': 'transferencia', 'label': 'Transferencia', 'locked': True},
    }
    for it in items or []:
        if not isinstance(it, dict):
            continue
        if it.get('active') is False:
            continue
        pid = str(it.get('id') or '').strip().lower()
        base = str(it.get('base') or '').strip().lower()
        label = str(it.get('label') or it.get('name') or '').strip()
        if not pid or pid in out:
            continue
        if base not in base_allowed:
            continue
        if not label:
            continue
        out[pid] = {'id': pid, 'base': base, 'label': label, 'locked': False}
    return out

def _format_payment_label(meta):
    if not meta:
        return ''
    label = str(meta.get('label') or '').strip()
    pid = str(meta.get('id') or '').strip()
    base = str(meta.get('base') or '').strip().lower()
    if not label:
        return pid or '-'
    if meta.get('locked'):
        return label
    base_label = _base_payment_label(base)
    low = label.lower()
    if base_label and low.startswith(base_label.lower()):
        return label
    if '·' in label:
        return label
    return f"{base_label} · {label}" if base_label else label

def _short_payment_label(full_label):
    txt = str(full_label or '').strip()
    if '·' in txt:
        parts = [p.strip() for p in txt.split('·') if p.strip()]
        if len(parts) >= 2:
            return parts[-1]
    return txt

@bp.route('/archive', methods=['GET'])
def get_archive():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant, role, perms, owner = _ctx()
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    can_access = (
        _has_reports_access()
        or _has_perm(perms, owner, role, 'orders_view')
        or _has_perm(perms, owner, role, 'cash_view')
        or _has_perm(perms, owner, role, 'cash_manage')
    )
    if not can_access:
        return jsonify({'error': 'sin permisos'}), 403
    a_type = request.args.get('type')
    limit = int(request.args.get('limit') or 100)
    offset = int(request.args.get('offset') or 0)
    q = request.args.get('q')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    order_type = request.args.get('order_type')
    date_field = (request.args.get('date_field') or 'archived').strip().lower()
    if date_field not in ('archived', 'order'):
        date_field = 'archived'
    def _norm_date(s, end=False):
        try:
            if s and len(s) == 10:
                return s + ('T23:59:59' if end else 'T00:00:00')
        except Exception:
            pass
        return s
    from_date = _norm_date(from_date, end=False)
    to_date = _norm_date(to_date, end=True)
    conn = get_db()
    cur = conn.cursor()
    pm_cfg = _load_payment_methods_config(tenant_slug)
    base = """
        SELECT o.id, o.created_at, o.order_type, o.table_number, o.address_json, o.total, o.status, o.customer_name, o.customer_phone, h.last_status, h.last_change,
               o.payment_method,
               (SELECT payload_json FROM order_events WHERE order_id = o.id AND event_type = 'payment' ORDER BY id DESC LIMIT 1) AS pay_payload
        FROM archived_orders a
        JOIN orders o ON o.id = a.order_id
        LEFT JOIN (
          SELECT x.order_id, x.status AS last_status, x.changed_at AS last_change
          FROM order_status_history x
          JOIN (
            SELECT order_id, MAX(changed_at) AS mc FROM order_status_history GROUP BY order_id
          ) y ON y.order_id = x.order_id AND y.mc = x.changed_at
        ) h ON h.order_id = o.id
        WHERE a.tenant_slug = ?
    """
    params = [tenant_slug]
    base, params = _apply_archive_filters(
        base,
        params,
        a_type=a_type,
        from_date=from_date,
        to_date=to_date,
        order_type=order_type,
        q=q,
        date_field=date_field,
    )
    base += " ORDER BY o.id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    cur.execute(base, params)
    rows = cur.fetchall()
    # total_count
    count_sql = """
        SELECT COUNT(*)
        FROM archived_orders a
        JOIN orders o ON o.id = a.order_id
        LEFT JOIN (
          SELECT x.order_id, x.status AS last_status, x.changed_at AS last_change
          FROM order_status_history x
          JOIN (
            SELECT order_id, MAX(changed_at) AS mc FROM order_status_history GROUP BY order_id
          ) y ON y.order_id = x.order_id AND y.mc = x.changed_at
        ) h ON h.order_id = o.id
        WHERE a.tenant_slug = ?
    """
    count_params = [tenant_slug]
    count_sql, count_params = _apply_archive_filters(
        count_sql,
        count_params,
        a_type=a_type,
        from_date=from_date,
        to_date=to_date,
        order_type=order_type,
        q=q,
        date_field=date_field,
    )
    cur.execute(count_sql, count_params)
    total_count = int(cur.fetchone()[0])
    data = [dict(r) for r in rows]
    for r in data:
        method = str(r.get('payment_method') or '').strip().lower()
        pay_payload = r.get('pay_payload') or ''
        details = None
        try:
            meta = json.loads(pay_payload) if pay_payload else {}
        except Exception:
            meta = {}
        if isinstance(meta, dict):
            m2 = str(meta.get('method') or '').strip().lower()
            if m2:
                method = m2
                r['payment_method'] = m2
            if isinstance(meta.get('details'), list):
                details = meta.get('details')
        r.pop('pay_payload', None)
        r['payment_details'] = details
        if method == 'mixed' and isinstance(details, list) and details:
            uniq = []
            seen = set()
            for d in details:
                if not isinstance(d, dict):
                    continue
                mid = str(d.get('method') or '').strip().lower()
                try:
                    amt = int(d.get('amount') or 0)
                except Exception:
                    amt = 0
                if not mid or amt <= 0:
                    continue
                meta_m = pm_cfg.get(mid) or {'id': mid, 'base': mid, 'label': mid, 'locked': False}
                full = _format_payment_label(meta_m)
                short = _short_payment_label(full) if not (meta_m.get('locked') or False) else full
                if short and short not in seen:
                    uniq.append(short)
                    seen.add(short)
            r['payment_method_display'] = f"Mixto · {' + '.join(uniq)}" if uniq else 'Mixto'
        else:
            meta_m = pm_cfg.get(method) or {'id': method, 'base': method, 'label': method, 'locked': False}
            r['payment_method_display'] = _format_payment_label(meta_m) if method else '-'
    if q:
        try:
            int(q)
        except Exception:
            def _norm(s):
                s = str(s or '').lower()
                return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
            nq = re.sub(r"^(destino|direccion|dir|cliente|tel|telefono)\s*:\s*", "", str(q), flags=re.IGNORECASE).strip()
            nq = _norm(nq)
            data = [r for r in data if (
                nq in _norm(r.get('address_json')) or
                nq in _norm(r.get('table_number')) or
                nq in _norm(r.get('customer_name')) or
                nq in _norm(r.get('customer_phone'))
            )]
    return jsonify({'archives': data, 'count': len(data), 'limit': limit, 'offset': offset, 'total_count': total_count})

@bp.route('/archive/eligible_count', methods=['GET'])
def archive_eligible_count():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    a_type = request.args.get('type')
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or ''
    session_tenant, role, perms, owner = _ctx()
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    can_access = (
        _has_reports_access()
        or _has_perm(perms, owner, role, 'orders_view')
        or _has_perm(perms, owner, role, 'cash_view')
        or _has_perm(perms, owner, role, 'cash_manage')
    )
    if not can_access:
        return jsonify({'error': 'sin permisos'}), 403
    hours = int(request.args.get('hours') or 24)
    if a_type not in ('delivered','canceled'):
        return jsonify({'error': 'type inválido'}), 400
    cutoff_dt = datetime.utcnow() - timedelta(hours=max(1, hours))
    cutoff = cutoff_dt.isoformat()
    conn = get_db()
    cur = conn.cursor()
    base_status = 'entregado' if a_type == 'delivered' else 'cancelado'
    sql = """
        SELECT COUNT(*)
        FROM orders o
        JOIN (
          SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = ? GROUP BY order_id
        ) h ON h.order_id = o.id
        LEFT JOIN archived_orders a ON a.order_id = o.id AND a.type = ?
        WHERE a.order_id IS NULL AND h.last_change <= ?
    """
    params = [base_status, a_type, cutoff]
    if tenant_slug:
        sql += " AND o.tenant_slug = ?"
        params.append(tenant_slug)
    cur.execute(sql, params)
    n = cur.fetchone()[0]
    return jsonify({'count': int(n), 'type': a_type, 'tenant_slug': tenant_slug or None, 'hours': hours})

@bp.route('/archive/export.csv', methods=['GET'])
@bp.route('/archive/export', methods=['GET'])
def archive_export():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant = str(session.get('tenant_slug') or '').strip()
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_reports_access():
        return jsonify({'error': 'sin permisos'}), 403
    a_type = request.args.get('type')
    q = request.args.get('q')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    order_type = request.args.get('order_type')
    date_field = (request.args.get('date_field') or 'archived').strip().lower()
    if date_field not in ('archived', 'order'):
        date_field = 'archived'
    def _norm_date(s, end=False):
        try:
            if s and len(s) == 10:
                return s + ('T23:59:59' if end else 'T00:00:00')
        except Exception:
            pass
        return s
    from_date = _norm_date(from_date, end=False)
    to_date = _norm_date(to_date, end=True)
    conn = get_db()
    cur = conn.cursor()
    base = """
        SELECT o.id, o.created_at, o.order_type, o.table_number, o.address_json, o.total, o.status, a.archived_at, o.customer_name, o.customer_phone, h.last_status, h.last_change
        FROM archived_orders a
        JOIN orders o ON o.id = a.order_id
        LEFT JOIN (
          SELECT x.order_id, x.status AS last_status, x.changed_at AS last_change
          FROM order_status_history x
          JOIN (
            SELECT order_id, MAX(changed_at) AS mc FROM order_status_history GROUP BY order_id
          ) y ON y.order_id = x.order_id AND y.mc = x.changed_at
        ) h ON h.order_id = o.id
        WHERE a.tenant_slug = ?
    """
    params = [tenant_slug]
    base, params = _apply_archive_filters(
        base,
        params,
        a_type=a_type,
        from_date=from_date,
        to_date=to_date,
        order_type=order_type,
        q=q,
        date_field=date_field,
    )
    base += " ORDER BY o.id DESC"
    cur.execute(base, params)
    rows = cur.fetchall()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "created_at", "order_type", "destination", "customer_phone", "total", "status", "archived_at", "customer_name", "last_status", "last_change", "payment_status"])
    for r in rows:
        dest = r[3] if r[2] == 'mesa' else (r[4] or '')
        total = int(r[5] or 0)
        writer.writerow([r[0], r[1], r[2], dest, r[9] or '', total, r[6], r[7], r[8], r[10] or '', r[11] or '', r[12] or ''])
    resp_val = output.getvalue()
    
    def _safe(s):
        return ''.join(c for c in str(s or '') if c.isalnum() or c in ('-', '_'))
    df = 'arch' if date_field == 'archived' else 'order'
    def _dpart(d):
        try:
            return str(d or 'all')[:10].replace('T','').replace(':','')
        except Exception:
            return 'all'
    fname = f"archives_{_safe(tenant_slug or 'tenant')}_{df}_{_dpart(from_date)}_{_dpart(to_date)}_{_safe(a_type or 'all')}.csv"
    return Response(resp_val, mimetype='text/csv', headers={'Content-Disposition': f'attachment; filename="{fname}"'})

@bp.route('/archive/metrics', methods=['GET'])
def archive_metrics():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant, role, perms, owner = _ctx()
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    can_access = (
        _has_reports_access()
        or _has_perm(perms, owner, role, 'orders_view')
        or _has_perm(perms, owner, role, 'cash_view')
        or _has_perm(perms, owner, role, 'cash_manage')
    )
    if not can_access:
        return jsonify({'error': 'sin permisos'}), 403
    q = request.args.get('q')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    order_type = request.args.get('order_type')
    date_field = (request.args.get('date_field') or 'archived').strip().lower()
    if date_field not in ('archived', 'order'):
        date_field = 'archived'
    def _norm_date(s, end=False):
        try:
            if s and len(s) == 10:
                return s + ('T23:59:59' if end else 'T00:00:00')
        except Exception:
            pass
        return s
    from_date = _norm_date(from_date, end=False)
    to_date = _norm_date(to_date, end=True)
    conn = get_db()
    cur = conn.cursor()
    base = """
        SELECT o.total, COALESCE(o.tip_amount, 0) AS tip_amount
        FROM archived_orders a JOIN orders o ON o.id = a.order_id
        WHERE a.tenant_slug = ?
    """
    params_del = [tenant_slug]
    sql_del, params_del = _apply_archive_filters(
        base,
        params_del,
        a_type='delivered',
        from_date=from_date,
        to_date=to_date,
        order_type=order_type,
        q=q,
        date_field=date_field,
    )
    params_can = [tenant_slug]
    sql_can, params_can = _apply_archive_filters(
        base,
        params_can,
        a_type='canceled',
        from_date=from_date,
        to_date=to_date,
        order_type=order_type,
        q=q,
        date_field=date_field,
    )
    params_reset = [tenant_slug]
    sql_reset, params_reset = _apply_archive_filters(
        base,
        params_reset,
        a_type='reset',
        from_date=from_date,
        to_date=to_date,
        order_type=order_type,
        q=q,
        date_field=date_field,
    )
    # Delivered metrics
    cur.execute(sql_del, params_del)
    rows_del = cur.fetchall()
    delivered_count = len(rows_del)
    delivered_total = int(sum(int(r[0] or 0) for r in rows_del))
    delivered_tip_total = int(sum(int(r[1] or 0) for r in rows_del))
    delivered_total_with_tip = delivered_total + delivered_tip_total
    # Canceled metrics
    cur.execute(sql_can, params_can)
    rows_can = cur.fetchall()
    canceled_count = len(rows_can)
    canceled_total = int(sum(int(r[0] or 0) for r in rows_can))
    # Reset metrics
    cur.execute(sql_reset, params_reset)
    rows_reset = cur.fetchall()
    reset_count = len(rows_reset)
    reset_total = int(sum(int(r[0] or 0) for r in rows_reset))
    return jsonify({
        'delivered_count': delivered_count,
        'delivered_total': delivered_total,
        'delivered_tip_total': delivered_tip_total,
        'delivered_total_with_tip': delivered_total_with_tip,
        'canceled_count': canceled_count,
        'canceled_total': canceled_total,
        'reset_count': reset_count,
        'reset_total': reset_total,
    })

@bp.route('/archive', methods=['POST'])
def post_archive():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    order_id = payload.get('order_id')
    a_type = payload.get('type')
    if not isinstance(order_id, int):
        try:
            order_id = int(order_id)
        except Exception:
            return jsonify({'error': 'order_id inválido'}), 400
    if a_type not in ('delivered', 'canceled', 'reset'):
        return jsonify({'error': 'type inválido'}), 400
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, tenant_slug, status, COALESCE(payment_status,'') FROM orders WHERE id = ?", (order_id,))
    r = cur.fetchone()
    if not r:
        return jsonify({'error': 'orden no encontrada'}), 404
    tenant_slug = r[1]
    status = str(r[2] or '').strip().lower()
    payment_status = str(r[3] or '').strip().lower()
    if a_type == 'delivered':
        if status != 'entregado':
            return jsonify({'error': 'solo se pueden archivar como entregados los pedidos en estado entregado'}), 400
        if payment_status != 'paid':
            return jsonify({'error': 'no se puede archivar un pedido entregado pendiente de cobro'}), 400
    elif a_type == 'canceled' and status != 'cancelado':
        return jsonify({'error': 'solo se pueden archivar como cancelados los pedidos en estado cancelado'}), 400
    cur.execute(
        "INSERT OR IGNORE INTO archived_orders (order_id, tenant_slug, type, archived_at) VALUES (?, ?, ?, ?)",
        (order_id, tenant_slug, a_type, datetime.utcnow().isoformat())
    )
    conn.commit()
    return jsonify({'ok': True, 'order_id': order_id, 'type': a_type})

@bp.route('/archive/reset', methods=['POST'])
def reset_active_orders():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    role = str(session.get('admin_role') or '').strip().lower()
    if not (bool(session.get('admin_owner')) or role == 'admin'):
        return jsonify({'error': 'solo admin puede limpiar pedidos activos'}), 403
    
    payload = request.get_json(silent=True) or {}
    tenant_slug = payload.get('tenant_slug')
    if not tenant_slug:
        return jsonify({'error': 'tenant_slug requerido'}), 400

    conn = get_db()
    cur = conn.cursor()
    
    # Select all active orders (not in archived_orders) for this tenant
    cur.execute("""
        SELECT id FROM orders 
        WHERE tenant_slug = ? 
        AND id NOT IN (SELECT order_id FROM archived_orders WHERE tenant_slug = ?)
    """, (tenant_slug, tenant_slug))
    
    rows = cur.fetchall()
    count = 0
    now_iso = datetime.utcnow().isoformat()
    
    for row in rows:
        order_id = row[0]
        cur.execute(
            "INSERT OR IGNORE INTO archived_orders (order_id, tenant_slug, type, archived_at) VALUES (?, ?, 'reset', ?)",
            (order_id, tenant_slug, now_iso)
        )
        count += 1
        
    conn.commit()
    return jsonify({'ok': True, 'count': count})

@bp.route('/metrics', methods=['GET'])
def metrics():
    try:
        if not is_authed():
            return jsonify({'error': 'no autorizado'}), 401
        tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
        session_tenant = str(session.get('tenant_slug') or '').strip()
        is_owner = bool(session.get('admin_owner'))
        if session_tenant and tenant_slug and session_tenant != tenant_slug:
            return jsonify({'error': 'acceso denegado al tenant'}), 403
        if not is_owner:
            return jsonify({'error': 'solo owner'}), 403
        from_date = request.args.get('from')
        to_date = request.args.get('to')
        def _norm_date(s, end=False):
            try:
                if s and len(s) == 10:
                    return s + ('T23:59:59' if end else 'T00:00:00')
            except Exception:
                pass
            return s
        from_date = _norm_date(from_date, end=False)
        to_date = _norm_date(to_date, end=True)
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM orders WHERE tenant_slug = ? AND status NOT IN ('entregado','cancelado') AND id NOT IN (SELECT order_id FROM archived_orders)", (tenant_slug,))
        active_count = cur.fetchone()[0]
        def _parse_iso_any(s):
            s = str(s or '').strip()
            if not s: return None
            for fmt in (
                '%Y-%m-%dT%H:%M:%S.%fZ','%Y-%m-%dT%H:%M:%S.%f','%Y-%m-%dT%H:%M:%SZ','%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f','%Y-%m-%d %H:%M:%S','%Y-%m-%d %H:%M','%Y-%m-%d',
            ):
                try: return datetime.strptime(s, fmt)
                except Exception: continue
            try: return datetime.fromisoformat(s.replace('Z',''))
            except Exception: return None
        _from_dt = _parse_iso_any(from_date)
        _to_dt = _parse_iso_any(to_date)
        _from_has_no_time = bool(from_date and len(str(from_date).strip()) == 10)
        _to_has_no_time = bool(to_date and len(str(to_date).strip()) == 10)
        if _from_dt is not None and _to_dt is not None and _to_dt < _from_dt:
            _from_dt, _to_dt = _to_dt, _from_dt
            _from_has_no_time, _to_has_no_time = _to_has_no_time, _from_has_no_time
            from_date, to_date = to_date, from_date
        elif from_date and to_date and len(str(from_date).strip()) == len(str(to_date).strip()) and str(to_date).strip() < str(from_date).strip():
            from_date, to_date = to_date, from_date
            _from_has_no_time, _to_has_no_time = _to_has_no_time, _from_has_no_time
        _start_buf = timedelta(hours=36) if (_from_has_no_time or (_from_dt is not None and _from_dt.hour==0 and _from_dt.minute==0 and _from_dt.second==0)) else timedelta(hours=0)
        _end_buf = timedelta(hours=36) if (_to_has_no_time or (_to_dt is not None and _to_dt.hour==23 and _to_dt.minute==59 and _to_dt.second==59)) else timedelta(hours=0)
        if _from_dt is None and _from_has_no_time:
            try: _from_dt = datetime.strptime(str(from_date).strip(), '%Y-%m-%d')
            except Exception: _from_dt = None
        if _to_dt is None and _to_has_no_time:
            try: _to_dt = datetime.strptime(str(to_date).strip(), '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
            except Exception: _to_dt = None
        if _from_dt is not None: _from_dt = _from_dt - _start_buf
        if _to_dt is not None: _to_dt = _to_dt + _end_buf
        _from_for_sql = _from_dt.strftime('%Y-%m-%dT%H:%M:%S') if _from_dt else (str(from_date).replace(' ', 'T') if from_date else None)
        _to_for_sql = _to_dt.strftime('%Y-%m-%dT%H:%M:%S') if _to_dt else (str(to_date).replace(' ', 'T') if to_date else None)
        closed_eff = "COALESCE(o.delivered_at, h.last_change, o.created_at)"
        def _base_agg(status_label, alias_h_status=None):
            hs = alias_h_status or status_label
            return (
                f"FROM orders o "
                f"LEFT JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = '{hs}' GROUP BY order_id) h ON h.order_id = o.id "
                f"WHERE o.tenant_slug = ? AND o.status = '{status_label}'"
            )
        base_from_del = _base_agg('entregado')
        base_from_can = _base_agg('cancelado')
        sql_date_where = ""
        params_date = []
        if str(_from_for_sql or ''):
            sql_date_where += f" AND {closed_eff} >= ?"; params_date.append(_from_for_sql)
        if str(_to_for_sql or ''):
            sql_date_where += f" AND {closed_eff} <= ?"; params_date.append(_to_for_sql)
        params_del = [tenant_slug] + params_date
        params_can = [tenant_slug] + params_date
        cur.execute(f"SELECT COUNT(*) {base_from_del} {sql_date_where}", params_del)
        delivered_count = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) {base_from_can} {sql_date_where}", params_can)
        canceled_count = cur.fetchone()[0]
        cur.execute(f"SELECT COALESCE(SUM(o.total),0) {base_from_del} {sql_date_where}", params_del)
        delivered_total = int(cur.fetchone()[0] or 0)
        tip = (delivered_total + 5) // 10
        delivered_total_with_tip = delivered_total + tip
        avg_prep = 0
        avg_listo = 0
        avg_entregado = 0
        try:
            where_clause = (
                f"EXISTS(SELECT 1 FROM order_status_history hh WHERE hh.order_id = o.id AND hh.status = 'entregado') "
                f"AND o.status = 'entregado'"
            )
            p2 = [tenant_slug]
            if str(_from_for_sql or ''):
                where_clause += f" AND {closed_eff} >= ?"; p2.append(_from_for_sql)
            if str(_to_for_sql or ''):
                where_clause += f" AND {closed_eff} <= ?"; p2.append(_to_for_sql)
            cur.execute(
                f"""
                SELECT o.id, o.created_at,
                       (SELECT hh.changed_at FROM order_status_history hh WHERE hh.order_id = o.id AND hh.status = 'preparacion' ORDER BY hh.id ASC LIMIT 1) AS prep_at,
                       (SELECT hh.changed_at FROM order_status_history hh WHERE hh.order_id = o.id AND hh.status = 'listo' ORDER BY hh.id ASC LIMIT 1) AS listo_at,
                       COALESCE(o.delivered_at, (SELECT hh.changed_at FROM order_status_history hh WHERE hh.order_id = o.id AND hh.status = 'entregado' ORDER BY hh.id DESC LIMIT 1)) AS entregado_at
                FROM orders o
                LEFT JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id
                WHERE o.tenant_slug = ? AND {where_clause}
                """,
                p2
            )
            rows = cur.fetchall()
            def _p(s):
                try:
                    if not s: return None
                    dt = None
                    if isinstance(s, str):
                        dt = datetime.fromisoformat(s)
                    elif isinstance(s, datetime):
                        dt = s
                    
                    if dt:
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        return dt
                    return None
                except Exception:
                    return None
            ps = []
            ls = []
            es = []
            for r in rows:
                created = _p(r[1])
                prep_at = _p(r[2])
                listo_at = _p(r[3])
                entregado_at = _p(r[4])
                if created and prep_at:
                    ps.append(max(0, int((prep_at - created).total_seconds() // 60)))
                if created and listo_at:
                    ls.append(max(0, int((listo_at - created).total_seconds() // 60)))
                if created and entregado_at:
                    es.append(max(0, int((entregado_at - created).total_seconds() // 60)))
            def _avg(a):
                try:
                    return int(sum(a) // max(1, len(a)))
                except Exception:
                    return 0
            avg_prep = _avg(ps)
            avg_listo = _avg(ls)
            avg_entregado = _avg(es)
        except Exception as e:
            print(f"Error calculating average metrics: {e}")
            avg_prep = 0
            avg_listo = 0
            avg_entregado = 0

        resp = jsonify({
            'active_count': active_count,
            'delivered_count': delivered_count,
            'canceled_count': canceled_count,
            'delivered_total': delivered_total,
            'delivered_tip_10': tip,
            'delivered_total_with_tip': delivered_total_with_tip,
            'avg_to_preparacion_min': avg_prep,
            'avg_to_listo_min': avg_listo,
            'avg_to_entregado_min': avg_entregado
        })
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp
    
    except Exception:
        try:
            return jsonify({
                'active_count': 0,
                'delivered_count': 0,
                'canceled_count': 0,
                'delivered_total': 0,
                'delivered_tip_10': 0,
                'delivered_total_with_tip': 0,
                'avg_to_preparacion_min': 0,
                'avg_to_listo_min': 0,
                'avg_to_entregado_min': 0
            })
        except Exception:
            return jsonify({'error': 'metrics unavailable'}), 500

@bp.route('/sales/analytics', methods=['GET'])
def sales_analytics():
    try:
        if not is_authed():
            return jsonify({'error': 'no autorizado'}), 401
        tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
        session_tenant = str(session.get('tenant_slug') or '').strip()
        if session_tenant and tenant_slug and session_tenant != tenant_slug:
            return jsonify({'error': 'acceso denegado al tenant'}), 403
        if not _has_reports_access():
            return jsonify({'error': 'sin permisos'}), 403

        from_dt, to_dt, from_iso, to_iso, truncated_to_now = _resolve_sales_range(
            request.args.get('from'),
            request.args.get('to'),
        )
        prev_to_dt = from_dt - timedelta(seconds=1)
        prev_from_dt = prev_to_dt - (to_dt - from_dt)
        prev_from_iso = prev_from_dt.isoformat()
        prev_to_iso = prev_to_dt.isoformat()

        conn = get_db()
        cur = conn.cursor()

        delivered_sql = """
            SELECT o.id, o.order_type, COALESCE(o.payment_method, '') AS payment_method,
                   COALESCE(o.total, 0) AS total, COALESCE(o.tip_amount, 0) AS tip_amount,
                   COALESCE(o.shipping_cost, 0) AS shipping_cost, o.created_at, h.last_change AS delivered_at
            FROM orders o
            JOIN (
                SELECT order_id, MAX(changed_at) AS last_change
                FROM order_status_history
                WHERE status = 'entregado'
                GROUP BY order_id
            ) h ON h.order_id = o.id
            WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ?
            ORDER BY h.last_change DESC
        """
        cur.execute(delivered_sql, (tenant_slug, from_iso, to_iso))
        delivered_rows = cur.fetchall()

        canceled_sql = """
            SELECT COALESCE(COUNT(*), 0) AS canceled_count, COALESCE(SUM(o.total), 0) AS canceled_total
            FROM orders o
            JOIN (
                SELECT order_id, MAX(changed_at) AS last_change
                FROM order_status_history
                WHERE status = 'cancelado'
                GROUP BY order_id
            ) h ON h.order_id = o.id
            WHERE o.tenant_slug = ? AND o.status = 'cancelado' AND h.last_change >= ? AND h.last_change <= ?
        """
        cur.execute(canceled_sql, (tenant_slug, from_iso, to_iso))
        canceled_row = cur.fetchone()
        canceled_count = _money(canceled_row[0] if canceled_row else 0)
        canceled_total = _money(canceled_row[1] if canceled_row else 0)

        def _summary_for_range(range_from_iso, range_to_iso):
            cur.execute(
                """
                SELECT COALESCE(COUNT(*), 0) AS delivered_count, COALESCE(SUM(o.total), 0) AS delivered_total,
                       COALESCE(SUM(o.tip_amount), 0) AS tip_total, COALESCE(SUM(o.shipping_cost), 0) AS shipping_total
                FROM orders o
                JOIN (
                    SELECT order_id, MAX(changed_at) AS last_change
                    FROM order_status_history
                    WHERE status = 'entregado'
                    GROUP BY order_id
                ) h ON h.order_id = o.id
                WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ?
                """,
                (tenant_slug, range_from_iso, range_to_iso),
            )
            delivered_row = cur.fetchone()
            cur.execute(canceled_sql, (tenant_slug, range_from_iso, range_to_iso))
            canceled_range_row = cur.fetchone()
            delivered_count_range = _money(delivered_row[0] if delivered_row else 0)
            delivered_total_range = _money(delivered_row[1] if delivered_row else 0)
            tip_total_range = _money(delivered_row[2] if delivered_row else 0)
            shipping_total_range = _money(delivered_row[3] if delivered_row else 0)
            canceled_count_range = _money(canceled_range_row[0] if canceled_range_row else 0)
            canceled_total_range = _money(canceled_range_row[1] if canceled_range_row else 0)
            cur.execute(
                """
                SELECT COALESCE(SUM(oi.qty), 0) AS items_total
                FROM order_items oi
                JOIN orders o ON o.id = oi.order_id
                JOIN (
                    SELECT order_id, MAX(changed_at) AS last_change
                    FROM order_status_history
                    WHERE status = 'entregado'
                    GROUP BY order_id
                ) h ON h.order_id = o.id
                WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ?
                """,
                (tenant_slug, range_from_iso, range_to_iso),
            )
            items_row = cur.fetchone()
            items_total_range = _money(items_row[0] if items_row else 0)
            avg_items_range = _round_metric(items_total_range / delivered_count_range, 1) if delivered_count_range else 0.0
            extras_total_range = tip_total_range + shipping_total_range
            extras_share_range = _percent(extras_total_range, delivered_total_range)
            canceled_denominator = delivered_count_range + canceled_count_range
            cancellation_rate_range = _percent(canceled_count_range, canceled_denominator) if canceled_denominator > 0 else 0.0
            return {
                'delivered_count': delivered_count_range,
                'delivered_total': delivered_total_range,
                'canceled_count': canceled_count_range,
                'canceled_amount': canceled_total_range,
                'cancellation_rate': cancellation_rate_range,
                'tips_total': tip_total_range,
                'shipping_total': shipping_total_range,
                'extras_total': extras_total_range,
                'extras_share_percent': extras_share_range,
                'items_sold_total': items_total_range,
                'avg_items_per_order': avg_items_range,
            }

        current_delivered_count = len(delivered_rows)
        current_net_sales = sum(_money(r[3]) for r in delivered_rows)
        current_tip_total = sum(_money(r[4]) for r in delivered_rows)
        current_shipping_total = sum(_money(r[5]) for r in delivered_rows)
        avg_ticket = _money(current_net_sales / current_delivered_count) if current_delivered_count else 0
        cancellation_rate = _percent(canceled_count, current_delivered_count + canceled_count)

        prev_summary = _summary_for_range(prev_from_iso, prev_to_iso)

        by_channel = {}
        by_payment = {}
        by_hour = {}
        by_day = {}
        for hour in range(24):
            by_hour[hour] = {
                'hour': hour,
                'label': f'{hour:02d}:00',
                'count': 0,
                'total': 0,
            }

        for row in delivered_rows:
            order_type = row[1]
            payment_method = row[2]
            total = _money(row[3])
            delivered_at = _parse_iso_dt(row[7])

            channel_key, channel_label = _norm_channel(order_type)
            bucket = by_channel.setdefault(channel_key, {
                'key': channel_key,
                'label': channel_label,
                'count': 0,
                'total': 0,
            })
            bucket['count'] += 1
            bucket['total'] += total

            pay_key, pay_label = _norm_payment_method(payment_method)
            pay_bucket = by_payment.setdefault(pay_key, {
                'key': pay_key,
                'label': pay_label,
                'count': 0,
                'total': 0,
            })
            pay_bucket['count'] += 1
            pay_bucket['total'] += total

            if delivered_at is not None:
                delivered_local = _utc_naive_to_local(delivered_at)
                hour_bucket = by_hour.get(delivered_local.hour if delivered_local is not None else delivered_at.hour)
                if hour_bucket is not None:
                    hour_bucket['count'] += 1
                    hour_bucket['total'] += total
                if delivered_local is not None:
                    day_key = delivered_local.strftime('%Y-%m-%d')
                    day_bucket = by_day.setdefault(day_key, {
                        'date': day_key,
                        'label': delivered_local.strftime('%d/%m'),
                        'count': 0,
                        'total': 0,
                    })
                    day_bucket['count'] += 1
                    day_bucket['total'] += total

        by_channel_list = sorted(by_channel.values(), key=lambda item: (-item['total'], -item['count'], item['label']))
        for item in by_channel_list:
            item['avg_ticket'] = _money(item['total'] / item['count']) if item['count'] else 0
            item['share_percent'] = _percent(item['total'], current_net_sales)

        by_payment_list = sorted(by_payment.values(), key=lambda item: (-item['total'], -item['count'], item['label']))
        for item in by_payment_list:
            item['avg_ticket'] = _money(item['total'] / item['count']) if item['count'] else 0
            item['share_percent'] = _percent(item['total'], current_net_sales)

        by_hour_list = [bucket for bucket in by_hour.values() if bucket['count'] > 0 or bucket['total'] > 0]
        by_day_list = sorted(by_day.values(), key=lambda item: item['date'])

        cur.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(oi.name), ''), '(Sin nombre)') AS product_name,
                   COALESCE(SUM(oi.qty), 0) AS qty_total,
                   COALESCE(SUM(oi.qty * oi.unit_price), 0) AS revenue_total
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            JOIN (
                SELECT order_id, MAX(changed_at) AS last_change
                FROM order_status_history
                WHERE status = 'entregado'
                GROUP BY order_id
            ) h ON h.order_id = o.id
            WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ?
            GROUP BY COALESCE(NULLIF(TRIM(oi.name), ''), '(Sin nombre)')
            """,
            (tenant_slug, from_iso, to_iso),
        )
        product_totals = []
        total_items_sold = 0
        for row in cur.fetchall():
            revenue = _money(row[2])
            qty_total = _money(row[1])
            total_items_sold += qty_total
            product_totals.append({
                'name': str(row[0] or '(Sin nombre)'),
                'qty': qty_total,
                'revenue': revenue,
            })

        top_products = [
            {
                **item,
                'share_percent': _percent(item['revenue'], current_net_sales),
            }
            for item in sorted(product_totals, key=lambda item: (-item['revenue'], -item['qty'], item['name']))[:10]
        ]
        top_products_by_qty = [
            {
                **item,
                'share_percent': _percent(item['qty'], total_items_sold),
            }
            for item in sorted(product_totals, key=lambda item: (-item['qty'], -item['revenue'], item['name']))[:10]
        ]

        current_from_local = _utc_naive_to_local(from_dt)
        current_to_local = _utc_naive_to_local(to_dt)
        prev_from_local = _utc_naive_to_local(prev_from_dt)
        prev_to_local = _utc_naive_to_local(prev_to_dt)
        top_hour = max(by_hour_list, key=lambda item: (item.get('total', 0), item.get('count', 0), -item.get('hour', 0))) if by_hour_list else None
        top_day = max(by_day_list, key=lambda item: (item.get('total', 0), item.get('count', 0), item.get('date', ''))) if by_day_list else None
        avg_items_per_order = _round_metric(total_items_sold / current_delivered_count, 1) if current_delivered_count else 0.0
        previous_avg_ticket = _money(prev_summary['delivered_total'] / prev_summary['delivered_count']) if prev_summary['delivered_count'] else 0
        extras_total = current_tip_total + current_shipping_total
        extras_share_percent = _percent(extras_total, current_net_sales)

        prev_canceled = prev_summary.get('canceled_count', 0) or 0
        prev_canceled_amount = prev_summary.get('canceled_amount', 0) or 0
        prev_items = prev_summary.get('items_sold_total', 0) or 0
        prev_avg_items = prev_summary.get('avg_items_per_order', 0.0) or 0.0
        prev_tips = prev_summary.get('tips_total', 0) or 0
        prev_shipping = prev_summary.get('shipping_total', 0) or 0
        prev_extras = prev_summary.get('extras_total', 0) or 0
        prev_extras_share = prev_summary.get('extras_share_percent', 0.0) or 0.0
        prev_cancel_rate = prev_summary.get('cancellation_rate', 0.0) or 0.0

        response = jsonify({
            'range': {
                'from': from_iso,
                'to': to_iso,
                'from_local': _format_dt_for_client(current_from_local),
                'to_local': _format_dt_for_client(current_to_local),
                'truncated_to_now': truncated_to_now,
            },
            'summary': {
                'net_sales': current_net_sales,
                'delivered_orders': current_delivered_count,
                'average_ticket': avg_ticket,
                'canceled_orders': canceled_count,
                'canceled_amount': canceled_total,
                'cancellation_rate': cancellation_rate,
                'tips_total': current_tip_total,
                'shipping_total': current_shipping_total,
                'extras_total': extras_total,
                'extras_share_percent': extras_share_percent,
                'items_sold_total': total_items_sold,
                'avg_items_per_order': avg_items_per_order,
            },
            'comparison': {
                'previous_from': prev_from_iso,
                'previous_to': prev_to_iso,
                'previous_from_local': _format_dt_for_client(prev_from_local),
                'previous_to_local': _format_dt_for_client(prev_to_local),
                'previous_net_sales': prev_summary['delivered_total'],
                'previous_delivered_orders': prev_summary['delivered_count'],
                'previous_canceled_orders': prev_canceled,
                'previous_canceled_amount': prev_canceled_amount,
                'previous_cancellation_rate': prev_cancel_rate,
                'previous_average_ticket': previous_avg_ticket,
                'previous_avg_ticket': previous_avg_ticket,
                'previous_items_sold_total': prev_items,
                'previous_avg_items_per_order': prev_avg_items,
                'previous_tips_total': prev_tips,
                'previous_shipping_total': prev_shipping,
                'previous_extras_total': prev_extras,
                'previous_extras_share_percent': prev_extras_share,
                'has_previous_sales_base': prev_summary['delivered_total'] > 0,
                'has_previous_orders_base': prev_summary['delivered_count'] > 0,
                'has_previous_ticket_base': previous_avg_ticket > 0,
                'has_previous_canceled_base': prev_canceled > 0,
                'has_previous_items_base': prev_items > 0,
                'has_previous_extras_base': prev_extras > 0,
                'delta_amount': current_net_sales - prev_summary['delivered_total'],
                'delta_percent': _delta_percent(current_net_sales, prev_summary['delivered_total']),
                'delta_orders': current_delivered_count - prev_summary['delivered_count'],
                'delta_orders_percent': _delta_percent(current_delivered_count, prev_summary['delivered_count']),
                'delta_avg_ticket': avg_ticket - previous_avg_ticket,
                'delta_avg_ticket_percent': _delta_percent(avg_ticket, previous_avg_ticket),
                'delta_canceled_orders': canceled_count - prev_canceled,
                'delta_canceled_percent': _delta_percent(canceled_count, prev_canceled),
                'delta_cancellation_rate': round(float(cancellation_rate or 0) - float(prev_cancel_rate or 0), 2),
                'delta_items_sold': total_items_sold - prev_items,
                'delta_items_sold_percent': _delta_percent(total_items_sold, prev_items),
                'delta_avg_items_per_order': round(float(avg_items_per_order or 0.0) - float(prev_avg_items or 0.0), 1),
                'delta_tips_total': current_tip_total - prev_tips,
                'delta_tips_percent': _delta_percent(current_tip_total, prev_tips),
                'delta_shipping_total': current_shipping_total - prev_shipping,
                'delta_shipping_percent': _delta_percent(current_shipping_total, prev_shipping),
                'delta_extras_total': extras_total - prev_extras,
                'delta_extras_percent': _delta_percent(extras_total, prev_extras),
                'delta_extras_share_percent': round(float(extras_share_percent or 0.0) - float(prev_extras_share or 0.0), 2),
            },
            'leaders': {
                'channel': by_channel_list[0]['label'] if by_channel_list else '',
                'payment_method': by_payment_list[0]['label'] if by_payment_list else '',
                'top_hour': top_hour['label'] if top_hour else '',
                'top_product': top_products[0]['name'] if top_products else '',
                'top_day': top_day['label'] if top_day else '',
            },
            'by_channel': by_channel_list,
            'by_payment_method': by_payment_list,
            'top_products': top_products,
            'top_products_by_qty': top_products_by_qty,
            'by_hour': by_hour_list,
            'by_day': by_day_list,
        })
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as exc:
        print(f"Error in sales_analytics: {exc}")
        return jsonify({'error': 'analytics unavailable'}), 500
