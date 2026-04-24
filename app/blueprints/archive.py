from flask import Blueprint, request, jsonify, session, Response
from app.database import get_db
from app.utils import is_authed, check_csrf
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
    return from_dt, to_dt, from_dt.isoformat(), to_dt.isoformat()

def _has_reports_access():
    role = str(session.get('admin_role') or '').strip().lower()
    if bool(session.get('admin_owner')) or role == 'admin':
        return True
    perms = _parse_perms_json(session.get('admin_perms') or '')
    return bool(perms.get('reports_view'))

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
    if value in ('efectivo', 'cash', 'contado', 'efvo'):
        return 'efectivo', 'Efectivo'
    if value in ('pos', 'pos/qr', 'qr', 'tarjeta', 'card', 'debito', 'credito', 'mercado pago', 'mercadopago'):
        return 'pos', 'POS/QR'
    if value in ('transferencia', 'transfer', 'trans', 'transferencia bancaria'):
        return 'transferencia', 'Transferencia'
    if value in ('mixed', 'mixto', 'pago mixto'):
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
        if previous_val == 0:
            return 100.0 if current_val > 0 else 0.0
        return round(((current_val - previous_val) / previous_val) * 100.0, 2)
    except Exception:
        return 0.0

@bp.route('/archive', methods=['GET'])
def get_archive():
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
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
    base = """
        SELECT o.id, o.created_at, o.order_type, o.table_number, o.address_json, o.total, o.status, o.customer_name, o.customer_phone, h.last_status, h.last_change
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
    if a_type:
        base += " AND a.type = ?"
        params.append(a_type)
    date_col = 'a.archived_at' if date_field == 'archived' else 'o.created_at'
    if from_date:
        base += f" AND {date_col} >= ?"
        params.append(from_date)
    if to_date:
        base += f" AND {date_col} <= ?"
        params.append(to_date)
    if order_type:
        base += " AND o.order_type = ?"
        params.append(order_type)
    if q:
        try:
            qid = int(q)
            base += " AND o.id = ?"
            params.append(qid)
        except Exception:
            nq = re.sub(r"^(destino|direccion|dir)\s*:\s*", "", str(q), flags=re.IGNORECASE).strip()
            like = f"%{nq.lower()}%"
            base += " AND (LOWER(COALESCE(o.address_json,'')) LIKE ? OR LOWER(COALESCE(o.table_number,'')) LIKE ? OR LOWER(COALESCE(o.customer_name,'')) LIKE ?)"
            params.extend([like, like, like])
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
    if a_type:
        count_sql += " AND a.type = ?"
        count_params.append(a_type)
    date_col = 'a.archived_at' if date_field == 'archived' else 'o.created_at'
    if from_date:
        count_sql += f" AND {date_col} >= ?"
        count_params.append(from_date)
    if to_date:
        count_sql += f" AND {date_col} <= ?"
        count_params.append(to_date)
    if order_type:
        count_sql += " AND o.order_type = ?"
        count_params.append(order_type)
    if q:
        try:
            qid = int(q)
            count_sql += " AND o.id = ?"
            count_params.append(qid)
        except Exception:
            nq = re.sub(r"^(destino|direccion|dir)\s*:\s*", "", str(q), flags=re.IGNORECASE).strip()
            like = f"%{nq.lower()}%"
            count_sql += " AND (LOWER(COALESCE(o.address_json,'')) LIKE ? OR LOWER(COALESCE(o.table_number,'')) LIKE ? OR LOWER(COALESCE(o.customer_name,'')) LIKE ?)"
            count_params.extend([like, like, like])
    cur.execute(count_sql, count_params)
    total_count = int(cur.fetchone()[0])
    data = [dict(r) for r in rows]
    if q:
        try:
            int(q)
        except Exception:
            def _norm(s):
                s = str(s or '').lower()
                return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
            nq = re.sub(r"^(destino|direccion|dir)\s*:\s*", "", str(q), flags=re.IGNORECASE).strip()
            nq = _norm(nq)
            data = [r for r in data if (nq in _norm(r.get('address_json')) or nq in _norm(r.get('table_number')) or nq in _norm(r.get('customer_name')))]
    return jsonify({'archives': data, 'count': len(data), 'limit': limit, 'offset': offset, 'total_count': total_count})

@bp.route('/archive/eligible_count', methods=['GET'])
def archive_eligible_count():
    a_type = request.args.get('type')
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or ''
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
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
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
    if a_type:
        base += " AND a.type = ?"
        params.append(a_type)
    date_col = 'a.archived_at' if date_field == 'archived' else 'o.created_at'
    if from_date:
        base += f" AND {date_col} >= ?"
        params.append(from_date)
    if to_date:
        base += f" AND {date_col} <= ?"
        params.append(to_date)
    if order_type:
        base += " AND o.order_type = ?"
        params.append(order_type)
    if q:
        try:
            qid = int(q)
            base += " AND o.id = ?"
            params.append(qid)
        except Exception:
            nq = re.sub(r"^(destino|direccion|dir)\s*:\s*", "", str(q), flags=re.IGNORECASE).strip()
            like = f"%{nq.lower()}%"
            base += " AND (LOWER(COALESCE(o.address_json,'')) LIKE ? OR LOWER(COALESCE(o.table_number,'')) LIKE ? OR LOWER(COALESCE(o.customer_name,'')) LIKE ?)"
            params.extend([like, like, like])
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
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
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
    date_col = 'a.archived_at' if date_field == 'archived' else 'o.created_at'
    base = f"""
        SELECT o.total
        FROM archived_orders a JOIN orders o ON o.id = a.order_id
        WHERE a.tenant_slug = ? AND a.type = ?
        {" AND " + date_col + " >= ?" if from_date else ''}
        {" AND " + date_col + " <= ?" if to_date else ''}
        {" AND o.order_type = ?" if order_type else ''}
    """
    # Delivered metrics
    params_del = [tenant_slug, 'delivered'] + ([from_date] if from_date else []) + ([to_date] if to_date else []) + ([order_type] if order_type else [])
    cur.execute(base, params_del)
    rows_del = cur.fetchall()
    delivered_count = len(rows_del)
    delivered_total = int(sum(int(r[0] or 0) for r in rows_del))
    tip = (delivered_total + 5) // 10
    delivered_total_with_tip = delivered_total + tip
    # Canceled metrics
    params_can = [tenant_slug, 'canceled'] + ([from_date] if from_date else []) + ([to_date] if to_date else []) + ([order_type] if order_type else [])
    cur.execute(base, params_can)
    rows_can = cur.fetchall()
    canceled_count = len(rows_can)
    canceled_total = int(sum(int(r[0] or 0) for r in rows_can))
    return jsonify({
        'delivered_count': delivered_count,
        'delivered_total': delivered_total,
        'delivered_tip_10': tip,
        'delivered_total_with_tip': delivered_total_with_tip,
        'canceled_count': canceled_count,
        'canceled_total': canceled_total
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
    cur.execute("SELECT id, tenant_slug, status FROM orders WHERE id = ?", (order_id,))
    r = cur.fetchone()
    if not r:
        return jsonify({'error': 'orden no encontrada'}), 404
    tenant_slug = r[1]
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
        AND id NOT IN (SELECT order_id FROM archived_orders)
    """, (tenant_slug,))
    
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
        base_join_del = (
            "SELECT COUNT(*) FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ? AND o.status = 'entregado'"
        )
        base_join_can = (
            "SELECT COUNT(*) FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'cancelado' GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ? AND o.status = 'cancelado'"
        )
        params_del = [tenant_slug]
        params_can = [tenant_slug]
        if from_date:
            base_join_del += " AND h.last_change >= ?"
            base_join_can += " AND h.last_change >= ?"
            params_del.append(from_date)
            params_can.append(from_date)
        if to_date:
            base_join_del += " AND h.last_change <= ?"
            base_join_can += " AND h.last_change <= ?"
            params_del.append(to_date)
            params_can.append(to_date)
        cur.execute(base_join_del, params_del)
        delivered_count = cur.fetchone()[0]
        cur.execute(base_join_can, params_can)
        canceled_count = cur.fetchone()[0]
        cur.execute(
            base_join_del.replace("SELECT COUNT(*)", "SELECT COALESCE(SUM(o.total),0)"),
            params_del
        )
        delivered_total = int(cur.fetchone()[0] or 0)
        tip = (delivered_total + 5) // 10
        delivered_total_with_tip = delivered_total + tip
        avg_prep = 0
        avg_listo = 0
        avg_entregado = 0
        try:
            where_exists = "EXISTS(SELECT 1 FROM order_status_history h WHERE h.order_id = o.id AND h.status = 'entregado'"
            p2 = [tenant_slug]
            if from_date:
                where_exists += " AND h.changed_at >= ?"
                p2.append(from_date)
            if to_date:
                where_exists += " AND h.changed_at <= ?"
                p2.append(to_date)
            where_exists += ")"
            cur.execute(
                f"""
                SELECT o.id, o.created_at,
                       (SELECT h.changed_at FROM order_status_history h WHERE h.order_id = o.id AND h.status = 'preparacion' ORDER BY h.id ASC LIMIT 1) AS prep_at,
                       (SELECT h.changed_at FROM order_status_history h WHERE h.order_id = o.id AND h.status = 'listo' ORDER BY h.id ASC LIMIT 1) AS listo_at,
                       (SELECT h.changed_at FROM order_status_history h WHERE h.order_id = o.id AND h.status = 'entregado' ORDER BY h.id ASC LIMIT 1) AS entregado_at
                FROM orders o
                WHERE o.tenant_slug = ? AND {where_exists}
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
                        # Normalize to naive UTC
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

        from_dt, to_dt, from_iso, to_iso = _resolve_sales_range(
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
                SELECT COALESCE(COUNT(*), 0) AS delivered_count, COALESCE(SUM(o.total), 0) AS delivered_total
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
            delivered_count = _money(delivered_row[0] if delivered_row else 0)
            delivered_total = _money(delivered_row[1] if delivered_row else 0)
            canceled_count_range = _money(canceled_range_row[0] if canceled_range_row else 0)
            return {
                'delivered_count': delivered_count,
                'delivered_total': delivered_total,
                'canceled_count': canceled_count_range,
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
            ORDER BY revenue_total DESC, qty_total DESC, product_name ASC
            LIMIT 10
            """,
            (tenant_slug, from_iso, to_iso),
        )
        top_products = []
        top_products_by_qty = []
        total_items_sold = 0
        for row in cur.fetchall():
            revenue = _money(row[2])
            qty_total = _money(row[1])
            total_items_sold += qty_total
            top_products.append({
                'name': str(row[0] or '(Sin nombre)'),
                'qty': qty_total,
                'revenue': revenue,
                'share_percent': _percent(revenue, current_net_sales),
            })
            top_products_by_qty.append({
                'name': str(row[0] or '(Sin nombre)'),
                'qty': qty_total,
                'revenue': revenue,
                'share_percent': 0.0,
            })
        top_products_by_qty = sorted(top_products_by_qty, key=lambda item: (-item['qty'], -item['revenue'], item['name']))[:10]
        for item in top_products_by_qty:
            item['share_percent'] = _percent(item['qty'], total_items_sold)

        current_from_local = _utc_naive_to_local(from_dt)
        current_to_local = _utc_naive_to_local(to_dt)
        prev_from_local = _utc_naive_to_local(prev_from_dt)
        prev_to_local = _utc_naive_to_local(prev_to_dt)
        top_hour = max(by_hour_list, key=lambda item: (item.get('total', 0), item.get('count', 0), -item.get('hour', 0))) if by_hour_list else None
        top_day = max(by_day_list, key=lambda item: (item.get('total', 0), item.get('count', 0), item.get('date', ''))) if by_day_list else None
        avg_items_per_order = _money(total_items_sold / current_delivered_count) if current_delivered_count else 0
        previous_avg_ticket = _money(prev_summary['delivered_total'] / prev_summary['delivered_count']) if prev_summary['delivered_count'] else 0

        response = jsonify({
            'range': {
                'from': from_iso,
                'to': to_iso,
                'from_local': _format_dt_for_client(current_from_local),
                'to_local': _format_dt_for_client(current_to_local),
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
                'previous_canceled_orders': prev_summary['canceled_count'],
                'previous_average_ticket': previous_avg_ticket,
                'delta_amount': current_net_sales - prev_summary['delivered_total'],
                'delta_percent': _delta_percent(current_net_sales, prev_summary['delivered_total']),
                'delta_orders': current_delivered_count - prev_summary['delivered_count'],
                'delta_orders_percent': _delta_percent(current_delivered_count, prev_summary['delivered_count']),
                'delta_avg_ticket': avg_ticket - previous_avg_ticket,
                'delta_avg_ticket_percent': _delta_percent(avg_ticket, previous_avg_ticket),
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


