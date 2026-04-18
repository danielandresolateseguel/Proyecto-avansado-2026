import json
import csv
import io
from datetime import datetime
from flask import Blueprint, request, jsonify, session, make_response
from app.database import get_db
from app.utils import is_authed, check_csrf

bp = Blueprint('cash', __name__, url_prefix='/api/cash')

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

def _ctx():
    role = str(session.get('admin_role') or '').strip().lower()
    actor = str(session.get('admin_user') or '').strip()
    perms = _parse_perms_json(session.get('admin_perms') or '')
    tenant = str(session.get('tenant_slug') or '').strip()
    owner = bool(session.get('admin_owner'))
    return tenant, actor, role, perms, owner

def _scope_for(role, owner=False):
    if owner or role == 'admin':
        return 'tenant'
    if role in ('mozo', 'caja', 'repartidor'):
        return 'user'
    return 'tenant'

def _has_perm(perms, owner, role, key):
    if owner or role == 'admin':
        return True
    return bool(perms.get(key))

def _enforce_tenant(tenant_slug, session_tenant):
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return False
    return True

def _session_where(tenant_slug, scope, actor=None):
    q = "tenant_slug = ? AND scope = ?"
    params = [tenant_slug, scope]
    if scope == 'user':
        q += " AND lower(opened_by) = lower(?)"
        params.append(actor or '')
    return q, params

_BREAKDOWN_KEYS = ('efectivo', 'pos', 'transferencia', 'otros')

def _safe_int(value, default=0):
    try:
        return int(value or 0)
    except Exception:
        return int(default or 0)

def _empty_breakdown():
    return {key: 0 for key in _BREAKDOWN_KEYS}

def _breakdown_total(breakdown):
    return sum(max(0, _safe_int((breakdown or {}).get(key))) for key in _BREAKDOWN_KEYS)

def _align_breakdown_to_amount(breakdown, target_amount):
    aligned = {key: max(0, _safe_int((breakdown or {}).get(key))) for key in _BREAKDOWN_KEYS}
    target = max(0, _safe_int(target_amount))
    delta = target - _breakdown_total(aligned)
    if delta == 0:
        return aligned
    if delta > 0:
        aligned['efectivo'] += delta
        return aligned
    remaining = abs(delta)
    for key in ('efectivo', 'otros', 'transferencia', 'pos'):
        if remaining <= 0:
            break
        current = aligned[key]
        take = min(current, remaining)
        aligned[key] = current - take
        remaining -= take
    return aligned

def _normalize_declared_breakdown(raw_breakdown, fallback_amount=0, prefer_fallback=False):
    normalized = _empty_breakdown()
    source = raw_breakdown if isinstance(raw_breakdown, dict) else {}
    aliases = {
        'efectivo': ('efectivo', 'cash'),
        'pos': ('pos', 'qr', 'tarjeta'),
        'transferencia': ('transferencia', 'trans'),
        'otros': ('otros', 'otro'),
    }
    for canonical, keys in aliases.items():
        for key in keys:
            if key in source:
                normalized[canonical] = max(0, _safe_int(source.get(key)))
                break
    fallback = max(0, _safe_int(fallback_amount))
    total = _breakdown_total(normalized)
    if total == 0 and fallback > 0:
        normalized['efectivo'] = fallback
        return normalized
    if prefer_fallback and fallback >= 0 and total != fallback:
        return _align_breakdown_to_amount(normalized, fallback)
    return normalized

def _aggregate_sales_by_payments(cur, tenant_slug, actor, start_at, end_at):
    base_total = 0
    tip_total = 0
    shipping_total = 0
    order_ids = set()
    cur.execute(
        "SELECT e.order_id, e.payload_json, COALESCE(o.shipping_cost, 0) "
        "FROM order_events e JOIN orders o ON e.order_id = o.id "
        "WHERE o.tenant_slug = ? AND e.event_type = 'payment' AND lower(e.actor) = lower(?) AND e.created_at >= ? AND e.created_at <= ? "
        "ORDER BY e.id ASC",
        (tenant_slug, actor or '', start_at, end_at),
    )
    for r in cur.fetchall() or []:
        try:
            oid = int(r[0])
        except Exception:
            continue
        payload_json = r[1] or ''
        try:
            meta = json.loads(payload_json) if payload_json else {}
        except Exception:
            meta = {}
        try:
            base_total += int(meta.get('amount') or 0)
        except Exception:
            pass
        try:
            tip_total += int(meta.get('tip') or 0)
        except Exception:
            pass
        try:
            shipping_total += int(r[2] or 0)
        except Exception:
            pass
        order_ids.add(oid)
    return {
        'delivered_count': len(order_ids),
        'base_delivered_total': int(base_total),
        'tip_total': int(tip_total),
        'shipping_total': int(shipping_total),
        'delivered_total': int(base_total) + int(tip_total),
    }

@bp.route('/session', methods=['GET'])
def cash_session_get():
    if not is_authed():
        return jsonify({'active': False})
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not (_has_perm(perms, owner, role, 'cash_view') or _has_perm(perms, owner, role, 'cash_manage')):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    conn = get_db()
    cur = conn.cursor()
    where, params = _session_where(tenant_slug, scope, actor=actor)
    cur.execute(
        "SELECT id, tenant_slug, scope, opened_at, opened_by, opening_amount, notes_open, closed_at, closed_by, closing_amount, notes_close "
        f"FROM cash_sessions WHERE {where} AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1",
        params,
    )
    row = cur.fetchone()
    if not row:
        return jsonify({'active': False})
    sess = dict(row)
    opened_at = sess['opened_at']
    now = datetime.utcnow().isoformat()
    if scope == 'user':
        sales = _aggregate_sales_by_payments(cur, tenant_slug, actor, opened_at, now)
        base_delivered_total = int(sales['base_delivered_total'])
        tip_total = int(sales['tip_total'])
        delivered_total = int(sales['delivered_total'])
        delivered_count = int(sales['delivered_count'])
        shipping_total = int(sales['shipping_total'])
    else:
        cur.execute(
            "SELECT COALESCE(SUM(o.total),0), COALESCE(SUM(COALESCE(o.tip_amount, 0)),0), COALESCE(COUNT(*),0), COALESCE(SUM(COALESCE(o.shipping_cost, 0)),0) FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ?",
            (tenant_slug, opened_at)
        )
        agg = cur.fetchone() or (0, 0, 0, 0)
        base_delivered_total = int(agg[0] or 0)
        tip_total = int(agg[1] or 0)
        delivered_total = base_delivered_total + tip_total
        delivered_count = int(agg[2] or 0)
        shipping_total = int(agg[3] or 0)
    
    cur.execute("SELECT type, payment_method, SUM(amount), COUNT(*) FROM cash_movements WHERE session_id = ? GROUP BY type, payment_method", (sess['id'],))
    rows_mov = cur.fetchall()
    
    breakdown = {
        'efectivo': int(sess['opening_amount']), 
        'pos': 0, 
        'transferencia': 0, 
        'otros': 0
    }
    breakdown_counts = {
        'efectivo': 0, 
        'pos': 0, 
        'transferencia': 0, 
        'otros': 0
    }
    entradas = 0
    salidas = 0
    
    for r in rows_mov:
        mtype = r[0] 
        pm = (r[1] or '').strip().lower() 
        amt = int(r[2] or 0)
        cnt = int(r[3] or 0)
        
        if mtype == 'entrada':
            entradas += amt
            if 'pos' in pm or 'qr' in pm or 'tarjeta' in pm:
                breakdown['pos'] += amt
                breakdown_counts['pos'] += cnt
            elif 'transferencia' in pm:
                breakdown['transferencia'] += amt
                breakdown_counts['transferencia'] += cnt
            elif 'otros' in pm:
                breakdown['otros'] += amt
                breakdown_counts['otros'] += cnt
            else:
                breakdown['efectivo'] += amt
                breakdown_counts['efectivo'] += cnt
        elif mtype == 'salida':
            salidas += amt
            if 'pos' in pm or 'qr' in pm or 'tarjeta' in pm:
                breakdown['pos'] -= amt
                breakdown_counts['pos'] += cnt
            elif 'transferencia' in pm:
                breakdown['transferencia'] -= amt
                breakdown_counts['transferencia'] += cnt
            elif 'otros' in pm:
                breakdown['otros'] -= amt
                breakdown_counts['otros'] += cnt
            else:
                breakdown['efectivo'] -= amt
                breakdown_counts['efectivo'] += cnt

    theoretical_cash = int(sess['opening_amount']) + entradas - salidas
    return jsonify({
        'active': True, 
        'session': sess, 
        'summary': {
            'delivered_count': delivered_count, 
            'delivered_total': delivered_total, 
            'base_delivered_total': base_delivered_total, 
            'tip_total': tip_total, 
            'shipping_total': shipping_total, 
            'entradas': entradas, 
            'salidas': salidas, 
            'theoretical_cash': theoretical_cash,
            'theoretical_breakdown': breakdown,
            'theoretical_breakdown_counts': breakdown_counts
        }
    })

@bp.route('/open', methods=['POST'])
def cash_open():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    tenant_slug = (payload.get('tenant_slug') or request.args.get('tenant_slug') or 'gastronomia-local1')
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_perm(perms, owner, role, 'cash_manage'):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    opening_amount = int(payload.get('opening_amount') or 0)
    if opening_amount < 0:
        return jsonify({'error': 'monto inicial inválido'}), 400
    notes_open = (payload.get('notes') or '').strip()
    now = datetime.utcnow().isoformat()
    conn = get_db()
    cur = conn.cursor()
    where, params = _session_where(tenant_slug, scope, actor=actor)
    cur.execute(f"SELECT id FROM cash_sessions WHERE {where} AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1", params)
    if cur.fetchone():
        return jsonify({'error': 'ya existe una sesión de caja abierta'}), 400
    cur.execute(
        "INSERT INTO cash_sessions (tenant_slug, scope, opened_at, opened_by, opening_amount, notes_open) VALUES (?, ?, ?, ?, ?, ?)",
        (tenant_slug, scope, now, actor, opening_amount, notes_open),
    )
    conn.commit()
    sid = cur.lastrowid
    return jsonify({'session_id': sid, 'tenant_slug': tenant_slug, 'scope': scope, 'opened_at': now, 'opening_amount': opening_amount})

@bp.route('/close', methods=['POST'])
def cash_close():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    tenant_slug = (payload.get('tenant_slug') or request.args.get('tenant_slug') or 'gastronomia-local1')
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_perm(perms, owner, role, 'cash_manage'):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    closing_amount = int(payload.get('closing_amount') or 0)
    if closing_amount < 0:
        return jsonify({'error': 'monto de cierre inválido'}), 400
    notes_close = (payload.get('notes') or '').strip()
    now = datetime.utcnow().isoformat()
    conn = get_db()
    cur = conn.cursor()
    where, params = _session_where(tenant_slug, scope, actor=actor)
    cur.execute(
        "SELECT id, opened_at, opening_amount FROM cash_sessions "
        f"WHERE {where} AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1",
        params,
    )
    row = cur.fetchone()
    if not row:
        return jsonify({'error': 'no hay sesión de caja abierta'}), 400
    sid = int(row[0])
    opened_at = str(row[1])
    opening_amount = int(row[2] or 0)
    
    if scope == 'user':
        sales = _aggregate_sales_by_payments(cur, tenant_slug, actor, opened_at, now)
        base_delivered_total = int(sales['base_delivered_total'])
        tip_total = int(sales['tip_total'])
        shipping_total = int(sales['shipping_total'])
        delivered_total = int(sales['delivered_total'])
    else:
        cur.execute(
            "SELECT COALESCE(SUM(o.total),0), COALESCE(SUM(COALESCE(o.tip_amount, 0)),0), COALESCE(SUM(COALESCE(o.shipping_cost, 0)),0) FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ?",
            (tenant_slug, opened_at, now)
        )
        row_totals = cur.fetchone() or (0, 0, 0)
        base_delivered_total = int(row_totals[0] or 0)
        tip_total = int(row_totals[1] or 0)
        shipping_total = int(row_totals[2] or 0)
        delivered_total = base_delivered_total + tip_total
    
    cur.execute("SELECT type, payment_method, SUM(amount), COUNT(*) FROM cash_movements WHERE session_id = ? GROUP BY type, payment_method", (sid,))
    rows_mov = cur.fetchall()
    
    breakdown = {'efectivo': opening_amount, 'pos': 0, 'transferencia': 0, 'otros': 0}
    breakdown_counts = {'efectivo': 0, 'pos': 0, 'transferencia': 0, 'otros': 0}
    entradas = 0
    salidas = 0
    
    for r in rows_mov:
        mtype = r[0]
        pm = (r[1] or '').strip().lower()
        amt = int(r[2] or 0)
        cnt = int(r[3] or 0)
        
        if mtype == 'entrada':
            entradas += amt
            if 'pos' in pm or 'qr' in pm or 'tarjeta' in pm:
                breakdown['pos'] += amt
                breakdown_counts['pos'] += cnt
            elif 'transferencia' in pm:
                breakdown['transferencia'] += amt
                breakdown_counts['transferencia'] += cnt
            elif 'otros' in pm:
                breakdown['otros'] += amt
                breakdown_counts['otros'] += cnt
            else:
                breakdown['efectivo'] += amt
                breakdown_counts['efectivo'] += cnt
        elif mtype == 'salida':
            salidas += amt
            if 'pos' in pm or 'qr' in pm or 'tarjeta' in pm:
                breakdown['pos'] -= amt
                breakdown_counts['pos'] += cnt
            elif 'transferencia' in pm:
                breakdown['transferencia'] -= amt
                breakdown_counts['transferencia'] += cnt
            elif 'otros' in pm:
                breakdown['otros'] -= amt
                breakdown_counts['otros'] += cnt
            else:
                breakdown['efectivo'] -= amt
                breakdown_counts['efectivo'] += cnt

    theoretical_cash = opening_amount + entradas - salidas
    declared_breakdown = _normalize_declared_breakdown(payload.get('breakdown') or {}, fallback_amount=closing_amount)
    closing_amount = _breakdown_total(declared_breakdown)
    closing_diff = closing_amount - theoretical_cash
    closing_metadata = json.dumps({'declared_breakdown': declared_breakdown})
    
    cur.execute("UPDATE cash_sessions SET closed_at = ?, closed_by = ?, closing_amount = ?, notes_close = ?, closing_diff = ?, closing_metadata = ? WHERE id = ?", (now, actor, closing_amount, notes_close, closing_diff, closing_metadata, sid))
    conn.commit()
    
    return jsonify({
        'session_id': sid, 
        'tenant_slug': tenant_slug, 
        'opened_at': opened_at, 
        'closed_at': now, 
        'closing_amount': closing_amount, 
        'summary': {
            'opening_amount': opening_amount, 
            'entradas': entradas, 
            'salidas': salidas, 
            'delivered_total': delivered_total, 
            'base_delivered_total': base_delivered_total, 
            'tip_total': tip_total, 
            'shipping_total': shipping_total, 
            'theoretical_cash': theoretical_cash, 
            'closing_diff': closing_diff,
            'theoretical_breakdown': breakdown,
            'theoretical_breakdown_counts': breakdown_counts,
            'declared_breakdown': declared_breakdown
        }
    })

@bp.route('/movements', methods=['GET'])
def cash_movements_list():
    if not is_authed():
        return jsonify({'tenant_slug': (request.args.get('tenant_slug') or request.args.get('slug') or ''), 'session_id': 0, 'movements': []})
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not (_has_perm(perms, owner, role, 'cash_view') or _has_perm(perms, owner, role, 'cash_manage')):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    session_id = request.args.get('session_id')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    try:
        sid = int(session_id or 0)
    except:
        sid = 0
    conn = get_db()
    cur = conn.cursor()
    movements = []
    if sid > 0:
        cur.execute("SELECT scope, opened_by FROM cash_sessions WHERE id = ? AND tenant_slug = ?", (sid, tenant_slug))
        srow = cur.fetchone()
        if not srow:
            return jsonify({'error': 'sesión no encontrada'}), 404
        sess_scope = str(srow[0] or 'tenant')
        sess_opened_by = str(srow[1] or '')
        if scope == 'user' and (sess_scope != 'user' or sess_opened_by.lower() != (actor or '').lower()):
            return jsonify({'error': 'acceso denegado a la sesión'}), 403
        if scope == 'tenant' and sess_scope != 'tenant':
            return jsonify({'error': 'acceso denegado a la sesión'}), 403
        cur.execute("SELECT id, session_id, type, amount, note, actor, created_at FROM cash_movements WHERE session_id = ? ORDER BY id ASC", (sid,))
        movements = [dict(r) for r in cur.fetchall()]
    elif from_date or to_date:
        q = (
            "SELECT m.id, m.session_id, m.type, m.amount, m.note, m.actor, m.created_at "
            "FROM cash_movements m JOIN cash_sessions s ON m.session_id = s.id "
            "WHERE s.tenant_slug = ? AND s.scope = ?"
        )
        params = [tenant_slug, scope]
        if scope == 'user':
            q += " AND lower(s.opened_by) = lower(?)"
            params.append(actor or '')
        if from_date:
            q += " AND m.created_at >= ?"
            params.append(from_date)
        if to_date:
            q += " AND m.created_at <= ?"
            params.append(to_date)
        q += " ORDER BY m.id ASC"
        cur.execute(q, params)
        movements = [dict(r) for r in cur.fetchall()]
    return jsonify({'tenant_slug': tenant_slug, 'session_id': sid, 'movements': movements})

@bp.route('/movement', methods=['POST'])
def cash_movement():
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    tenant_slug = (payload.get('tenant_slug') or request.args.get('tenant_slug') or 'gastronomia-local1')
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_perm(perms, owner, role, 'cash_manage'):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    mtype = (payload.get('type') or '').strip().lower()
    amount = int(payload.get('amount') or 0)
    note = (payload.get('note') or '').strip()
    if mtype not in ('entrada','salida'): return jsonify({'error': 'tipo inválido'}), 400
    if amount <= 0: return jsonify({'error': 'monto inválido'}), 400
    now = datetime.utcnow().isoformat()
    conn = get_db()
    cur = conn.cursor()
    where, params = _session_where(tenant_slug, scope, actor=actor)
    cur.execute(f"SELECT id FROM cash_sessions WHERE {where} AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1", params)
    row = cur.fetchone()
    if not row: return jsonify({'error': 'no hay sesión de caja abierta'}), 400
    sid = int(row[0])
    cur.execute("INSERT INTO cash_movements (session_id, type, amount, note, actor, created_at, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?)", (sid, mtype, amount, note, actor, now, (payload.get('payment_method') or '').strip()))
    conn.commit()
    return jsonify({'session_id': sid, 'type': mtype, 'amount': amount, 'note': note, 'created_at': now})

@bp.route('/session/orders', methods=['GET'])
def cash_session_orders():
    if not is_authed():
        return jsonify({'orders': []})
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not (_has_perm(perms, owner, role, 'cash_view') or _has_perm(perms, owner, role, 'cash_manage')):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    session_id = request.args.get('session_id')
    to_date = request.args.get('to')
    try:
        sid = int(session_id or 0)
    except:
        sid = 0
    conn = get_db()
    cur = conn.cursor()
    opened_at = None
    closed_at = None
    sess_scope = None
    sess_opened_by = None
    if sid > 0:
        cur.execute("SELECT opened_at, closed_at, scope, opened_by FROM cash_sessions WHERE id = ? AND tenant_slug = ?", (sid, tenant_slug))
        row = cur.fetchone()
        if row:
            opened_at = str(row[0] or '')
            closed_at = str(row[1] or '')
            sess_scope = str(row[2] or 'tenant')
            sess_opened_by = str(row[3] or '')
    else:
        where, params = _session_where(tenant_slug, scope, actor=actor)
        cur.execute(
            "SELECT id, opened_at, closed_at, scope, opened_by FROM cash_sessions "
            f"WHERE {where} AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1",
            params,
        )
        row = cur.fetchone()
        if row:
            sid = int(row[0])
            opened_at = str(row[1] or '')
            closed_at = str(row[2] or '')
            sess_scope = str(row[3] or scope)
            sess_opened_by = str(row[4] or '')
            
    if not opened_at:
        return jsonify({'orders': []})
    if scope == 'user':
        if sess_scope != 'user' or (sess_opened_by or '').lower() != (actor or '').lower():
            return jsonify({'error': 'acceso denegado a la sesión'}), 403
    if scope == 'tenant' and sess_scope != 'tenant':
        return jsonify({'error': 'acceso denegado a la sesión'}), 403
        
    end_at = to_date if to_date else (closed_at or datetime.utcnow().isoformat())
    if scope == 'user':
        cur.execute(
            "SELECT o.id, o.created_at, e.payload_json "
            "FROM order_events e JOIN orders o ON e.order_id = o.id "
            "WHERE o.tenant_slug = ? AND e.event_type = 'payment' AND lower(e.actor) = lower(?) AND e.created_at >= ? AND e.created_at <= ? "
            "ORDER BY o.id DESC",
            (tenant_slug, actor or '', opened_at, end_at),
        )
        out = []
        for r in cur.fetchall() or []:
            try:
                oid = int(r[0])
            except Exception:
                continue
            created_at = r[1]
            payload_json = r[2] or ''
            try:
                meta = json.loads(payload_json) if payload_json else {}
            except Exception:
                meta = {}
            method = meta.get('method') or ''
            try:
                amt = int(meta.get('amount') or 0) + int(meta.get('tip') or 0)
            except Exception:
                amt = 0
            out.append({'id': oid, 'created_at': created_at, 'total': int(amt), 'payment_method': method})
        return jsonify({'orders': out, 'session_id': sid, 'from': opened_at, 'to': end_at})
    else:
        base = (
            "SELECT o.id, o.created_at, o.total, o.payment_method FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ? "
            "ORDER BY o.id DESC"
        )
        cur.execute(base, (tenant_slug, opened_at, end_at))
        rows = cur.fetchall()
        return jsonify({'orders': [ {'id': int(r[0]), 'created_at': r[1], 'total': int(r[2] or 0), 'payment_method': r[3] } for r in rows ], 'session_id': sid, 'from': opened_at, 'to': end_at})

@bp.route('/sessions', methods=['GET'])
def cash_sessions_list():
    if not is_authed():
        return jsonify({'sessions': [], 'limit': int(request.args.get('limit') or 50), 'offset': int(request.args.get('offset') or 0), 'count': 0})
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not (_has_perm(perms, owner, role, 'cash_view') or _has_perm(perms, owner, role, 'cash_manage')):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    limit = int(request.args.get('limit') or 50)
    offset = int(request.args.get('offset') or 0)
    date_field = (request.args.get('date_field') or 'closed').strip().lower()
    if date_field not in ('closed', 'opened'): date_field = 'closed'
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    
    def _norm_date(s, end=False):
        try:
            if s and len(s) == 10: return s + ('T23:59:59' if end else 'T00:00:00')
        except: pass
        return s
        
    from_date = _norm_date(from_date, end=False)
    to_date = _norm_date(to_date, end=True)
    
    conn = get_db()
    cur = conn.cursor()
    base = "SELECT id, tenant_slug, scope, opened_at, opened_by, opening_amount, notes_open, closed_at, closed_by, closing_amount, notes_close, closing_diff, closing_metadata FROM cash_sessions WHERE tenant_slug = ? AND scope = ? AND closed_at IS NOT NULL"
    params = [tenant_slug, scope]
    if scope == 'user':
        base += " AND lower(opened_by) = lower(?)"
        params.append(actor or '')
    col = 'closed_at' if date_field == 'closed' else 'opened_at'
    if from_date:
        base += f" AND {col} >= ?"
        params.append(from_date)
    if to_date:
        base += f" AND {col} <= ?"
        params.append(to_date)
    base += " ORDER BY closed_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    cur.execute(base, params)
    rows = cur.fetchall()
    sessions = []
    
    for r in rows:
        s = dict(r)
        sid = int(s['id'])
        opened_at = s.get('opened_at')
        closed_at = s.get('closed_at')
        sess_scope = str(s.get('scope') or scope)
        sess_opened_by = str(s.get('opened_by') or '')
        if sess_scope == 'user':
            sales = _aggregate_sales_by_payments(cur, tenant_slug, sess_opened_by, opened_at, closed_at)
            base_delivered_total = int(sales['base_delivered_total'])
            tip_total = int(sales['tip_total'])
            delivered_total = int(sales['delivered_total'])
            delivered_count = int(sales['delivered_count'])
            shipping_total = int(sales['shipping_total'])
        else:
            cur.execute(
                "SELECT COALESCE(SUM(o.total),0), COALESCE(SUM(COALESCE(o.tip_amount, 0)),0), COALESCE(COUNT(*),0), COALESCE(SUM(COALESCE(o.shipping_cost, 0)),0) FROM orders o "
                "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
                "WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ?",
                (tenant_slug, opened_at, closed_at)
            )
            trow = cur.fetchone() or (0,0,0,0)
            base_delivered_total = int(trow[0] or 0)
            tip_total = int(trow[1] or 0)
            delivered_total = base_delivered_total + tip_total
            delivered_count = int(trow[2] or 0)
            shipping_total = int(trow[3] or 0)
        
        cur.execute("SELECT COALESCE(SUM(CASE WHEN type='entrada' THEN amount ELSE 0 END),0), COALESCE(SUM(CASE WHEN type='salida' THEN amount ELSE 0 END),0) FROM cash_movements WHERE session_id = ?", (sid,))
        mrow = cur.fetchone() or (0,0)
        entradas = int(mrow[0] or 0)
        salidas = int(mrow[1] or 0)
        theoretical_cash = int(s.get('opening_amount') or 0) + entradas - salidas
        
        cur.execute("SELECT type, payment_method, SUM(amount), COUNT(*) FROM cash_movements WHERE session_id = ? GROUP BY type, payment_method", (sid,))
        rows_mov = cur.fetchall()
        breakdown = {'efectivo': int(s.get('opening_amount') or 0), 'pos': 0, 'transferencia': 0, 'otros': 0}
        breakdown_counts = {'efectivo': 0, 'pos': 0, 'transferencia': 0, 'otros': 0}
        for rm in rows_mov:
            mtype = rm[0]
            pm = (rm[1] or '').strip().lower()
            amt = int(rm[2] or 0)
            cnt = int(rm[3] or 0)
            if mtype == 'entrada':
                if 'pos' in pm or 'qr' in pm or 'tarjeta' in pm:
                    breakdown['pos'] += amt
                    breakdown_counts['pos'] += cnt
                elif 'transferencia' in pm:
                    breakdown['transferencia'] += amt
                    breakdown_counts['transferencia'] += cnt
                elif 'otros' in pm:
                    breakdown['otros'] += amt
                    breakdown_counts['otros'] += cnt
                else:
                    breakdown['efectivo'] += amt
                    breakdown_counts['efectivo'] += cnt
            elif mtype == 'salida':
                if 'pos' in pm or 'qr' in pm or 'tarjeta' in pm:
                    breakdown['pos'] -= amt
                    breakdown_counts['pos'] += cnt
                elif 'transferencia' in pm:
                    breakdown['transferencia'] -= amt
                    breakdown_counts['transferencia'] += cnt
                elif 'otros' in pm:
                    breakdown['otros'] -= amt
                    breakdown_counts['otros'] += cnt
                else:
                    breakdown['efectivo'] -= amt
                    breakdown_counts['efectivo'] += cnt

        declared_breakdown = _normalize_declared_breakdown({}, fallback_amount=int(s.get('closing_amount') or 0))
        try:
            meta = s.get('closing_metadata')
            if meta:
                meta_obj = json.loads(meta)
                declared_breakdown = _normalize_declared_breakdown(
                    meta_obj.get('declared_breakdown') or {},
                    fallback_amount=int(s.get('closing_amount') or 0),
                    prefer_fallback=True,
                )
        except:
            pass

        s['summary'] = {
            'delivered_total': delivered_total,
            'delivered_count': delivered_count,
            'entradas': entradas,
            'salidas': salidas,
            'theoretical_cash': theoretical_cash,
            'theoretical_breakdown': breakdown,
            'theoretical_breakdown_counts': breakdown_counts,
            'declared_breakdown': declared_breakdown,
            'base_delivered_total': base_delivered_total,
            'tip_total': tip_total,
            'shipping_total': shipping_total,
            'closing_diff': int(s.get('closing_diff') or 0)
        }
        sessions.append(s)
        
    return jsonify({'sessions': sessions, 'limit': limit, 'offset': offset, 'count': len(sessions)})

@bp.route('/sessions/export.csv', methods=['GET'])
def cash_sessions_export_csv():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not (_has_perm(perms, owner, role, 'cash_view') or _has_perm(perms, owner, role, 'cash_manage')):
        return jsonify({'error': 'sin permisos'}), 403
    scope = _scope_for(role, owner=owner)
    date_field = (request.args.get('date_field') or 'closed').strip().lower()
    if date_field not in ('closed', 'opened'): date_field = 'closed'
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    
    def _norm_date(s, end=False):
        try:
            if s and len(s) == 10: return s + ('T23:59:59' if end else 'T00:00:00')
        except: pass
        return s
        
    from_date = _norm_date(from_date, end=False)
    to_date = _norm_date(to_date, end=True)
    
    conn = get_db()
    cur = conn.cursor()
    base = "SELECT id, tenant_slug, scope, opened_at, opened_by, opening_amount, notes_open, closed_at, closed_by, closing_amount, notes_close, closing_diff FROM cash_sessions WHERE tenant_slug = ? AND scope = ? AND closed_at IS NOT NULL"
    params = [tenant_slug, scope]
    if scope == 'user':
        base += " AND lower(opened_by) = lower(?)"
        params.append(actor or '')
    col = 'closed_at' if date_field == 'closed' else 'opened_at'
    if from_date:
        base += f" AND {col} >= ?"
        params.append(from_date)
    if to_date:
        base += f" AND {col} <= ?"
        params.append(to_date)
    base += " ORDER BY closed_at DESC"
    cur.execute(base, params)
    rows = cur.fetchall()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['tenant_slug', 'opened_at', 'opened_by', 'opening_amount', 'notes_open', 'closed_at', 'closed_by', 'closing_amount', 'notes_close', 'base_delivered_total', 'tip_total', 'shipping_total', 'delivered_total', 'entradas', 'salidas', 'theoretical_cash', 'closing_diff'])
    
    for r in rows:
        s = dict(r)
        sid = int(s['id'])
        opened_at = s.get('opened_at')
        closed_at = s.get('closed_at')
        sess_scope = str(s.get('scope') or scope)
        sess_opened_by = str(s.get('opened_by') or '')
        if sess_scope == 'user':
            sales = _aggregate_sales_by_payments(cur, tenant_slug, sess_opened_by, opened_at, closed_at)
            base_delivered_total = int(sales['base_delivered_total'])
            tip_total = int(sales['tip_total'])
            shipping_total = int(sales['shipping_total'])
            delivered_total = int(sales['delivered_total'])
        else:
            cur.execute(
                "SELECT COALESCE(SUM(o.total),0), COALESCE(SUM(COALESCE(o.tip_amount, 0)),0), COALESCE(SUM(COALESCE(o.shipping_cost, 0)),0) FROM orders o "
                "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
                "WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ? AND h.last_change <= ?",
                (tenant_slug, opened_at, closed_at)
            )
            trow = cur.fetchone() or (0,0,0)
            base_delivered_total = int(trow[0] or 0)
            tip_total = int(trow[1] or 0)
            shipping_total = int(trow[2] or 0)
            delivered_total = base_delivered_total + tip_total
        
        cur.execute("SELECT COALESCE(SUM(CASE WHEN type='entrada' THEN amount ELSE 0 END),0), COALESCE(SUM(CASE WHEN type='salida' THEN amount ELSE 0 END),0) FROM cash_movements WHERE session_id = ?", (sid,))
        mrow = cur.fetchone() or (0,0)
        entradas = int(mrow[0] or 0)
        salidas = int(mrow[1] or 0)
        theoretical_cash = int(s.get('opening_amount') or 0) + entradas - salidas
        
        writer.writerow([
            s.get('tenant_slug'), s.get('opened_at'), s.get('opened_by'), int(s.get('opening_amount') or 0), s.get('notes_open'), s.get('closed_at'), s.get('closed_by'), int(s.get('closing_amount') or 0), s.get('notes_close'), base_delivered_total, tip_total, shipping_total, delivered_total, entradas, salidas, theoretical_cash, int(s.get('closing_diff') or 0)
        ])
        
    resp = make_response(output.getvalue())
    resp.headers['Content-Type'] = 'text/csv'
    resp.headers['Content-Disposition'] = 'attachment; filename="cash_sessions.csv"'
    return resp
