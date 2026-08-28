import json
import hmac
import hashlib
import threading
import logging
import time
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app, session
try:
    import requests as _requests
except Exception:
    _requests = None

from app.database import get_db, is_postgres

bp = Blueprint('delivery', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

STATUS_MAP_QPLATO_TO_PEDIDOSYA = {
    'por_aprobar': 'PENDING',
    'pendiente': 'PENDING',
    'confirmado': 'CONFIRMED',
    'preparacion': 'PREPARING',
    'listo': 'READY_FOR_PICKUP',
    'en_camino': 'OUT_FOR_DELIVERY',
    'entregado': 'DELIVERED',
    'cancelado': 'CANCELLED',
}


def _parse_variants_json(raw):
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw or '{}') or {}
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _load_tenant_delivery_config(cur, tenant_slug):
    cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = %s" if is_postgres() else "SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (tenant_slug,))
    row = cur.fetchone()
    cfg = {}
    if row and row[0]:
        try:
            cfg = json.loads(row[0]) or {}
        except Exception:
            cfg = {}
    if not isinstance(cfg, dict):
        cfg = {}
    delivery_cfg = cfg.get('delivery_integrations') or {}
    if not isinstance(delivery_cfg, dict):
        delivery_cfg = {}
    return delivery_cfg, cfg


def _save_tenant_delivery_config(cur, tenant_slug, full_cfg):
    raw_json = json.dumps(full_cfg, ensure_ascii=False)
    if is_postgres():
        cur.execute(
            "INSERT INTO tenant_config (tenant_slug, config_json) VALUES (%s, %s) "
            "ON CONFLICT (tenant_slug) DO UPDATE SET config_json = EXCLUDED.config_json",
            (tenant_slug, raw_json)
        )
    else:
        cur.execute(
            "INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)",
            (tenant_slug, raw_json)
        )


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _now_iso():
    return datetime.utcnow().isoformat()


def _allocate_tenant_order_number(cur, tenant_slug):
    tenant_slug = str(tenant_slug or '').strip()
    if not tenant_slug:
        return None
    try:
        if is_postgres():
            cur.execute(
                "INSERT INTO tenant_counters (tenant_slug, next_order_number) VALUES (%s, 2) "
                "ON CONFLICT (tenant_slug) DO UPDATE SET next_order_number = tenant_counters.next_order_number + 1 "
                "RETURNING next_order_number",
                (tenant_slug,)
            )
            row = cur.fetchone()
            new_next = int((row[0] if row else 2) or 2)
            return max(1, new_next - 1)
        cur.execute("INSERT OR IGNORE INTO tenant_counters (tenant_slug, next_order_number) VALUES (?, 1)", (tenant_slug,))
        cur.execute("UPDATE tenant_counters SET next_order_number = next_order_number + 1 WHERE tenant_slug = ?", (tenant_slug,))
        cur.execute("SELECT next_order_number - 1 FROM tenant_counters WHERE tenant_slug = ?", (tenant_slug,))
        row = cur.fetchone()
        return int((row[0] if row else 1) or 1)
    except Exception:
        return None


def _ph(v):
    return '%s' if is_postgres() else '?'


def _insert_order(cur, tenant_slug, fields):
    cols = list(fields.keys())
    vals = [fields[k] for k in cols]
    placeholders = ",".join([_ph(v) for v in vals])
    cols_sql = ",".join(cols)
    sql = f"INSERT INTO orders ({cols_sql}) VALUES ({placeholders})"
    if is_postgres():
        sql += " RETURNING id"
        cur.execute(sql, vals)
        rid = cur.fetchone()
        return int(rid[0]) if rid else None
    cur.execute(sql, vals)
    return int(cur.lastrowid)


def _insert_order_item(cur, fields):
    cols = list(fields.keys())
    vals = [fields[k] for k in cols]
    placeholders = ",".join([_ph(v) for v in vals])
    cols_sql = ",".join(cols)
    sql = f"INSERT INTO order_items ({cols_sql}) VALUES ({placeholders})"
    cur.execute(sql, vals)


def _insert_order_status_history(cur, order_id, status, changed_by='system:pedidosya'):
    ph = _ph('x')
    sql = (
        f"INSERT INTO order_status_history (order_id, status, changed_at, changed_by) "
        f"VALUES ({ph}, {ph}, {ph}, {ph})"
    )
    cur.execute(sql, (order_id, status, _now_iso(), changed_by))


def _insert_order_event(cur, order_id, event_type, payload, actor='system:pedidosya'):
    ph = _ph('x')
    sql = (
        f"INSERT INTO order_events (order_id, event_type, actor, payload_json, created_at) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph})"
    )
    cur.execute(sql, (order_id, event_type, actor, json.dumps(payload, ensure_ascii=False), _now_iso()))


def _lookup_product(cur, tenant_slug, matchers):
    for m in matchers:
        if not m:
            continue
        cur.execute(
            f"SELECT product_id, name, price, COALESCE(cost_price, 0) AS cost_price, stock, active, COALESCE(variants_json, '') AS variants_json "
            f"FROM products WHERE tenant_slug = {_ph(1)} AND LOWER(TRIM(COALESCE(product_id,''))) = LOWER(TRIM({_ph(2)})) LIMIT 1",
            (tenant_slug, str(m))
        )
        r = cur.fetchone()
        if r:
            return r
    return None


def _compute_unit_cost_from_product(cur, tenant_slug, product_row, unit_price_paid):
    if product_row is not None:
        try:
            cost_price = _safe_int(product_row[3] if len(product_row) > 3 else 0)
            if cost_price > 0:
                return cost_price
        except Exception:
            pass
    return 0


def _process_pedidosya_new_order(tenant_slug, webhook_body, request_timestamp, signature, raw_body_bytes=None):
    try:
        with current_app.app_context():
            conn = get_db()
            cur = conn.cursor()

            delivery_cfg, full_cfg = _load_tenant_delivery_config(cur, tenant_slug)
            pya_cfg = delivery_cfg.get('pedidosya') or {}
            if not isinstance(pya_cfg, dict):
                pya_cfg = {}

            webhook_secret = str(pya_cfg.get('webhook_secret') or '').strip()
            explicit_disabled = pya_cfg.get('enabled', None) is False
            dev_mode = (not webhook_secret) or webhook_secret.lower().startswith(('dev', 'test', 'sandbox', 'demo'))

            if explicit_disabled and not dev_mode:
                logger.warning(f"[pedidosya][{tenant_slug}] PedidosYa integration explicitly disabled")
                return
            if not dev_mode and not pya_cfg.get('enabled', False):
                logger.warning(f"[pedidosya][{tenant_slug}] PedidosYa integration not enabled (config missing)")
                return

            default_commission = 25 if dev_mode else 0
            commission_percent = max(0, min(100, _safe_int(pya_cfg.get('commission_percent'), default_commission)))

            if webhook_secret and not dev_mode:
                expected_a = ''
                expected_b = ''
                try:
                    expected_a = hmac.new(
                        webhook_secret.encode('utf-8'),
                        (str(request_timestamp or '') + json.dumps(webhook_body, separators=(',', ':'), ensure_ascii=False)).encode('utf-8'),
                        hashlib.sha256
                    ).hexdigest()
                except Exception:
                    expected_a = ''
                try:
                    if raw_body_bytes is None:
                        raw_body_bytes = json.dumps(webhook_body, ensure_ascii=False).encode('utf-8')
                    expected_b = hmac.new(
                        webhook_secret.encode('utf-8'),
                        raw_body_bytes,
                        hashlib.sha256
                    ).hexdigest()
                except Exception:
                    expected_b = ''
                if signature:
                    sig_cmp = str(signature or '').strip().lower()
                    ok = False
                    if expected_a and hmac.compare_digest(expected_a.lower(), sig_cmp):
                        ok = True
                    if not ok and expected_b and hmac.compare_digest(expected_b.lower(), sig_cmp):
                        ok = True
                    if not ok:
                        logger.warning(f"[pedidosya][{tenant_slug}] Invalid HMAC signature. (len sig={len(signature or '')})")
                        return
                else:
                    logger.warning(f"[pedidosya][{tenant_slug}] PedidosYa webhook secret configured but signature missing in headers")
                    return

            ev = webhook_body or {}
            event_type = str(ev.get('event') or ev.get('eventType') or ev.get('type') or 'new_order').strip().lower()

            # CANCELACIÓN DESDE PEDIDOSYA
            if event_type in ('order_cancelled', 'cancelled', 'canceled', 'order_canceled', 'cancel'):
                order_node = ev.get('order') or ev.get('data') or ev.get('payload') or ev
                if not isinstance(order_node, dict):
                    order_node = {}
                external_order_id = str(
                    order_node.get('id') or order_node.get('orderId') or order_node.get('external_id') or order_node.get('code') or ''
                ).strip()
                if not external_order_id:
                    return
                cur.execute(f"SELECT id, status, order_notes FROM orders WHERE tenant_slug={_ph(1)} AND external_order_id={_ph(2)} LIMIT 1", (tenant_slug, external_order_id))
                row = cur.fetchone()
                if not row:
                    logger.info(f"[pedidosya][{tenant_slug}] Cancel webhook for unknown external_id={external_order_id}")
                    return
                order_id = int(row[0])
                current_status = str(row[1] or '').strip().lower()
                current_notes = str(row[2] or '')
                if current_status in ('cancelado', 'entregado'):
                    logger.info(f"[pedidosya][{tenant_slug}] Cancel ignored because id={order_id} already status={current_status}")
                    return
                cancel_node = (order_node.get('cancellation') if isinstance(order_node.get('cancellation'), dict) else {}) or {}
                cancel_reason = str(
                    cancel_node.get('comment') or cancel_node.get('reason') or order_node.get('cancelReason') or 'Sin motivo'
                ).strip()
                cancel_code = str(cancel_node.get('code') or cancel_node.get('reasonCode') or '').strip()
                new_notes = (current_notes + ('\n' if current_notes else '') + f"[CANCEL POR PEDIDOSYA] code={cancel_code} motivo=\"{cancel_reason}\"").strip()[:2000]
                cur.execute(f"UPDATE orders SET status={_ph(1)}, order_notes={_ph(2)}, delivery_status={_ph(3)} WHERE id={_ph(4)}", ('cancelado', new_notes, 'cancelled', order_id))
                _insert_order_status_history(cur, order_id, 'cancelado', 'system:pedidosya')
                _insert_order_event(cur, order_id, 'pedidosya_cancelled', {
                    'external_order_id': external_order_id,
                    'cancel_code': cancel_code,
                    'cancel_reason': cancel_reason
                }, 'system:pedidosya')
                conn.commit()
                logger.info(f"[pedidosya][{tenant_slug}] Cancelled order id={order_id} (external={external_order_id})")
                return

            # WHITELIST DE EVENTOS DE CREACIÓN
            if event_type not in ('new_order', 'order_created', 'created', 'order_confirmed', 'confirmed'):
                logger.info(f"[pedidosya][{tenant_slug}] Ignored event type: {event_type}")
                return

            order_node = ev.get('order') or ev.get('data') or ev.get('payload') or ev
            if not isinstance(order_node, dict):
                order_node = {}

            external_order_id = str(
                order_node.get('id') or
                order_node.get('orderId') or
                order_node.get('external_id') or
                order_node.get('code') or
                ''
            ).strip()
            if not external_order_id:
                logger.warning(f"[pedidosya][{tenant_slug}] Webhook without external_order_id")
                return

            cur.execute(f"SELECT id FROM orders WHERE tenant_slug = {_ph(1)} AND external_order_id = {_ph(2)} LIMIT 1", (tenant_slug, external_order_id))
            dup = cur.fetchone()
            if dup is not None:
                logger.info(f"[pedidosya][{tenant_slug}] Order already exists external_id={external_order_id} internal_id={dup[0]}")
                return

            details_node = order_node.get('details') if isinstance(order_node.get('details'), dict) else {}
            customer_name = str(
                (details_node.get('firstName') or '') + ' ' + (details_node.get('lastName') or '')
            ).strip() or str(
                order_node.get('customer', {}).get('name') if isinstance(order_node.get('customer'), dict) else ''
                or order_node.get('customerName') or ''
            ).strip() or 'Cliente PedidosYa'

            customer_phone = str(
                details_node.get('phone') or
                (order_node.get('customer', {}).get('phone') if isinstance(order_node.get('customer'), dict) else '')
                or order_node.get('customerPhone') or ''
            ).strip()

            address_node = (details_node.get('address') if isinstance(details_node.get('address'), dict) else {}) or order_node.get('shippingAddress') or order_node.get('address') or {}
            if not isinstance(address_node, dict):
                address_node = {}
            address_json = json.dumps({
                'source': 'pedidosya',
                'street': str(address_node.get('street') or address_node.get('addressLine1') or '').strip(),
                'number': str(address_node.get('number') or address_node.get('streetNumber') or '').strip(),
                'apartment': str(address_node.get('apartment') or address_node.get('apt') or '').strip(),
                'city': str(address_node.get('city') or address_node.get('locality') or '').strip(),
                'zip': str(address_node.get('zip') or address_node.get('postalCode') or '').strip(),
                'comment': str(address_node.get('notes') or address_node.get('comment') or '').strip(),
                'latitude': str(address_node.get('latitude') or address_node.get('lat') or '').strip(),
                'longitude': str(address_node.get('longitude') or address_node.get('lng') or '').strip(),
                'reference': str(address_node.get('reference') or '').strip()
            }, ensure_ascii=False)

            order_notes = str(order_node.get('notes') or order_node.get('comment') or order_node.get('customerNotes') or '').strip()

            items_node = order_node.get('items') or order_node.get('products') or []
            if not isinstance(items_node, list):
                items_node = []

            total = 0
            items_rows = []
            for it in items_node:
                if not isinstance(it, dict):
                    continue
                external_pid = str(it.get('id') or it.get('productId') or it.get('sku') or '').strip()
                external_name = str(it.get('name') or it.get('title') or 'Producto').strip()
                qty = max(1, _safe_int(it.get('quantity') or it.get('qty'), 1))
                unit_price = max(0, _safe_int(it.get('unitPrice') or it.get('price') or it.get('unit_price'), 0))
                if unit_price == 0:
                    total_raw = max(0, _safe_int(it.get('totalPrice') or it.get('total') or it.get('subtotal'), 0))
                    unit_price = total_raw // qty if qty > 0 and total_raw > 0 else 0
                subtotal = unit_price * qty
                total += subtotal
                modifiers_json = json.dumps({
                    'external_item_id': external_pid,
                    'external_options': it.get('options') or it.get('modifiers') or [],
                    'notes': str(it.get('notes') or it.get('comment') or '').strip()
                }, ensure_ascii=False)
                item_notes = str(it.get('notes') or it.get('comment') or '').strip()
                items_rows.append({
                    'external_pid': external_pid,
                    'name': external_name,
                    'qty': qty,
                    'unit_price': unit_price,
                    'subtotal': subtotal,
                    'modifiers_json': modifiers_json,
                    'notes': item_notes
                })

            shipping_node = order_node.get('shipping') if isinstance(order_node.get('shipping'), dict) else {}
            shipping_cost = max(0, _safe_int(
                shipping_node.get('shippingCost') or
                order_node.get('shippingCost') or
                order_node.get('shipping_cost') or
                order_node.get('deliveryFee') or
                order_node.get('delivery_fee') or
                0, 0
            ))
            tip_node = order_node.get('tip') if isinstance(order_node.get('tip'), dict) else {}
            tip_amount = max(0, _safe_int(tip_node.get('amount') or order_node.get('tip') or order_node.get('tipAmount') or 0, 0))
            total_with_shipping = total + shipping_cost + tip_amount

            commission_amount = 0
            if commission_percent > 0 and total_with_shipping > 0:
                commission_amount = int(round(total_with_shipping * commission_percent / 100.0))

            tenant_order_number = _allocate_tenant_order_number(cur, tenant_slug)

            order_fields = {
                'tenant_slug': tenant_slug,
                'tenant_order_number': tenant_order_number,
                'customer_name': customer_name,
                'customer_phone': customer_phone,
                'order_type': 'direccion',
                'table_number': None,
                'address_json': address_json,
                'status': 'confirmado',
                'total': total_with_shipping,
                'payment_method': str(order_node.get('paymentMethod') or (order_node.get('payment') or {}).get('methodId') if isinstance(order_node.get('payment'), dict) else '' or order_node.get('payment_method') or 'pedidosya').strip().lower() or 'pedidosya',
                'payment_status': str((order_node.get('payment') or {}).get('status') if isinstance(order_node.get('payment'), dict) else '' or order_node.get('paymentStatus') or order_node.get('payment_status') or 'paid').strip().lower() or 'paid',
                'tip_amount': tip_amount,
                'created_at': _now_iso(),
                'order_notes': order_notes,
                'shipping_cost': shipping_cost,
                'delivery_status': 'pending',
                'source': 'pedidos_ya',
                'external_order_id': external_order_id,
                'external_fee_amount': commission_amount,
                'external_payload_json': json.dumps(webhook_body, ensure_ascii=False)
            }

            order_id = _insert_order(cur, tenant_slug, order_fields)
            if order_id is None:
                raise RuntimeError("Failed to insert order")

            for it in items_rows:
                product_row = _lookup_product(cur, tenant_slug, [
                    it['external_pid'],
                ])
                unit_cost = _compute_unit_cost_from_product(cur, tenant_slug, product_row, it['unit_price'])
                product_id_for_db = None
                if product_row is not None:
                    try:
                        product_id_for_db = str(product_row[0] or '').strip() or None
                    except Exception:
                        product_id_for_db = None
                _insert_order_item(cur, {
                    'order_id': order_id,
                    'tenant_slug': tenant_slug,
                    'product_id': product_id_for_db,
                    'name': it['name'],
                    'qty': it['qty'],
                    'unit_price': it['unit_price'],
                    'unit_cost': unit_cost,
                    'modifiers_json': it['modifiers_json'],
                    'notes': it['notes']
                })

            _insert_order_status_history(cur, order_id, 'confirmado', 'system:pedidosya')
            _insert_order_event(cur, order_id, 'pedidosya_new_order', {
                'external_order_id': external_order_id,
                'commission_percent': commission_percent,
                'commission_amount': commission_amount,
                'items_count': len(items_rows),
                'matched_products': sum(1 for it in items_rows if it['external_pid'])
            }, 'system:pedidosya')

            conn.commit()
            logger.info(f"[pedidosya][{tenant_slug}] Created order id={order_id} number={tenant_order_number} external={external_order_id} total={total_with_shipping}")

    except Exception as exc:
        logger.exception(f"[pedidosya][{tenant_slug}] Failed to process new order webhook: {exc}")
        try:
            with current_app.app_context():
                conn = get_db()
                cur = conn.cursor()
                cur.execute(
                    f"INSERT INTO order_events (order_id, event_type, actor, payload_json, created_at) VALUES ({_ph(1)}, {_ph(2)}, {_ph(3)}, {_ph(4)}, {_ph(5)})",
                    (
                        0,
                        'pedidosya_webhook_error',
                        'system:pedidosya',
                        json.dumps({
                            'error': str(exc),
                            'tenant_slug': tenant_slug,
                            'received_keys': list((webhook_body or {}).keys()) if isinstance(webhook_body, dict) else []
                        }, ensure_ascii=False),
                        _now_iso()
                    )
                )
                conn.commit()
        except Exception:
            pass


def _parse_perms_json(s):
    if not s:
        return {}
    try:
        v = json.loads(s or '')
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


def _has_perm(perms, owner, role, key):
    if owner or role == 'admin':
        return True
    return bool(perms.get(key))


def _enforce_tenant(tenant_slug, session_tenant):
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return False
    return True


@bp.route('/webhooks/pedidosya/<tenant_slug>', methods=['POST'])
def pedidosya_webhook(tenant_slug):
    tenant_slug = str(tenant_slug or '').strip()
    if not tenant_slug:
        return jsonify({'error': 'tenant_slug_required'}), 400

    payload = request.get_json(silent=True)
    if payload is None:
        payload = {}
    raw_body = request.get_data(as_text=False) or b''

    ts_header = str(request.headers.get('X-PedidosYa-Timestamp') or request.headers.get('X-Timestamp') or request.args.get('ts') or '')
    sig_header = str(request.headers.get('X-PedidosYa-Signature') or request.headers.get('X-Signature') or request.args.get('sig') or '')

    remote_addr = str(request.remote_addr or '').strip()
    is_loopback = remote_addr in ('127.0.0.1', '::1', 'localhost', '')

    try:
        with current_app.app_context():
            conn = get_db()
            cur = conn.cursor()
            delivery_cfg, _ = _load_tenant_delivery_config(cur, tenant_slug)
            pya_cfg = delivery_cfg.get('pedidosya') or {}
            if not isinstance(pya_cfg, dict):
                pya_cfg = {}
            webhook_secret = str(pya_cfg.get('webhook_secret') or '').strip()
            force_sync_dev = (not webhook_secret) or webhook_secret.lower().startswith(('dev', 'test', 'sandbox', 'demo'))
    except Exception:
        force_sync_dev = False

    sync_raw = str(
        request.values.get('sync') or
        request.args.get('sync') or
        request.form.get('sync') or
        request.headers.get('X-Sync') or
        request.headers.get('X-Sync-Mode') or
        ''
    ).strip().lower()
    sync_arg_true = sync_raw in ('1', 'true', 'yes', 'on', 'y', 's', 'si')
    sync_mode = is_loopback or sync_arg_true or force_sync_dev
    processing_error = []
    result_trail = []

    def run():
        try:
            _process_pedidosya_new_order(tenant_slug, payload, ts_header, sig_header, raw_body)
        except Exception as exc:
            processing_error.append(str(exc))
            logger.exception(f"[pedidosya][{tenant_slug}] Unhandled thread exception: {exc}")

    if sync_mode:
        run()
        if processing_error:
            return jsonify({
                'status': 'error',
                'error': processing_error[0],
                'received_at': _now_iso(),
                'tenant_slug': tenant_slug,
                '_debug': {
                    'remote_addr': remote_addr,
                    'is_loopback': is_loopback,
                    'sync_raw': sync_raw,
                    'force_sync_dev': force_sync_dev
                }
            }), 500
        return jsonify({
            'status': 'processed',
            'received_at': _now_iso(),
            'tenant_slug': tenant_slug,
            '_debug': {
                'remote_addr': remote_addr,
                'is_loopback': is_loopback,
                'sync_raw': sync_raw,
                'force_sync_dev': force_sync_dev
            }
        }), 200

    threading.Thread(
        target=run,
        daemon=True
    ).start()

    return jsonify({
        'status': 'accepted',
        'received_at': _now_iso(),
        'tenant_slug': tenant_slug,
        '_debug': {
            'remote_addr': remote_addr,
            'is_loopback': is_loopback,
            'sync_raw': sync_raw,
            'force_sync_dev': force_sync_dev
        }
    }), 200


def _map_status_for_pedidosya(qplato_status):
    s = str(qplato_status or '').strip().lower()
    if not s:
        return None
    return STATUS_MAP_QPLATO_TO_PEDIDOSYA.get(s)


def _insert_sync_event(cur, order_id, result, payload, actor='system:pedidosya'):
    ph = _ph('x')
    try:
        base = {'result': result}
        if isinstance(payload, dict):
            base.update(payload)
        else:
            base['detail'] = str(payload or '')
        cur.execute(
            f"INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) "
            f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
            (
                order_id,
                'pedidosya_status_sync_out',
                actor or 'system:pedidosya',
                0,
                json.dumps(base, ensure_ascii=False),
                _now_iso()
            )
        )
    except Exception as e:
        try:
            safe = {
                'result': str(result or ''),
                '_parse_fallback': f"json.dumps failed: {type(e).__name__}: {str(e)[:300]}",
                'actor': actor or '',
            }
            if isinstance(payload, dict):
                for k in ('new_status', 'qplato_status', 'mapped_pedidosya_status', 'new_status_qplato',
                          'new_status_pedidosya', 'external_order_id', 'http_method', 'http_url',
                          'http_status', 'latency_ms', 'error', 'idempotency_key', 'reason',
                          'enabled_flag', 'api_key_masked', 'has_requests_lib'):
                    if k in payload and payload[k] is not None:
                        v = payload[k]
                        if isinstance(v, str):
                            safe[k] = v[:250]
                        elif isinstance(v, (int, float, bool)) or v is None:
                            safe[k] = v
                        else:
                            safe[k] = str(v)[:250]
            cur.execute(
                f"INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) "
                f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
                (
                    order_id, 'pedidosya_status_sync_out', actor or 'system:pedidosya', 0,
                    json.dumps(safe, ensure_ascii=False), _now_iso()
                )
            )
        except Exception as inner_e:
            logger.warning(f"[pedidosya][sync_out] Failed safe-fallback log event: {inner_e}")


def try_sync_pedidosya_status_outbound(tenant_slug, order_id, new_status, reason=None, actor=None):
    """
    Fase 3 — Sync Qplato → PedidosYa (outbound status change).
    NUNCA lanza excepciones, nunca bloquea el flujo principal.
    Retorna True si se envió, False si se skippeó (log queda en order_events siempre que corresponda).
    """
    try:
        tenant_slug = str(tenant_slug or '').strip()
        new_status_norm = str(new_status or '').strip().lower()
        order_id_int = int(order_id)
        if not tenant_slug or not order_id_int or not new_status_norm:
            return False

        conn = get_db()
        cur = conn.cursor()

        mapped_status = _map_status_for_pedidosya(new_status_norm)
        if mapped_status is None:
            return False

        ph = _ph('x')
        cur.execute(
            f"SELECT COALESCE(source,''), COALESCE(external_order_id,''), tenant_slug FROM orders WHERE id = {ph}",
            (order_id_int,)
        )
        order_row = cur.fetchone()
        if not order_row:
            return False
        src, external_id, row_tenant = order_row
        src = str(src or '').strip().lower()
        external_id = str(external_id or '').strip()
        row_tenant = str(row_tenant or '').strip()
        if row_tenant != tenant_slug:
            return False
        if src != 'pedidos_ya':
            return False
        if not external_id:
            _insert_sync_event(cur, order_id_int, 'skipped', {
                'reason': 'no external_order_id',
                'new_status': new_status_norm,
                'mapped': mapped_status,
            }, actor=actor or 'system:pedidosya')
            try: conn.commit()
            except Exception: pass
            return False

        delivery_cfg, _ = _load_tenant_delivery_config(cur, tenant_slug)
        pya_cfg = delivery_cfg.get('pedidosya') if isinstance(delivery_cfg.get('pedidosya'), dict) else {}
        enabled = bool(pya_cfg.get('enabled'))
        api_key = str(pya_cfg.get('api_key') or '').strip()
        api_key_masked = ''
        if api_key:
            if len(api_key) >= 10:
                api_key_masked = api_key[:4] + '*' * (len(api_key)-8) + api_key[-4:]
            elif len(api_key) >= 4:
                api_key_masked = api_key[:2] + '*' * (len(api_key)-4) + api_key[-2:]
            else:
                api_key_masked = '*' * len(api_key)

        if not enabled and not api_key:
            return False
        if not enabled:
            _insert_sync_event(cur, order_id_int, 'skipped', {
                'reason': 'integration not enabled (delivery_integrations.pedidosya.enabled=false)',
                'external_order_id': external_id,
                'qplato_status': new_status_norm,
                'mapped_pedidosya_status': mapped_status,
                'api_key_masked': api_key_masked or None,
                'enabled_flag': False,
            }, actor=actor or 'system:pedidosya')
            try: conn.commit()
            except Exception: pass
            return False
        if not api_key:
            _insert_sync_event(cur, order_id_int, 'skipped', {
                'reason': 'no api_key configured',
                'external_order_id': external_id,
                'qplato_status': new_status_norm,
                'mapped_pedidosya_status': mapped_status,
                'enabled_flag': enabled,
            }, actor=actor or 'system:pedidosya')
            try: conn.commit()
            except Exception: pass
            return False

        cur.execute(
            f"SELECT id FROM order_events WHERE order_id = {ph} AND event_type = 'pedidosya_status_sync_out' "
            f"ORDER BY id DESC LIMIT 1",
            (order_id_int,)
        )
        last_ev = cur.fetchone()
        dedupe_key = f"{mapped_status}|{new_status_norm}"
        try:
            if last_ev:
                cur.execute(
                    f"SELECT payload_json FROM order_events WHERE id = {ph}",
                    (int(last_ev[0]),)
                )
                payload_row = cur.fetchone()
                if payload_row and payload_row[0]:
                    prev = json.loads(payload_row[0]) if isinstance(payload_row[0], str) else (payload_row[0] or {})
                    if isinstance(prev, dict) and str(prev.get('new_status') or '').lower() == new_status_norm and str(prev.get('result') or '') == 'sent':
                        _insert_sync_event(cur, order_id_int, 'skipped', {
                            'reason': 'idempotent duplicate',
                            'external_order_id': external_id,
                            'new_status': new_status_norm,
                            'mapped': mapped_status,
                            'dedupe': dedupe_key,
                        }, actor=actor or 'system:pedidosya')
                        try: conn.commit()
                        except Exception: pass
                        return False
        except Exception:
            pass

        if _requests is None:
            _insert_sync_event(cur, order_id_int, 'error', {
                'reason': 'requests library missing in environment',
                'external_order_id': external_id,
                'qplato_status': new_status_norm,
                'mapped_pedidosya_status': mapped_status,
                'enabled_flag': enabled,
                'api_key_masked': api_key_masked or None,
                'has_requests_lib': False,
            }, actor=actor or 'system:pedidosya')
            try: conn.commit()
            except Exception: pass
            return False

        api_base = str(pya_cfg.get('api_base_url') or pya_cfg.get('base_url') or 'https://api.pedidosya.com').strip().rstrip('/')
        endpoint_path = str(pya_cfg.get('status_endpoint') or '/v1/orders/{external_order_id}/status').strip()
        try:
            full_url = api_base + endpoint_path.format(external_order_id=external_id)
        except Exception:
            full_url = f"{api_base}/v1/orders/{external_id}/status"

        http_method = str(pya_cfg.get('status_http_method') or 'PATCH').strip().upper()
        if http_method not in ('PUT', 'POST', 'PATCH'):
            http_method = 'PATCH'

        merchant_id = str(pya_cfg.get('merchant_id') or '').strip()
        timeout_sec = max(1, min(20, _safe_int(pya_cfg.get('http_timeout_seconds'), 6)))

        body = {
            'status': mapped_status,
            'order_id': external_id,
            'new_status': mapped_status,
        }
        if new_status_norm == 'cancelado' and reason:
            body['reason'] = str(reason).strip()

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {api_key}",
            'X-Merchant-Id': merchant_id,
            'X-PedidosYa-Status-Source': 'qplato',
            'User-Agent': f"Qplato/{_safe_int(pya_cfg.get('_version'), 1)}",
        }
        if not headers.get('X-Merchant-Id'):
            headers.pop('X-Merchant-Id', None)

        http_ok = False
        http_status = None
        http_text_safe = ''
        http_err = None
        started_at = time.time()
        try:
            req = _requests.Request(method=http_method, url=full_url, json=body, headers=headers)
            prepped = req.prepare()
            with _requests.Session() as sess:
                resp = sess.send(prepped, timeout=timeout_sec)
                http_status = int(getattr(resp, 'status_code', 0) or 0)
                try:
                    raw_text = str(getattr(resp, 'text', '') or '')
                    http_text_safe = (raw_text[:800] if raw_text else '')
                except Exception:
                    http_text_safe = ''
                if 200 <= http_status < 300:
                    http_ok = True
                else:
                    http_err = f"HTTP {http_status}"
        except Exception as e:
            http_err = f"{type(e).__name__}: {str(e)[:300]}"
        latency_ms = int((time.time() - started_at) * 1000)

        payload = {
            'external_order_id': external_id,
            'qplato_status': new_status_norm,
            'mapped_pedidosya_status': mapped_status,
            'http_method': http_method,
            'http_url': full_url,
            'http_status': http_status,
            'latency_ms': latency_ms,
            'body_sent_keys': sorted(list(body.keys())),
            'error': http_err,
            'idempotency_key': dedupe_key,
            'enabled_flag': enabled,
            'api_key_masked': api_key_masked or None,
            'has_requests_lib': True,
        }
        if not http_ok:
            payload['response_error_snippet'] = http_text_safe
        _insert_sync_event(cur, order_id_int, 'sent' if http_ok else 'error', payload,
                           actor=actor or 'system:pedidosya')
        try:
            conn.commit()
        except Exception as commit_e:
            logger.warning(f"[pedidosya][sync_out] commit failed order={order_id_int}: {commit_e}")
        return http_ok

    except Exception as outer_e:
        try:
            logger.exception(f"[pedidosya][sync_out] Fatal fallback exception for order={order_id}")
            conn = get_db()
            cur = conn.cursor()
            _insert_sync_event(cur, int(order_id or 0), 'fatal', {
                'new_status': str(new_status or ''),
                'exception': f"{type(outer_e).__name__}: {str(outer_e)[:400]}",
            }, actor=actor or 'system:pedidosya')
            try: conn.commit()
            except Exception: pass
        except Exception:
            pass
        return False


@bp.route('/delivery/config/<tenant_slug>', methods=['GET'])
def get_delivery_config(tenant_slug):
    from app.utils import is_authed
    if not is_authed():
        return jsonify({'error': 'unauthorized'}), 401
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        if not (owner or role == 'master'):
            return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_perm(perms, owner, role, 'tenant_manage') and not (owner or role == 'admin'):
        return jsonify({'error': 'permiso denegado'}), 403

    try:
        conn = get_db()
        cur = conn.cursor()
        delivery_cfg, _ = _load_tenant_delivery_config(cur, tenant_slug)
        pedidosya_raw = delivery_cfg.get('pedidosya') if isinstance(delivery_cfg, dict) else None
        if not isinstance(pedidosya_raw, dict):
            pedidosya_raw = {}
        defaults_pedidosya = {
            'enabled': False,
            'api_key': '',
            'webhook_secret': '',
            'merchant_id': '',
            'commission_percent': 25,
            'api_base_url': 'https://api.pedidosya.com',
            'status_endpoint': '/v1/orders/{external_order_id}/status',
            'status_http_method': 'PATCH',
            'http_timeout_seconds': 6,
            '_version': 1,
        }
        for k, v in defaults_pedidosya.items():
            if k not in pedidosya_raw or pedidosya_raw.get(k) is None or pedidosya_raw.get(k) == '':
                pedidosya_raw.setdefault(k, v)
        if isinstance(pedidosya_raw.get('commission_percent'), (int, float)):
            pedidosya_raw['commission_percent'] = max(0, min(100, int(pedidosya_raw['commission_percent'])))
        else:
            pedidosya_raw['commission_percent'] = 25
        if isinstance(pedidosya_raw.get('http_timeout_seconds'), (int, float)):
            pedidosya_raw['http_timeout_seconds'] = max(1, min(20, int(pedidosya_raw['http_timeout_seconds'])))
        else:
            pedidosya_raw['http_timeout_seconds'] = 6
        if pedidosya_raw.get('api_base_url'):
            pedidosya_raw['api_base_url'] = str(pedidosya_raw['api_base_url']).rstrip('/')
        delivery_cfg['pedidosya'] = pedidosya_raw
        return jsonify({
            'tenant_slug': tenant_slug,
            'delivery_integrations': delivery_cfg
        })
    except Exception as e:
        logger.exception("get_delivery_config failed")
        return jsonify({'error': str(e)}), 500


@bp.route('/delivery/config/<tenant_slug>', methods=['POST'])
def update_delivery_config(tenant_slug):
    from app.utils import is_authed, check_csrf
    if not is_authed():
        return jsonify({'error': 'unauthorized'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        if not (owner or role == 'master'):
            return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_perm(perms, owner, role, 'tenant_manage') and not (owner or role == 'admin'):
        return jsonify({'error': 'permiso denegado'}), 403

    body = request.get_json(silent=True) or {}
    # Acepta 2 formatos de payload:
    #   Formato A (nested / UI): { delivery_integrations: { pedidosya: { enabled, ... } } }
    #   Formato B (flat / scripts): { enabled, api_key, webhook_secret, merchant_id, commission_percent }
    delivery_integrations = body.get('delivery_integrations')
    if not isinstance(delivery_integrations, dict):
        delivery_integrations = {}

    keys_flat_pedidosya = {'enabled', 'api_key', 'webhook_secret', 'merchant_id', 'commission_percent'}
    body_keys = set((body or {}).keys())
    has_flat_pya = bool(keys_flat_pedidosya & body_keys)
    if has_flat_pya:
        pya_existing = delivery_integrations.get('pedidosya') if isinstance(delivery_integrations.get('pedidosya'), dict) else {}
        merged = dict(pya_existing)
        for k in ('enabled', 'api_key', 'webhook_secret', 'merchant_id', 'commission_percent'):
            if k in body:
                merged[k] = body[k]
        delivery_integrations['pedidosya'] = merged

    pedidosya = delivery_integrations.get('pedidosya') or {}
    if pedidosya:
        if not isinstance(pedidosya, dict):
            return jsonify({'error': 'pedidosya must be object'}), 400
        pedidosya['commission_percent'] = max(0, min(100, _safe_int(pedidosya.get('commission_percent'), 0)))
        pedidosya['http_timeout_seconds'] = max(1, min(20, _safe_int(pedidosya.get('http_timeout_seconds'), 6)))
        if not isinstance(pedidosya.get('api_base_url'), str) or not pedidosya.get('api_base_url'):
            pedidosya['api_base_url'] = 'https://api.pedidosya.com'
        if not isinstance(pedidosya.get('status_endpoint'), str) or not pedidosya.get('status_endpoint'):
            pedidosya['status_endpoint'] = '/v1/orders/{external_order_id}/status'
        if not isinstance(pedidosya.get('status_http_method'), str) or not pedidosya.get('status_http_method'):
            pedidosya['status_http_method'] = 'PATCH'
        pedidosya['_version'] = max(1, _safe_int(pedidosya.get('_version'), 1))
        # api_key / webhook_secret / merchant_id se guardan como string
        for k in ('api_key', 'webhook_secret', 'merchant_id'):
            pedidosya[k] = str(pedidosya.get(k) or '')

    try:
        conn = get_db()
        cur = conn.cursor()
        _, full_cfg = _load_tenant_delivery_config(cur, tenant_slug)
        full_cfg['delivery_integrations'] = delivery_integrations
        _save_tenant_delivery_config(cur, tenant_slug, full_cfg)
        conn.commit()
        try:
            from app.utils import invalidate_tenant_config
            invalidate_tenant_config(str(tenant_slug or ''))
        except Exception:
            pass
        return jsonify({
            'status': 'ok',
            'tenant_slug': tenant_slug,
            'delivery_integrations': delivery_integrations
        })
    except Exception as e:
        logger.exception("update_delivery_config failed")
        return jsonify({'error': str(e)}), 500


@bp.route('/delivery/config/<tenant_slug>/test', methods=['POST'])
def test_delivery_config(tenant_slug):
    from app.utils import is_authed, check_csrf
    if not is_authed():
        return jsonify({'error': 'unauthorized'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _enforce_tenant(tenant_slug, session_tenant):
        if not (owner or role == 'master'):
            return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_perm(perms, owner, role, 'tenant_manage') and not (owner or role == 'admin'):
        return jsonify({'error': 'permiso denegado'}), 403

    try:
        conn = get_db()
        cur = conn.cursor()
        delivery_cfg, _ = _load_tenant_delivery_config(cur, tenant_slug)
        pya_cfg = delivery_cfg.get('pedidosya') or {}
        if not isinstance(pya_cfg, dict):
            pya_cfg = {}
        enabled = bool(pya_cfg.get('enabled'))
        api_key = str(pya_cfg.get('api_key') or '').strip()
        if not enabled or not api_key:
            return jsonify({
                'status': 'skip',
                'reason': 'disabled_or_empty_key',
                'message': ('Integración deshabilitada (enabled=false)' if not enabled else 'API Key vacía')
            })

        api_base = str(pya_cfg.get('api_base_url') or 'https://api.pedidosya.com').strip().rstrip('/')
        status_ep = str(pya_cfg.get('status_endpoint') or '/v1/orders/{external_order_id}/status').strip()
        method = str(pya_cfg.get('status_http_method') or 'PATCH').strip().upper()
        timeout = max(1, min(20, _safe_int(pya_cfg.get('http_timeout_seconds'), 6)))
        merchant_id = str(pya_cfg.get('merchant_id') or '').strip()
        masked_key = (api_key[:4] + '***' + api_key[-4:]) if len(api_key) >= 8 else ('***' + api_key[-4:] if len(api_key) >= 4 else '***')

        if not _requests:
            return jsonify({'status': 'error', 'reason': 'requests_unavailable', 'message': 'Librería requests no instalada en entorno server'}), 500

        test_url = api_base + status_ep.replace('{external_order_id}', 'QPLATO-TEST-ORDER-NOT-EXIST')
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {api_key}",
            'X-PedidosYa-Status-Source': 'qplato',
            'User-Agent': f"Qplato/{_safe_int(pya_cfg.get('_version'), 1)}",
        }
        if merchant_id:
            headers['X-Merchant-Id'] = merchant_id

        http_status = None
        latency_ms = 0
        err_msg = ''
        started_at = time.time()
        try:
            body = {'status': 'PENDING', 'reason': 'Qplato Connection Test'}
            req = _requests.Request(method=method, url=test_url, json=body, headers=headers)
            prepped = req.prepare()
            with _requests.Session() as sess:
                resp = sess.send(prepped, timeout=timeout)
                http_status = int(getattr(resp, 'status_code', 0) or 0)
        except Exception as te:
            err_msg = f"{type(te).__name__}: {str(te)[:400]}"
        latency_ms = int((time.time() - started_at) * 1000)

        ok = bool(200 <= http_status < 500) if http_status else False
        # HTTP 404/401/403 son signos de conexión OK (auth reachable, API contestó)
        connectivity_ok = bool(http_status is not None)
        return jsonify({
            'status': 'ok' if (ok and connectivity_ok) else (('connectivity_ok' if connectivity_ok else 'error')),
            'http_status': http_status,
            'latency_ms': latency_ms,
            'connectivity_ok': connectivity_ok,
            'auth_ok': bool(http_status != 401 and http_status != 403) if http_status else False,
            'api_key_masked': masked_key,
            'api_base_url': api_base,
            'merchant_id_sent': bool(merchant_id),
            'error': err_msg if err_msg else None,
            'note': ('HTTP 404 es normal al probar con order_id inexistente. Conexión OK, credenciales alcanzan la API.' if http_status == 404 else (
                     'HTTP 401/403 = API Key inválida o merchant_id sin permisos.' if http_status in (401, 403) else ''))
        })
    except Exception as e:
        logger.exception("test_delivery_config failed")
        return jsonify({'status': 'error', 'error': str(e)[:500]}), 500
