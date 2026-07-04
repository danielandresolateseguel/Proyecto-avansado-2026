import json
import re
import math
from datetime import datetime, timedelta, timezone
from flask import Blueprint, request, jsonify, session, Response
from app.database import get_db, is_postgres
from app.utils import is_authed, check_csrf, get_cached_tenant_config, invalidate_tenant_config
import io
import csv

bp = Blueprint('orders', __name__, url_prefix='/api')

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

def _has_perm(perms, owner, role, key):
    if owner or role == 'admin':
        return True
    return bool(perms.get(key))

def _scope_for(role, owner=False):
    if owner or role == 'admin':
        return 'tenant'
    if role in ('mozo', 'caja', 'repartidor'):
        return 'user'
    return 'tenant'

def _build_order_creation_event(is_admin_origin, actor='', customer_name='', customer_phone=''):
    actor = str(actor or '').strip()
    customer_name = str(customer_name or '').strip()
    customer_phone = str(customer_phone or '').strip()
    if is_admin_origin:
        return (
            actor or 'panel',
            {
                'source': 'panel',
                'source_label': 'Panel admin',
                'creator_label': actor or 'Panel admin',
            },
        )
    creator_label = customer_name or customer_phone or 'Carta online'
    return (
        creator_label,
        {
            'source': 'carta_online',
            'source_label': 'Carta online',
            'creator_label': creator_label,
        },
    )

def _load_tenant_config_row(cur, slug):
    cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = ?", (slug,))
    row = cur.fetchone()
    current_cfg = {}
    if row and row[0]:
        try:
            current_cfg = json.loads(row[0])
        except Exception:
            current_cfg = {}
    if not isinstance(current_cfg, dict):
        current_cfg = {}
    return current_cfg

def _safe_float(value, default=None):
    try:
        n = float(value)
        if math.isfinite(n):
            return n
        return default
    except Exception:
        return default

def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default

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

def _normalize_food_categories_local(value):
    if isinstance(value, list):
        return [str(v or '').strip().lower() for v in value if str(v or '').strip()]
    if isinstance(value, str):
        return [part.strip().lower() for part in value.split(',') if part.strip()]
    return []

def _build_mix_summary(parts):
    if not isinstance(parts, list):
        return ''
    labels = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        name = str(part.get('name') or '').strip() or 'Pizza'
        labels.append(f"1/2 {name}")
    return ' + '.join(labels)

def _resolve_mix_price(cur, tenant_slug, base_product_id, base_variants, modifiers):
    mix_builder = base_variants.get('mix_builder') if isinstance(base_variants, dict) else None
    if not isinstance(mix_builder, dict) or mix_builder.get('enabled') is False:
        return None, modifiers
    mix_items = modifiers.get('mix') if isinstance(modifiers, dict) else None
    if not isinstance(mix_items, list) or not mix_items:
        raise ValueError('faltan las mitades de la pizza mixta')

    parts = max(1, _safe_int(mix_builder.get('parts'), 2))
    if len(mix_items) != parts:
        raise ValueError('la pizza mixta requiere exactamente dos mitades')

    source_category = str(mix_builder.get('source_category') or '').strip().lower()
    only_mixable = bool(mix_builder.get('only_mixable', True))
    fraction_default = _safe_float(mix_builder.get('part_fraction'), 0.5)
    if fraction_default is None or fraction_default <= 0:
        fraction_default = 0.5

    allowed_product_ids = mix_builder.get('allowed_product_ids') or []
    if isinstance(allowed_product_ids, str):
        allowed_product_ids = [part.strip() for part in allowed_product_ids.split(',') if part.strip()]
    if not isinstance(allowed_product_ids, list):
        allowed_product_ids = []
    allowed_product_ids = [str(pid or '').strip() for pid in allowed_product_ids if str(pid or '').strip()]

    sanitized_parts = []
    unit_price = 0
    for raw_part in mix_items:
        if not isinstance(raw_part, dict):
            raise ValueError('configuración inválida para la pizza mixta')
        component_id = str(raw_part.get('product_id') or raw_part.get('id') or '').strip()
        if not component_id:
            raise ValueError('una mitad de la pizza mixta no tiene producto asociado')
        if component_id == str(base_product_id):
            raise ValueError('la pizza mixta no puede mezclarse consigo misma')
        cur.execute(
            "SELECT name, price, active, COALESCE(variants_json, '') FROM products WHERE tenant_slug = ? AND product_id = ?",
            (tenant_slug, component_id)
        )
        component_row = cur.fetchone()
        if not component_row:
            raise ValueError(f'producto no encontrado para pizza mixta: {component_id}')
        component_name = str(component_row[0] or component_id).strip()
        component_price = int(component_row[1] or 0)
        component_active = bool(component_row[2])
        component_variants = _parse_variants_json(component_row[3] if len(component_row) > 3 else '')
        if not component_active:
            raise ValueError(f'la mitad seleccionada no está activa: {component_name}')
        if allowed_product_ids and component_id not in allowed_product_ids:
            raise ValueError(f'la mitad seleccionada no está permitida: {component_name}')
        categories = _normalize_food_categories_local(component_variants.get('food_categories') or [])
        if source_category and source_category not in categories:
            raise ValueError(f'la mitad seleccionada no pertenece a la categoría {source_category}: {component_name}')
        if only_mixable and not bool(component_variants.get('mixable')):
            raise ValueError(f'la pizza no está marcada como combinable: {component_name}')
        fraction = _safe_float(raw_part.get('fraction'), fraction_default)
        if fraction is None or fraction <= 0:
            fraction = fraction_default
        applied_price = int(round(component_price * fraction))
        unit_price += applied_price
        sanitized_parts.append({
            'product_id': component_id,
            'name': component_name,
            'fraction': fraction,
            'base_price': component_price,
            'applied_price': applied_price
        })

    next_modifiers = dict(modifiers or {})
    next_modifiers['mix'] = sanitized_parts
    next_modifiers['mix_summary'] = _build_mix_summary(sanitized_parts)
    next_modifiers['mix_builder'] = {
        'source_category': source_category,
        'parts': parts,
        'pricing_mode': str(mix_builder.get('pricing_mode') or 'sum_parts')
    }
    return unit_price, next_modifiers

def _extract_lat_lng(obj):
    if not isinstance(obj, dict):
        return (None, None)
    lat = _safe_float(obj.get('lat', obj.get('latitude')))
    lng = _safe_float(obj.get('lng', obj.get('lon', obj.get('longitude'))))
    if lat is None or lng is None:
        return (None, None)
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        return (None, None)
    return (lat, lng)

def _haversine_km(lat1, lng1, lat2, lng2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2.0) ** 2) + math.cos(p1) * math.cos(p2) * (math.sin(dlng / 2.0) ** 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c

def _compute_shipping_cost(cfg, order_type, address_json):
    order_type_norm = str(order_type or '').strip().lower()
    if order_type_norm != 'direccion':
        return 0, None

    base_cost = _safe_int((cfg or {}).get('shipping_cost', 0), 0)
    distance_cfg = (cfg or {}).get('shipping_distance')
    if not isinstance(distance_cfg, dict) or not bool(distance_cfg.get('enabled')):
        return max(0, base_cost), None

    origin = distance_cfg.get('origin') if isinstance(distance_cfg.get('origin'), dict) else {}
    o_lat, o_lng = _extract_lat_lng(origin)
    if o_lat is None or o_lng is None:
        return max(0, base_cost), None

    addr = address_json if isinstance(address_json, dict) else {}
    geo = addr.get('geo') if isinstance(addr.get('geo'), dict) else (addr.get('location') if isinstance(addr.get('location'), dict) else {})
    d_lat, d_lng = _extract_lat_lng(geo)
    if d_lat is None or d_lng is None:
        return max(0, base_cost), None

    included_km = _safe_float(distance_cfg.get('included_km'), 0.0)
    if included_km is None or included_km < 0:
        included_km = 0.0
    extra_per_km = _safe_int(distance_cfg.get('extra_per_km'), 0)
    if extra_per_km <= 0:
        return max(0, base_cost), None

    try:
        dist_km = _haversine_km(o_lat, o_lng, d_lat, d_lng)
    except Exception:
        return max(0, base_cost), None

    extra_km = max(0.0, float(dist_km) - float(included_km))
    extra_cost = int(math.ceil(extra_km * float(extra_per_km)))
    shipping_cost = max(0, base_cost + max(0, extra_cost))

    max_cost = _safe_int(distance_cfg.get('max_cost'), 0)
    if max_cost > 0:
        shipping_cost = min(shipping_cost, max_cost)

    return shipping_cost, float(dist_km)

def _normalize_quick_order_shortcuts(raw):
    out = []
    seen_ids = set()
    if not isinstance(raw, list):
        return out
    for idx, shortcut in enumerate(raw):
        if not isinstance(shortcut, dict):
            continue
        name = str(shortcut.get('name') or '').strip()
        if not name:
            continue
        raw_id = str(shortcut.get('id') or '').strip()
        raw_id = re.sub(r'[^a-zA-Z0-9_-]+', '-', raw_id).strip('-_')[:48]
        if not raw_id:
            raw_id = f"shortcut-{idx + 1}"
        shortcut_id = raw_id
        suffix = 2
        while shortcut_id in seen_ids:
            shortcut_id = f"{raw_id}-{suffix}"
            suffix += 1
        seen_ids.add(shortcut_id)

        items = []
        total_qty = 0
        for item in (shortcut.get('items') or []):
            if not isinstance(item, dict):
                continue
            product_id = str(item.get('product_id') or item.get('id') or '').strip()
            if not product_id:
                continue
            try:
                qty = int(item.get('qty') or item.get('quantity') or 0)
            except Exception:
                qty = 0
            if qty <= 0:
                continue
            qty = max(1, min(99, qty))
            items.append({
                'product_id': product_id[:64],
                'qty': qty
            })
            total_qty += qty
            if total_qty >= 200:
                break
        if not items:
            continue
        out.append({
            'id': shortcut_id,
            'name': name[:80],
            'active': bool(shortcut.get('active', True)),
            'items': items[:20]
        })
        if len(out) >= 50:
            break
    return out

def _enrich_quick_order_shortcuts(cur, tenant_slug, shortcuts):
    shortcuts = _normalize_quick_order_shortcuts(shortcuts)
    product_ids = []
    seen = set()
    for shortcut in shortcuts:
        for item in shortcut.get('items') or []:
            pid = str(item.get('product_id') or '').strip()
            if pid and pid not in seen:
                seen.add(pid)
                product_ids.append(pid)
    product_map = {}
    if product_ids:
        placeholders = ",".join(["?"] * len(product_ids))
        cur.execute(
            f"""
            SELECT product_id, name, price, stock, active, COALESCE(details,'') AS details
            FROM products
            WHERE tenant_slug = ? AND product_id IN ({placeholders})
            """,
            [tenant_slug] + product_ids
        )
        for row in (cur.fetchall() or []):
            product_map[str(row[0])] = {
                'product_id': str(row[0]),
                'name': str(row[1] or row[0]),
                'price': int(row[2] or 0),
                'stock': int(row[3] or 0),
                'active': bool(row[4]),
                'details': str(row[5] or '')
            }

    enriched = []
    for shortcut in shortcuts:
        items = []
        total = 0
        total_qty = 0
        available = True
        for item in shortcut.get('items') or []:
            pid = str(item.get('product_id') or '').strip()
            qty = int(item.get('qty') or 0)
            product = product_map.get(pid)
            unit_price = int(product.get('price') or 0) if product else 0
            item_active = bool(product.get('active')) if product else False
            missing = product is None
            if missing or not item_active:
                available = False
            total += unit_price * qty
            total_qty += qty
            items.append({
                'product_id': pid,
                'qty': qty,
                'name': product.get('name') if product else pid,
                'price': unit_price,
                'stock': int(product.get('stock') or 0) if product else 0,
                'details': product.get('details') if product else '',
                'active': item_active,
                'missing': missing
            })
        enriched.append({
            'id': shortcut.get('id'),
            'name': shortcut.get('name'),
            'active': bool(shortcut.get('active', True)),
            'available': available,
            'total': total,
            'item_count': total_qty,
            'items': items
        })
    return enriched

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

def _extract_cancel_reason(order_notes, events=None):
    for ev in reversed(events or []):
        if str(ev.get('event_type') or '').strip().lower() != 'canceled':
            continue
        try:
            payload = json.loads(ev.get('payload_json') or '{}') or {}
        except Exception:
            payload = {}
        reason = str(payload.get('reason') or '').strip()
        if reason:
            return reason
    notes = str(order_notes or '')
    m = re.search(r'\[\s*Cancelado:\s*(.*?)\s*\]\s*$', notes, flags=re.IGNORECASE)
    if m:
        return str(m.group(1) or '').strip()
    return ''

def ensure_orders_delivery_columns(conn, cur):
    try:
        cur.execute("PRAGMA table_info(orders)")
        cols = [r[1] for r in (cur.fetchall() or [])]
        stmts = []
        if 'delivery_assigned_to' not in cols:
            stmts.append("ALTER TABLE orders ADD COLUMN delivery_assigned_to TEXT")
        if 'delivery_status' not in cols:
            stmts.append("ALTER TABLE orders ADD COLUMN delivery_status TEXT DEFAULT 'pending'")
        if 'delivery_sequence' not in cols:
            stmts.append("ALTER TABLE orders ADD COLUMN delivery_sequence INTEGER")
        if 'delivery_notes' not in cols:
            stmts.append("ALTER TABLE orders ADD COLUMN delivery_notes TEXT")
        if 'delivery_assigned_at' not in cols:
            stmts.append("ALTER TABLE orders ADD COLUMN delivery_assigned_at TEXT")
        if 'delivered_at' not in cols:
            stmts.append("ALTER TABLE orders ADD COLUMN delivered_at TEXT")
        if stmts:
            for s in stmts:
                cur.execute(s)
            conn.commit()
        return
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    pg_cols = [
        ("delivery_assigned_to", "delivery_assigned_to TEXT"),
        ("delivery_status", "delivery_status TEXT DEFAULT 'pending'"),
        ("delivery_sequence", "delivery_sequence INTEGER"),
        ("delivery_notes", "delivery_notes TEXT"),
        ("delivery_assigned_at", "delivery_assigned_at TEXT"),
        ("delivered_at", "delivered_at TEXT"),
    ]
    for _, ddl in pg_cols:
        try:
            cur.execute(f"ALTER TABLE orders ADD COLUMN IF NOT EXISTS {ddl}")
        except Exception:
            pass
    try:
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def ensure_delivery_run_tables(conn, cur):
    if is_postgres():
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_runs (
                id SERIAL PRIMARY KEY,
                tenant_slug TEXT NOT NULL,
                driver_username TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                started_at TEXT NOT NULL,
                closed_at TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_runs_tenant_driver_status ON delivery_runs(tenant_slug, driver_username, status)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS delivery_run_orders (
                id SERIAL PRIMARY KEY,
                run_id INTEGER NOT NULL,
                order_id INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                added_at TEXT NOT NULL,
                UNIQUE(run_id, order_id)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_run_seq ON delivery_run_orders(run_id, sequence)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_order ON delivery_run_orders(order_id)")
        return

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL,
            driver_username TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            started_at TEXT NOT NULL,
            closed_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_runs_tenant_driver_status ON delivery_runs(tenant_slug, driver_username, status)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_run_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            order_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            added_at TEXT NOT NULL,
            UNIQUE(run_id, order_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_run_seq ON delivery_run_orders(run_id, sequence)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_order ON delivery_run_orders(order_id)")

def _get_active_run_id(cur, tenant_slug, driver_username):
    cur.execute(
        "SELECT id FROM delivery_runs WHERE tenant_slug = ? AND lower(driver_username) = lower(?) AND status = 'open' ORDER BY id DESC LIMIT 1",
        (tenant_slug, driver_username),
    )
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None

def _auto_close_run_if_empty(cur, tenant_slug, driver_username):
    try:
        rid = _get_active_run_id(cur, tenant_slug, driver_username)
        if not rid:
            return False
        cur.execute(
            """
            SELECT COUNT(1)
            FROM delivery_run_orders ro
            JOIN orders o ON o.id = ro.order_id
            WHERE ro.run_id = ?
              AND o.tenant_slug = ?
              AND lower(COALESCE(o.delivery_assigned_to,'')) = lower(?)
              AND lower(COALESCE(o.delivery_status,'pending')) != 'delivered'
            """,
            (rid, tenant_slug, driver_username),
        )
        row = cur.fetchone()
        cnt = int(row[0] or 0) if row else 0
        if cnt > 0:
            return False
        now = datetime.utcnow().isoformat()
        cur.execute("UPDATE delivery_runs SET status = 'closed', closed_at = ? WHERE id = ?", (now, rid))
        return True
    except Exception:
        return False

def _create_run(conn, cur, tenant_slug, driver_username):
    now = datetime.utcnow().isoformat()
    if is_postgres():
        cur.execute(
            "INSERT INTO delivery_runs (tenant_slug, driver_username, status, started_at) VALUES (?, ?, 'open', ?) RETURNING id",
            (tenant_slug, driver_username, now),
        )
        rid = cur.fetchone()
        return int(rid[0])
    cur.execute(
        "INSERT INTO delivery_runs (tenant_slug, driver_username, status, started_at) VALUES (?, ?, 'open', ?)",
        (tenant_slug, driver_username, now),
    )
    return int(cur.lastrowid)

def _get_or_create_active_run(conn, cur, tenant_slug, driver_username):
    rid = _get_active_run_id(cur, tenant_slug, driver_username)
    if rid:
        return rid
    return _create_run(conn, cur, tenant_slug, driver_username)

def _next_run_sequence(cur, run_id):
    cur.execute("SELECT COALESCE(MAX(sequence), 0) FROM delivery_run_orders WHERE run_id = ?", (run_id,))
    row = cur.fetchone()
    n = int(row[0] or 0) if row else 0
    return n + 1

def _upsert_run_order(conn, cur, run_id, order_id, sequence=None):
    cur.execute("SELECT id, sequence FROM delivery_run_orders WHERE run_id = ? AND order_id = ?", (run_id, order_id))
    row = cur.fetchone()
    now = datetime.utcnow().isoformat()
    if row:
        if sequence is not None and int(row[1] or 0) != int(sequence):
            cur.execute("UPDATE delivery_run_orders SET sequence = ? WHERE run_id = ? AND order_id = ?", (int(sequence), run_id, order_id))
        return int(row[1] or 0)
    if sequence is None:
        sequence = _next_run_sequence(cur, run_id)
    try:
        cur.execute(
            "INSERT INTO delivery_run_orders (run_id, order_id, sequence, added_at) VALUES (?, ?, ?, ?)",
            (run_id, order_id, int(sequence), now),
        )
    except Exception:
        cur.execute("SELECT id, sequence FROM delivery_run_orders WHERE run_id = ? AND order_id = ?", (run_id, order_id))
        row2 = cur.fetchone()
        if row2 and row2[1] is not None:
            return int(row2[1] or 0)
    return int(sequence)

def ensure_orders_tenant_number_columns(conn, cur):
    if not is_postgres():
        try:
            cur.execute("PRAGMA table_info(orders)")
            cols = [r[1] for r in (cur.fetchall() or [])]
            if 'tenant_order_number' not in cols:
                cur.execute("ALTER TABLE orders ADD COLUMN tenant_order_number INTEGER")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tenant_counters (
                    tenant_slug TEXT PRIMARY KEY,
                    next_order_number INTEGER NOT NULL
                )
                """
            )
            try:
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_tenant_order_number ON orders(tenant_slug, tenant_order_number) WHERE tenant_order_number IS NOT NULL"
                )
            except Exception:
                pass
            conn.commit()
            return
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    try:
        cur.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS tenant_order_number INTEGER")
    except Exception:
        pass
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tenant_counters (
                tenant_slug TEXT PRIMARY KEY,
                next_order_number INTEGER NOT NULL
            )
            """
        )
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_tenant_order_number ON orders(tenant_slug, tenant_order_number) WHERE tenant_order_number IS NOT NULL"
        )
    except Exception:
        pass
    try:
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

def allocate_tenant_order_number(cur, tenant_slug):
    tenant_slug = str(tenant_slug or '').strip()
    if not tenant_slug:
        return None
    try:
        if is_postgres():
            cur.execute(
                "INSERT INTO tenant_counters (tenant_slug, next_order_number) VALUES (?, 2) "
                "ON CONFLICT (tenant_slug) DO UPDATE SET next_order_number = tenant_counters.next_order_number + 1 "
                "RETURNING next_order_number",
                (tenant_slug,),
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

def compute_total(items):
    total = 0
    for it in items:
        try:
            price = int(it.get('price', 0))
            qty = int(it.get('quantity', it.get('qty', 1)))
            total += price * qty
        except Exception:
            pass
    return total

def calculate_average_times(conn, slug):
    avgs = {}
    try:
        cur = conn.cursor()
        metrics = [
            ('time_mesa', 'mesa', 'listo'),
            ('time_espera', 'espera', 'listo'),
            ('time_delivery', 'direccion', 'entregado')
        ]
        limit_date = (datetime.utcnow() - timedelta(days=7)).isoformat()
        
        for cfg_key, otype, target_status in metrics:
            cur.execute(f"""
                SELECT o.created_at, h.changed_at 
                FROM orders o
                JOIN order_status_history h ON o.id = h.order_id
                WHERE o.tenant_slug = ? 
                  AND o.order_type = ? 
                  AND h.status = ?
                  AND o.created_at >= ?
                ORDER BY o.id DESC LIMIT 50
            """, (slug, otype, target_status, limit_date))
            
            rows = cur.fetchall()
            durations = []
            for r in rows:
                try:
                    start = datetime.fromisoformat(r[0])
                    if start.tzinfo is not None:
                         start = start.astimezone(timezone.utc).replace(tzinfo=None)
                         
                    end = datetime.fromisoformat(r[1])
                    if end.tzinfo is not None:
                         end = end.astimezone(timezone.utc).replace(tzinfo=None)
                         
                    diff = (end - start).total_seconds() / 60
                    if 2 < diff < 180:
                        durations.append(diff)
                except Exception as e:
                    # print(f"Error parsing dates in orders.py: {e}") 
                    pass
            
            if durations:
                avgs[cfg_key] = int(sum(durations) / len(durations))
    except Exception as e:
        print(f"Error calculating auto times: {e}")
        pass
    return avgs

@bp.route('/config', methods=['GET'])
def get_tenant_config():
    slug = request.args.get('slug') or 'gastronomia-local1'
    
    cfg = get_cached_tenant_config(slug)
    if 'require_order_approval' not in cfg:
        cfg = cfg.copy()
        cfg['require_order_approval'] = True
    default_fail_reasons = [
        "No atiende",
        "Dirección incorrecta",
        "Reprogramar",
        "No tiene efectivo / Pago pendiente",
        "No se pudo acceder",
    ]
    r = cfg.get('delivery_fail_reasons')
    if not isinstance(r, list) or not [str(x).strip() for x in r if str(x).strip()]:
        cfg = cfg.copy()
        cfg['delivery_fail_reasons'] = default_fail_reasons
            
    if cfg.get('time_auto'):
        conn = get_db()
        auto_times = calculate_average_times(conn, slug)
        # Create copy to avoid mutating cache
        cfg = cfg.copy()
        for k, v in auto_times.items():
            if v > 0:
                cfg[k] = v
                
    resp = jsonify(cfg)
    resp.headers['Cache-Control'] = 'no-store, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

@bp.route('/config', methods=['POST'])
def update_tenant_config():
    if not is_authed(): return jsonify({'error': 'unauthorized'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    slug = payload.get('slug') or 'gastronomia-local1'
    _, _, role, _, owner = _ctx()
    session_tenant = str(session.get('tenant_slug') or '').strip()
    if session_tenant and slug and session_tenant != slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    can_manage_admin_order_settings = bool(owner or role == 'admin')
    if 'shipping_cost' in payload and not can_manage_admin_order_settings:
        return jsonify({'error': 'solo admin puede modificar el costo de envío'}), 403
    if 'shipping_distance' in payload and not can_manage_admin_order_settings:
        return jsonify({'error': 'solo admin puede modificar la configuración de envío por distancia'}), 403
    
    conn = get_db()
    cur = conn.cursor()
    current_cfg = _load_tenant_config_row(cur, slug)
    
    for key in ['shipping_cost', 'time_mesa', 'time_espera', 'time_delivery']:
        if key in payload:
            try:
                current_cfg[key] = int(payload[key])
            except:
                pass

    if 'shipping_distance' in payload:
        raw = payload.get('shipping_distance')
        if isinstance(raw, dict):
            enabled = bool(raw.get('enabled'))
            origin = raw.get('origin') if isinstance(raw.get('origin'), dict) else {}
            o_lat, o_lng = _extract_lat_lng(origin)
            included_km = _safe_float(raw.get('included_km'), 0.0)
            extra_per_km = _safe_int(raw.get('extra_per_km'), 0)
            max_cost = _safe_int(raw.get('max_cost'), 0)
            normalized = {
                'enabled': enabled,
                'origin': {'lat': o_lat, 'lng': o_lng} if (o_lat is not None and o_lng is not None) else {},
                'included_km': float(included_km or 0.0),
                'extra_per_km': int(extra_per_km or 0),
                'max_cost': int(max_cost or 0),
            }
            current_cfg['shipping_distance'] = normalized
            
    if 'time_auto' in payload:
        current_cfg['time_auto'] = bool(payload['time_auto'])

    if 'delivery_fail_reasons' in payload:
        reasons = payload.get('delivery_fail_reasons')
        parsed = []
        if isinstance(reasons, list):
            parsed = [str(x).strip() for x in reasons if str(x).strip()]
        else:
            raw = str(reasons or '')
            parsed = [s.strip() for s in raw.splitlines() if s.strip()]
        current_cfg['delivery_fail_reasons'] = parsed

    if 'require_order_approval' in payload:
        current_cfg['require_order_approval'] = bool(payload.get('require_order_approval'))
    
    cur.execute("INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)", (slug, json.dumps(current_cfg, ensure_ascii=False)))
    conn.commit()
    invalidate_tenant_config(slug)
    resp = jsonify(current_cfg)
    resp.headers['Cache-Control'] = 'no-store, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

@bp.route('/quick_order_shortcuts', methods=['GET'])
def get_quick_order_shortcuts():
    if not is_authed():
        return jsonify({'error': 'unauthorized'}), 401
    slug = request.args.get('slug') or request.args.get('tenant_slug') or 'gastronomia-local1'
    session_tenant, _, role, perms, owner = _ctx()
    if session_tenant and slug and session_tenant != slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not _has_perm(perms, owner, role, 'orders_create'):
        return jsonify({'error': 'sin permisos para usar pedidos frecuentes'}), 403
    conn = get_db()
    cur = conn.cursor()
    cfg = get_cached_tenant_config(slug) or {}
    shortcuts = _normalize_quick_order_shortcuts(cfg.get('quick_order_shortcuts') or [])
    return jsonify({
        'tenant_slug': slug,
        'shortcuts': _enrich_quick_order_shortcuts(cur, slug, shortcuts)
    })

@bp.route('/quick_order_shortcuts', methods=['POST'])
def save_quick_order_shortcuts():
    if not is_authed():
        return jsonify({'error': 'unauthorized'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    slug = payload.get('slug') or payload.get('tenant_slug') or 'gastronomia-local1'
    session_tenant, _, role, _, owner = _ctx()
    if session_tenant and slug and session_tenant != slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if not (owner or role == 'admin'):
        return jsonify({'error': 'solo admin puede modificar pedidos frecuentes'}), 403
    shortcuts = _normalize_quick_order_shortcuts(payload.get('shortcuts') or [])
    conn = get_db()
    cur = conn.cursor()
    current_cfg = _load_tenant_config_row(cur, slug)
    current_cfg['quick_order_shortcuts'] = shortcuts
    cur.execute("INSERT OR REPLACE INTO tenant_config (tenant_slug, config_json) VALUES (?, ?)", (slug, json.dumps(current_cfg, ensure_ascii=False)))
    conn.commit()
    invalidate_tenant_config(slug)
    return jsonify({
        'ok': True,
        'tenant_slug': slug,
        'shortcuts': _enrich_quick_order_shortcuts(cur, slug, shortcuts)
    })

@bp.route('/orders', methods=['POST'])
def create_order():
    try:
        payload = request.get_json(silent=True) or {}
        tenant_slug = payload.get('tenant_slug') or payload.get('slug') or 'gastronomia-local1'
        order_type = (payload.get('order_type') or 'mesa').lower()
        
        if order_type not in ('mesa', 'direccion', 'espera', 'none'):
            return jsonify({'error': 'order_type inválido'}), 400
            
        table_number = payload.get('table_number') or ''
        address_json = payload.get('address') or {}
        if isinstance(address_json, str):
            raw = address_json.strip()
            if raw:
                try:
                    address_json = json.loads(raw)
                except Exception:
                    address_json = {'address': raw}
            else:
                address_json = {}
        if not isinstance(address_json, dict):
            address_json = {}
        items = payload.get('items') or []
        customer_name = payload.get('customer_name') or ''
        customer_phone = payload.get('customer_phone') or ''

        if order_type == 'mesa' and not table_number:
            return jsonify({'error': 'Número de mesa requerido'}), 400
        if order_type == 'direccion' and not address_json:
            return jsonify({'error': 'Dirección requerida'}), 400
        if order_type == 'espera':
            if not customer_name:
                return jsonify({'error': 'Nombre requerido para pedidos en espera'}), 400
            if not customer_phone:
                return jsonify({'error': 'Teléfono requerido para pedidos en espera'}), 400
        if not items:
            return jsonify({'error': 'Carrito vacío'}), 400

        total = compute_total(items)
        created_at = datetime.utcnow().isoformat() + 'Z'
        status = 'pendiente'

        conn = get_db()
        cur = conn.cursor()
        try:
            ensure_orders_tenant_number_columns(conn, cur)
        except Exception:
            pass

        cfg = {}
        try:
            cfg = get_cached_tenant_config(tenant_slug) or {}
        except Exception:
            cfg = {}

        is_admin_origin = False
        try:
            is_admin_origin = bool(is_authed() and check_csrf())
        except Exception:
            is_admin_origin = False

        if bool(cfg.get('require_order_approval')) and (not is_admin_origin):
            status = 'por_aprobar'
        
        shipping_cost = 0
        distance_km = None
        shipping_manual = None
        if order_type == 'direccion':
            try:
                if is_admin_origin:
                    try:
                        _, _, role, perms, owner = _ctx()
                        allow_manual = _has_perm(perms, owner, role, 'orders_create')
                    except Exception:
                        allow_manual = False
                    if allow_manual and 'shipping_cost' in payload:
                        try:
                            shipping_manual = int(payload.get('shipping_cost') or 0)
                        except Exception:
                            shipping_manual = None
                        if shipping_manual is not None:
                            shipping_manual = max(0, min(1000000, int(shipping_manual)))
                if shipping_manual is not None:
                    shipping_cost = shipping_manual
                    distance_km = None
                else:
                    shipping_cost, distance_km = _compute_shipping_cost(cfg, order_type, address_json)
            except Exception:
                try:
                    shipping_cost = int(cfg.get('shipping_cost', 0))
                except Exception:
                    shipping_cost = 0
        
        total += shipping_cost
        
        order_notes = (payload.get('order_notes') or '').strip()
        tenant_order_number = allocate_tenant_order_number(cur, tenant_slug)
        creation_actor, creation_meta = _build_order_creation_event(
            is_admin_origin,
            actor=session.get('admin_user') or '',
            customer_name=customer_name,
            customer_phone=customer_phone,
        )
        
        # Insert Order
        try:
            cur.execute(
                """
                INSERT INTO orders (tenant_slug, tenant_order_number, customer_name, customer_phone, order_type, table_number, address_json, status, total, payment_method, payment_status, created_at, order_notes, shipping_cost)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (tenant_slug, tenant_order_number, customer_name, customer_phone, order_type, table_number, json.dumps(address_json, ensure_ascii=False), status, total, None, None, created_at, order_notes, shipping_cost)
            )
            order_id = cur.lastrowid
        except Exception as e:
            print(f"Error executing INSERT orders: {e}")
            raise e

        cur.execute(
            "INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                order_id,
                'created',
                creation_actor,
                0,
                json.dumps(dict(
                    creation_meta or {},
                    **({
                        'shipping_km': distance_km
                    } if isinstance(distance_km, (int, float)) and math.isfinite(float(distance_km)) else {}),
                    **({
                        'shipping_manual': shipping_manual
                    } if isinstance(shipping_manual, int) else {}),
                )),
                created_at,
            ),
        )

        items_total = 0
        # Process Items
        for it in items:
            qty = int(it.get('quantity', it.get('qty', 1)) or 1)
            pid = it.get('product_id') or it.get('base_id') or it.get('id')
            pack_id = str(it.get('pack_id') or '').strip()
            pack_label = str(it.get('pack_label') or '').strip()
            try:
                pack_size = int(it.get('pack_size') or 1)
            except Exception:
                pack_size = 1
            if pack_size <= 0:
                pack_size = 1
            requested_price = int(it.get('price', 0) or 0)
            
            # Check/Create Product
            cur.execute("SELECT stock, COALESCE(variants_json, '') FROM products WHERE tenant_slug = ? AND product_id = ?", (tenant_slug, pid))
            row = cur.fetchone()
            if not row:
                try:
                    nm = str(it.get('name') or '').strip() or 'Producto'
                    pr = requested_price
                    # Using INSERT OR IGNORE wrapper logic in database.py
                    cur.execute(
                        "INSERT OR IGNORE INTO products (tenant_slug, product_id, name, price, stock, active) VALUES (?, ?, ?, ?, ?, 1)",
                        (tenant_slug, pid, nm, max(0, pr), 1000)
                    )
                    conn.commit()
                    # Re-fetch
                    cur.execute("SELECT stock, COALESCE(variants_json, '') FROM products WHERE tenant_slug = ? AND product_id = ?", (tenant_slug, pid))
                    row = cur.fetchone()
                except Exception as e:
                    print(f"Error auto-creating product {pid}: {e}")
                    conn.rollback()
                    return jsonify({'error': 'producto no encontrado y fallo al crear', 'product_id': pid}), 400
            
            stock = int((row[0] if row else 0) or 0)
            variants_raw = row[1] if row and len(row) > 1 else ''
            variants = _parse_variants_json(variants_raw)
            modifiers = it.get('modifiers') or {}
            if not isinstance(modifiers, dict):
                modifiers = {}
            unit_price = requested_price
            try:
                mixed_price, modifiers = _resolve_mix_price(cur, tenant_slug, pid, variants, modifiers)
                if mixed_price is not None and mixed_price > 0:
                    unit_price = mixed_price
            except ValueError as e:
                conn.rollback()
                return jsonify({'error': str(e), 'product_id': pid}), 400
            if pack_id and variants:
                try:
                    packs = variants.get('packs') or variants.get('pack_options') or variants.get('sale_packs') or []
                    if isinstance(packs, str):
                        packs = json.loads(packs or '[]') or []
                    if isinstance(packs, list):
                        for p in packs:
                            if not isinstance(p, dict):
                                continue
                            if str(p.get('id') or '').strip() != pack_id:
                                continue
                            try:
                                p_price = int(p.get('price') or 0)
                                if p_price > 0:
                                    unit_price = p_price
                            except Exception:
                                pass
                            try:
                                p_size = int(p.get('size') or p.get('qty') or p.get('multiplier') or p.get('units') or 1)
                                if p_size > 0:
                                    pack_size = p_size
                            except Exception:
                                pass
                            if not pack_label:
                                pack_label = str(p.get('label') or p.get('name') or '').strip()
                            break
                except Exception:
                    pass
            inv_qty = qty * pack_size
            if stock < inv_qty:
                conn.rollback()
                return jsonify({'error': 'stock insuficiente', 'product_id': pid, 'stock': stock, 'requested': inv_qty}), 400
            
            # Insert Order Item
            if pack_id:
                modifiers['pack'] = {'id': pack_id, 'label': pack_label, 'size': pack_size}
            cur.execute(
                """
                INSERT INTO order_items (order_id, tenant_slug, product_id, name, qty, unit_price, modifiers_json, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    tenant_slug,
                    pid,
                    it.get('name'),
                    qty,
                    unit_price,
                    json.dumps(modifiers, ensure_ascii=False),
                    it.get('notes') or ''
                )
            )
            items_total += unit_price * qty
            
            # Update Stock
            cur.execute("UPDATE products SET stock = stock - ? WHERE tenant_slug = ? AND product_id = ?", (inv_qty, tenant_slug, pid))
        
        total = int(items_total) + int(shipping_cost)
        cur.execute("UPDATE orders SET total = ? WHERE id = ?", (total, order_id))
        conn.commit()
        return jsonify({'order_id': order_id, 'tenant_order_number': tenant_order_number, 'status': status, 'total': total, 'tenant_slug': tenant_slug}), 201

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"CRITICAL ERROR in create_order: {e}")
        return jsonify({'error': f'Error interno crítico: {str(e)}', 'details': str(e)}), 500

@bp.route('/orders', methods=['GET'])
def list_orders():
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    status = request.args.get('status')
    limit = int(request.args.get('limit') or 50)
    offset = int(request.args.get('offset') or 0)
    q = request.args.get('q')
    qid_param = request.args.get('id')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    exclude_archived = request.args.get('exclude_archived')
    date_field = str(request.args.get('date_field') or 'created').strip().lower()
    if date_field not in ('created', 'closed'):
        date_field = 'created'
    use_closed_date = (date_field == 'closed' and str(status or '').strip().lower() in ('entregado', 'cancelado'))
    
    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_orders_tenant_number_columns(conn, cur)
    except Exception:
        pass
    if use_closed_date:
        base = (
            "SELECT o.id, o.tenant_slug, o.tenant_order_number, o.order_type, o.table_number, o.address_json, o.status, o.total, o.created_at, "
            "o.customer_phone, o.customer_name, o.payment_status, o.payment_method, o.tip_amount, o.shipping_cost, o.delivery_assigned_to, "
            "o.delivery_status, o.delivery_sequence, o.delivery_notes, o.delivery_assigned_at, o.delivered_at, h.last_change AS closed_at "
            "FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = ? GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ?"
        )
        params = [str(status).strip().lower(), tenant_slug]
    else:
        base = "SELECT id, tenant_slug, tenant_order_number, order_type, table_number, address_json, status, total, created_at, customer_phone, customer_name, payment_status, payment_method, tip_amount, shipping_cost, delivery_assigned_to, delivery_status, delivery_sequence, delivery_notes, delivery_assigned_at, delivered_at FROM orders WHERE tenant_slug = ?"
        params = [tenant_slug]
    if exclude_archived == 'true':
        base += " AND " + ("o.id" if use_closed_date else "id") + " NOT IN (SELECT order_id FROM archived_orders)"
    if status:
        base += " AND " + ("o.status" if use_closed_date else "status") + " = ?"
        params.append(status)
    if qid_param:
        try:
            exact_id = int(qid_param)
            visible_num = _parse_visible_order_number(qid_param)
            if visible_num:
                base += " AND (" + ("o.id" if use_closed_date else "id") + " = ? OR " + ("o.tenant_order_number" if use_closed_date else "tenant_order_number") + " = ?)"
                params.extend([exact_id, visible_num])
            else:
                base += " AND " + ("o.id" if use_closed_date else "id") + " = ?"
                params.append(exact_id)
        except:
            pass
    elif q:
        visible_num = _parse_visible_order_number(q)
        if visible_num is not None:
            try:
                qid = int(str(q).strip())
                base += " AND (" + ("o.id" if use_closed_date else "id") + " = ? OR " + ("o.tenant_order_number" if use_closed_date else "tenant_order_number") + " = ?)"
                params.extend([qid, visible_num])
            except Exception:
                base += " AND " + ("o.tenant_order_number" if use_closed_date else "tenant_order_number") + " = ?"
                params.append(visible_num)
        else:
            like = f"%{q}%"
            base += (
                " AND (COALESCE(" + ("o.address_json" if use_closed_date else "address_json") + ",'') LIKE ?"
                " OR COALESCE(" + ("o.customer_name" if use_closed_date else "customer_name") + ",'') LIKE ?"
                " OR COALESCE(" + ("o.customer_phone" if use_closed_date else "customer_phone") + ",'') LIKE ?"
                " OR COALESCE(" + ("o.table_number" if use_closed_date else "table_number") + ",'') LIKE ?)"
            )
            params.extend([like, like, like, like])
    if from_date:
        base += " AND " + ("h.last_change" if use_closed_date else "created_at") + " >= ?"
        params.append(from_date)
    if to_date:
        base += " AND " + ("h.last_change" if use_closed_date else "created_at") + " <= ?"
        params.append(to_date)
    base += " ORDER BY " + ("h.last_change DESC, o.id DESC" if use_closed_date else "id DESC") + " LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    cur.execute(base, params)
    rows = cur.fetchall()
    data = [dict(r) for r in rows]
    
    # Count query (simplified for brevity)
    if use_closed_date:
        count_sql = (
            "SELECT COUNT(*) "
            "FROM orders o "
            "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = ? GROUP BY order_id) h ON h.order_id = o.id "
            "WHERE o.tenant_slug = ?"
        )
        count_params = [str(status).strip().lower(), tenant_slug]
    else:
        count_sql = "SELECT COUNT(*) FROM orders WHERE tenant_slug = ?"
        count_params = [tenant_slug]
    if exclude_archived == 'true':
        count_sql += " AND " + ("o.id" if use_closed_date else "id") + " NOT IN (SELECT order_id FROM archived_orders)"
    if status:
        count_sql += " AND " + ("o.status" if use_closed_date else "status") + " = ?"
        count_params.append(status)
    if qid_param:
        try:
            exact_id = int(qid_param)
            visible_num = _parse_visible_order_number(qid_param)
            if visible_num:
                count_sql += " AND (" + ("o.id" if use_closed_date else "id") + " = ? OR " + ("o.tenant_order_number" if use_closed_date else "tenant_order_number") + " = ?)"
                count_params.extend([exact_id, visible_num])
            else:
                count_sql += " AND " + ("o.id" if use_closed_date else "id") + " = ?"
                count_params.append(exact_id)
        except Exception:
            pass
    elif q:
        visible_num = _parse_visible_order_number(q)
        if visible_num is not None:
            try:
                qid = int(str(q).strip())
                count_sql += " AND (" + ("o.id" if use_closed_date else "id") + " = ? OR " + ("o.tenant_order_number" if use_closed_date else "tenant_order_number") + " = ?)"
                count_params.extend([qid, visible_num])
            except Exception:
                count_sql += " AND " + ("o.tenant_order_number" if use_closed_date else "tenant_order_number") + " = ?"
                count_params.append(visible_num)
        else:
            like = f"%{q}%"
            count_sql += (
                " AND (COALESCE(" + ("o.address_json" if use_closed_date else "address_json") + ",'') LIKE ?"
                " OR COALESCE(" + ("o.customer_name" if use_closed_date else "customer_name") + ",'') LIKE ?"
                " OR COALESCE(" + ("o.customer_phone" if use_closed_date else "customer_phone") + ",'') LIKE ?"
                " OR COALESCE(" + ("o.table_number" if use_closed_date else "table_number") + ",'') LIKE ?)"
            )
            count_params.extend([like, like, like, like])
    if from_date:
        count_sql += " AND " + ("h.last_change" if use_closed_date else "created_at") + " >= ?"
        count_params.append(from_date)
    if to_date:
        count_sql += " AND " + ("h.last_change" if use_closed_date else "created_at") + " <= ?"
        count_params.append(to_date)

    cur.execute(count_sql, count_params)
    total_count = cur.fetchone()[0]
    
    resp = jsonify({'orders': data, 'count': len(data), 'total': total_count, 'limit': limit, 'offset': offset})
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

@bp.route('/orders/<int:order_id>', methods=['GET'])
def get_order_detail(order_id):
    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_orders_tenant_number_columns(conn, cur)
    except Exception:
        pass
    cur.execute(
        """
        SELECT id, tenant_slug, tenant_order_number, customer_name, customer_phone, order_type, table_number, address_json, status, total, payment_method, payment_status, created_at, order_notes, tip_amount, shipping_cost, delivery_assigned_to, delivery_status, delivery_sequence, delivery_notes, delivery_assigned_at, delivered_at
        FROM orders WHERE id = ?
        """,
        (order_id,)
    )
    order_row = cur.fetchone()
    if not order_row:
        return jsonify({'error': 'Orden no encontrada'}), 404
    
    cur.execute(
        """
        SELECT id, product_id, name, qty, unit_price, modifiers_json, notes
        FROM order_items WHERE order_id = ? ORDER BY id ASC
        """,
        (order_id,)
    )
    item_rows = cur.fetchall()
    
    cur.execute("SELECT status, changed_at, changed_by FROM order_status_history WHERE order_id = ? ORDER BY id ASC", (order_id,))
    hist_rows = cur.fetchall()

    cur.execute(
        "SELECT event_type, actor, terminal, amount_delta, payload_json, created_at FROM order_events WHERE order_id = ? ORDER BY id ASC",
        (order_id,)
    )
    ev_rows = cur.fetchall()
    
    order = dict(order_row)
    items = [dict(r) for r in item_rows]
    history = [dict(r) for r in hist_rows]
    events = [dict(r) for r in ev_rows]
    order['cancel_reason'] = _extract_cancel_reason(order.get('order_notes'), events)
    try:
        closing_hist = next((h for h in reversed(history) if str(h.get('status') or '').strip().lower() in ('entregado', 'cancelado')), None)
    except Exception:
        closing_hist = None
    if closing_hist:
        if not order.get('closed_at'):
            order['closed_at'] = closing_hist.get('changed_at')
        order['closed_by'] = closing_hist.get('changed_by') or ''
    else:
        order['closed_at'] = order.get('delivered_at') or ''
        order['closed_by'] = ''
    try:
        payment_ev = next((e for e in reversed(events) if str(e.get('event_type') or '').strip().lower() == 'payment'), None)
    except Exception:
        payment_ev = None
    order['paid_at'] = payment_ev.get('created_at') if payment_ev else ''
    
    # Si no es admin, retornar versión sanitizada (seguridad)
    if not is_authed():
        sanitized_order = {
            'id': order['id'],
            'tenant_slug': order['tenant_slug'],
            'tenant_order_number': order.get('tenant_order_number'),
            'status': order['status'],
            'total': order['total'],
            'created_at': order['created_at'],
            'order_type': order['order_type'],
            'table_number': order['table_number']
        }
        return jsonify({'order': sanitized_order, 'items': items})
    
    return jsonify({'order': order, 'items': items, 'history': history, 'events': events})

@bp.route('/orders/<int:order_id>/status', methods=['PATCH'])
def update_order_status(order_id):
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'orders_update_status'):
        return jsonify({'error': 'sin permisos'}), 403
    
    payload = request.get_json(silent=True) or {}
    new_status = payload.get('status')
    reason = (payload.get('reason') or '').strip()
    if new_status not in ('por_aprobar', 'pendiente', 'preparacion', 'listo', 'en_camino', 'entregado', 'cancelado'):
        return jsonify({'error': 'status inválido'}), 400
        
    conn = get_db()
    cur = conn.cursor()
    
    # Security Check: Prevent modifying finalized orders
    cur.execute("SELECT status, tenant_slug, order_type, COALESCE(delivery_status,''), delivered_at FROM orders WHERE id = ?", (order_id,))
    row_check = cur.fetchone()
    if row_check and row_check[0] == 'entregado' and new_status != 'entregado':
         return jsonify({'error': 'no se puede cambiar el estado de una orden entregada. Utilice la función de anulación/reembolso si es necesario.'}), 400
    if not row_check:
        return jsonify({'error': 'orden no encontrada'}), 404
    current_status, tenant_slug, order_type, current_delivery_status, delivered_at = row_check
    tenant_slug = str(tenant_slug or '')
    order_type = str(order_type or '').strip().lower()
    current_status = str(current_status or '').strip().lower()

    # Cuando el tenant exige aprobación, la transición inicial debe pasar por pendiente.
    if current_status == 'por_aprobar' and new_status not in ('pendiente', 'cancelado'):
        return jsonify({'error': 'el pedido debe aprobarse antes de avanzar de estado'}), 400

    if new_status == 'entregado':
        if session_tenant and tenant_slug and session_tenant != tenant_slug:
            return jsonify({'error': 'acceso denegado al tenant'}), 403
        scope = _scope_for(role, owner=owner)
        if scope == 'user':
            cur.execute(
                "SELECT id FROM cash_sessions WHERE tenant_slug = ? AND scope = 'user' AND closed_at IS NULL AND lower(opened_by) = lower(?) ORDER BY opened_at DESC LIMIT 1",
                (tenant_slug, actor or ''),
            )
        else:
            cur.execute("SELECT id FROM cash_sessions WHERE tenant_slug = ? AND scope = 'tenant' AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1", (tenant_slug,))
        if not cur.fetchone():
            return jsonify({'error': 'no hay sesión de caja abierta'}), 400
            
    if new_status == 'entregado' and order_type == 'direccion':
        now = datetime.utcnow().isoformat()
        delivered_ts = delivered_at or now
        cur.execute(
            "UPDATE orders SET status = ?, delivery_status = 'delivered', delivered_at = ? WHERE id = ?",
            (new_status, delivered_ts, order_id),
        )
    else:
        cur.execute("UPDATE orders SET status = ? WHERE id = ?", (new_status, order_id))
    if new_status == 'cancelado' and reason:
        cur.execute("UPDATE orders SET order_notes = COALESCE(order_notes, '') || ? WHERE id = ?", (f" [Cancelado: {reason}]", order_id))
        
    actor = session.get('admin_user') or ''
    cur.execute("INSERT INTO order_status_history (order_id, status, changed_at, changed_by) VALUES (?, ?, ?, ?)", (order_id, new_status, datetime.utcnow().isoformat(), actor))
    conn.commit()
    
    # Event log
    try:
        meta = {}
        if reason and new_status == 'cancelado': meta['reason'] = reason
        cur.execute(
            "INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, ('canceled' if new_status == 'cancelado' else 'status_change'), actor, 0, json.dumps(meta), datetime.utcnow().isoformat())
        )
        conn.commit()
    except:
        pass
        
    return jsonify({'order_id': order_id, 'status': new_status})

@bp.route('/delivery/orders', methods=['GET'])
def list_delivery_orders():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'orders_view'):
        return jsonify({'error': 'sin permisos'}), 403

    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or session_tenant or 'gastronomia-local1'
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403

    f = str(request.args.get('filter') or '').strip().lower()
    if not f:
        f = 'unassigned' if role == 'repartidor' else 'all'
    delivery_status = (request.args.get('delivery_status') or '').strip().lower()
    exclude_archived = request.args.get('exclude_archived')
    exclude_canceled = request.args.get('exclude_canceled')
    if exclude_canceled is None:
        exclude_canceled = 'true'
    limit = int(request.args.get('limit') or 100)
    offset = int(request.args.get('offset') or 0)
    q = (request.args.get('q') or '').strip()

    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_orders_delivery_columns(conn, cur)
    except Exception:
        pass
    try:
        ensure_orders_tenant_number_columns(conn, cur)
    except Exception:
        pass
    sql = (
        "SELECT id, tenant_slug, tenant_order_number, customer_name, customer_phone, order_type, table_number, address_json, status, total, "
        "payment_method, payment_status, tip_amount, shipping_cost, created_at, order_notes, "
        "delivery_assigned_to, delivery_status, delivery_sequence, delivery_notes, delivery_assigned_at, delivered_at, "
        "CASE WHEN "
        "COALESCE((SELECT MAX(created_at) FROM order_events WHERE order_id = orders.id AND event_type = 'delivery_unassign'), '') != '' "
        "AND COALESCE((SELECT MAX(created_at) FROM order_events WHERE order_id = orders.id AND event_type = 'delivery_unassign'), '') > "
        "COALESCE((SELECT MAX(created_at) FROM order_events WHERE order_id = orders.id AND event_type = 'delivery_assign'), '') "
        "THEN 1 ELSE 0 END AS delivery_returned "
        "FROM orders WHERE tenant_slug = ? AND lower(trim(COALESCE(order_type,''))) = 'direccion'"
    )
    params = [tenant_slug]
    if exclude_archived == 'true':
        sql += " AND id NOT IN (SELECT order_id FROM archived_orders)"
    if str(exclude_canceled).strip().lower() == 'true':
        sql += " AND lower(COALESCE(status,'')) != 'cancelado'"
    if delivery_status:
        sql += " AND lower(COALESCE(delivery_status, '')) = lower(?)"
        params.append(delivery_status)
    if f == 'mine':
        sql += " AND lower(COALESCE(delivery_assigned_to, '')) = lower(?)"
        params.append(actor or '')
    elif f == 'unassigned':
        sql += " AND (delivery_assigned_to IS NULL OR trim(COALESCE(delivery_assigned_to,'')) = '')"
    elif f == 'assigned':
        sql += " AND (delivery_assigned_to IS NOT NULL AND trim(COALESCE(delivery_assigned_to,'')) != '')"
    elif f == 'open':
        sql += " AND lower(COALESCE(delivery_status, 'pending')) != 'delivered' AND lower(COALESCE(status,'')) != 'entregado'"
    if q:
        try:
            qid = int(q)
            sql += " AND id = ?"
            params.append(qid)
        except Exception:
            like = f"%{q}%"
            sql += " AND (COALESCE(address_json,'') LIKE ? OR COALESCE(customer_name,'') LIKE ? OR COALESCE(customer_phone,'') LIKE ?)"
            params.extend([like, like, like])

    sql += (
        " ORDER BY "
        "CASE lower(COALESCE(delivery_status, 'pending')) "
        "WHEN 'pending' THEN 0 WHEN 'assigned' THEN 1 WHEN 'en_route' THEN 2 WHEN 'failed' THEN 3 WHEN 'delivered' THEN 4 ELSE 9 END, "
        "COALESCE(delivery_sequence, 999999) ASC, id ASC "
        "LIMIT ? OFFSET ?"
    )
    params.extend([limit, offset])
    cur.execute(sql, params)
    rows = cur.fetchall()
    return jsonify({'orders': [dict(r) for r in rows], 'limit': limit, 'offset': offset})

@bp.route('/delivery/orders/<int:order_id>/assign', methods=['PATCH'])
def assign_delivery_order(order_id):
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'delivery_manage'):
        return jsonify({'error': 'sin permisos'}), 403

    payload = request.get_json(silent=True) or {}
    assigned_to = str(payload.get('assigned_to') or actor or '').strip()
    if not assigned_to:
        return jsonify({'error': 'assigned_to requerido'}), 400
    if not (owner or role == 'admin') and assigned_to.lower() != (actor or '').lower():
        return jsonify({'error': 'sin permisos para asignar a otro usuario'}), 403

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT tenant_slug, order_type, status, COALESCE(delivery_assigned_to,'') FROM orders WHERE id = ?", (order_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({'error': 'orden no encontrada'}), 404
    tenant_slug, order_type, st, current_assigned = row
    tenant_slug = str(tenant_slug or '')
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if str(order_type or '').strip().lower() != 'direccion':
        return jsonify({'error': 'orden no es de delivery'}), 400
    if str(st or '').strip().lower() == 'cancelado':
        return jsonify({'error': 'orden cancelada'}), 400

    now = datetime.utcnow().isoformat()
    cur.execute(
        "UPDATE orders SET delivery_assigned_to = ?, delivery_assigned_at = ?, "
        "delivery_status = CASE WHEN delivery_status IS NULL OR trim(COALESCE(delivery_status,'')) = '' OR lower(delivery_status) = 'pending' THEN 'assigned' ELSE delivery_status END "
        "WHERE id = ?",
        (assigned_to, now, order_id),
    )
    try:
        ensure_delivery_run_tables(conn, cur)
        if str(current_assigned or '').strip() and str(current_assigned or '').strip().lower() != assigned_to.lower():
            prev_run_id = _get_active_run_id(cur, tenant_slug, str(current_assigned or '').strip())
            if prev_run_id:
                cur.execute("DELETE FROM delivery_run_orders WHERE run_id = ? AND order_id = ?", (prev_run_id, order_id))
        run_id = _get_or_create_active_run(conn, cur, tenant_slug, assigned_to)
        seq = _upsert_run_order(conn, cur, run_id, order_id, None)
        cur.execute("UPDATE orders SET delivery_sequence = ? WHERE id = ? AND tenant_slug = ?", (seq, order_id, tenant_slug))
    except Exception:
        pass
    try:
        cur.execute(
            "INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, 'delivery_assign', actor or '', 0, json.dumps({'assigned_to': assigned_to, 'prev_assigned_to': str(current_assigned or '').strip()}), now),
        )
    except Exception:
        pass
    conn.commit()
    return jsonify({'order_id': order_id, 'assigned_to': assigned_to, 'delivery_status': 'assigned'})

@bp.route('/delivery/orders/<int:order_id>/delivery_status', methods=['PATCH'])
def update_delivery_status(order_id):
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'delivery_manage'):
        return jsonify({'error': 'sin permisos'}), 403

    payload = request.get_json(silent=True) or {}
    raw = str(payload.get('delivery_status') or payload.get('status') or '').strip().lower()
    m = {
        'pendiente': 'pending',
        'asignado': 'assigned',
        'en_camino': 'en_route',
        'entregado': 'delivered',
        'fallo': 'failed',
    }
    new_status = m.get(raw, raw)
    if new_status not in ('pending', 'assigned', 'en_route', 'delivered', 'failed'):
        return jsonify({'error': 'delivery_status inválido'}), 400

    delivery_notes = payload.get('delivery_notes')
    if delivery_notes is None:
        delivery_notes = payload.get('notes')
    if delivery_notes is not None:
        delivery_notes = str(delivery_notes).strip()
        if delivery_notes == '':
            delivery_notes = None

    if new_status == 'failed' and not delivery_notes:
        return jsonify({'error': 'motivo requerido'}), 400

    seq = payload.get('delivery_sequence')
    seq_val = None
    if seq is not None:
        try:
            seq_val = int(seq)
        except Exception:
            return jsonify({'error': 'delivery_sequence inválido'}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT tenant_slug, order_type, status, COALESCE(delivery_assigned_to,''), COALESCE(delivery_status,'pending') FROM orders WHERE id = ?",
        (order_id,),
    )
    row = cur.fetchone()
    if not row:
        return jsonify({'error': 'orden no encontrada'}), 404
    tenant_slug, order_type, st, assigned_to, current_delivery_status = row
    tenant_slug = str(tenant_slug or '')
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if str(order_type or '').strip().lower() != 'direccion':
        return jsonify({'error': 'orden no es de delivery'}), 400

    if role == 'repartidor' and (assigned_to or '').strip() and (assigned_to or '').strip().lower() != (actor or '').lower():
        return jsonify({'error': 'orden asignada a otro repartidor'}), 403
    if role == 'repartidor' and not (assigned_to or '').strip() and new_status in ('en_route', 'delivered', 'failed'):
        return jsonify({'error': 'primero debe asignarse la orden'}), 400

    if str(st or '').strip().lower() == 'entregado' and new_status != 'delivered':
        return jsonify({'error': 'no se puede cambiar el estado de una orden entregada'}), 400

    st_norm = str(st or '').strip().lower()
    if new_status == 'en_route' and st_norm not in ('listo', 'en_camino'):
        return jsonify({'error': 'la orden debe estar lista antes de salir a reparto'}), 400

    current_delivery_status_norm = str(current_delivery_status or '').strip().lower() or 'pending'
    if new_status in ('delivered', 'failed') and current_delivery_status_norm != 'en_route':
        return jsonify({'error': 'la orden debe estar en camino antes de marcarse como entregada o fallida'}), 400

    now = datetime.utcnow().isoformat()
    delivered_at = now if new_status == 'delivered' else None

    sets = ["delivery_status = ?"]
    params = [new_status]
    if delivered_at:
        sets.append("delivered_at = ?")
        params.append(delivered_at)
    if delivery_notes is not None:
        sets.append("delivery_notes = ?")
        params.append(delivery_notes)
    if seq_val is not None:
        sets.append("delivery_sequence = ?")
        params.append(seq_val)
    if new_status in ('assigned', 'en_route', 'delivered') and not (assigned_to or '').strip():
        sets.append("delivery_assigned_to = ?")
        params.append(actor or '')
        sets.append("delivery_assigned_at = ?")
        params.append(now)
    params.append(order_id)
    cur.execute(f"UPDATE orders SET {', '.join(sets)} WHERE id = ?", params)

    new_main = None
    if new_status == 'en_route' and st_norm not in ('cancelado', 'entregado'):
        new_main = 'en_camino'
    if new_status == 'delivered' and st_norm != 'entregado':
        new_main = 'entregado'
    if new_status == 'failed' and st_norm == 'en_camino':
        new_main = 'listo'

    if new_status == 'failed':
        try:
            cur.execute(
                "INSERT INTO order_status_history (order_id, status, changed_at, changed_by) VALUES (?, ?, ?, ?)",
                (order_id, 'fallo', now, actor or ''),
            )
        except Exception:
            pass

    if new_main == 'entregado':
        scope = _scope_for(role, owner=owner)
        if scope == 'user':
            cur.execute(
                "SELECT id FROM cash_sessions WHERE tenant_slug = ? AND scope = 'user' AND closed_at IS NULL AND lower(opened_by) = lower(?) ORDER BY opened_at DESC LIMIT 1",
                (tenant_slug, actor or ''),
            )
        else:
            cur.execute("SELECT id FROM cash_sessions WHERE tenant_slug = ? AND scope = 'tenant' AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1", (tenant_slug,))
        if not cur.fetchone():
            return jsonify({'error': 'no hay sesión de caja abierta'}), 400

    if new_main:
        cur.execute("UPDATE orders SET status = ? WHERE id = ?", (new_main, order_id))
        changed_by = actor or ''
        if new_status == 'failed' and new_main == 'listo':
            changed_by = 'sistema'
        cur.execute(
            "INSERT INTO order_status_history (order_id, status, changed_at, changed_by) VALUES (?, ?, ?, ?)",
            (order_id, new_main, now, changed_by),
        )

    try:
        cur.execute(
            "INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                order_id,
                'delivery_status_change',
                actor or '',
                0,
                json.dumps({'from': str(current_delivery_status or 'pending'), 'to': new_status, 'order_status': new_main, 'delivery_notes': delivery_notes}),
                now,
            ),
        )
    except Exception:
        pass

    closed = False
    try:
        ensure_delivery_run_tables(conn, cur)
        driver_for_run = (actor or '') if role == 'repartidor' else (assigned_to or '')
        if driver_for_run:
            closed = _auto_close_run_if_empty(cur, tenant_slug, driver_for_run)
    except Exception:
        closed = False

    conn.commit()
    return jsonify({'order_id': order_id, 'delivery_status': new_status, 'order_status': new_main, 'run_closed': closed})


@bp.route('/delivery/orders/<int:order_id>/unassign', methods=['POST', 'PATCH'])
def unassign_delivery_order(order_id):
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'delivery_manage'):
        return jsonify({'error': 'sin permisos'}), 403

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT tenant_slug, order_type, status, COALESCE(delivery_assigned_to,''), COALESCE(delivery_status,'pending') FROM orders WHERE id = ?",
        (order_id,),
    )
    row = cur.fetchone()
    if not row:
        return jsonify({'error': 'orden no encontrada'}), 404
    tenant_slug, order_type, st, assigned_to, delivery_status = row
    tenant_slug = str(tenant_slug or '')
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if str(order_type or '').strip().lower() != 'direccion':
        return jsonify({'error': 'orden no es de delivery'}), 400

    if role == 'repartidor' and (assigned_to or '').strip().lower() != (actor or '').lower():
        return jsonify({'error': 'orden asignada a otro repartidor'}), 403

    st_norm = str(st or '').strip().lower()
    ds_norm = str(delivery_status or '').strip().lower() or 'pending'
    if st_norm == 'entregado' or ds_norm == 'delivered':
        return jsonify({'error': 'no se puede devolver una orden entregada'}), 400

    now = datetime.utcnow().isoformat()
    cur.execute(
        "UPDATE orders SET delivery_assigned_to = NULL, delivery_assigned_at = NULL, delivery_sequence = NULL, delivery_status = 'pending' WHERE id = ?",
        (order_id,),
    )

    if st_norm == 'en_camino':
        cur.execute("UPDATE orders SET status = 'listo' WHERE id = ?", (order_id,))
        cur.execute(
            "INSERT INTO order_status_history (order_id, status, changed_at, changed_by) VALUES (?, ?, ?, ?)",
            (order_id, 'listo', now, actor or ''),
        )

    try:
        ensure_delivery_run_tables(conn, cur)
        if str(assigned_to or '').strip():
            run_id = _get_active_run_id(cur, tenant_slug, str(assigned_to or '').strip())
            if run_id:
                cur.execute("DELETE FROM delivery_run_orders WHERE run_id = ? AND order_id = ?", (run_id, order_id))
    except Exception:
        pass

    try:
        cur.execute(
            "INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, 'delivery_unassign', actor or '', 0, json.dumps({'prev_assigned_to': str(assigned_to or '').strip()}), now),
        )
    except Exception:
        pass

    closed = False
    try:
        ensure_delivery_run_tables(conn, cur)
        if str(assigned_to or '').strip():
            closed = _auto_close_run_if_empty(cur, tenant_slug, str(assigned_to or '').strip())
    except Exception:
        closed = False

    conn.commit()
    return jsonify({'ok': True, 'order_id': order_id, 'assigned_to': None, 'delivery_status': 'pending', 'order_status': 'listo' if st_norm == 'en_camino' else None, 'run_closed': closed})

@bp.route('/delivery/route', methods=['PATCH'])
def update_delivery_route():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'delivery_manage'):
        return jsonify({'error': 'sin permisos'}), 403

    payload = request.get_json(silent=True) or {}
    tenant_slug = str(payload.get('tenant_slug') or session_tenant or '').strip()
    if not tenant_slug:
        return jsonify({'error': 'tenant_slug requerido'}), 400
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403

    items = payload.get('orders') or payload.get('items') or []
    if not isinstance(items, list) or not items:
        return jsonify({'error': 'orders requerido'}), 400

    updates = []
    for it in items:
        try:
            oid = int(it.get('id'))
            seq = int(it.get('sequence'))
            updates.append((oid, seq))
        except Exception:
            return jsonify({'error': 'orders inválido'}), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_delivery_run_tables(conn, cur)
    except Exception:
        pass
    run_id = None
    if role == 'repartidor':
        try:
            run_id = _get_or_create_active_run(conn, cur, tenant_slug, actor or '')
        except Exception:
            run_id = None
    for oid, seq in updates:
        cur.execute(
            "SELECT order_type, COALESCE(delivery_assigned_to,'') FROM orders WHERE id = ? AND tenant_slug = ?",
            (oid, tenant_slug),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({'error': f'orden no encontrada: {oid}'}), 404
        order_type, assigned_to = row
        if str(order_type or '').strip().lower() != 'direccion':
            return jsonify({'error': f'orden no es de delivery: {oid}'}), 400
        if role == 'repartidor' and (assigned_to or '').strip().lower() != (actor or '').lower():
            return jsonify({'error': f'orden asignada a otro repartidor: {oid}'}), 403
        cur.execute("UPDATE orders SET delivery_sequence = ? WHERE id = ? AND tenant_slug = ?", (seq, oid, tenant_slug))
        if run_id:
            try:
                _upsert_run_order(conn, cur, run_id, oid, seq)
            except Exception:
                pass

    conn.commit()
    return jsonify({'ok': True, 'count': len(updates)})

@bp.route('/delivery/run/active', methods=['GET'])
def get_active_delivery_run():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'orders_view'):
        return jsonify({'error': 'sin permisos'}), 403

    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or session_tenant or 'gastronomia-local1'
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403

    driver = str(request.args.get('driver') or '').strip()
    if role == 'repartidor':
        driver = actor or ''
    if not driver:
        return jsonify({'run': None, 'orders': []})

    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_orders_delivery_columns(conn, cur)
    except Exception:
        pass
    try:
        ensure_orders_tenant_number_columns(conn, cur)
    except Exception:
        pass
    try:
        ensure_delivery_run_tables(conn, cur)
    except Exception:
        pass

    run_id = _get_active_run_id(cur, tenant_slug, driver)
    if not run_id:
        return jsonify({'run': None, 'orders': []})

    cur.execute("SELECT id, tenant_slug, driver_username, status, started_at, closed_at FROM delivery_runs WHERE id = ?", (run_id,))
    run_row = cur.fetchone()
    run = dict(run_row) if run_row else {'id': run_id}

    cur.execute(
        """
        SELECT o.id, o.tenant_slug, o.tenant_order_number, o.customer_name, o.customer_phone, o.order_type, o.table_number, o.address_json, o.status, o.total,
               o.payment_method, o.payment_status, o.tip_amount, o.shipping_cost, o.created_at, o.order_notes,
               o.delivery_assigned_to, o.delivery_status, ro.sequence AS delivery_sequence, o.delivery_notes, o.delivery_assigned_at, o.delivered_at
        FROM delivery_run_orders ro
        JOIN orders o ON o.id = ro.order_id
        WHERE ro.run_id = ? AND o.tenant_slug = ?
        ORDER BY ro.sequence ASC, o.id ASC
        """,
        (run_id, tenant_slug),
    )
    rows = cur.fetchall()
    return jsonify({'run': run, 'orders': [dict(r) for r in rows]})

@bp.route('/delivery/run/close', methods=['POST'])
def close_active_delivery_run():
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'delivery_manage'):
        return jsonify({'error': 'sin permisos'}), 403

    payload = request.get_json(silent=True) or {}
    tenant_slug = str(payload.get('tenant_slug') or session_tenant or '').strip() or 'gastronomia-local1'
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403

    driver = str(payload.get('driver') or payload.get('driver_username') or '').strip()
    if role == 'repartidor':
        driver = actor or ''
    if not driver:
        return jsonify({'error': 'driver requerido'}), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_delivery_run_tables(conn, cur)
    except Exception:
        pass

    run_id = _get_active_run_id(cur, tenant_slug, driver)
    if not run_id:
        return jsonify({'ok': True, 'closed': False})

    now = datetime.utcnow().isoformat()
    cur.execute("UPDATE delivery_runs SET status = 'closed', closed_at = ? WHERE id = ?", (now, run_id))
    conn.commit()
    return jsonify({'ok': True, 'closed': True, 'run_id': run_id})

@bp.route('/orders/<int:order_id>/pay', methods=['POST'])
def pay_order(order_id):
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    session_tenant, actor, role, perms, owner = _ctx()
    if not _has_perm(perms, owner, role, 'cash_manage'):
        return jsonify({'error': 'sin permisos'}), 403
    
    payload = request.get_json(silent=True) or {}
    method = str(payload.get('payment_method') or '').strip().lower()
    tip_amount = int(payload.get('tip_amount') or 0)
    details = payload.get('details') or []
    
    if method == 'mixed':
        if not details or not isinstance(details, list):
             return jsonify({'error': 'detalles de pago mixto requeridos'}), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        if is_postgres():
            cur.execute("SELECT id, tenant_slug, total, payment_status, order_type FROM orders WHERE id = ? FOR UPDATE", (order_id,))
        else:
            try:
                cur.execute("BEGIN IMMEDIATE")
            except Exception:
                pass
            cur.execute("SELECT id, tenant_slug, total, payment_status, order_type FROM orders WHERE id = ?", (order_id,))

        row = cur.fetchone()
        if not row:
            try: conn.rollback()
            except Exception: pass
            return jsonify({'error': 'orden no encontrada'}), 404

        oid, tenant, total, current_pay_status, order_type = row
        if session_tenant and tenant and session_tenant != tenant:
            try: conn.rollback()
            except Exception: pass
            return jsonify({'error': 'acceso denegado al tenant'}), 403

        if str(current_pay_status or '').strip().lower() == 'paid':
            try: conn.rollback()
            except Exception: pass
            return jsonify({'order_id': order_id, 'payment_status': 'paid'}), 200

        if tip_amount < 0:
            try: conn.rollback()
            except Exception: pass
            return jsonify({'error': 'propina inválida'}), 400

        cfg = get_cached_tenant_config(tenant) or {}
        base_allowed = ('contado', 'pos', 'qr', 'transferencia')
        allowed_map = {k: k for k in base_allowed}
        pm_cfg = cfg.get('payment_methods') or {}
        pm_list = []
        if isinstance(pm_cfg, dict):
            pm_list = pm_cfg.get('methods') if isinstance(pm_cfg.get('methods'), list) else []
        elif isinstance(pm_cfg, list):
            pm_list = pm_cfg
        for it in pm_list or []:
            if not isinstance(it, dict):
                continue
            if it.get('active') is False:
                continue
            pid = str(it.get('id') or '').strip().lower()
            base = str(it.get('base') or '').strip().lower()
            if not pid or not base or base not in base_allowed:
                continue
            allowed_map[pid] = base
        
        if method != 'mixed' and method not in allowed_map:
            try: conn.rollback()
            except Exception: pass
            return jsonify({'error': 'método de pago inválido'}), 400

        payments_to_register = []
        sanitized_details = []
        if method == 'mixed':
            sum_details = 0
            for d in details:
                try:
                    pm = str(d.get('method') or '').strip().lower()
                    amt = int(d.get('amount') or 0)
                except Exception:
                    pm = ''
                    amt = 0
                if pm not in allowed_map or amt < 0:
                    try: conn.rollback()
                    except Exception: pass
                    return jsonify({'error': 'detalles de pago mixto inválidos'}), 400
                if amt > 0:
                    payments_to_register.append({'method': pm, 'amount': amt})
                    sum_details += amt
                    sanitized_details.append({'method': pm, 'amount': amt})
            if sum_details != (int(total or 0) + tip_amount):
                try: conn.rollback()
                except Exception: pass
                return jsonify({'error': f'suma de pagos ({sum_details}) no coincide con total ({int(total or 0) + tip_amount})'}), 400
        else:
            payments_to_register.append({'method': method, 'amount': int(total or 0) + tip_amount})

        scope = _scope_for(role, owner=owner)
        if scope == 'user':
            cur.execute(
                "SELECT id FROM cash_sessions WHERE tenant_slug = ? AND scope = 'user' AND closed_at IS NULL AND lower(opened_by) = lower(?) ORDER BY opened_at DESC LIMIT 1",
                (tenant, actor or ''),
            )
        else:
            cur.execute("SELECT id FROM cash_sessions WHERE tenant_slug = ? AND scope = 'tenant' AND closed_at IS NULL ORDER BY opened_at DESC LIMIT 1", (tenant,))
        sess = cur.fetchone()
        if not sess:
            try: conn.rollback()
            except Exception: pass
            return jsonify({'error': 'no hay sesión de caja abierta'}), 400
        session_id = sess[0]

        cur.execute("UPDATE orders SET payment_status = 'paid', payment_method = ?, tip_amount = ? WHERE id = ?", (method, tip_amount, order_id))

        created_at = datetime.utcnow().isoformat()
        cur.execute(
            "INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, 'payment', actor or '', 0, json.dumps({'method': method, 'amount': total, 'tip': tip_amount, 'details': sanitized_details if method == 'mixed' else None}), created_at)
        )

        for pay in payments_to_register:
            pm = pay['method']
            amt = pay['amount']
            base_pm = allowed_map.get(pm) or pm
            note = f"Cobro pedido #{order_id} ({base_pm})"
            if base_pm != pm:
                note += f" [{pm}]"
            if method != 'mixed' and tip_amount > 0:
                 note += f" (incl. propina ${tip_amount})"
            cur.execute(
                "INSERT INTO cash_movements (session_id, type, amount, note, actor, created_at, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (session_id, 'entrada', amt, note, actor, created_at, base_pm)
            )

        conn.commit()
        return jsonify({'order_id': order_id, 'payment_status': 'paid', 'payment_method': method, 'tip_amount': tip_amount})
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise

@bp.route('/orders/<int:order_id>/events', methods=['POST'])
def create_order_event(order_id):
    if not is_authed(): return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf(): return jsonify({'error': 'csrf inválido'}), 403
    payload = request.get_json(silent=True) or {}
    ev_type = (payload.get('type') or '').strip().lower()
    if not ev_type: return jsonify({'error': 'tipo de evento requerido'}), 400
    
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO order_events (order_id, event_type, actor, terminal, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (order_id, ev_type, session.get('admin_user') or '', payload.get('terminal') or '', int(payload.get('amount_delta') or 0), json.dumps(payload.get('meta') or {}), datetime.utcnow().isoformat())
    )
    conn.commit()
    return jsonify({'order_id': order_id, 'type': ev_type})

@bp.route('/orders/<int:order_id>/events', methods=['GET'])
def list_order_events(order_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, event_type, actor, terminal, amount_delta, payload_json, created_at FROM order_events WHERE order_id = ? ORDER BY id ASC", (order_id,))
    rows = cur.fetchall()
    return jsonify({'events': [dict(r) for r in rows]})

@bp.route('/orders/<int:order_id>', methods=['PUT'])
def update_order_content(order_id):
    if not is_authed():
        return jsonify({'error': 'no autorizado'}), 401
    if not check_csrf():
        return jsonify({'error': 'csrf inválido'}), 403
    
    payload = request.get_json(silent=True) or {}
    new_items = payload.get('items')
    
    if new_items is None:
        return jsonify({'error': 'items requeridos'}), 400
        
    conn = get_db()
    cur = conn.cursor()
    
    # Verificar existencia y estado
    cur.execute("SELECT status, tenant_slug, payment_status, order_type, shipping_cost FROM orders WHERE id = ?", (order_id,))
    row = cur.fetchone()
    if not row:
        return jsonify({'error': 'orden no encontrada'}), 404
    
    status, tenant_slug, _, order_type, shipping_cost = row
    session_tenant = str(session.get('tenant_slug') or '').strip()
    if session_tenant and tenant_slug and session_tenant != tenant_slug:
        return jsonify({'error': 'acceso denegado al tenant'}), 403
    if status in ('entregado', 'cancelado'):
        return jsonify({'error': 'no se puede editar una orden finalizada'}), 400

    tenant_slug = str(tenant_slug or '').strip()
    order_type_norm = str(order_type or '').strip().lower()
    base_shipping = 0
    try:
        base_shipping = int(shipping_cost or 0)
    except Exception:
        base_shipping = 0
    if order_type_norm != 'direccion':
        base_shipping = 0

    cur.execute("SELECT id, product_id, qty, modifiers_json FROM order_items WHERE order_id = ?", (order_id,))
    old_rows = cur.fetchall()

    def _pack_size_from_modifiers(raw):
        try:
            if raw is None:
                return 1
            s = raw if isinstance(raw, str) else str(raw)
            s = s.strip()
            if not s:
                return 1
            j = json.loads(s) if s.startswith('{') else {}
            if not isinstance(j, dict):
                return 1
            pack = j.get('pack') if isinstance(j.get('pack'), dict) else {}
            try:
                sz = int(pack.get('size') or pack.get('pack_size') or pack.get('qty') or 1)
            except Exception:
                sz = 1
            return max(1, sz)
        except Exception:
            return 1

    old_units_by_product = {}
    for r in old_rows:
        pid = str(r[1] or '').strip()
        if not pid:
            continue
        try:
            qty = int(r[2] or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        pack_size = _pack_size_from_modifiers(r[3])
        old_units_by_product[pid] = old_units_by_product.get(pid, 0) + (qty * pack_size)

    # Calcular nuevo total
    items_total = 0
    valid_items = []
    new_units_by_product = {}
    for it in new_items:
        try:
            qty = int(it.get('quantity', it.get('qty', 1)))
            if qty <= 0: continue
            price = int(it.get('price', it.get('unit_price', 0) or 0))
            pid = it.get('product_id') or it.get('id')
            pid = str(pid or '').strip()
            if not pid:
                continue

            pack_id = str(it.get('pack_id') or '').strip()
            pack_label = str(it.get('pack_label') or '').strip()
            pack_size_raw = it.get('pack_size')
            try:
                pack_size = int(pack_size_raw or 1)
            except Exception:
                pack_size = 1
            pack_size = max(1, pack_size)

            modifiers = it.get('modifiers') or {}
            if not isinstance(modifiers, dict):
                modifiers = {}
            if pack_id:
                modifiers['pack'] = {'id': pack_id, 'label': pack_label, 'size': pack_size}

            items_total += price * qty
            new_units_by_product[pid] = new_units_by_product.get(pid, 0) + (qty * pack_size)
            valid_items.append({
                'product_id': pid, # Product ID
                'item_id': it.get('item_id'), # DB ID (si existe)
                'name': it.get('name', pid) or pid,
                'price': price,
                'qty': qty,
                'notes': it.get('notes', ''),
                'modifiers_json': json.dumps(modifiers, ensure_ascii=False)
            })
        except:
            continue
            
    # Obtener notas generales
    order_notes = payload.get('order_notes')
            
    try:
        deltas = {}
        for pid, units in old_units_by_product.items():
            deltas[pid] = deltas.get(pid, 0) - int(units or 0)
        for pid, units in new_units_by_product.items():
            deltas[pid] = deltas.get(pid, 0) + int(units or 0)

        for pid, delta_units in deltas.items():
            if not delta_units:
                continue
            if delta_units > 0:
                cur.execute("SELECT stock FROM products WHERE tenant_slug = ? AND product_id = ?", (tenant_slug, pid))
                prow = cur.fetchone()
                if not prow:
                    conn.rollback()
                    return jsonify({'error': 'producto no encontrado', 'product_id': pid}), 400
                try:
                    stock = int(prow[0] or 0)
                except Exception:
                    stock = 0
                if stock < delta_units:
                    conn.rollback()
                    return jsonify({'error': 'stock insuficiente', 'product_id': pid, 'stock': stock, 'requested': delta_units}), 400

        for pid, delta_units in deltas.items():
            if not delta_units:
                continue
            cur.execute(
                "UPDATE products SET stock = stock - ? WHERE tenant_slug = ? AND product_id = ?",
                (int(delta_units), tenant_slug, pid),
            )

        total = int(items_total) + int(base_shipping)

        # Smart Update Strategy:
        # 1. Eliminar items que no están en la lista de IDs a mantener
        ids_to_keep = [it['item_id'] for it in valid_items if it.get('item_id')]
        
        if ids_to_keep:
            # Usar formateo seguro para la lista de IDs
            placeholders = ','.join(['?'] * len(ids_to_keep))
            # Nota: params debe ser tuple
            params = [order_id]
            params.extend(ids_to_keep)
            cur.execute(f"DELETE FROM order_items WHERE order_id = ? AND id NOT IN ({placeholders})", tuple(params))
        else:
            # Si no hay IDs para mantener, borrar todo lo previo (asumiendo reemplazo total o todos nuevos)
            cur.execute("DELETE FROM order_items WHERE order_id = ?", (order_id,))
        
        # 2. Insertar o Actualizar
        for item in valid_items:
            if item.get('item_id'):
                cur.execute(
                    "UPDATE order_items SET product_id = ?, name = ?, qty = ?, unit_price = ?, modifiers_json = ?, notes = ? WHERE id = ? AND order_id = ?",
                    (item['product_id'], item['name'], item['qty'], item['price'], item['modifiers_json'], item['notes'], item['item_id'], order_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO order_items (order_id, tenant_slug, product_id, name, qty, unit_price, modifiers_json, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (order_id, tenant_slug, item['product_id'], item['name'], item['qty'], item['price'], item['modifiers_json'], item['notes'])
                )
            
        # Actualizar Total Orden y Notas Generales si se proveen
        if order_notes is not None:
            cur.execute("UPDATE orders SET total = ?, order_notes = ?, shipping_cost = ? WHERE id = ?", (total, order_notes, base_shipping, order_id))
        else:
            cur.execute("UPDATE orders SET total = ?, shipping_cost = ? WHERE id = ?", (total, base_shipping, order_id))
        
        # Registrar Evento
        actor = session.get('admin_user') or 'admin'
        cur.execute(
            "INSERT INTO order_events (order_id, event_type, actor, amount_delta, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, 'order_updated', actor, 0, json.dumps({'new_total': total, 'items_count': len(valid_items)}), datetime.utcnow().isoformat())
        )
        
        conn.commit()
        return jsonify({'ok': True, 'order_id': order_id, 'total': total, 'items': valid_items})
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500

@bp.route('/orders/export.csv', methods=['GET'])
def export_orders_csv():
    if not is_authed():
        return Response('unauthorized', status=401)
    tenant_slug = request.args.get('tenant_slug') or request.args.get('slug') or 'gastronomia-local1'
    status = request.args.get('status')
    q = request.args.get('q')
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    conn = get_db()
    cur = conn.cursor()
    base = "SELECT id, created_at, order_type, table_number, address_json, total, status, customer_phone FROM orders WHERE tenant_slug = ?"
    params = [tenant_slug]
    if status:
        base += " AND status = ?"
        params.append(status)
    if q:
        try:
            qid = int(q)
            base += " AND id = ?"
            params.append(qid)
        except Exception:
            like = f"%{q}%"
            base += " AND (COALESCE(address_json,'') LIKE ? OR COALESCE(customer_name,'') LIKE ? OR COALESCE(customer_phone,'') LIKE ? OR COALESCE(table_number,'') LIKE ?)"
            params.extend([like, like, like, like])
    if from_date:
        base += " AND created_at >= ?"
        params.append(from_date)
    if to_date:
        base += " AND created_at <= ?"
        params.append(to_date)
    base += " ORDER BY id DESC"
    cur.execute(base, params)
    rows = cur.fetchall()
    # Construir CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "created_at", "order_type", "destination", "customer_phone", "total", "tip_10_percent", "total_with_tip", "status"])
    for r in rows:
        dest = r[3] if r[2] == 'mesa' else (r[4] or '')
        total = int(r[5] or 0)
        # Propina 10% con redondeo "half up" para coincidir con Math.round
        tip = (total + 5) // 10
        total_with_tip = total + tip
        phone = r[7] or ''
        writer.writerow([r[0], r[1], r[2], dest, phone, total, tip, total_with_tip, r[6]])
    resp = output.getvalue()
    return Response(resp, mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename="orders_export.csv"'})
