import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import get_db


def run_backfill(tenant_slug=None, dry_run=True, force=False):
    conn = get_db()
    cur = conn.cursor()
    try:
        if tenant_slug:
            cur.execute(
                "SELECT COUNT(*) FROM orders WHERE tenant_slug = ?",
                (tenant_slug,)
            )
        else:
            cur.execute("SELECT COUNT(*) FROM orders")
        total_orders_row = cur.fetchone()
        total_orders = int(total_orders_row[0] or 0) if total_orders_row else 0
        if not total_orders:
            print(f"[skip] No se encontraron pedidos para analizar.")
            return {'ok': True, 'dry_run': dry_run, 'updated': 0, 'skipped': 0, 'without_cost_product': 0, 'without_snapshot': 0}
        cur.execute("SELECT DISTINCT tenant_slug FROM orders")
        tenants_rows = cur.fetchall() or []
        tenants = [t[0] for t in tenants_rows if t and t[0]]
        if not tenants:
            print(f"[skip] No hay tenants con pedidos.")
            return {'ok': True, 'dry_run': dry_run, 'updated': 0, 'skipped': 0, 'without_cost_product': 0, 'without_snapshot': 0}
        if tenant_slug and tenant_slug not in tenants:
            print(f"[warn] Tenant {tenant_slug} no tiene pedidos, usando tenants reales.")
        target_tenants = [tenant_slug] if tenant_slug and tenant_slug in tenants else tenants
        total_items = 0
        total_to_update = 0
        total_items_without_snapshot = 0
        total_products_without_cost = 0
        updated = 0
        skipped = 0
        for ts in target_tenants:
            print(f"[info] Procesando tenant={ts} ...")
            cur.execute(
                "SELECT product_id, cost_price FROM products WHERE tenant_slug = ?",
                (ts,)
            )
            prod_rows = cur.fetchall() or []
            product_cost = {}
            for pr in prod_rows:
                pid = str(pr[0] or '').strip()
                cp = int(pr[1] or 0)
                if pid:
                    product_cost[pid] = cp
            cur.execute(
                """
                SELECT DISTINCT oi.id, oi.product_id, oi.tenant_slug, oi.qty, oi.unit_cost
                FROM order_items oi
                JOIN orders o ON o.id = oi.order_id
                WHERE o.tenant_slug = ? AND (oi.unit_cost IS NULL OR oi.unit_cost = 0)
                """,
                (ts,)
            )
            rows = cur.fetchall() or []
            items_count = len(rows)
            total_items += items_count
            if not items_count:
                print(f"  · sin items sin snapshot.")
                continue
            batch_to_update = []
            no_snapshot = 0
            no_cost = 0
            for r in rows:
                oid = r[0]
                pid = str(r[1] or '').strip()
                ts_item = r[2] or ts
                qty = int(r[3] or 1)
                uc = int(r[4] or 0)
                if uc > 0:
                    skipped += 1
                    continue
                no_snapshot += 1
                total_items_without_snapshot += 1
                cp = product_cost.get(pid)
                if cp is None:
                    cur.execute(
                        "SELECT cost_price FROM products WHERE tenant_slug = ? AND product_id = ?",
                        (ts_item, pid)
                    )
                    row2 = cur.fetchone()
                    cp = int(row2[0] or 0) if row2 else 0
                    product_cost[pid] = cp
                if cp <= 0:
                    total_products_without_cost += 1
                    no_cost += 1
                    skipped += 1
                    continue
                total_to_update += 1
                batch_to_update.append((cp, oid, ts_item, pid, qty))
            if not dry_run and batch_to_update:
                cur.executemany(
                    "UPDATE order_items SET unit_cost = ? WHERE id = ? AND tenant_slug = ? AND product_id = ? AND ABS(COALESCE(qty,0) - ?) < 0.001",
                    batch_to_update
                )
                conn.commit()
                updated += cur.rowcount or len(batch_to_update)
            else:
                updated += len(batch_to_update)
            print(f"  · items sin snapshot: {no_snapshot} | productos sin costo en db: {no_cost} | a corregir: {len(batch_to_update)}")
        result = {
            'ok': True,
            'dry_run': bool(dry_run),
            'force': bool(force),
            'tenants_processed': len(target_tenants),
            'items_evaluados': total_items,
            'items_sin_snapshot': total_items_without_snapshot,
            'items_sin_costo_en_producto': total_products_without_cost,
            'updated': int(updated),
            'skipped': int(skipped),
            'nota': 'updated = filas con unit_cost corregido. Si dry_run=True es la cantidad estimada.'
        }
        if dry_run:
            print("-" * 60)
            print(f"[DRY RUN] Resumen estimado (sin aplicar cambios):")
            print(f"  · Tenants procesados: {result['tenants_processed']}")
            print(f"  · Items evaluados sin snapshot: {result['items_evaluados']}")
            print(f"  · Items a corregir: {result['updated']}")
            print(f"  · Items que quedan sin costo (producto.cost_price = 0): {result['items_sin_costo_en_producto']}")
            print("Para aplicar, volver a correr con --apply o dry_run=False desde código.")
            conn.rollback()
        else:
            print("-" * 60)
            print(f"[APPLY] Backfill finalizado y commit aplicado:")
            print(f"  · Items actualizados: {result['updated']}")
            print(f"  · Items skippeados (sin costo configurado): {result['items_sin_costo_en_producto']}")
        return result
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[error] {e}")
        raise


def _print_help():
    print("Uso:")
    print("  python migrate_costs_backfill.py --dry-run --tenant SLUG")
    print("  python migrate_costs_backfill.py --apply   --tenant SLUG")
    print("Sin --tenant se ejecuta en todos los tenants con pedidos.")


if __name__ == '__main__':
    args = sys.argv[1:]
    if '--help' in args or '-h' in args:
        _print_help()
        sys.exit(0)
    apply_flag = '--apply' in args
    tenant_arg = None
    for a in args:
        if a.startswith('--tenant='):
            tenant_arg = a.split('=', 1)[1]
        elif a == '--tenant':
            idx = args.index(a)
            if idx + 1 < len(args):
                tenant_arg = args[idx + 1]
    dry = not apply_flag
    print(f"[start] migrate_costs_backfill | tenant={tenant_arg or 'ALL'} | dry_run={dry}")
    run_backfill(tenant_slug=tenant_arg, dry_run=dry)
