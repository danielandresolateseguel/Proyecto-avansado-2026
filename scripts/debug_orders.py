import sqlite3
import os

DB_PATH = 'orders.db'

def run():
    if not os.path.exists(DB_PATH):
        print("No DB found")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    print("--- Orders Summary ---")
    cur.execute("SELECT status, COUNT(*) as c FROM orders GROUP BY status")
    for r in cur.fetchall():
        print(f"Status: {r['status']}, Count: {r['c']}")

    print("\n--- Top 10 Recent Orders ---")
    cur.execute("SELECT id, status, created_at, order_type FROM orders ORDER BY id DESC LIMIT 10")
    for r in cur.fetchall():
        print(f"ID: {r['id']}, Status: {r['status']}, Created: {r['created_at']}, Type: {r['order_type']}")

    print("\n--- Archived Orders ---")
    cur.execute("SELECT COUNT(*) as c FROM archived_orders")
    print(f"Total Archived: {cur.fetchone()['c']}")

    # User request: "dejalo en cero los pedidos activos"
    # Active statuses: pendiente, preparacion, listo, en_camino
    print("\n--- Clearing Active Orders ---")
    active_statuses = ('pendiente', 'preparacion', 'listo', 'en_camino')
    # Use execute with parameter substitution for 'IN' clause needs placeholders
    # Or just use execute multiple times or formatting (safe enough here for local script)
    placeholders = ','.join('?' for _ in active_statuses)
    cur.execute(f"SELECT id FROM orders WHERE status IN ({placeholders})", active_statuses)
    active_ids = [r['id'] for r in cur.fetchall()]
    
    if active_ids:
        print(f"Found active IDs: {active_ids}")
        # Update to 'cancelado' to clear them from active view but keep record
        # Or delete them? User said "dejalo en cero... quizas fueron pruebas".
        # I'll update to 'cancelado' with a note.
        cur.execute(f"UPDATE orders SET status='cancelado', order_notes='Auto-cancelled by admin reset' WHERE id IN ({','.join(map(str, active_ids))})")
        conn.commit()
        print(f"Updated {len(active_ids)} orders to 'cancelado'.")
    else:
        print("No active orders found.")

    conn.close()

if __name__ == '__main__':
    run()
