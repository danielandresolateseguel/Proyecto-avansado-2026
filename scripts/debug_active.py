import sqlite3
import os

DB_PATH = os.path.join(os.getcwd(), 'orders.db')

def check():
    if not os.path.exists(DB_PATH):
        print("Database not found")
        return

    with open('debug_active_out.txt', 'w') as f:
        def log(msg):
            print(msg)
            f.write(msg + '\n')
            
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        log("--- ALL ACTIVE ORDERS (Not Delivered, Not Canceled) ---")
        cur.execute("SELECT id, status, payment_status, created_at FROM orders WHERE status NOT IN ('entregado', 'cancelado')")
        rows = cur.fetchall()
        if not rows:
            log("No active orders found.")
        for r in rows:
            log(f"Order #{r['id']} Status: {r['status']} Pay: {r['payment_status']} Created: {r['created_at']}")
            # Check archive status
            cur.execute("SELECT 1 FROM archived_orders WHERE order_id = ?", (r['id'],))
            archived = cur.fetchone()
            if archived:
                log(f"  WARNING: THIS ORDER IS ARCHIVED!")
        conn.close()

if __name__ == '__main__':
    check()
