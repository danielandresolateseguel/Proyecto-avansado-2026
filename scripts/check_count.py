import sqlite3
db_path = 'orders.db'
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products WHERE tenant_slug='gastronomia-local1'")
    count = cursor.fetchone()[0]
    print(f"Total productos: {count}", flush=True)
    conn.close()
except Exception as e:
    print(f"Error: {e}", flush=True)
