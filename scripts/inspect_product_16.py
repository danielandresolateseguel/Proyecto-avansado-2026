import sqlite3

conn = sqlite3.connect('orders.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT tenant_slug, product_id, name, active, variants_json FROM products WHERE product_id = '16'")
rows = cur.fetchall()

with open('inspect_16_result.txt', 'w', encoding='utf-8') as f:
    f.write("--- DB PRODUCTS WITH ID 16 ---\n")
    if not rows:
        f.write("Not found\n")
    else:
        for row in rows:
            f.write(f"tenant_slug: {row['tenant_slug']}\n")
            f.write(f"Name: {row['name']}\n")
            f.write(f"Active: {row['active']}\n")
            f.write(f"Variants JSON: {row['variants_json']}\n")
            f.write("---\n")

conn.close()
