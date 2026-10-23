
import sqlite3
import os

db_path = 'orders.db'
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("Checking orders for tenant: planeta-pancho")
cur.execute("SELECT id, tenant_slug, customer_name, created_at, status FROM orders WHERE tenant_slug = 'planeta-pancho' ORDER BY id DESC LIMIT 5")
rows = cur.fetchall()

if not rows:
    print("No orders found for planeta-pancho.")
else:
    for row in rows:
        print(f"Order ID: {row[0]}, Tenant: {row[1]}, Customer: {row[2]}, Created: {row[3]}, Status: {row[4]}")

print("\nChecking all orders (last 5):")
cur.execute("SELECT id, tenant_slug, customer_name, created_at, status FROM orders ORDER BY id DESC LIMIT 5")
all_rows = cur.fetchall()
for row in all_rows:
    print(f"Order ID: {row[0]}, Tenant: {row[1]}, Customer: {row[2]}, Created: {row[3]}, Status: {row[4]}")

conn.close()
