
import sqlite3
import os

db_path = 'orders.db'
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("--- Schema of orders table ---")
cur.execute("PRAGMA table_info(orders)")
for col in cur.fetchall():
    print(col)

print("\n--- Foreign Keys in orders table ---")
cur.execute("PRAGMA foreign_key_list(orders)")
for fk in cur.fetchall():
    print(fk)

print("\n--- Tenants ---")
try:
    cur.execute("SELECT * FROM tenants")
    tenants = cur.fetchall()
    if not tenants:
        print("No tenants found.")
    else:
        for t in tenants:
            print(t)
except Exception as e:
    print(f"Error querying tenants: {e}")

print("\n--- Checking specific tenant 'planeta-pancho' ---")
cur.execute("SELECT * FROM tenants WHERE tenant_slug = 'planeta-pancho'")
pp = cur.fetchone()
if pp:
    print(f"Found: {pp}")
else:
    print("Tenant 'planeta-pancho' NOT FOUND in tenants table.")

conn.close()
