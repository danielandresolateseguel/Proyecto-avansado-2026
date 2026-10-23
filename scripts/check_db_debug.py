import sqlite3
import os
import json

# Adjust path if necessary
db_path = os.path.join('instance', 'orders.db')
if not os.path.exists(db_path):
    db_path = 'orders.db'

print(f"Checking database at: {db_path}")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n--- Tenants (table: tenants) ---")
    try:
        # Corrected column name from 'slug' to 'tenant_slug'
        cursor.execute("SELECT id, name, tenant_slug, status FROM tenants")
        tenants = cursor.fetchall()
        for t in tenants:
            print(t)
    except Exception as e:
        print(f"Error querying tenants: {e}")

    print("\n--- Tenant Configs (table: tenant_config) ---")
    try:
        cursor.execute("SELECT tenant_slug, config_json FROM tenant_config")
        configs = cursor.fetchall()
        for c in configs:
            print(f"Config for {c[0]}: {c[1][:50]}...") # Print first 50 chars
    except Exception as e:
        print(f"Error querying tenant_config: {e}")

    print("\n--- Recent Orders (Top 5) ---")
    try:
        cursor.execute("SELECT id, tenant_slug, customer_name, total, status, created_at FROM orders ORDER BY id DESC LIMIT 5")
        orders = cursor.fetchall()
        for o in orders:
            print(o)
    except Exception as e:
        print(f"Error querying orders: {e}")

    conn.close()

except Exception as e:
    print(f"Database connection error: {e}")
