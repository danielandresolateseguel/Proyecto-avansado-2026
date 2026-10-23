
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

database_url = os.environ.get('DATABASE_URL')
if not database_url:
    print("DATABASE_URL not set in .env")
    exit(1)

print(f"Connecting to: {database_url}")
try:
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()

    print("--- Checking tenants table ---")
    try:
        cur.execute("SELECT * FROM tenants")
        tenants = cur.fetchall()
        if not tenants:
            print("No tenants found.")
        else:
            for t in tenants:
                print(f"Tenant: {t}")
    except Exception as e:
        print(f"Error querying tenants: {e}")
        conn.rollback()

    print("\n--- Checking specific tenant 'planeta-pancho' ---")
    try:
        cur.execute("SELECT * FROM tenants WHERE tenant_slug = 'planeta-pancho'")
        pp = cur.fetchone()
        if pp:
            print(f"Found: {pp}")
        else:
            print("Tenant 'planeta-pancho' NOT FOUND in tenants table.")
    except Exception as e:
        print(f"Error querying specific tenant: {e}")

    print("\n--- Checking orders table ---")
    try:
        cur.execute("SELECT id, tenant_slug, created_at, status FROM orders ORDER BY id DESC LIMIT 5")
        orders = cur.fetchall()
        for o in orders:
            print(f"Order: {o}")
    except Exception as e:
        print(f"Error querying orders: {e}")

    conn.close()

except Exception as e:
    print(f"Connection failed: {e}")
