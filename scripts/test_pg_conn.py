import psycopg2
import os

url = "postgresql://postgres:d1o2239@localhost:5432/orders_db"
print(f"Connecting to {url}...")
try:
    conn = psycopg2.connect(url)
    print("Connected successfully.")
    cur = conn.cursor()
    cur.execute("SELECT id, tenant_slug, name FROM tenants")
    rows = cur.fetchall()
    print(f"Tenants count: {len(rows)}")
    for row in rows:
        print(f"ID: {row[0]}, Slug: {row[1]}, Name: {row[2]}")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
