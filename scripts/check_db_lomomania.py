import psycopg2
import json
import os
import sqlite3

# Try Postgres first, then SQLite fallback
DB_URL = "postgresql://postgres:d1o2239@localhost:5432/orders_db"
SQLITE_PATH = "orders.db"

def check_postgres(slug):
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        print(f"Checking PostgreSQL for {slug}...")
        cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = %s", (slug,))
        row = cur.fetchone()
        if row:
            config = json.loads(row[0])
            print(f"Postgres Config found: {config}")
            print(f"announcement_active: {config.get('announcement_active')}")
            print(f"announcement_text: {config.get('announcement_text')}")
            print(f"opening_hours: {json.dumps(config.get('opening_hours'), indent=2)}")
        else:
            print(f"No config found in Postgres for {slug}")
        conn.close()
        return True
    except Exception as e:
        print(f"Postgres check failed: {e}")
        return False

import sys

if __name__ == "__main__":
    slug = sys.argv[1] if len(sys.argv) > 1 else "lomomania"
    check_postgres(slug)
