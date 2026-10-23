
import sqlite3
from datetime import datetime

conn = sqlite3.connect('orders.db')
cur = conn.cursor()

tenants = [
    ('gastronomia-local1', 'Gastronomía Local 1'),
    ('planeta-pancho', 'Planeta Pancho')
]

for slug, name in tenants:
    try:
        cur.execute("SELECT id FROM tenants WHERE tenant_slug = ?", (slug,))
        if cur.fetchone():
            print(f"Tenant '{slug}' already exists.")
        else:
            now = datetime.utcnow().isoformat()
            cur.execute(
                "INSERT INTO tenants (tenant_slug, name, status, created_at) VALUES (?, ?, 'active', ?)",
                (slug, name, now)
            )
            print(f"Inserted tenant '{slug}'.")
    except Exception as e:
        print(f"Error checking/inserting tenant '{slug}': {e}")

conn.commit()
conn.close()
