import sqlite3

def check_counts():
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()
    slugs = ['comercio', 'comercio1', 'site-base']
    for slug in slugs:
        count = cursor.execute("SELECT COUNT(id) FROM products WHERE tenant_slug=?", (slug,)).fetchone()[0]
        print(f"{slug}: {count}")
    conn.close()

if __name__ == "__main__":
    check_counts()
