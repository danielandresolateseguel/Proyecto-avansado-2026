import sqlite3


def main():
    conn = sqlite3.connect("orders.db")
    cur = conn.cursor()
    cur.execute(
        "SELECT tenant_slug, username FROM admin_users ORDER BY tenant_slug, username"
    )
    rows = cur.fetchall()
    conn.close()
    for slug, username in rows:
        print(slug, username)


if __name__ == "__main__":
    main()

