
import sqlite3
import json
import os

DB_PATH = os.path.join(os.getcwd(), 'orders.db')


def check_products():
    lines = []
    lines.append(f"DB: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        lines.append("DB file does not exist!")
    else:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT product_id, name, active, variants_json "
            "FROM products ORDER BY last_modified DESC LIMIT 20"
        )
        rows = cur.fetchall()
        conn.close()
        lines.append(f"Found {len(rows)} products")
        for row in rows:
            pid, name, active, variants = row
            lines.append("-----")
            lines.append(f"ID: {pid}")
            lines.append(f"Name: {name}")
            lines.append(f"Active: {active}")
            lines.append(f"Variants raw: {variants}")
            try:
                v = json.loads(variants or "{}")
                lines.append(f"Variants parsed: {v}")
            except Exception as e:
                lines.append(f"Error parsing variants: {e}")
    out_path = os.path.join(os.getcwd(), "debug_products_out.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    check_products()
