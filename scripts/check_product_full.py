import sqlite3
import os
import sys

db_path = 'orders.db'
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products WHERE product_id='i1-2'")
    row = cursor.fetchone()
    if row:
        print("Encontrado:", flush=True)
        for key in row.keys():
            print(f"{key}: {row[key]}", flush=True)
    else:
        print("No encontrado", flush=True)
    conn.close()
except Exception as e:
    print(f"Error: {e}", flush=True)
