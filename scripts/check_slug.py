import sqlite3
import os
import sys

print("Iniciando check...", flush=True)
db_path = 'orders.db'
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT tenant_slug FROM products WHERE product_id='i1-2'")
    row = cursor.fetchone()
    if row:
        slug = row[0]
        print(f"Slug: '{slug}'", flush=True)
        print(f"Largo: {len(slug)}", flush=True)
        expected = 'gastronomia-local1'
        if slug != expected:
            print(f"DIFERENTE! Esperaba '{expected}' (len {len(expected)})", flush=True)
            cursor.execute("UPDATE products SET tenant_slug=? WHERE product_id='i1-2'", (expected,))
            conn.commit()
            print("Corregido.", flush=True)
        else:
            print("Slug correcto.", flush=True)
    else:
        print("Producto no encontrado", flush=True)
    conn.close()
except Exception as e:
    print(f"Error: {e}", flush=True)
