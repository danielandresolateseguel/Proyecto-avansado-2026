import requests
import sqlite3
import json
import os

DB_PATH = 'database.db'
BASE_URL = 'http://localhost:8000'

def get_db():
    return sqlite3.connect(DB_PATH)

def test_visibility():
    print("Testing product visibility...")
    
    # 1. Direct DB Insert (Active)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM products WHERE product_id='test-vis-1'")
    conn.commit()
    
    cur.execute("""
        INSERT INTO products (tenant_slug, product_id, name, price, stock, active, details, variants_json, image_url, last_modified)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
    """, ('gastronomia-local1', 'test-vis-1', 'Test Visible', 100, 10, 'Details', '{}', '', ''))
    conn.commit()
    conn.close()
    
    # 2. Check API (Should exist)
    try:
        resp = requests.get(f"{BASE_URL}/api/products?tenant_slug=gastronomia-local1")
        data = resp.json()
        products = data.get('products', [])
        found = any(p['id'] == 'test-vis-1' for p in products)
        print(f"Active product visible in API: {found}")
        if not found:
            print("FAILURE: Active product not found")
    except Exception as e:
        print(f"API Error: {e}")

    # 3. Direct DB Update (Inactive)
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE products SET active=0 WHERE product_id='test-vis-1'")
    conn.commit()
    conn.close()
    
    # 4. Check API (Should NOT exist)
    try:
        resp = requests.get(f"{BASE_URL}/api/products?tenant_slug=gastronomia-local1")
        data = resp.json()
        products = data.get('products', [])
        found = any(p['id'] == 'test-vis-1' for p in products)
        print(f"Inactive product visible in API: {found}")
        if found:
            print("FAILURE: Inactive product found (should be hidden)")
        else:
            print("SUCCESS: Inactive product is hidden")
    except Exception as e:
        print(f"API Error: {e}")
        
    # Cleanup
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM products WHERE product_id='test-vis-1'")
    conn.commit()
    conn.close()

if __name__ == '__main__':
    test_visibility()
