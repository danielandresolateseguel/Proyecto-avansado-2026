import json
import random
import urllib.request
import urllib.error
import sqlite3

def test_create_payload():
    url = "http://127.0.0.1:8000/api/products"
    
    # Simulate exactly what the JS sends
    payload = {
        "tenant_slug": "gastronomia-local1",
        "id": f"test-cat-{random.randint(1000,9999)}",
        "name": "Test Pizza Category",
        "price": 1200,
        "stock": 10,
        "section": "main",
        "interest_tag": "",
        "food_categories": ["pizzas", "comen-dos"],
        "image_url": ""
    }
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    
    with open('test_cat_result.txt', 'w') as f:
        f.write(f"Sending payload: {json.dumps(payload, indent=2)}\n")
        
        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.getcode()
                response_text = resp.read().decode('utf-8')
                
                f.write(f"Status: {status}\n")
                f.write(f"Response: {response_text}\n")
                
                if status == 200:
                    conn = sqlite3.connect('orders.db')
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute("SELECT variants_json FROM products WHERE product_id=?", (payload['id'],))
                    row = cur.fetchone()
                    if row:
                        f.write(f"Saved variants_json: {row[0]}\n")
                    else:
                        f.write("Product not found in DB!\n")
                    conn.close()
        except urllib.error.HTTPError as e:
             f.write(f"HTTP Error: {e.code} {e.reason}\n")
             f.write(f"Error content: {e.read().decode('utf-8')}\n")
        except Exception as e:
            f.write(f"Error: {e}\n")

if __name__ == "__main__":
    test_create_payload()
