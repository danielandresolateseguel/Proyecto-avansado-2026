import sqlite3
import os

def check():
    output_file = 'check_product_output_final.txt'
    abs_output = os.path.abspath(output_file)
    
    with open(abs_output, 'w') as f:
        try:
            conn = sqlite3.connect('orders.db')
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            f.write("--- PRODUCT ID 11 ---\n")
            # Check strictly for the one the user likely created
            cursor.execute("SELECT * FROM products WHERE product_id = '11'")
            rows = cursor.fetchall()
            if not rows: f.write("No product found with product_id='11'\n")
            for r in rows:
                f.write(f"ID: {r['id']}\n")
                f.write(f"Product ID: {r['product_id']}\n")
                f.write(f"Name: {r['name']}\n")
                f.write(f"Variants: {r['variants_json']}\n")
            
            conn.close()
        except Exception as e:
            f.write(f"Error: {e}\n")

if __name__ == '__main__':
    check()
