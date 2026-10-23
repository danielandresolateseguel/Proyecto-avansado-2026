
import sys
import os
from app import create_app
from app.database import get_db

with open('verify_output.txt', 'w', encoding='utf-8') as f:
    try:
        f.write("Starting verify script...\n")
        app = create_app()
        with app.app_context():
            f.write("Inside app context.\n")
            db = get_db()
            cur = db.cursor()
            
            f.write("Querying products...\n")
            cur.execute("SELECT tenant_slug, product_id, name FROM products WHERE tenant_slug='planeta-pancho'")
            rows = cur.fetchall()
            f.write(f"Found {len(rows)} products for planeta-pancho:\n")
            for row in rows:
                f.write(f"{row}\n")
                
    except Exception as e:
        f.write(f"ERROR: {e}\n")
        import traceback
        traceback.print_exc(file=f)

print("Verify script finished.")
