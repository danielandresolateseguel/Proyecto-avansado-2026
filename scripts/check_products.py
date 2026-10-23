import sys
import time
from app.database import get_db
from app import create_app

print("Starting script...", flush=True)

try:
    app = create_app()
    print("App created.", flush=True)
    
    with app.app_context():
        print("Inside app context.", flush=True)
        db = get_db()
        cur = db.cursor()
        
        print("Executing SELECT * FROM products LIMIT 1...", flush=True)
        cur.execute("SELECT * FROM products LIMIT 1")
        
        if cur.description:
            cols = [desc[0] for desc in cur.description]
            print(f"COLUMNS FOUND: {cols}", flush=True)
        else:
            print("No description available.", flush=True)
            
        print("--- Planeta Pancho ---", flush=True)
        cur.execute("SELECT * FROM products WHERE tenant_slug='planeta-pancho'")
        rows = cur.fetchall()
        print(f"Found {len(rows)} products for planeta-pancho", flush=True)
        for row in rows:
            print(row, flush=True)

except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc()

print("Script finished.", flush=True)
