import sys
from app.database import get_db
from app import create_app

try:
    print("Init app...", flush=True)
    app = create_app()
    with app.app_context():
        print("Get DB...", flush=True)
        db = get_db()
        cur = db.cursor()
        
        print("Querying one product...", flush=True)
        cur.execute("SELECT * FROM products LIMIT 1")
        if cur.description:
            print("Columns:", [desc[0] for desc in cur.description], flush=True)
        else:
            print("No description available", flush=True)
            
        rows = cur.fetchall()
        print("First row:", rows[0] if rows else "No rows", flush=True)

except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr, flush=True)
