print("Starting script...")
import sys
from app import create_app
from app.database import get_db

print("Creating app...")
try:
    app = create_app()
    print("App created.")
    with app.app_context():
        print("Querying tenants via app context...")
        try:
            db = get_db()
            print(f"DB Object: {db}")
            cur = db.cursor()
            
            # Check if we are using Postgres or SQLite
            is_pg = app.config.get('IS_POSTGRES', False)
            print(f"Using Postgres: {is_pg}")

            print("Executing query...")
            cur.execute("SELECT id, tenant_slug, name FROM tenants")
            rows = cur.fetchall()
            print(f"Tenants count: {len(rows)}")
            for row in rows:
                print(f"ID: {row[0]}, Slug: {row[1]}, Name: {row[2]}")
                
        except Exception as e:
            print(f"Error querying tenants: {e}")
            import traceback
            traceback.print_exc()
except Exception as e:
    print(f"Global Error: {e}")
    import traceback
    traceback.print_exc()
print("Script finished.")

