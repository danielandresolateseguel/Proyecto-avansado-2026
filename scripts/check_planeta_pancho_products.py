
print("Starting script...", flush=True)
import sys
import os
import traceback
from app import create_app
from app.database import get_db
import json

print("Imports done.", flush=True)

try:
    app = create_app()
    print("App created.", flush=True)

    with app.app_context():
        print("Context entered.", flush=True)
        try:
            conn = get_db()
            print("DB connection got.", flush=True)
            cur = conn.cursor()
            
            print("Checking products for tenant 'planeta-pancho'...", flush=True)
            cur.execute("SELECT id, name, price, variants, active FROM products WHERE tenant_slug = 'planeta-pancho'")
            rows = cur.fetchall()
            
            if not rows:
                print("No products found for 'planeta-pancho'.", flush=True)
            else:
                print(f"Found {len(rows)} products.", flush=True)
                for row in rows:
                    try:
                        # Row is likely a tuple or custom object depending on DB backend
                        # If using the Postgres wrapper, row is PostgresRow which supports index access
                        id_val = row[0]
                        name = row[1]
                        price = row[2]
                        variants_raw = row[3]
                        active = row[4]
                        
                        print(f"ID: {id_val}, Name: {name}, Price: {price}, Active: {active}", flush=True)
                        
                        variants = {}
                        if variants_raw:
                            if isinstance(variants_raw, str):
                                variants = json.loads(variants_raw)
                            elif isinstance(variants_raw, dict):
                                variants = variants_raw
                            else:
                                print(f"Warning: variants is type {type(variants_raw)}", flush=True)
                        
                        section = variants.get('section', 'main')
                        print(f"-> Section: {section}", flush=True)
                        print("-" * 20, flush=True)
                    except Exception as e:
                        print(f"Error processing row: {e}", flush=True)
                        traceback.print_exc()

            # conn.close() # Don't close, let teardown handle it or just leave it
        except Exception as e:
            print(f"Error in script: {e}", flush=True)
            traceback.print_exc()

except Exception as e:
    print(f"Error initializing app: {e}", flush=True)
    traceback.print_exc()
