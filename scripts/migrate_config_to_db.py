import os
import json
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')
CONFIG_DIR = os.path.join(os.getcwd(), 'config')

def migrate_configs():
    if not DATABASE_URL:
        print("DATABASE_URL not found.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        print(f"Reading configs from {CONFIG_DIR}...")
        
        for name in os.listdir(CONFIG_DIR):
            if not name.endswith('.json'):
                continue
                
            file_path = os.path.join(CONFIG_DIR, name)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
                
                # Determine slug from file content or filename
                meta = file_data.get('meta') or {}
                slug = meta.get('slug') or name.replace('.json', '')
                
                print(f"Processing {slug}...")
                
                # Check if exists in DB
                cur.execute("SELECT config_json FROM tenant_config WHERE tenant_slug = %s", (slug,))
                row = cur.fetchone()
                
                db_data = {}
                if row and row[0]:
                    try:
                        db_data = json.loads(row[0])
                    except:
                        pass
                
                # Merge: File data takes precedence for static info (branding, meta), 
                # but preserve dynamic DB settings (shipping_cost, times) if they exist
                
                # We'll start with file_data as base
                merged_data = file_data.copy()
                
                # Ensure 'meta' and 'branding' are preserved from file
                merged_data['meta'] = file_data.get('meta', {})
                merged_data['prefs'] = file_data.get('prefs', {})
                merged_data['sla'] = file_data.get('sla', {})
                
                # Preserve DB-specific keys if they are set in DB but missing/different in file
                # The current DB usage stores: shipping_cost, time_mesa, time_espera, time_delivery, time_auto
                # We should keep these from DB if they exist
                for key in ['shipping_cost', 'time_mesa', 'time_espera', 'time_delivery', 'time_auto']:
                    if key in db_data:
                        merged_data[key] = db_data[key]

                # Update DB
                cur.execute("""
                    INSERT INTO tenant_config (tenant_slug, config_json) 
                    VALUES (%s, %s)
                    ON CONFLICT (tenant_slug) 
                    DO UPDATE SET config_json = EXCLUDED.config_json
                """, (slug, json.dumps(merged_data)))
                
                print(f"  Migrated {slug}")
                
            except Exception as e:
                print(f"  Error processing {name}: {e}")

        conn.commit()
        conn.close()
        print("Config migration completed.")
        
    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    migrate_configs()
