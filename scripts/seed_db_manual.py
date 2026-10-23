
import sys
import os
from app import create_app
from app import database

print("Starting seed script...", flush=True)

try:
    app = create_app()
    with app.app_context():
        print("Seeding products from config...", flush=True)
        database.seed_products_from_config(app.config['CONFIG_DIR'])
        
        print("Backfilling details...", flush=True)
        database.backfill_product_details_from_config(app.config['CONFIG_DIR'])
        
        print("Backfilling variants...", flush=True)
        database.backfill_product_variants_from_config(app.config['CONFIG_DIR'])
        
        print("Backfilling images...", flush=True)
        database.backfill_product_images_from_config(app.config['CONFIG_DIR'])
        
        print("Seeding admin users...", flush=True)
        database.seed_admin_users_from_env(app.config['CONFIG_DIR'])
        
        print("Seed completed successfully.", flush=True)

except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc()
