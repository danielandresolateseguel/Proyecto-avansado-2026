import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')

def migrate():
    print(f"DATABASE_URL found: {'Yes' if DATABASE_URL else 'No'}")
    if not DATABASE_URL:
        return

    try:
        print("Connecting to Postgres...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        print("Adding created_at column to order_items...")
        # Add column if not exists
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='order_items' AND column_name='created_at') THEN
                    ALTER TABLE order_items ADD COLUMN created_at TIMESTAMP;
                END IF;
            END
            $$;
        """)
        
        print("Updating existing records...")
        # Update existing records with order's created_at
        cur.execute("""
            UPDATE order_items 
            SET created_at = CAST(o.created_at AS TIMESTAMP)
            FROM orders o 
            WHERE order_items.order_id = o.id 
            AND order_items.created_at IS NULL;
        """)
        
        # Also ensure orders.created_at is castable or handled correctly. 
        # In this codebase created_at is TEXT ISO8601, so casting to TIMESTAMP works in PG.
        
        conn.commit()
        print("Migration successful.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    migrate()
