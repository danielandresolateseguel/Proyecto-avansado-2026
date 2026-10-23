import os
import sys
import traceback

# Force absolute path for log
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, 'crash_log_v2.txt')

def log_crash(msg):
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(str(msg) + '\n')
    except Exception as e:
        pass

log_crash("Script starting (v2.2)...")

try:
    import sqlite3
    import psycopg2
    from psycopg2.extras import execute_values
    from dotenv import load_dotenv
    log_crash("Imports successful.")
except Exception as e:
    log_crash(f"Import error: {e}")
    log_crash(traceback.format_exc())
    sys.exit(1)

def log(msg):
    log_crash(msg)

load_dotenv()
SQLITE_DB_PATH = os.path.join(BASE_DIR, 'orders.db')
DATABASE_URL = os.environ.get('DATABASE_URL')

TABLES = [
    'orders',
    'order_items',
    'order_status_history',
    'archived_orders',
    'tenant_config',
    'products',
    'admin_users',
    'order_events',
    'cash_sessions',
    'cash_movements',
    'carousel_slides'
]

def migrate():
    log(f"DATABASE_URL: {DATABASE_URL}")
    
    if not DATABASE_URL or not DATABASE_URL.startswith('postgres'):
        log("Error: DATABASE_URL not set or not postgres.")
        return

    if not os.path.exists(SQLITE_DB_PATH):
        log(f"Error: SQLite database {SQLITE_DB_PATH} not found.")
        return

    # Initialize PostgreSQL Schema
    log("Initializing PostgreSQL schema...")
    try:
        from app import create_app, database
        app = create_app()
        with app.app_context():
            database.init_db()
        log("Schema initialized.")
    except Exception as e:
        log(f"Error initializing schema: {e}")
        log(traceback.format_exc())
        return

    # Connect to SQLite
    try:
        log("Connecting to SQLite...")
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cur = sqlite_conn.cursor()

        # Connect to Postgres
        log("Connecting to Postgres...")
        pg_conn = psycopg2.connect(DATABASE_URL)
        pg_cur = pg_conn.cursor()
    except Exception as e:
        log(f"Connection error: {e}")
        return

    try:
        for table in TABLES:
            log(f"Migrating table: {table}...")
            
            # Get columns from SQLite
            try:
                sqlite_cur.execute(f"PRAGMA table_info({table})")
                columns_info = sqlite_cur.fetchall()
                columns = [c['name'] for c in columns_info]
                
                if not columns:
                    log(f"  Skipping {table} (no columns found)")
                    continue

                # Select data from SQLite with Orphan Cleaning
                placeholders = ', '.join(['?' for _ in columns])
                col_names = ', '.join(columns)
                
                query = f"SELECT {col_names} FROM {table}"
                if table == 'order_events':
                     query += " WHERE order_id IN (SELECT id FROM orders)"
                elif table == 'order_items':
                     query += " WHERE order_id IN (SELECT id FROM orders)"
                elif table == 'order_status_history':
                     query += " WHERE order_id IN (SELECT id FROM orders)"
                elif table == 'archived_orders':
                     query += " WHERE order_id IN (SELECT id FROM orders)"
                elif table == 'cash_movements':
                     query += " WHERE session_id IN (SELECT id FROM cash_sessions)"
                
                sqlite_cur.execute(query)
                rows = sqlite_cur.fetchall()
                
                if not rows:
                    log(f"  Table {table} is empty (or all rows were orphans).")
                    continue
                
                log(f"  Found {len(rows)} valid rows in {table}.")
                
                data = [tuple(row) for row in rows]
                
                pg_col_names = ', '.join(columns)
                insert_query = f"INSERT INTO {table} ({pg_col_names}) VALUES %s ON CONFLICT DO NOTHING"
                
                execute_values(pg_cur, insert_query, data)
                pg_conn.commit() # Commit per table
                log(f"  Inserted/Skipped {len(data)} rows.")
                
            except Exception as e:
                pg_conn.rollback() # Rollback only this failed table/transaction
                log(f"  Error migrating {table}: {e}")

        log("Migration completed successfully.")
        
    except Exception as e:
        # Should not reach here due to inner try/except, but just in case
        pg_conn.rollback()
        log(f"Migration failed: {e}")
        log(traceback.format_exc())
    finally:
        sqlite_conn.close()
        pg_conn.close()

if __name__ == '__main__':
    migrate()
