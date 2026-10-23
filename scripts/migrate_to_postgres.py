import os
import sys
import traceback

def debug_log(msg):
    try:
        with open('debug_migration.txt', 'a') as f:
            f.write(str(msg) + '\n')
    except:
        pass

debug_log("Script started")

try:
    import sqlite3
    debug_log("sqlite3 imported")
    import psycopg2
    debug_log("psycopg2 imported")
    from psycopg2.extras import execute_values
    debug_log("execute_values imported")
    from dotenv import load_dotenv
    debug_log("dotenv imported")
    from app import create_app, database
    debug_log("app imported")
except Exception as e:
    debug_log(f"Import error: {e}")
    debug_log(traceback.format_exc())
    sys.exit(1)

load_dotenv()
debug_log("dotenv loaded")

SQLITE_DB_PATH = 'orders.db'
DATABASE_URL = os.environ.get('DATABASE_URL')
debug_log(f"DB URL: {DATABASE_URL}")

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

def log(msg):
    print(msg)
    sys.stdout.flush()
    with open('migration.log', 'a', encoding='utf-8') as f:
        f.write(str(msg) + '\n')

def migrate():
    log("Starting migration...")
    if not DATABASE_URL or not DATABASE_URL.startswith('postgres'):
        log("Error: DATABASE_URL not set or not postgres.")
        return

    if not os.path.exists(SQLITE_DB_PATH):
        log(f"Error: SQLite database {SQLITE_DB_PATH} not found.")
        return

    log(f"Migrating from {SQLITE_DB_PATH} to PostgreSQL...")

    # Initialize PostgreSQL Schema
    log("Initializing PostgreSQL schema...")
    try:
        app = create_app()
        with app.app_context():
            database.init_db()
        log("Schema initialized.")
    except Exception as e:
        log(f"Error initializing schema: {e}")
        import traceback
        log(traceback.format_exc())
        return

    # Connect to SQLite
    try:
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_cur = sqlite_conn.cursor()

        # Connect to Postgres
        pg_conn = psycopg2.connect(DATABASE_URL)
        pg_cur = pg_conn.cursor()
    except Exception as e:
        log(f"Connection error: {e}")
        return

    try:
        for table in TABLES:
            log(f"Migrating table: {table}...")
            
            # Get columns from SQLite
            sqlite_cur.execute(f"PRAGMA table_info({table})")
            columns_info = sqlite_cur.fetchall()
            columns = [c['name'] for c in columns_info]
            
            if not columns:
                log(f"  Skipping {table} (no columns found)")
                continue

            # Select data from SQLite
            col_str = ", ".join(columns)
            sqlite_cur.execute(f"SELECT {col_str} FROM {table}")
            rows = sqlite_cur.fetchall()
            
            if not rows:
                log("  No data found.")
                continue
            
            log(f"  Found {len(rows)} rows.")

            # Prepare INSERT statement
            # execute_values expects a single %s where the values list will be injected
            query = f"INSERT INTO {table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"
            
            # Convert rows to list of tuples
            data = [tuple(row) for row in rows]
            
            # Execute batch insert
            execute_values(pg_cur, query, data)
            
            # Reset sequence if table has id
            if 'id' in columns and table != 'tenant_config':
                log(f"  Resetting sequence for {table}...")
                pg_cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), coalesce(max(id), 1)) FROM {table}")

        pg_conn.commit()
        log("Migration completed successfully.")

    except Exception as e:
        pg_conn.rollback()
        log(f"Error during migration: {e}")
        import traceback
        log(traceback.format_exc())
    finally:
        sqlite_conn.close()
        pg_conn.close()

if __name__ == '__main__':
    debug_log("In main block")
    print("In main block")
    # Clear log
    with open('migration.log', 'w') as f: f.write('')
    migrate()
    debug_log("Migrate called")
