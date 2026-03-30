import os
import sqlite3
import json
from flask import g, current_app
from werkzeug.security import generate_password_hash

# Try to import psycopg2
try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import pool
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Global connection pool
pg_pool = None

def init_pool():
    global pg_pool
    if pg_pool is None and HAS_PSYCOPG2:
        database_url = os.environ.get('DATABASE_URL')
        if database_url and (database_url.startswith('postgres://') or database_url.startswith('postgresql://')):
            try:
                pg_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, database_url)
                print("PostgreSQL Connection Pool initialized.")
            except Exception as e:
                print(f"Error initializing connection pool: {e}")

class PostgresRow:
    def __init__(self, cursor, row):
        self._row = row
        self._col_map = {d[0]: i for i, d in enumerate(cursor.description)}

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._row[item]
        return self._row[self._col_map[item]]
    
    def get(self, item, default=None):
        try:
            return self[item]
        except (KeyError, IndexError):
            return default

    def keys(self):
        return self._col_map.keys()

class PostgresCursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor
        self._lastrowid = None

    def execute(self, query, params=None):
        # 1. Handle Placeholders
        q = query.replace('?', '%s')
        
        # 2. Handle INSERT OR IGNORE
        if 'INSERT OR IGNORE' in q:
            q = q.replace('INSERT OR IGNORE', 'INSERT')
            q += ' ON CONFLICT DO NOTHING'
            
        # 3. Handle INSERT OR REPLACE (Specific to tenant_config)
        if 'INSERT OR REPLACE INTO tenant_config' in q:
            q = q.replace('INSERT OR REPLACE INTO', 'INSERT INTO')
            q += ' ON CONFLICT (tenant_slug) DO UPDATE SET config_json = EXCLUDED.config_json'
            
        # 4. Handle lastrowid via RETURNING id
        # Only for INSERTs that don't already have RETURNING
        is_insert = q.strip().upper().startswith('INSERT')
        if is_insert and 'RETURNING' not in q.upper():
            # Special case: tenant_config has no ID column, skip it
            if 'tenant_config' not in q:
                q += ' RETURNING id'
                try:
                    self.cursor.execute(q, params)
                    res = self.cursor.fetchone()
                    self._lastrowid = res[0] if res else None
                    return
                except Exception as e:
                    # If table has no id column, it might fail?
                    # But we assume other tables have id.
                    raise e
            
        self.cursor.execute(q, params)

    @property
    def lastrowid(self):
        return self._lastrowid
        
    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None: return None
        return PostgresRow(self.cursor, row)

    def fetchall(self):
        rows = self.cursor.fetchall()
        return [PostgresRow(self.cursor, row) for row in rows]
    
    def __getattr__(self, name):
        return getattr(self.cursor, name)
    
    @property
    def rowcount(self):
        return self.cursor.rowcount
    
    @property
    def description(self):
        return self.cursor.description

class PostgresConnectionWrapper:
    def __init__(self, conn):
        self.conn = conn
    
    def cursor(self, *args, **kwargs):
        return PostgresCursorWrapper(self.conn.cursor(*args, **kwargs))
    
    def commit(self):
        return self.conn.commit()
        
    def rollback(self):
        return self.conn.rollback()
        
    def close(self):
        return self.conn.close()
        
    def __getattr__(self, name):
        return getattr(self.conn, name)

def is_postgres():
    return current_app.config.get('IS_POSTGRES', False)

def get_db():
    if 'db' not in g:
        database_url = os.environ.get('DATABASE_URL')
        if database_url and (database_url.startswith('postgres://') or database_url.startswith('postgresql://')):
            if not HAS_PSYCOPG2:
                raise ImportError("psycopg2 is required for PostgreSQL")
            
            # Initialize pool if needed
            global pg_pool
            if pg_pool is None:
                init_pool()
            
            # Get connection from pool
            if pg_pool:
                try:
                    conn = pg_pool.getconn()
                    g.db = PostgresConnectionWrapper(conn)
                    current_app.config['IS_POSTGRES'] = True
                except Exception as e:
                    # Fallback or error
                    print(f"Error getting connection from pool: {e}")
                    raise e
            else:
                # Fallback to direct connection if pool failed (shouldn't happen if init_pool works)
                conn = psycopg2.connect(database_url)
                g.db = PostgresConnectionWrapper(conn)
                current_app.config['IS_POSTGRES'] = True

        else:
            # SQLite path may be read-only in some PaaS. Try configured path first, then fallback to /tmp.
            db_path = current_app.config['DATABASE']
            try:
                g.db = sqlite3.connect(db_path)
                g.db.row_factory = sqlite3.Row
                current_app.config['IS_POSTGRES'] = False
            except Exception as e:
                try:
                    tmp_path = os.environ.get('DATABASE_PATH') or os.path.join('/tmp', 'orders.db')
                    # Ensure directory exists for tmp_path
                    os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
                    g.db = sqlite3.connect(tmp_path)
                    g.db.row_factory = sqlite3.Row
                    current_app.config['DATABASE'] = tmp_path
                    current_app.config['IS_POSTGRES'] = False
                    print(f"SQLite fallback: using {tmp_path} due to error opening {db_path}: {e}")
                except Exception as e2:
                    print(f"SQLite connection failed for both {db_path} and /tmp fallback: {e2}")
                    raise e2
            
    return g.db

def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        if is_postgres() and pg_pool:
            # Return to pool
            pg_pool.putconn(db.conn)
        else:
            db.close()

def fix_postgres_sequences(cur):
    try:
        tables = [
            'tenants', 'orders', 'order_items', 'order_status_history',
            'archived_orders', 'products', 'admin_users', 'order_events',
            'cash_sessions', 'cash_movements', 'carousel_slides'
        ]
        for table in tables:
            try:
                # Reset sequence to max(id)
                # pg_get_serial_sequence returns the sequence name for the column
                cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), coalesce(max(id), 1)) FROM {table}")
            except Exception:
                pass
    except Exception:
        pass

def init_db_postgres(cur):
    # Master users (for Panel Master)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS master_users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id SERIAL PRIMARY KEY,
            tenant_slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            contact_email TEXT,
            contact_phone TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            status_message TEXT DEFAULT '',
            plan TEXT NOT NULL DEFAULT 'standard',
            max_users INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status)")
    try:
        cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS status_message TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'standard'")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_users INTEGER NOT NULL DEFAULT 3")
    except Exception:
        pass
    # Tabla de pedidos
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            tenant_slug TEXT NOT NULL,
            tenant_order_number INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            order_type TEXT NOT NULL,
            table_number TEXT,
            address_json TEXT,
            status TEXT NOT NULL,
            total INTEGER NOT NULL,
            payment_method TEXT,
            payment_status TEXT,
            tip_amount INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            order_notes TEXT,
            shipping_cost INTEGER DEFAULT 0,
            delivery_assigned_to TEXT,
            delivery_status TEXT DEFAULT 'pending',
            delivery_sequence INTEGER,
            delivery_notes TEXT,
            delivery_assigned_at TEXT,
            delivered_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_runs (
            id SERIAL PRIMARY KEY,
            tenant_slug TEXT NOT NULL,
            driver_username TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            started_at TEXT NOT NULL,
            closed_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_runs_tenant_driver_status ON delivery_runs(tenant_slug, driver_username, status)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_run_orders (
            id SERIAL PRIMARY KEY,
            run_id INTEGER NOT NULL,
            order_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            added_at TEXT NOT NULL,
            UNIQUE(run_id, order_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_run_seq ON delivery_run_orders(run_id, sequence)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_order ON delivery_run_orders(order_id)")
    
    # Check columns (Postgres way)
    cur.execute("SAVEPOINT add_order_notes")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN order_notes TEXT")
        cur.execute("RELEASE SAVEPOINT add_order_notes")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_order_notes")

    cur.execute("SAVEPOINT add_tip_amount")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN tip_amount INTEGER DEFAULT 0")
        cur.execute("RELEASE SAVEPOINT add_tip_amount")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_tip_amount")

    cur.execute("SAVEPOINT add_shipping_cost")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN shipping_cost INTEGER DEFAULT 0")
        cur.execute("RELEASE SAVEPOINT add_shipping_cost")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_shipping_cost")

    cur.execute("SAVEPOINT add_delivery_assigned_to")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN delivery_assigned_to TEXT")
        cur.execute("RELEASE SAVEPOINT add_delivery_assigned_to")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_delivery_assigned_to")

    cur.execute("SAVEPOINT add_delivery_status")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN delivery_status TEXT DEFAULT 'pending'")
        cur.execute("RELEASE SAVEPOINT add_delivery_status")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_delivery_status")

    cur.execute("SAVEPOINT add_delivery_sequence")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN delivery_sequence INTEGER")
        cur.execute("RELEASE SAVEPOINT add_delivery_sequence")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_delivery_sequence")

    cur.execute("SAVEPOINT add_delivery_notes")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN delivery_notes TEXT")
        cur.execute("RELEASE SAVEPOINT add_delivery_notes")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_delivery_notes")

    cur.execute("SAVEPOINT add_delivery_assigned_at")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN delivery_assigned_at TEXT")
        cur.execute("RELEASE SAVEPOINT add_delivery_assigned_at")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_delivery_assigned_at")

    cur.execute("SAVEPOINT add_delivered_at")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN delivered_at TEXT")
        cur.execute("RELEASE SAVEPOINT add_delivered_at")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_delivered_at")

    cur.execute("SAVEPOINT add_tenant_order_number")
    try:
        cur.execute("ALTER TABLE orders ADD COLUMN tenant_order_number INTEGER")
        cur.execute("RELEASE SAVEPOINT add_tenant_order_number")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_tenant_order_number")

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tenant_counters (
                tenant_slug TEXT PRIMARY KEY,
                next_order_number INTEGER NOT NULL
            )
            """
        )
    except Exception:
        pass
    try:
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_tenant_order_number ON orders(tenant_slug, tenant_order_number) WHERE tenant_order_number IS NOT NULL"
        )
    except Exception:
        pass

    # Ítems del pedido
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS order_items (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            tenant_slug TEXT NOT NULL,
            product_id TEXT,
            name TEXT NOT NULL,
            qty INTEGER NOT NULL,
            unit_price INTEGER NOT NULL,
            modifiers_json TEXT,
            notes TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )

    # Índices básicos
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_tenant_status ON orders(tenant_slug, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)")

    # Historial de cambios de estado
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS order_status_history (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            changed_by TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_history_order ON order_status_history(order_id)")
    cur.execute("SAVEPOINT add_changed_by")
    try:
        cur.execute("ALTER TABLE order_status_history ADD COLUMN changed_by TEXT")
        cur.execute("RELEASE SAVEPOINT add_changed_by")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT add_changed_by")

    # Archived Orders
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS archived_orders (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            tenant_slug TEXT NOT NULL,
            type TEXT NOT NULL,
            archived_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_archived_unique ON archived_orders(order_id, type)")
    
    # Configuración del tenant
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tenant_config (
            tenant_slug TEXT PRIMARY KEY,
            config_json TEXT
        )
        """
    )
    
    # Inventario de productos
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            tenant_slug TEXT NOT NULL,
            product_id TEXT NOT NULL,
            name TEXT NOT NULL,
            price INTEGER NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            details TEXT,
            variants_json TEXT,
            last_modified TEXT,
            image_url TEXT,
            UNIQUE(tenant_slug, product_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_tenant ON products(tenant_slug)")
    
    # Admin Users
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            id SERIAL PRIMARY KEY,
            tenant_slug TEXT NOT NULL,
            username TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'admin',
            permissions_json TEXT DEFAULT '',
            is_owner INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT,
            UNIQUE(tenant_slug, username)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_users_tenant ON admin_users(tenant_slug)")
    try:
        cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'admin'")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS permissions_json TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS is_owner INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE admin_users ADD COLUMN IF NOT EXISTS last_seen_at TEXT")
    except Exception:
        pass

    # Auditoría de eventos
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS order_events (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            actor TEXT,
            terminal TEXT,
            amount_delta INTEGER,
            payload_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_order ON order_events(order_id)")

    # Sesiones de caja
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cash_sessions (
            id SERIAL PRIMARY KEY,
            tenant_slug TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'tenant',
            opened_at TEXT NOT NULL,
            opened_by TEXT,
            opening_amount INTEGER NOT NULL,
            notes_open TEXT,
            closed_at TEXT,
            closed_by TEXT,
            closing_amount INTEGER,
            notes_close TEXT,
            closing_diff INTEGER,
            closing_metadata TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_sessions_tenant ON cash_sessions(tenant_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_sessions_open ON cash_sessions(tenant_slug, opened_at)")
    try:
        cur.execute("ALTER TABLE cash_sessions ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'tenant'")
    except Exception:
        try:
            cur.connection.rollback()
        except Exception:
            pass

    # Movimientos de caja
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cash_movements (
            id SERIAL PRIMARY KEY,
            session_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            amount INTEGER NOT NULL,
            note TEXT,
            actor TEXT,
            created_at TEXT NOT NULL,
            payment_method TEXT,
            FOREIGN KEY(session_id) REFERENCES cash_sessions(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_movements_session ON cash_movements(session_id)")

    # Carrusel
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS carousel_slides (
            id SERIAL PRIMARY KEY,
            tenant_slug TEXT NOT NULL,
            image_url TEXT NOT NULL,
            title TEXT,
            text TEXT,
            position INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TEXT,
            title_color TEXT,
            text_color TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_carousel_tenant ON carousel_slides(tenant_slug)")
    
    # Fix sequences after table creation (in case of migration)
    fix_postgres_sequences(cur)

def init_db_sqlite(cur):
    # Master users (for Panel Master)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS master_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            contact_email TEXT,
            contact_phone TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            status_message TEXT DEFAULT '',
            plan TEXT NOT NULL DEFAULT 'standard',
            max_users INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status)")
    try:
        cur.execute("PRAGMA table_info(tenants)")
        cols = [r[1] for r in cur.fetchall()]
        if 'status_message' not in cols:
            cur.execute("ALTER TABLE tenants ADD COLUMN status_message TEXT DEFAULT ''")
    except Exception:
        pass
    # Tabla de pedidos
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL,
            tenant_order_number INTEGER,
            customer_name TEXT,
            customer_phone TEXT,
            order_type TEXT NOT NULL,
            table_number TEXT,
            address_json TEXT,
            status TEXT NOT NULL,
            total INTEGER NOT NULL,
            payment_method TEXT,
            payment_status TEXT,
            tip_amount INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            order_notes TEXT,
            shipping_cost INTEGER DEFAULT 0,
            delivery_assigned_to TEXT,
            delivery_status TEXT DEFAULT 'pending',
            delivery_sequence INTEGER,
            delivery_notes TEXT,
            delivery_assigned_at TEXT,
            delivered_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL,
            driver_username TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            started_at TEXT NOT NULL,
            closed_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_runs_tenant_driver_status ON delivery_runs(tenant_slug, driver_username, status)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS delivery_run_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            order_id INTEGER NOT NULL,
            sequence INTEGER NOT NULL,
            added_at TEXT NOT NULL,
            UNIQUE(run_id, order_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_run_seq ON delivery_run_orders(run_id, sequence)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delivery_run_orders_order ON delivery_run_orders(order_id)")
    
    # Check columns
    try:
        cur.execute("PRAGMA table_info(orders)")
        cols = [r[1] for r in cur.fetchall()]
        if 'order_notes' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN order_notes TEXT")
        if 'tip_amount' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN tip_amount INTEGER DEFAULT 0")
        if 'shipping_cost' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN shipping_cost INTEGER DEFAULT 0")
        if 'delivery_assigned_to' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN delivery_assigned_to TEXT")
        if 'delivery_status' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN delivery_status TEXT DEFAULT 'pending'")
        if 'delivery_sequence' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN delivery_sequence INTEGER")
        if 'delivery_notes' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN delivery_notes TEXT")
        if 'delivery_assigned_at' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN delivery_assigned_at TEXT")
        if 'delivered_at' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN delivered_at TEXT")
        if 'tenant_order_number' not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN tenant_order_number INTEGER")
    except Exception:
        pass

    # Ítems del pedido
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            tenant_slug TEXT NOT NULL,
            product_id TEXT,
            name TEXT NOT NULL,
            qty INTEGER NOT NULL,
            unit_price INTEGER NOT NULL,
            modifiers_json TEXT,
            notes TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )

    # Índices básicos
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_tenant_status ON orders(tenant_slug, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)")
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_tenant_order_number ON orders(tenant_slug, tenant_order_number) WHERE tenant_order_number IS NOT NULL")
    except Exception:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tenant_counters (
            tenant_slug TEXT PRIMARY KEY,
            next_order_number INTEGER NOT NULL
        )
        """
    )

    # Historial de cambios de estado
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS order_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            changed_by TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_history_order ON order_status_history(order_id)")
    try:
        cur.execute("PRAGMA table_info(order_status_history)")
        cols_hist = [r[1] for r in cur.fetchall()]
        if 'changed_by' not in cols_hist:
            cur.execute("ALTER TABLE order_status_history ADD COLUMN changed_by TEXT")
    except Exception:
        pass

    # Archived Orders
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS archived_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            tenant_slug TEXT NOT NULL,
            type TEXT NOT NULL,
            archived_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_archived_unique ON archived_orders(order_id, type)")
    
    # Configuración del tenant
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tenant_config (
            tenant_slug TEXT PRIMARY KEY,
            config_json TEXT
        )
        """
    )
    
    # Inventario de productos
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL,
            product_id TEXT NOT NULL,
            name TEXT NOT NULL,
            price INTEGER NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            details TEXT,
            variants_json TEXT,
            last_modified TEXT,
            image_url TEXT,
            UNIQUE(tenant_slug, product_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_tenant ON products(tenant_slug)")
    
    # Admin Users
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL,
            username TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'admin',
            permissions_json TEXT DEFAULT '',
            is_owner INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT,
            UNIQUE(tenant_slug, username)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_users_tenant ON admin_users(tenant_slug)")

    # Auditoría de eventos
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS order_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            actor TEXT,
            terminal TEXT,
            amount_delta INTEGER,
            payload_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_order ON order_events(order_id)")

    # Sesiones de caja
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cash_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'tenant',
            opened_at TEXT NOT NULL,
            opened_by TEXT,
            opening_amount INTEGER NOT NULL,
            notes_open TEXT,
            closed_at TEXT,
            closed_by TEXT,
            closing_amount INTEGER,
            notes_close TEXT,
            closing_diff INTEGER,
            closing_metadata TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_sessions_tenant ON cash_sessions(tenant_slug)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_sessions_open ON cash_sessions(tenant_slug, opened_at)")
    try:
        cur.execute("PRAGMA table_info(cash_sessions)")
        cols = [r[1] for r in cur.fetchall()]
        if 'scope' not in cols:
            cur.execute("ALTER TABLE cash_sessions ADD COLUMN scope TEXT NOT NULL DEFAULT 'tenant'")
    except Exception:
        pass

    # Movimientos de caja
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cash_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            amount INTEGER NOT NULL,
            note TEXT,
            actor TEXT,
            created_at TEXT NOT NULL,
            payment_method TEXT,
            FOREIGN KEY(session_id) REFERENCES cash_sessions(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_movements_session ON cash_movements(session_id)")

    # Carrusel
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS carousel_slides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_slug TEXT NOT NULL,
            image_url TEXT NOT NULL,
            title TEXT,
            text TEXT,
            position INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_at TEXT,
            title_color TEXT,
            text_color TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_carousel_tenant ON carousel_slides(tenant_slug)")

def init_db():
    try:
        db = get_db()
        cur = db.cursor()
        
        if is_postgres():
            init_db_postgres(cur)
        else:
            init_db_sqlite(cur)
        
        db.commit()
    except Exception as e:
        print(f"WARNING: Database initialization failed: {e}")
        # Don't crash the app, just log the error.
        # This allows the app to start even if DB is temporarily unreachable.
        pass

def seed_products_from_config(config_dir):
    try:
        db = get_db()
        cur = db.cursor()
        for name in os.listdir(config_dir):
            if not name.endswith('.json'):
                continue
            p = os.path.join(config_dir, name)
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    j = json.load(f)
                meta = j.get('meta') or {}
                slug = meta.get('slug') or name.replace('.json','')
                catalog = j.get('catalog') or []
                for it in catalog:
                    pid = str(it.get('id') or '').strip()
                    nm = str(it.get('name') or '').strip()
                    price = int(it.get('price') or 0)
                    img = str(it.get('image') or '').strip()
                    if not pid or not nm:
                        continue
                    cur.execute(
                        "INSERT OR IGNORE INTO products (tenant_slug, product_id, name, price, stock, active, image_url) VALUES (?, ?, ?, ?, ?, 1, ?)",
                        (slug, pid, nm, price, 50, img)
                    )
            except Exception:
                continue
        db.commit()
    except Exception:
        pass

def backfill_product_details_from_config(config_dir):
    try:
        db = get_db()
        cur = db.cursor()
        for name in os.listdir(config_dir):
            if not name.endswith('.json'):
                continue
            p = os.path.join(config_dir, name)
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    j = json.load(f)
                meta = j.get('meta') or {}
                slug = meta.get('slug') or name.replace('.json','')
                catalog = j.get('catalog') or []
                for it in catalog:
                    pid = str(it.get('id') or '').strip()
                    desc = str(it.get('description') or '').strip()
                    if not pid or not desc:
                        continue
                    cur.execute(
                        "UPDATE products SET details = ? WHERE tenant_slug = ? AND product_id = ? AND (details IS NULL OR TRIM(details) = '')",
                        (desc, slug, pid)
                    )
            except Exception:
                continue
        db.commit()
    except Exception:
        pass

def backfill_product_variants_from_config(config_dir):
    try:
        db = get_db()
        cur = db.cursor()
        for name in os.listdir(config_dir):
            if not name.endswith('.json'):
                continue
            p = os.path.join(config_dir, name)
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    j = json.load(f)
                meta = j.get('meta') or {}
                slug = meta.get('slug') or name.replace('.json','')
                catalog = j.get('catalog') or []
                for it in catalog:
                    pid = str(it.get('id') or '').strip()
                    if not pid: continue
                    
                    # Build variants object
                    variants = {}
                    
                    # 1. Food Categories
                    cats = it.get('categories')
                    if cats:
                        if isinstance(cats, list):
                            variants['food_categories'] = cats
                        else:
                            variants['food_categories'] = [str(cats)]
                    
                    # 2. Section / Interest Tag
                    tags = it.get('tags') or []
                    if tags:
                        # Logic for tags
                        if "Destacados" in tags:
                             variants['section'] = 'featured'
                        elif "Liquidaciones" in tags:
                             variants['section'] = 'interest'
                             variants['interest_tag'] = 'oferta' # Default for Liquidaciones
                        elif "Promociones" in tags:
                             variants['section'] = 'interest'
                             variants['interest_tag'] = 'promocion'
                        elif "2x1" in tags:
                             variants['section'] = 'interest'
                             variants['interest_tag'] = '2x1'
                    
                    if not variants:
                        continue
                        
                    v_json = json.dumps(variants)
                    
                    cur.execute(
                        "UPDATE products SET variants_json = ? WHERE tenant_slug = ? AND product_id = ?",
                        (v_json, slug, pid)
                    )
            except Exception:
                continue
        db.commit()
    except Exception:
        pass

def backfill_product_images_from_config(config_dir):
    try:
        db = get_db()
        cur = db.cursor()
        for name in os.listdir(config_dir):
            if not name.endswith('.json'):
                continue
            p = os.path.join(config_dir, name)
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    j = json.load(f)
                meta = j.get('meta') or {}
                slug = meta.get('slug') or name.replace('.json','')
                catalog = j.get('catalog') or []
                for it in catalog:
                    pid = str(it.get('id') or '').strip()
                    img = str(it.get('image') or '').strip()
                    if not pid or not img:
                        continue
                    cur.execute(
                        "UPDATE products SET image_url = ? WHERE tenant_slug = ? AND product_id = ? AND (image_url IS NULL OR TRIM(image_url) = '')",
                        (img, slug, pid)
                    )
            except Exception:
                continue
        db.commit()
    except Exception:
        pass

def seed_admin_users_from_env(config_dir):
    try:
        admin_user = os.environ.get('ADMIN_USERNAME') or 'admin'
        admin_pass = os.environ.get('ADMIN_PASSWORD') or 'admin123'
        admin_legacy_pass = os.environ.get('ADMIN_LEGACY_PASSWORD') or 'GastroPanel!123'
        
        db = get_db()
        cur = db.cursor()
        for name in os.listdir(config_dir):
            if not name.endswith('.json'):
                continue
            p = os.path.join(config_dir, name)
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    j = json.load(f)
                meta = j.get('meta') or {}
                slug = meta.get('slug') or name.replace('.json','')
                ph = generate_password_hash(admin_pass)
                cur.execute(
                    "INSERT OR IGNORE INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)",
                    (slug, admin_user, ph)
                )
                # Inserta también admin con contraseña legacy si corresponde
                if admin_legacy_pass:
                    ph_legacy = generate_password_hash(admin_legacy_pass)
                    cur.execute(
                        "INSERT OR IGNORE INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)",
                        (slug, 'admin', ph_legacy)
                    )
                admins = j.get('admins') or meta.get('admins') or []
                for adm in admins:
                    try:
                        un = str(adm.get('username') or '').strip()
                        pw = str(adm.get('password') or '')
                        if not un or not pw:
                            continue
                        ph_adm = generate_password_hash(pw)
                        cur.execute(
                            "INSERT OR IGNORE INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)",
                            (slug, un, ph_adm)
                        )
                    except: continue
            except Exception:
                continue
        db.commit()
    except Exception:
        pass

def init_app(app):
    app.teardown_appcontext(close_db)
    # Ensure config keys exist
    if 'DATABASE' not in app.config:
        app.config['DATABASE'] = os.path.join(app.root_path, '..', 'orders.db')
    if 'CONFIG_DIR' not in app.config:
        app.config['CONFIG_DIR'] = os.path.join(app.root_path, '..', 'config')
