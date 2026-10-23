import sqlite3
import sys
import os
import argparse
from werkzeug.security import generate_password_hash

# Configuración
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'orders.db')

def get_db():
    return sqlite3.connect(DB_PATH)

def list_users(tenant=None):
    conn = get_db()
    cur = conn.cursor()
    if tenant:
        cur.execute("SELECT id, tenant_slug, username FROM admin_users WHERE tenant_slug = ? ORDER BY username", (tenant,))
    else:
        cur.execute("SELECT id, tenant_slug, username FROM admin_users ORDER BY tenant_slug, username")
    rows = cur.fetchall()
    conn.close()
    
    if not rows:
        print("No users found.")
        return

    print(f"{'ID':<5} {'TENANT':<30} {'USERNAME'}")
    print("-" * 50)
    for r in rows:
        print(f"{r[0]:<5} {r[1]:<30} {r[2]}")

def add_user(tenant, username, password):
    if not tenant or not username or not password:
        print("Error: tenant, username, and password are required.")
        return

    conn = get_db()
    cur = conn.cursor()
    ph = generate_password_hash(password)
    try:
        cur.execute("INSERT INTO admin_users (tenant_slug, username, password_hash) VALUES (?, ?, ?)",
                    (tenant, username, ph))
        conn.commit()
        print(f"User '{username}' created successfully for tenant '{tenant}'.")
    except sqlite3.IntegrityError:
        print(f"Error: User '{username}' already exists for tenant '{tenant}'.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def delete_user(tenant, username):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM admin_users WHERE tenant_slug = ? AND username = ?", (tenant, username))
    if cur.rowcount > 0:
        print(f"User '{username}' deleted from tenant '{tenant}'.")
    else:
        print(f"User '{username}' not found in tenant '{tenant}'.")
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Manage Admin Users for Tenants")
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # List
    list_parser = subparsers.add_parser('list', help='List users')
    list_parser.add_argument('--tenant', help='Filter by tenant slug')

    # Add
    add_parser = subparsers.add_parser('add', help='Add a new user')
    add_parser.add_argument('tenant', help='Tenant Slug (e.g. gastronomia-local1)')
    add_parser.add_argument('username', help='Username')
    add_parser.add_argument('password', help='Password')

    # Delete
    del_parser = subparsers.add_parser('delete', help='Delete a user')
    del_parser.add_argument('tenant', help='Tenant Slug')
    del_parser.add_argument('username', help='Username')

    args = parser.parse_args()

    if args.command == 'list':
        list_users(args.tenant)
    elif args.command == 'add':
        add_user(args.tenant, args.username, args.password)
    elif args.command == 'delete':
        delete_user(args.tenant, args.username)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
