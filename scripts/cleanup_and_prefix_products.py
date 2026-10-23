import sqlite3
import os
import re

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'orders.db')

def get_db():
    print(f"Connecting to database at: {DB_PATH}")
    return sqlite3.connect(DB_PATH)

def get_prefix(slug):
    # Heuristic to determine prefix
    if slug == 'gastronomia-local1': return 'l1-'
    if slug == 'gastronomia-local2': return 'l2-'
    if slug == 'gastronomia-local3': return 'l3-'
    if slug == 'gastronomia-local4': return 'l4-'
    if slug == 'gastronomia-local5': return 'l5-'
    if slug == 'comercio': return 'c1-'
    if slug == 'comercio1': return 'c2-'
    if slug == 'gastronomia': return 'g-'
    if slug == 'site-base': return 'sb-'
    
    # Fallback: first letter + number if present, or first 3 chars
    m = re.search(r'local(\d+)', slug)
    if m:
        return f"l{m.group(1)}-"
    
    return slug[:3] + '-'

def main():
    print("Starting product cleanup and ID prefixing...")
    conn = get_db()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get all products
    cur.execute("SELECT id, tenant_slug, product_id FROM products")
    products = cur.fetchall()
    
    updates_count = 0
    deletes_count = 0
    items_updated = 0
    
    for p in products:
        pid = p['id']
        slug = p['tenant_slug']
        prod_id = p['product_id']
        
        prefix = get_prefix(slug)
        
        # If already starts with prefix, skip (unless it's a double prefix? no, assume correct)
        if prod_id.startswith(prefix):
            continue
            
        # If it's a 'legacy' ID (e.g. 'plato1'), we want to migrate it to 'l1-plato1'
        target_id = prefix + prod_id
        
        # Check if target_id already exists in this tenant
        cur.execute("SELECT id FROM products WHERE tenant_slug = ? AND product_id = ?", (slug, target_id))
        existing_target = cur.fetchone()
        
        if existing_target:
            # The prefixed version ALREADY exists.
            # This means 'prod_id' is likely a duplicate/legacy version.
            # We should DELETE 'prod_id' and point any orders to 'target_id'.
            print(f"[{slug}] Merging '{prod_id}' into existing '{target_id}'...")
            
            # 1. Update order_items
            cur.execute("""
                UPDATE order_items 
                SET product_id = ? 
                WHERE tenant_slug = ? AND product_id = ?
            """, (target_id, slug, prod_id))
            items_updated += cur.rowcount
            
            # 2. Delete the legacy product
            cur.execute("DELETE FROM products WHERE id = ?", (pid,))
            deletes_count += 1
            
        else:
            # The prefixed version does NOT exist.
            # We simply RENAME 'prod_id' to 'target_id'.
            print(f"[{slug}] Renaming '{prod_id}' to '{target_id}'...")
            
            # 1. Update products table
            cur.execute("UPDATE products SET product_id = ? WHERE id = ?", (target_id, pid))
            updates_count += 1
            
            # 2. Update order_items
            cur.execute("""
                UPDATE order_items 
                SET product_id = ? 
                WHERE tenant_slug = ? AND product_id = ?
            """, (target_id, slug, prod_id))
            items_updated += cur.rowcount

    conn.commit()
    conn.close()
    
    print("-" * 30)
    print(f"Cleanup Complete.")
    print(f"Renamed Products: {updates_count}")
    print(f"Merged/Deleted Products: {deletes_count}")
    print(f"Order Items Updated: {items_updated}")

if __name__ == '__main__':
    main()
