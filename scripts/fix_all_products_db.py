import sqlite3
import json
import re
import os

DB_PATH = 'orders.db'

FILES = [
    'gastronomia-local2.html',
    'gastronomia-local3.html',
    'gastronomia-local4.html',
    'gastronomia-local5.html',
    'gastronomia-independiente.html'
]

def get_tenant_slug(content):
    m = re.search(r'data-slug="([^"]+)"', content)
    if m:
        return m.group(1)
    return None

def parse_products(content):
    products = []
    
    # We need to find products and determine their section.
    # Since regex is global, we can't easily know which section we are in unless we split the content.
    
    # Split by sections
    sections = {
        'featured': re.search(r'id="featured-dishes"(.*?)id="menu-gastronomia"', content, re.DOTALL),
        'main': re.search(r'id="menu-gastronomia"(.*?)class="products interest-products', content, re.DOTALL),
        'interest': re.search(r'class="products interest-products(.*?)id="restaurant-info"', content, re.DOTALL)
    }
    
    # Fallback if structure varies slightly (e.g. interest-products id might differ)
    if not sections['interest']:
         sections['interest'] = re.search(r'class="products interest-products(.*?)id="restaurant-info"', content, re.DOTALL)
    
    for sec_name, match in sections.items():
        if not match:
            continue
        
        section_content = match.group(1)
        
        # Regex for product card
        # looking for data-id on button
        # <button ... data-id="l2-dest1" data-name="..." data-price="...">
        
        # We need to find the button attributes.
        # Also need categories if available.
        
        # Strategy: find all buttons with data-id in this section content
        button_matches = re.finditer(r'<button[^>]*class="add-to-cart-btn"[^>]*data-id="([^"]+)"[^>]*data-name="([^"]+)"[^>]*data-price="([^"]+)"', section_content)
        
        for bm in button_matches:
            pid = bm.group(1)
            name = bm.group(2)
            price = int(bm.group(3))
            
            # Find the card container for this button to get categories
            # This is hard with regex backward search.
            # However, in the HTML, data-food-category is on the parent div .product-card
            
            # We can try to find the div that contains this button.
            # OR we can regex for the div and then find the button inside it.
            
            # Let's try regex for the whole card.
            # <div class="product-card ... id="..." ...> ... <button ...>
            pass

    # Better approach: Find all product cards, extract attributes, and guess section from ID.
    
    # Regex for product card div start
    # <div class="product-card ... " id="..." ...>
    
    card_pattern = re.compile(r'<div class="product-card([^"]*)" id="([^"]+)"(.*?)</div>\s*</div>', re.DOTALL)
    # The closing div might be tricky.
    
    # Let's just iterate over buttons and infer everything else.
    # ID prefixes: 
    # dest -> featured
    # plato -> main
    # i -> interest
    
    all_buttons = re.finditer(r'<button[^>]*class="add-to-cart-btn"[^>]*data-id="([^"]+)"[^>]*data-name="([^"]+)"[^>]*data-price="([^"]+)"', content)
    
    for bm in all_buttons:
        pid = bm.group(1)
        name = bm.group(2)
        price = int(bm.group(3))
        
        section = 'main' # default
        interest_tag = None
        food_categories = []
        
        if 'dest' in pid:
            section = 'featured'
        elif 'plato' in pid:
            section = 'main'
            # We can't easily get food categories from button. 
            # But we can look at what the user likely wants.
            # Or we can look at the file content again to find data-food-category for this ID.
            
            # Find the div with this id (or similar id on div?)
            # The div id often matches the button id or is related.
            # in local2: div id="l2-plato1", button data-id="l2-plato1".
            
            div_match = re.search(r'<div[^>]*id="' + re.escape(pid) + r'"[^>]*data-food-category="([^"]+)"', content)
            if div_match:
                food_categories = [x.strip() for x in div_match.group(1).split(',')]
            
        elif 'i' in pid and not 'dest' in pid and not 'plato' in pid:
            section = 'interest'
            # Find interest category
            div_match = re.search(r'<div[^>]*id="[^"]*"[^>]*data-interest-category="([^"]+)"[^>]*>.*?data-id="' + re.escape(pid) + r'"', content, re.DOTALL)
            # Note: div id might not be pid. In local2: div id="l2-interest-product-2x1", button data-id="l2-i1".
            
            # So we search for a div that contains this button and has data-interest-category
            # We can scan backwards from the button match index in original content?
            # Or just regex for div with interest category that contains this button ID.
            
            # Since we iterate buttons, we can search the whole content for the div wrapping this button.
            # But regex is greedy.
            
            # Let's try to match the div with interest category that contains the button.
            # <div ... data-interest-category="cat" ...> ... data-id="pid" ... </div>
            
            # This is expensive/complex.
            # Simpler: ID mapping for interest tags based on ID suffix/number?
            # i1 -> 2x1
            # i2 -> promocion
            # i3 -> especialidad
            # i1-2 -> 2x1
            # i2-2 -> promocion
            # i3-2 -> especialidad
            
            if 'i1' in pid:
                interest_tag = '2x1'
            elif 'i2' in pid:
                interest_tag = 'promocion'
            elif 'i3' in pid:
                interest_tag = 'especialidad'
                
        products.append({
            'id': pid,
            'name': name,
            'price': price,
            'section': section,
            'food_categories': food_categories,
            'interest_tag': interest_tag
        })
        
    return products

def update_db(tenant, products):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    print(f"Updating tenant: {tenant}")
    
    for p in products:
        variants = {
            "section": p['section'],
            "food_categories": p['food_categories']
        }
        if p['interest_tag']:
            variants["interest_tag"] = p['interest_tag']

        v_json = json.dumps(variants)
        
        # Check existence
        cur.execute("SELECT id FROM products WHERE product_id=? AND tenant_slug=?", (p['id'], tenant))
        row = cur.fetchone()

        if row:
            cur.execute("""
                UPDATE products 
                SET variants_json=?, active=1, name=?, price=?
                WHERE product_id=? AND tenant_slug=?
            """, (v_json, p['name'], p['price'], p['id'], tenant))
            # print(f"Updated {p['id']}")
        else:
            cur.execute("""
                INSERT INTO products (tenant_slug, product_id, name, price, stock, active, variants_json)
                VALUES (?, ?, ?, ?, 100, 1, ?)
            """, (tenant, p['id'], p['name'], p['price'], v_json))
            print(f"Inserted {p['id']}")

    conn.commit()
    conn.close()

def run():
    base_dir = os.getcwd()
    for fname in FILES:
        fpath = os.path.join(base_dir, fname)
        if not os.path.exists(fpath):
            print(f"Skipping {fname} (not found)")
            continue
            
        with open(fpath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        tenant = get_tenant_slug(content)
        if not tenant:
            print(f"Skipping {fname} (no tenant slug)")
            continue
            
        products = parse_products(content)
        update_db(tenant, products)
        print(f"Processed {fname}: {len(products)} products updated/inserted.")

if __name__ == '__main__':
    run()
