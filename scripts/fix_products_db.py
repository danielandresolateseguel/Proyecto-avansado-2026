import sqlite3
import json

DB_PATH = 'orders.db'

def run():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Helper to update product
    def update_prod(pid, name, price, section, categories, interest_tag=None, active=1):
        variants = {
            "section": section,
            "food_categories": categories
        }
        if interest_tag:
            variants["interest_tag"] = interest_tag
            
        v_json = json.dumps(variants)
        
        # Check if exists
        cur.execute("SELECT id FROM products WHERE product_id=? AND tenant_slug='gastronomia-local1'", (pid,))
        row = cur.fetchone()
        
        if row:
            print(f"Updating {pid}...")
            cur.execute("""
                UPDATE products 
                SET variants_json=?, active=?, name=?, price=?
                WHERE product_id=? AND tenant_slug='gastronomia-local1'
            """, (v_json, active, name, price, pid))
        else:
            print(f"Inserting {pid}...")
            cur.execute("""
                INSERT INTO products (tenant_slug, product_id, name, price, stock, active, variants_json)
                VALUES ('gastronomia-local1', ?, ?, ?, 100, ?, ?)
            """, (pid, name, price, active, v_json))

    # Main Menu
    update_prod('plato1', 'Pizza Margarita', 6500, 'main', ['pizzas', 'comen-dos'])
    update_prod('plato2', 'Empanadas (docena)', 9204, 'main', ['sandwich', 'comen-dos']) # Keeping 'sandwich' to match HTML data, though odd
    update_prod('plato3', 'Ensalada César', 5200, 'main', ['ensaladas'])
    update_prod('plato4', 'Limonada Casera', 2500, 'main', ['bebidas'])

    # Featured
    update_prod('dest1', 'Pizza Napolitana', 6800, 'featured', ['pizzas'])
    update_prod('dest2', 'Milanesa con puré', 5400, 'featured', ['al-plato'])
    update_prod('dest3', 'Ensalada César', 4930, 'featured', ['ensaladas']) # Note: Price diff from plato3
    update_prod('dest4', 'Ravioles de ricota', 6396, 'featured', ['al-plato'])

    # Interest
    update_prod('i1', 'Pizza Margarita 2x1', 4500, 'interest', ['pizzas', 'combos'], '2x1')
    update_prod('i1-2', 'Hamburguesa Doble 2x1', 9800, 'interest', ['sandwich', 'combos'], '2x1')
    update_prod('i2', 'Empanadas de Carne (Promo)', 6200, 'interest', ['empanadas', 'promociones'], 'promocion')
    update_prod('i2-2', 'Mini Pizzas (Entrada)', 3500, 'interest', ['pizzas', 'promociones'], 'promocion')
    update_prod('i3', 'Lomo a lo Pobre (Especialidad)', 7800, 'interest', ['al-plato', 'especialidad'], 'especialidad')
    update_prod('i3-2', 'Sorrentinos Especiales', 7200, 'interest', ['al-plato', 'especialidad'], 'especialidad')

    conn.commit()
    conn.close()
    print("Database updated successfully.")

if __name__ == '__main__':
    run()
