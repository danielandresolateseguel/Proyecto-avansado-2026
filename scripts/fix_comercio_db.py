import sqlite3
import json

def fix_comercio_db():
    conn = sqlite3.connect('orders.db')
    cursor = conn.cursor()

    slugs = ['comercio', 'comercio1', 'site-base']
    
    # Products list based on comercio.html analysis
    products = [
        # Discounts (section='featured')
        {
            'id': 'd1', 'name': 'Xbox Series X', 'price': 89999, 'image_url': 'Imagenes/Xbox-01-copia-1024x1024.png',
            'details': 'Consola de nueva generación con 4K nativo, 120fps y SSD ultra rápido.',
            'section': 'featured', 'food_categories': 'consolas'
        },
        {
            'id': 'd2', 'name': 'ASUS ProArt P16', 'price': 129999, 'image_url': 'Imagenes/asus-proart-p16.png',
            'details': 'Laptop profesional para creadores con pantalla OLED 4K y RTX 4060.',
            'section': 'featured', 'food_categories': 'laptops'
        },
        {
            'id': 'd3', 'name': 'Monitor Gaming 4K', 'price': 39999, 'image_url': 'Imagenes/producto1.webp.avif',
            'details': 'Monitor 27" 4K HDR con 144Hz, perfecto para gaming y trabajo profesional.',
            'section': 'featured', 'food_categories': 'monitores'
        },
        {
            'id': 'd4', 'name': 'Teclado Mecánico RGB', 'price': 13749, 'image_url': 'Imagenes/asus-proart-p16.png',
            'details': 'Teclado gaming con switches mecánicos, iluminación RGB y teclas programables.',
            'section': 'featured', 'food_categories': 'teclados'
        },
        {
            'id': 'd5', 'name': 'Mouse Gaming Pro', 'price': 13299, 'image_url': 'Imagenes/xbox.png',
            'details': 'Mouse inalámbrico de alta precisión con sensor óptico de 25,000 DPI.',
            'section': 'featured', 'food_categories': 'mouse'
        },
        # Regular Products (section='main')
        {
            'id': '1', 'name': 'Smartphone XYZ', 'price': 59999, 'image_url': 'Imagenes/asus-proart-p16.png',
            'details': 'Smartphone de última generación con pantalla AMOLED de 6.5", cámara de 108MP y batería de 5000mAh.',
            'section': 'main', 'food_categories': 'smartphone'
        },
        {
            'id': '2', 'name': 'Laptop Pro', 'price': 129999, 'image_url': 'Imagenes/xbox.png',
            'details': 'Laptop ultradelgada con procesador de última generación, 16GB de RAM y 512GB SSD.',
            'section': 'main', 'food_categories': 'laptos,laptops'
        },
        {
            'id': '3', 'name': 'Auriculares Inalámbricos', 'price': 14999, 'image_url': 'Imagenes/xbox.png',
            'details': 'Auriculares con cancelación de ruido, 30 horas de batería y sonido de alta fidelidad.',
            'section': 'main', 'food_categories': 'auriculares'
        },
        {
            'id': '4', 'name': 'Smartwatch Fitness', 'price': 19999, 'image_url': 'Imagenes/asus-proart-p16.png',
            'details': 'Reloj inteligente con monitor de ritmo cardíaco, GPS y más de 20 modos deportivos.',
            'section': 'main', 'food_categories': 'accesorios'
        },
        {
            'id': '5', 'name': 'Tablet Ultra', 'price': 34999, 'image_url': 'Imagenes/asus-proart-p16.png',
            'details': 'Tablet con pantalla de 10.5", procesador octa-core y 128GB de almacenamiento.',
            'section': 'main', 'food_categories': 'accesorios'
        },
        {
            'id': '6', 'name': 'Cámara DSLR', 'price': 79999, 'image_url': 'Imagenes/images.jfif',
            'details': 'Cámara profesional con sensor de 24MP, grabación 4K y conectividad WiFi.',
            'section': 'main', 'food_categories': 'accesorios'
        },
        {
            'id': '7', 'name': 'Altavoz Bluetooth', 'price': 8999, 'image_url': 'Imagenes/Xbox-01-copia-1024x1024.png',
            'details': 'Altavoz portátil resistente al agua con 20 horas de batería y sonido 360°.',
            'section': 'main', 'food_categories': 'accesorios'
        },
        {
            'id': '8', 'name': 'Monitor Curvo', 'price': 39999, 'image_url': 'Imagenes/xbox.png',
            'details': 'Monitor gaming de 32" con tasa de refresco de 144Hz y resolución 2K.',
            'section': 'main', 'food_categories': 'accesorios'
        },
        {
            'id': '9', 'name': 'Teclado Mecánico', 'price': 12999, 'image_url': 'Imagenes/asus-proart-p16.png',
            'details': 'Teclado gaming con switches mecánicos, retroiluminación RGB y teclas programables.',
            'section': 'main', 'food_categories': 'teclados'
        },
        {
            'id': '10', 'name': 'Drone 4K', 'price': 69999, 'image_url': 'Imagenes/Xbox-01-copia-1024x1024.png',
            'details': 'Drone con cámara 4K, 30 minutos de vuelo y sistema de estabilización avanzado.',
            'section': 'main', 'food_categories': 'accesorios'
        }
    ]

    for slug in slugs:
        print(f"Processing slug: {slug}")
        for p in products:
            variants = {
                'section': p['section'],
                'food_categories': p['food_categories'],
                'interest_tag': '' # Not used for now, but part of schema
            }
            
            # Check if exists
            exists = cursor.execute(
                "SELECT id FROM products WHERE product_id=? AND tenant_slug=?", 
                (p['id'], slug)
            ).fetchone()
            
            if exists:
                print(f"  Updating {p['id']}")
                cursor.execute("""
                    UPDATE products 
                    SET name=?, price=?, image_url=?, active=1, variants_json=?, details=?
                    WHERE product_id=? AND tenant_slug=?
                """, (p['name'], p['price'], p['image_url'], json.dumps(variants), p['details'], p['id'], slug))
            else:
                print(f"  Inserting {p['id']}")
                cursor.execute("""
                    INSERT INTO products (product_id, tenant_slug, name, price, image_url, active, variants_json, details)
                    VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """, (p['id'], slug, p['name'], p['price'], p['image_url'], json.dumps(variants), p['details']))
    
    conn.commit()
    conn.close()
    print("Comercio DB fix completed.")

if __name__ == "__main__":
    fix_comercio_db()
