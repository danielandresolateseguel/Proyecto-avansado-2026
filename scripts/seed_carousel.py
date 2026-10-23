import sqlite3
import datetime

conn = sqlite3.connect('orders.db')
cursor = conn.cursor()

# Check if already exists to avoid duplicates
cursor.execute("SELECT id FROM carousel_slides WHERE title='Slide de Prueba'")
if cursor.fetchone():
    print("Slide de prueba ya existe.")
else:
    cursor.execute("""
        INSERT INTO carousel_slides (tenant_slug, image_url, title, text, position, active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, ('gastronomia-local1', 'https://images.unsplash.com/photo-1546069901-ba9599a7e63c', 'Slide de Prueba', 'Este slide fue insertado via script para probar', 1, 1, datetime.datetime.utcnow().isoformat()))
    conn.commit()
    print("Slide insertado correctamente.")

conn.close()
