import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

def apply_indexes():
    if not DATABASE_URL:
        print("Error: DATABASE_URL no configurada.")
        return

    print("Conectando a PostgreSQL para aplicar índices de optimización...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Índices existentes (asegurar que existan)
        print("Verificando índices base...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_tenant_status ON orders(tenant_slug, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)")
        
        # NUEVOS Índices compuestos para consultas frecuentes
        print("Creando índices compuestos optimizados...")
        
        # Para filtrado por fecha dentro de un tenant (muy común en reportes y listas)
        # SELECT * FROM orders WHERE tenant_slug = ? AND created_at >= ?
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_tenant_created ON orders(tenant_slug, created_at)")
        
        # Para filtrado por tipo de pedido (ej. estadísticas de 'mesa' vs 'delivery')
        # SELECT * FROM orders WHERE tenant_slug = ? AND order_type = ?
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_tenant_type ON orders(tenant_slug, order_type)")

        # Para búsquedas por teléfono (frecuente en clientes recurrentes)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_tenant_phone ON orders(tenant_slug, customer_phone)")

        conn.commit()
        print("¡Índices aplicados correctamente!")
        print(" - idx_orders_tenant_status: OK")
        print(" - idx_orders_created: OK")
        print(" - idx_orders_tenant_created: Creado/Verificado (Nuevo)")
        print(" - idx_orders_tenant_type: Creado/Verificado (Nuevo)")
        print(" - idx_orders_tenant_phone: Creado/Verificado (Nuevo)")

        conn.close()
    except Exception as e:
        print(f"Error aplicando índices: {e}")

if __name__ == "__main__":
    apply_indexes()
