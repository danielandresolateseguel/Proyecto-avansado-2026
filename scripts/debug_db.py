
import sqlite3
import os

DB_PATH = os.path.join(os.getcwd(), 'orders.db')

def check_latest_order():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get latest order
    cur.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 1")
    order = cur.fetchone()
    
    if not order:
        print("No orders found.")
        return

    print(f"Order ID: {order['id']}")
    print(f"Order Notes: {order['order_notes']}")
    
    # Update items for order 269
    cur.execute("UPDATE order_items SET notes='TEST NOTE MANUAL' WHERE order_id=269")
    conn.commit()
    print("Updated order 269 items with test notes.")

    # Get items
    cur.execute("SELECT * FROM order_items WHERE order_id = ?", (order['id'],))
    items = cur.fetchall()
    
    print("\nItems:")
    for item in items:
        print(f"Item: {item['name']}")
        print(f"  Notes: '{item['notes']}' (Type: {type(item['notes'])})")

    conn.close()

if __name__ == "__main__":
    check_latest_order()
