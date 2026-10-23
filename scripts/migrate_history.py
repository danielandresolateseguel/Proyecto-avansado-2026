import sqlite3
import os

DB_PATH = os.path.join(os.getcwd(), 'orders.db')

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Check if changed_by column exists in order_status_history
    cur.execute("PRAGMA table_info(order_status_history)")
    columns = [row[1] for row in cur.fetchall()]
    
    if 'changed_by' not in columns:
        print("Adding changed_by column to order_status_history...")
        try:
            cur.execute("ALTER TABLE order_status_history ADD COLUMN changed_by TEXT")
            conn.commit()
            print("Column added successfully.")
        except Exception as e:
            print(f"Error adding column: {e}")
    else:
        print("Column changed_by already exists.")

    conn.close()

if __name__ == '__main__':
    migrate()
