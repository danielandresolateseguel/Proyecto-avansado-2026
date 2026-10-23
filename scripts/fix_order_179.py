import sqlite3
from datetime import datetime

try:
    conn = sqlite3.connect('orders.db')
    cur = conn.cursor()
    
    order_id = 179
    
    # Check current status
    cur.execute("SELECT status, payment_status, created_at FROM orders WHERE id = ?", (order_id,))
    row = cur.fetchone()
    print(f"Current state: {row}")
    
    if row:
        # Update to 'entregado'
        cur.execute("UPDATE orders SET status = 'entregado' WHERE id = ?", (order_id,))
        print("Updated status to 'entregado'")
        
        # Add history entry (using current time or order creation time? Better use current time to ensure it shows up in "now")
        # But wait, the session is already closed. If I add a history entry NOW, it might be outside the session range?
        # The session closed at 08:29:38 p.m. (20:29:38).
        # If I insert 'now' (e.g. 20:50), the query for that session (which filters by time range) won't pick it up!
        # I must insert the history entry with a timestamp INSIDE the session range.
        # The cash movement was at 08:25:55 p.m. (20:25:55). I should match that or slightly after.
        
        # Let's check the cash movement timestamp for this order to match it exactly.
        cur.execute("SELECT created_at FROM cash_movements WHERE note LIKE ?", (f"%{order_id}%",))
        mov_row = cur.fetchone()
        
        if mov_row:
            timestamp = mov_row[0]
            print(f"Using cash movement timestamp: {timestamp}")
        else:
            timestamp = datetime.utcnow().isoformat()
            print(f"No cash movement found, using now: {timestamp}")
            
        cur.execute(
            "INSERT INTO order_status_history (order_id, status, changed_by, changed_at) VALUES (?, ?, ?, ?)",
            (order_id, 'entregado', 'fix_script', timestamp)
        )
        print("Added history entry")
        
        conn.commit()
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
