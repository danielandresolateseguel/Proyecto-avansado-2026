import sqlite3

def check_movements():
    try:
        conn = sqlite3.connect('orders.db')
        cur = conn.cursor()
        
        print("--- Recent Cash Movements (with Session ID) ---")
        cur.execute("SELECT id, session_id, type, amount, note, created_at FROM cash_movements ORDER BY id DESC LIMIT 10")
        rows = cur.fetchall()
        for r in rows:
            print(r)

        print("\n--- Testing Session 60 ---")
        sid = 60
        cur.execute("SELECT opened_at, tenant_slug, opening_amount FROM cash_sessions WHERE id = ?", (sid,))
        s_data = cur.fetchone()
        if s_data:
            opened_at = s_data[0]
            tenant_slug = s_data[1]
            opening = s_data[2]
            print(f"Session {sid} Opened At: {opened_at}, Tenant: {tenant_slug}, Opening: {opening}")
            
            # Entradas (Unfiltered - New Logic)
            query = "SELECT COALESCE(SUM(CASE WHEN type='entrada' THEN amount ELSE 0 END),0) FROM cash_movements WHERE session_id = ?"
            cur.execute(query, (sid,))
            entradas = cur.fetchone()[0]
            print(f"Calculated Entradas (Unfiltered): {entradas}")
            
            # Delivered Total
            query_del = (
                "SELECT COALESCE(SUM(o.total),0) FROM orders o "
                "JOIN (SELECT order_id, MAX(changed_at) AS last_change FROM order_status_history WHERE status = 'entregado' GROUP BY order_id) h ON h.order_id = o.id "
                "WHERE o.tenant_slug = ? AND o.status = 'entregado' AND h.last_change >= ?"
            )
            cur.execute("SELECT closed_at FROM cash_sessions WHERE id = ?", (sid,))
            closed_at = cur.fetchone()[0]
            
            query_del += " AND h.last_change <= ?"
            cur.execute(query_del, (tenant_slug, opened_at, closed_at))
            del_total = cur.fetchone()[0]
            print(f"Calculated Delivered Total: {del_total}")
            
            # New Theoretical Formula: Opening + Entradas - Salidas (Salidas assumed 0 here)
            theoretical_new = opening + entradas
            print(f"New Theoretical (Opening + Entradas): {theoretical_new}")
            
            print(f"Old Theoretical (Double Count): {opening + del_total + entradas}")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_movements()
