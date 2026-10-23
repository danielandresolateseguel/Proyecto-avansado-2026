import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

with open('db_check.txt', 'w') as f:
    try:
        url = os.environ.get('DATABASE_URL')
        f.write(f"URL: {url}\n")
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        
        # Check tables
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = cur.fetchall()
        f.write(f"Tables: {tables}\n")
        
        # Check orders count
        if tables:
            cur.execute("SELECT count(*) FROM orders")
            count = cur.fetchone()[0]
            f.write(f"Orders count: {count}\n")
            
        conn.close()
        f.write("Success\n")
    except Exception as e:
        f.write(f"Error: {e}\n")
