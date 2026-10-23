import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
try:
    url = os.environ.get('DATABASE_URL')
    print(f"Connecting to {url}")
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = cur.fetchall()
    print("Tables:", tables)
    
    if tables:
        cur.execute("SELECT count(*) FROM orders")
        print("Orders count:", cur.fetchone()[0])
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
