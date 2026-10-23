
import os
import sys
import psycopg2
from dotenv import load_dotenv

with open('deps_check.txt', 'w') as f:
    f.write("Start\n")
    try:
        import sqlite3
        f.write("sqlite3 ok\n")
        from psycopg2.extras import execute_values
        f.write("extras ok\n")
        f.write("Success\n")
    except Exception as e:
        f.write(f"Error: {e}\n")
