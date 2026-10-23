
with open('sqlite_check.txt', 'w') as f:
    f.write("Start\n")
    try:
        import sqlite3
        f.write("sqlite3 ok\n")
    except Exception as e:
        f.write(f"Error: {e}\n")
