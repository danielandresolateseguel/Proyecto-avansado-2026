
with open('extras_check.txt', 'w') as f:
    f.write("Start\n")
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        f.write("extras ok\n")
    except Exception as e:
        f.write(f"Error: {e}\n")
