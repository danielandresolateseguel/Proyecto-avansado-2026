
import sys
import traceback

with open('check_psycopg2.log', 'w') as f:
    f.write("Starting check...\n")
    try:
        import psycopg2
        f.write("psycopg2 imported\n")
        import psycopg2.extras
        f.write("psycopg2.extras imported\n")
    except ImportError:
        f.write("ImportError caught\n")
    except Exception as e:
        f.write(f"Error: {e}\n")
        f.write(traceback.format_exc())
    f.write("Done.\n")
