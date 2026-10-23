
import sys
import traceback

with open('import_log.txt', 'w') as f:
    f.write("Starting import check...\n")
    try:
        from app import create_app, database
        f.write("Import successful\n")
    except Exception as e:
        f.write(f"Import failed: {e}\n")
        f.write(traceback.format_exc())
