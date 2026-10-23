
from app import create_app
from app.database import init_db

app = create_app()
with app.app_context():
    print("Running init_db()...")
    try:
        init_db()
        print("init_db() completed.")
    except Exception as e:
        print(f"init_db() failed: {e}")
