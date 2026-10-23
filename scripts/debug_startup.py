
from app import create_app
print("Creating app...")
try:
    app = create_app()
    print("App created successfully.")
except Exception as e:
    print(f"Error creating app: {e}")
    import traceback
    traceback.print_exc()
