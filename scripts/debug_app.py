print("Start import")
try:
    from app import create_app
    print("Imported create_app")
    app = create_app()
    print("Created app")
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"Error: {e}")
