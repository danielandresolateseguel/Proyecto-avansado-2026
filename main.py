from app import create_app
import os
from waitress import serve

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    threads = int(os.environ.get('WAITRESS_THREADS', '6') or '6')
    print(f"Starting production server on port {port} using Waitress...", flush=True)
    serve(app, host='0.0.0.0', port=port, threads=max(1, threads))
