
import requests
import json

base_url = "http://localhost:5000"
slug = "gastronomia-local1"

print(f"Getting config for {slug}...")
try:
    r = requests.get(f"{base_url}/api/tenant_header?tenant_slug={slug}")
    if r.status_code == 200:
        data = r.json()
        print("Keys in response:", list(data.keys()))
        print(f"Header BG: {data.get('header_bg_color')}")
        print(f"Featured BG: {data.get('featured_bg_color')}")
    else:
        print(f"Error: {r.status_code}")
except Exception as e:
    print(f"Exception: {e}")
