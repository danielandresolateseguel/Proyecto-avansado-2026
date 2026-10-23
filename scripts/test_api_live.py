import urllib.request
import json

def test_api():
    try:
        url = "http://localhost:5000/api/config?slug=gastronomia-local1"
        print(f"Fetching {url}...")
        with urllib.request.urlopen(url) as response:
            data = response.read()
            text = data.decode('utf-8')
            print(f"Response length: {len(text)}")
            
            try:
                json_data = json.loads(text)
                checkout = json_data.get('checkout', {})
                template = checkout.get('whatsappTemplate', '')
                
                print("\n--- API TEMPLATE START ---")
                print(template)
                print("--- API TEMPLATE END ---")
                
                print("\n--- REPR ---")
                print(repr(template))
                
                if '\ufffd' in template:
                    print("\n[FAIL] Found Unicode Replacement Character (Diamond)!")
                else:
                    print("\n[PASS] No corrupt characters found.")
                    
            except json.JSONDecodeError:
                print("Failed to decode JSON")
                print(text[:200])
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_api()
