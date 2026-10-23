import urllib.request
import urllib.parse
import json

BASE_URL = "http://localhost:8000/api"
TENANT = "gastronomia-local1"

def request(method, url, data=None):
    req = urllib.request.Request(url, method=method)
    if data:
        json_data = json.dumps(data).encode('utf-8')
        req.add_header('Content-Type', 'application/json')
        req.data = json_data
    
    try:
        with urllib.request.urlopen(req) as response:
            status = response.getcode()
            if status >= 200 and status < 300:
                content = response.read().decode('utf-8')
                if content:
                    return json.loads(content)
                return {}
    except urllib.error.HTTPError as e:
        print(f"HTTPError: {e.code} {e.reason}")
        print(e.read().decode('utf-8'))
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def test_add_slide():
    print("Testing Add Slide...")
    payload = {
        "tenant_slug": TENANT,
        "image_url": "/test_image.jpg",
        "title": "Test Slide Title",
        "text": "Test Slide Text",
        "position": 10
    }
    return request("POST", f"{BASE_URL}/carousel", payload)

def test_list_slides():
    print("Testing List Slides...")
    return request("GET", f"{BASE_URL}/carousel?tenant_slug={TENANT}")

def test_update_slide(slide_id):
    print(f"Testing Update Slide {slide_id}...")
    payload = {"title": "Updated Title"}
    return request("PATCH", f"{BASE_URL}/carousel/{slide_id}", payload)

def test_delete_slide(slide_id):
    print(f"Testing Delete Slide {slide_id}...")
    return request("DELETE", f"{BASE_URL}/carousel/{slide_id}")

if __name__ == "__main__":
    added = test_add_slide()
    if added:
        print("Added:", added)
        slide_id = added.get('id')
        
        slides_data = test_list_slides()
        print("Slides:", slides_data)
        
        if slide_id:
            updated = test_update_slide(slide_id)
            print("Updated:", updated)
            
            slides_after = test_list_slides()
            # print("Slides after update:", slides_after)
            
            deleted = test_delete_slide(slide_id)
            print("Deleted:", deleted)
            
            final_slides = test_list_slides()
            print("Final Slides:", final_slides)
