import json
import urllib.request
import urllib.error


def main():
    payload = {
        "username": "admin",
        "password": "admin123",
        "tenant_slug": "gastronomia-local1",
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "http://127.0.0.1:8000/api/auth/login",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            print("STATUS", resp.status)
            print("HEADERS", dict(resp.getheaders()))
            print("BODY", body)
    except Exception as e:
        print("ERROR", repr(e))


if __name__ == "__main__":
    main()

