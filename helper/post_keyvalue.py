import sys
import json
import urllib.request
import time 

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <num_requests>")
        return

    n = int(sys.argv[1])
    url = "http://localhost:7770/v1/data"

    for i in range(1, n + 1):
        payload = {
            "key": str(i),
            "value": str(i)
        }

        data = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read().decode("utf-8")
                print(f"[{i}] Status: {resp.status}, Response: {body}")
        except Exception as e:
            print(f"[{i}] Error: {e}")
        time.sleep(0.1)

if __name__ == "__main__":
    # python3 post_keyvalue.py 10
    main()