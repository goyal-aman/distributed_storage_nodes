import sys
import urllib.request
import time

def main():
    if len(sys.argv) < 3:
        print("Usage: python script.py <start> <end>")
        return

    start = int(sys.argv[1])
    end = int(sys.argv[2])

    base_url = "http://localhost:7770/v1/data"

    i = start
    while True:
        url = f"{base_url}?key={i}"

        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = resp.read().decode("utf-8")
                print(f"[{i}] {resp.status}, {body}")
        except Exception as e:
            print(f"[{i}] Error: {e}")

        # Move to next key, wrap around
        i += 1
        if i > end:
            i = start

        # Optional: small delay (avoid hammering too hard)

        time.sleep(0.05)

if __name__ == "__main__":
    # python3 get_keyvalue.py 0 100
    main()