#!/usr/bin/env python3
"""
Simple helper to POST a file with Authorization header and print the full response.
Usage (PowerShell):
  .\venv\Scripts\Activate.ps1
  python .\test_upload.py https://bechapra.com.mx/extractor-api/extract/banorte C:\path\to\ejemplo.pdf "TU_TOKEN"

Prints status code, response headers, and JSON or raw text body.
"""
import sys, os
try:
    import requests
except Exception:
    print("requests missing. Install in venv: pip install requests")
    sys.exit(2)

if len(sys.argv) < 4:
    print("Usage: python test_upload.py <url> <file-path> <token>")
    sys.exit(1)

url = sys.argv[1]
filepath = sys.argv[2]
token = sys.argv[3]

if not os.path.exists(filepath):
    print("file not found:", filepath)
    sys.exit(2)

headers = {"Authorization": f"Bearer {token}"}
files = {"file": open(filepath, "rb")}

try:
    r = requests.post(url, headers=headers, files=files, timeout=60)
except Exception as e:
    print("Request error:", e)
    sys.exit(3)

print("Status:", r.status_code)
print("Response headers:")
for k, v in r.headers.items():
    print(f"{k}: {v}")

ctype = r.headers.get("content-type", "")
if "application/json" in ctype:
    try:
        print("JSON body:")
        print(r.json())
    except Exception:
        print("Failed to decode JSON. Raw body:")
        print(r.text)
else:
    print("Body:\n", r.text)
