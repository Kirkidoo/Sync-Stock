import os
import requests
import json

# --- CONFIGURATION ---
# We try to get the token from the environment, but you can hardcode it here for a quick local test
# REPLACE "ENTER_YOUR_TOKEN_HERE" with your actual Thibault API Token if running locally.
SUPPLIER_API_TOKEN = os.environ.get("SUPPLIER_API_TOKEN", "36|eDGwYdErGUwst82KoCZCha3paH53TJSFt0Ocp8a9")
SUPPLIER_API_URL = "https://api.importationsthibault.com/api/v1/stock"

# --- TEST 1: Standard Bearer Format ---
print(f"--- TEST 1: Testing Token: {SUPPLIER_API_TOKEN[:5]}... ---")

headers = {
    "Authorization": f"Bearer {SUPPLIER_API_TOKEN}",
    "Accept": "application/json"
}

# We request a random SKU just to trigger the auth check
params = {"sku": "6268059", "language": "en"} 

try:
    response = requests.get(SUPPLIER_API_URL, headers=headers, params=params)
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
    
    if response.status_code == 200 or response.status_code == 400:
        print("✅ SUCCESS: Token is valid.")
    elif response.status_code == 401:
        print("❌ FAILED: Unauthenticated. The token is wrong or expired.")
    else:
        print("⚠️  Result unclear (Check message above).")

except Exception as e:
    print(f"CRASH: {e}")
