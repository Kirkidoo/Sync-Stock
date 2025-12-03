import os
import requests
import json
import time

# --- CONFIGURATION ---
SHOP_URL = os.environ.get("SHOP_URL")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
MOTOVAN_KEY = os.environ.get("MOTOVAN_API_KEY")
MOTOVAN_CUST_NUM = os.environ.get("MOTOVAN_CUSTOMER_NUMBER")
TARGET_LOCATION_ID = "gid://shopify/Location/111098265917" # Motovan Location

if not SHOP_URL or not ACCESS_TOKEN or not MOTOVAN_KEY or not MOTOVAN_CUST_NUM:
    raise ValueError("Missing Motovan or Shopify secrets.")

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/2024-01/graphql.json"

def run_query(query, variables=None):
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables})
    if response.status_code != 200:
        raise Exception(f"GraphQL failed: {response.status_code} - {response.text}")
    data = response.json()
    if 'errors' in data:
        if 'THROTTLED' in str(data['errors']):
            time.sleep(2)
            return run_query(query, variables)
        print(f"GraphQL Errors: {data['errors']}")
    return data

def get_products_at_location():
    print(f"Fetching products assigned to Motovan (Location: {TARGET_LOCATION_ID})...")
    product_map = {}
    has_next_page = True
    cursor = None

    query = """
    query ($locationId: ID!, $cursor: String) {
      location(id: $locationId) {
        inventoryLevels(first: 250, after: $cursor) {
          pageInfo { hasNextPage, endCursor }
          edges {
            node {
              item { 
                id
                tracked
                variant { sku }
              }
            }
          }
        }
      }
    }
    """

    while has_next_page:
        variables = {"locationId": TARGET_LOCATION_ID, "cursor": cursor}
        data = run_query(query, variables)
        
        if not data.get('data') or not data['data'].get('location'):
            print("Location not found.")
            break

        inventory_levels = data['data']['location']['inventoryLevels']['edges']
        
        for level in inventory_levels:
            item = level['node']['item']
            variant = item.get('variant')
            if item.get('tracked') and variant and variant.get('sku'):
                sku = str(variant['sku']).strip()
                product_map[sku] = item['id']
        
        page_info = data['data']['location']['inventoryLevels']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        
    print(f"✅ Found {len(product_map)} variants at Motovan.")
    return product_map

def get_motovan_inventory(sku_list):
    if not sku_list: return {}
    print(f"Fetching Motovan data for {len(sku_list)} SKUs...")
    inventory_map = {}
    
    base_url = "https://api.motovan.com/inventory"
    headers = {"X-Api-Key": MOTOVAN_KEY}
    session = requests.Session()
    session.headers.update(headers)

    for index, sku in enumerate(sku_list):
        try:
            params = {
                "customerNumber": MOTOVAN_CUST_NUM,
                "partNumber": sku
            }
            
            response = session.get(base_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                warehouses = data.get('inventoryLvl', [])
                total_qty = sum(int(w.get('quantity', 0)) for w in warehouses)
                inventory_map[sku] = total_qty
                
            elif response.status_code == 400:
                inventory_map[sku] = 0
                
        except Exception as e:
            print(f"Motovan Error on {sku}: {e}")
            
        if index % 20 == 0:
            time.sleep(0.1)

    return inventory_map

def bulk_update_inventory(updates):
    if not updates: 
        print("No updates to send.")
        return

    print(f"Sending {len(updates)} inventory updates to Shopify...")
    mutation = """
    mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) {
        userErrors { field, message }
        inventoryAdjustmentGroup { reason, changes { name, delta } }
      }
    }
    """
    
    BATCH_SIZE = 100
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        variables = {
            "input": {
                "reason": "correction",
                "name": "available",
                "ignoreCompareQuantity": True,
                "quantities": batch
            }
        }
        try:
            data = run_query(mutation, variables)
            if data.get('data', {}).get('inventorySetQuantities', {}).get('userErrors'):
                 print("Errors:", data['data']['inventorySetQuantities']['userErrors'])
            else:
                 print(f"Batch {i//BATCH_SIZE + 1} Success.")
        except Exception as e:
            print(f"Update Batch Failed: {e}")
        time.sleep(1)

def main():
    print("--- STARTING MOTOVAN SYNC ---")
    shopify_map = get_products_at_location()
    
    if shopify_map:
        supplier_stock = get_motovan_inventory(list(shopify_map.keys()))
        updates = []
        for sku, qty in supplier_stock.items():
            if sku in shopify_map:
                updates.append({
                    "inventoryItemId": shopify_map[sku],
                    "locationId": TARGET_LOCATION_ID,
                    "quantity": int(qty)
                })
        bulk_update_inventory(updates)
    else:
        print("No products found at Motovan Location.")

if __name__ == "__main__":
    main()
