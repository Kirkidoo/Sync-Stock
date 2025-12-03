import os
import requests
import json
import time

# --- CONFIGURATION ---
SHOP_URL = os.environ.get("SHOP_URL")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
SUPPLIER_URL = os.environ.get("SUPPLIER_API_URL")
SUPPLIER_TOKEN = os.environ.get("SUPPLIER_API_TOKEN")
TARGET_LOCATION_ID = "gid://shopify/Location/105008496957" # Thibault Location

if not SHOP_URL or not ACCESS_TOKEN or not SUPPLIER_URL or not SUPPLIER_TOKEN:
    raise ValueError("Missing Thibault or Shopify secrets.")

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
    print(f"Fetching products assigned to Thibault (Location: {TARGET_LOCATION_ID})...")
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
        
    print(f"✅ Found {len(product_map)} variants at Thibault.")
    return product_map

def get_thibault_inventory(sku_list):
    if not sku_list: return {}
    print(f"Fetching Thibault data for {len(sku_list)} SKUs...")
    
    inventory_map = {}
    CHUNK_SIZE = 50
    chunks = [sku_list[i:i + CHUNK_SIZE] for i in range(0, len(sku_list), CHUNK_SIZE)]
    headers = {"Authorization": f"Bearer {SUPPLIER_TOKEN}", "Accept": "application/json"}

    for i, batch in enumerate(chunks):
        try:
            response = requests.get(
                SUPPLIER_URL, 
                headers=headers, 
                params={"sku": ",".join(batch), "language": "en"}, 
                timeout=30
            )
            
            if response.status_code in [200, 400]:
                try:
                    data = response.json()
                    items = data.get('items', [])
                    if isinstance(items, dict): items = [items]
                    
                    for item in items:
                        if isinstance(item, dict):
                            sku = str(item.get('sku')).strip()
                            qty = item.get('quantity', {}).get('value')
                            if sku and qty is not None:
                                inventory_map[sku] = int(qty)
                except: pass
        except Exception as e:
            print(f"Thibault Batch Error: {e}")
        time.sleep(0.5)
        
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
    print("--- STARTING THIBAULT SYNC ---")
    shopify_map = get_products_at_location()
    
    if shopify_map:
        supplier_stock = get_thibault_inventory(list(shopify_map.keys()))
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
        print("No products found at Thibault Location.")

if __name__ == "__main__":
    main()
