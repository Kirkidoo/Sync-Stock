import os
import requests
import json
import time

# --- CONFIGURATION ---
SHOP_URL = os.environ.get("SHOP_URL")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
SUPPLIER_API_URL = os.environ.get("SUPPLIER_API_URL")
SUPPLIER_API_TOKEN = os.environ.get("SUPPLIER_API_TOKEN")

# --- DEBUG TRACER ---
# Enter the SKU you are having trouble with here. 
# The script will print DETAILED logs just for this item.
TRACE_SKU = "6268059" 

if not SHOP_URL or not ACCESS_TOKEN or not SUPPLIER_API_URL or not SUPPLIER_API_TOKEN:
    raise ValueError("Missing secrets in GitHub.")

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

TARGET_LOCATION_ID = "gid://shopify/Location/105008496957"
GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/2024-01/graphql.json"

def run_query(query, variables=None):
    payload = {"query": query, "variables": variables}
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload)
    if response.status_code != 200:
        raise Exception(f"Query failed: {response.status_code} - {response.text}")
    data = response.json()
    if 'errors' in data:
        raise Exception(f"GraphQL Errors: {data['errors']}")
    return data

def get_shopify_product_map():
    print(f"Fetching products ONLY from Location {TARGET_LOCATION_ID}...")
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
            raise Exception(f"Location {TARGET_LOCATION_ID} not found.")

        inventory_levels = data['data']['location']['inventoryLevels']['edges']
        
        for level in inventory_levels:
            item = level['node']['item']
            if item.get('variant'):
                sku = item['variant']['sku']
                item_id = item['id']
                if sku:
                    # Strip whitespace to ensure match
                    clean_sku = str(sku).strip()
                    product_map[clean_sku] = item_id
                    
                    # TRACER
                    if clean_sku == TRACE_SKU:
                        print(f"🕵️ TRACER: Found {TRACE_SKU} in Shopify. Item ID: {item_id}")
                        if not item.get('tracked'):
                            print(f"🕵️ TRACER WARNING: {TRACE_SKU} is NOT tracking quantity in Shopify!")

        page_info = data['data']['location']['inventoryLevels']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        
    print(f"Mapped {len(product_map)} variants.")
    return product_map

def get_supplier_inventory(sku_list):
    if not sku_list: return {}
    print(f"Fetching supplier data for {len(sku_list)} SKUs...")
    inventory_map = {}
    
    CHUNK_SIZE = 50
    chunks = [sku_list[i:i + CHUNK_SIZE] for i in range(0, len(sku_list), CHUNK_SIZE)]
    headers = {"Authorization": f"Bearer {SUPPLIER_API_TOKEN}", "Accept": "application/json"}

    for i, batch in enumerate(chunks):
        sku_query = ",".join(batch)
        params = {"sku": sku_query, "language": "en"}
        
        try:
            response = requests.get(SUPPLIER_API_URL, headers=headers, params=params, timeout=30)
            
            if response.status_code in [200, 400]:
                try:
                    data = response.json()
                except ValueError:
                    continue

                if isinstance(data, dict):
                    items = data.get('items', [])
                    
                    # FIX: Handle case where 'items' is a single dict (not a list)
                    if isinstance(items, dict):
                        items = [items]
                    
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict):
                                item_sku = str(item.get('sku')).strip()
                                qty_data = item.get('quantity', {})
                                if isinstance(qty_data, dict):
                                    qty = qty_data.get('value')
                                    if item_sku and qty is not None:
                                        inventory_map[item_sku] = int(qty)
                                        
                                        # TRACER
                                        if item_sku == TRACE_SKU:
                                            print(f"🕵️ TRACER: Supplier returned {TRACE_SKU} with Quantity: {qty}")
            else:
                print(f"Batch {i+1} status: {response.status_code}")

        except Exception as e:
            print(f"Exception in batch {i+1}: {e}")
        
        time.sleep(0.5)
        
    # Final check for Tracer
    if TRACE_SKU not in inventory_map:
        print(f"🕵️ TRACER WARNING: {TRACE_SKU} was NOT found in the Supplier response data.")
    
    return inventory_map

def bulk_update_inventory(location_id, updates):
    if not updates: return

    # Check tracer in updates list
    found_in_updates = False
    for u in updates:
        # We can't see the SKU here easily, but we can verify count
        pass 

    mutation = """
    mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) {
        userErrors { field, message }
        inventoryAdjustmentGroup {
          reason
          changes { name, delta }
        }
      }
    }
    """
    
    BATCH_SIZE = 100
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        
        # TRACER: Check if our item is in this batch
        for item in batch:
            # We don't have the SKU key here, but if the count matches expected tracer count...
            pass

        variables = {
            "input": {
                "reason": "correction",
                "name": "available",
                "ignoreCompareQuantity": True,
                "quantities": batch
            }
        }
        
        print(f"Sending Shopify update batch {i//BATCH_SIZE + 1}...")
        try:
            data = run_query(mutation, variables)
            if data.get('data') and data['data'].get('inventorySetQuantities'):
                 user_errors = data['data']['inventorySetQuantities']['userErrors']
                 if user_errors:
                     print("Errors in batch:", user_errors)
                 else:
                     print("Batch success.")
        except Exception as e:
            print(f"Failed to send batch: {e}")
        
        time.sleep(1)

def main():
    location_id = TARGET_LOCATION_ID
    print(f"Syncing to Location ID: {location_id}")
    
    shopify_map = get_shopify_product_map()
    all_skus = list(shopify_map.keys())
    
    if not all_skus:
        print("No products found.")
        return

    supplier_data = get_supplier_inventory(all_skus)
    
    updates = []
    tracer_quantity_to_send = None

    for sku, qty in supplier_data.items():
        if sku in shopify_map:
            inventory_item_id = shopify_map[sku]
            
            # TRACER
            if sku == TRACE_SKU:
                tracer_quantity_to_send = qty
                print(f"🕵️ TRACER: Preparing update for {TRACE_SKU}. Item ID: {inventory_item_id} -> New Qty: {qty}")

            updates.append({
                "inventoryItemId": inventory_item_id,
                "locationId": location_id,
                "quantity": int(qty)
            })
    
    if tracer_quantity_to_send is None:
        print(f"🕵️ TRACER ALERT: {TRACE_SKU} was NOT added to the update list. Check if SKUs match exactly between Shopify and Supplier.")

    print(f"Prepared {len(updates)} updates.")
    bulk_update_inventory(location_id, updates)

if __name__ == "__main__":
    main()
