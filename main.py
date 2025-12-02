import os
import requests
import json
import time

# --- CONFIGURATION ---
SHOP_URL = os.environ.get("SHOP_URL")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

# Supplier Credentials
SUPPLIER_API_URL = os.environ.get("SUPPLIER_API_URL")
SUPPLIER_API_TOKEN = os.environ.get("SUPPLIER_API_TOKEN")

if not SHOP_URL or not ACCESS_TOKEN:
    raise ValueError("Shopify credentials missing in GitHub Secrets.")

if not SUPPLIER_API_URL or not SUPPLIER_API_TOKEN:
    raise ValueError("Supplier credentials missing in GitHub Secrets.")

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# Target Location: Thibault
TARGET_LOCATION_ID = "gid://shopify/Location/105008496957"
GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/2024-01/graphql.json"

def run_query(query, variables=None):
    """Helper function to execute GraphQL queries."""
    payload = {"query": query, "variables": variables}
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload)
    
    if response.status_code != 200:
        raise Exception(f"Query failed: {response.status_code} - {response.text}")
    
    data = response.json()
    if 'errors' in data:
        raise Exception(f"GraphQL Errors: {data['errors']}")
        
    return data

def get_shopify_product_map():
    """
    Fetches ONLY inventory items assigned to the Thibault Location.
    """
    print(f"Fetching products ONLY from Location {TARGET_LOCATION_ID}...")
    product_map = {}
    has_next_page = True
    cursor = None

    query = """
    query ($locationId: ID!, $cursor: String) {
      location(id: $locationId) {
        inventoryLevels(first: 250, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          edges {
            node {
              item { 
                id
                variant {
                  sku
                }
              }
            }
          }
        }
      }
    }
    """

    while has_next_page:
        variables = {
            "locationId": TARGET_LOCATION_ID,
            "cursor": cursor
        }
        
        data = run_query(query, variables)
        
        if not data.get('data') or not data['data'].get('location'):
            raise Exception(f"Location {TARGET_LOCATION_ID} not found or access denied.")

        inventory_levels = data['data']['location']['inventoryLevels']['edges']
        
        for level in inventory_levels:
            item = level['node']['item']
            if item.get('variant'):
                sku = item['variant']['sku']
                item_id = item['id']
                if sku:
                    product_map[sku] = item_id
        
        page_info = data['data']['location']['inventoryLevels']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        
    print(f"Mapped {len(product_map)} variants specifically assigned to Thibault.")
    return product_map

def get_supplier_inventory(sku_list):
    """
    Fetches stock from Importations Thibault.
    Handles Status 400 gracefully if valid data is present.
    """
    if not sku_list:
        return {}

    print(f"Fetching supplier data for {len(sku_list)} SKUs...")
    inventory_map = {}
    
    # Batch size
    CHUNK_SIZE = 50
    chunks = [sku_list[i:i + CHUNK_SIZE] for i in range(0, len(sku_list), CHUNK_SIZE)]

    headers = {
        "Authorization": f"Bearer {SUPPLIER_API_TOKEN}",
        "Accept": "application/json"
    }

    for i, batch in enumerate(chunks):
        sku_query = ",".join(batch)
        params = {"sku": sku_query, "language": "en"}
        
        try:
            print(f"Requesting supplier batch {i+1}/{len(chunks)}...")
            response = requests.get(SUPPLIER_API_URL, headers=headers, params=params, timeout=30)
            
            # Accept 400 as a valid response because Thibault sends 400 if ANY SKU is invalid,
            # but still returns data for the valid ones.
            if response.status_code in [200, 400]:
                try:
                    data = response.json()
                except ValueError:
                    print(f"Batch {i+1} returned invalid JSON.")
                    continue

                if isinstance(data, dict):
                    items = data.get('items', [])
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict):
                                item_sku = item.get('sku')
                                qty_data = item.get('quantity', {})
                                if isinstance(qty_data, dict):
                                    qty = qty_data.get('value')
                                    if item_sku and qty is not None:
                                        inventory_map[item_sku] = int(qty)
                    
                    if 'errors' in data:
                        print(f"Batch {i+1} had warnings: {data['errors']}")
                else:
                    print(f"Batch {i+1} unexpected format: {type(data)}")

            else:
                print(f"Error fetching batch {i+1}: Status {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Exception in batch {i+1}: {e}")
        
        time.sleep(0.5)
        
    print(f"Successfully fetched stock for {len(inventory_map)} items.")
    return inventory_map

def bulk_update_inventory(location_id, updates):
    """Sends a bulk mutation to Shopify to update inventory."""
    if not updates:
        print("No updates to send.")
        return

    # We only ask for 'name' and 'delta' to avoid the GraphQL error.
    mutation = """
    mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) {
        userErrors {
          field
          message
        }
        inventoryAdjustmentGroup {
          reason
          changes {
            name
            delta
          }
        }
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
                "ignoreCompareQuantity": True, # <--- FIX ADDED HERE
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
            else:
                 print("Batch failed, unknown response structure:", data)
        except Exception as e:
            print(f"Failed to send batch: {e}")
        
        time.sleep(1)

def main():
    location_id = TARGET_LOCATION_ID
    print(f"Syncing to Location ID: {location_id}")
    
    shopify_map = get_shopify_product_map()
    all_skus = list(shopify_map.keys())
    
    if not all_skus:
        print("No products found in Shopify for this location.")
        return

    supplier_data = get_supplier_inventory(all_skus)
    
    updates = []
    for sku, qty in supplier_data.items():
        if sku in shopify_map:
            inventory_item_id = shopify_map[sku]
            
            updates.append({
                "inventoryItemId": inventory_item_id,
                "locationId": location_id,
                "quantity": int(qty)
            })
            
    print(f"Prepared {len(updates)} updates.")
    bulk_update_inventory(location_id, updates)

if __name__ == "__main__":
    main()
