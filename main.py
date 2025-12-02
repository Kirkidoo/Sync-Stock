import os
import requests
import json
import time

# --- CONFIGURATION ---
SHOP_URL = os.environ.get("SHOP_URL")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

# Supplier Credentials
# Value in Secrets should be: https://api.importationsthibault.com/api/v1/stock
SUPPLIER_API_URL = os.environ.get("SUPPLIER_API_URL")
# Value in Secrets should be your Bearer token (e.g., "12345...")
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
    """Fetches all variants and creates a map: SKU -> InventoryItemID."""
    print("Fetching Shopify products...")
    product_map = {}
    has_next_page = True
    cursor = None

    query = """
    query ($cursor: String) {
      productVariants(first: 250, after: $cursor) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            sku
            inventoryItem {
              id
            }
          }
        }
      }
    }
    """

    while has_next_page:
        data = run_query(query, variables={"cursor": cursor})
        variants = data['data']['productVariants']['edges']
        
        for v in variants:
            sku = v['node']['sku']
            item_id = v['node']['inventoryItem']['id']
            # Only map if SKU exists
            if sku:
                product_map[sku] = item_id
        
        page_info = data['data']['productVariants']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        
    print(f"Mapped {len(product_map)} variants from Shopify.")
    return product_map

def get_supplier_inventory(sku_list):
    """
    Fetches stock from Importations Thibault for the given SKUs.
    We must chunk the SKUs because the API expects a comma-separated list
    and URL length is limited.
    """
    print(f"Fetching supplier data for {len(sku_list)} SKUs...")
    
    inventory_map = {}
    
    # Chunk SKUs into batches of 50 to keep URL short
    CHUNK_SIZE = 50
    chunks = [sku_list[i:i + CHUNK_SIZE] for i in range(0, len(sku_list), CHUNK_SIZE)]

    headers = {
        "Authorization": f"Bearer {SUPPLIER_API_TOKEN}",
        "Accept": "application/json"
    }

    for i, batch in enumerate(chunks):
        # Join SKUs with commas: "SKU1,SKU2,SKU3"
        sku_query = ",".join(batch)
        
        params = {
            "sku": sku_query,
            "language": "en" # Optional, but good practice
        }
        
        try:
            print(f"Requesting supplier batch {i+1}/{len(chunks)}...")
            response = requests.get(SUPPLIER_API_URL, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                
                for item in items:
                    item_sku = item.get('sku')
                    # Access nested quantity.value
                    # JSON structure: {"quantity": {"value": 8, "unit": ...}}
                    qty_data = item.get('quantity', {})
                    qty = qty_data.get('value')
                    
                    if item_sku and qty is not None:
                        inventory_map[item_sku] = int(qty)
            else:
                print(f"Error fetching batch {i+1}: Status {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Exception in batch {i+1}: {e}")
        
        # Be polite to their API
        time.sleep(1)
        
    print(f"Successfully fetched stock for {len(inventory_map)} items.")
    return inventory_map

def bulk_update_inventory(location_id, updates):
    """Sends a bulk mutation to Shopify to update inventory."""
    if not updates:
        print("No updates to send.")
        return

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
            quantity
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
                "quantities": batch
            }
        }
        
        print(f"Sending Shopify update batch {i//BATCH_SIZE + 1}...")
        data = run_query(mutation, variables)
        
        user_errors = data['data']['inventorySetQuantities']['userErrors']
        if user_errors:
            print("Errors in batch:", user_errors)
        else:
            print("Batch success.")
        
        time.sleep(1)

def main():
    location_id = TARGET_LOCATION_ID
    print(f"Syncing to Location ID: {location_id}")
    
    # 1. Get all SKUs from Shopify
    shopify_map = get_shopify_product_map()
    all_skus = list(shopify_map.keys())
    
    if not all_skus:
        print("No products found in Shopify.")
        return

    # 2. Ask Supplier for stock of THESE specific SKUs
    supplier_data = get_supplier_inventory(all_skus)
    
    # 3. Match them up
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
