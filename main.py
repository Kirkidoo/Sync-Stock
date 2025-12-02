import os
import requests
import json
import time

# --- CONFIGURATION ---
# Load secrets from Environment Variables (set by GitHub Actions)
SHOP_URL = os.environ.get("SHOP_URL")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")

if not SHOP_URL:
    raise ValueError("Shop URL is missing. Add 'SHOP_URL' to GitHub Secrets.")

if not ACCESS_TOKEN:
    raise ValueError("Access Token is missing. Add 'SHOPIFY_ACCESS_TOKEN' to GitHub Secrets.")

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

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

def get_primary_location_id():
    """Fetches the first active location ID to store inventory."""
    query = """
    query {
      locations(first: 5) {
        edges {
          node {
            id
            name
            isActive
          }
        }
      }
    }
    """
    data = run_query(query)
    locations = data['data']['locations']['edges']
    
    for loc in locations:
        if loc['node']['isActive']:
            print(f"Found Location: {loc['node']['name']}")
            return loc['node']['id']
            
    raise Exception("No active location found!")

def get_shopify_product_map():
    """
    Fetches all variants and creates a map: SKU -> InventoryItemID.
    This is needed because GraphQL updates inventory using the Item ID, not SKU.
    """
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
            if sku:
                product_map[sku] = item_id
        
        page_info = data['data']['productVariants']['pageInfo']
        has_next_page = page_info['hasNextPage']
        cursor = page_info['endCursor']
        
    print(f"Mapped {len(product_map)} variants from Shopify.")
    return product_map

def get_supplier_inventory():
    """
    REPLACE THIS FUNCTION with your actual logic to get supplier data.
    Should return a dictionary: {'SKU': quantity}
    """
    print("Fetching supplier data...")
    
    # --- EXAMPLE LOGIC (DELETE THIS AND ADD YOUR OWN) ---
    # response = requests.get("https://supplier.com/feed.csv")
    # parse_csv(response.text) ...
    
    # For now, we return dummy data for testing
    return {
        "SHIRT-BLUE-L": 50,
        "SHIRT-RED-M": 0,
        "PANTS-JEAN-32": 15
    }
    # ----------------------------------------------------

def bulk_update_inventory(location_id, updates):
    """
    Sends a bulk mutation to Shopify to update inventory.
    """
    if not updates:
        print("No updates to send.")
        return

    # GraphQL mutation for setting quantities
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
    
    # Shopify recommends batches of ~250 max. We will do 100 to be safe.
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
        
        print(f"Sending batch {i//BATCH_SIZE + 1} with {len(batch)} items...")
        data = run_query(mutation, variables)
        
        # Check for user errors (business logic errors)
        user_errors = data['data']['inventorySetQuantities']['userErrors']
        if user_errors:
            print("Errors in batch:", user_errors)
        else:
            print("Batch success.")
        
        # Basic rate limit handling
        time.sleep(1)

def main():
    # 1. Get Location
    location_id = get_primary_location_id()
    
    # 2. Get Shopify Map (SKU -> InventoryItemID)
    shopify_map = get_shopify_product_map()
    
    # 3. Get Supplier Data (SKU -> Quantity)
    supplier_data = get_supplier_inventory()
    
    # 4. Prepare Updates
    updates = []
    for sku, qty in supplier_data.items():
        if sku in shopify_map:
            inventory_item_id = shopify_map[sku]
            
            updates.append({
                "inventoryItemId": inventory_item_id,
                "locationId": location_id,
                "quantity": int(qty)
            })
        else:
            print(f"Warning: Supplier SKU '{sku}' not found in Shopify.")
            
    print(f"Prepared {len(updates)} updates.")
    
    # 5. Execute Updates
    bulk_update_inventory(location_id, updates)

if __name__ == "__main__":
    main()
