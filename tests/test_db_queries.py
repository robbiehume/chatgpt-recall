import boto3
from boto3.dynamodb.conditions import Key
import os
import sys

# Default Configuration (used when run standalone)
DYNAMODB_ENDPOINT = "http://localhost:8000"
DEFAULT_TABLE_NAME = "ChatConversations"

def run_all_queries(table_name, endpoint_url):
    """Connects to DynamoDB and runs a series of test queries against the specified table."""
    print(f"Connecting to DynamoDB at {endpoint_url}...")
    try:
        dynamodb = boto3.resource('dynamodb',
                                  region_name='us-east-2',
                                  endpoint_url=endpoint_url)
        table = dynamodb.Table(table_name)
        table.item_count # Try a simple operation
        print(f"Successfully connected to table '{table_name}'.")
    except Exception as e:
        print(f"Error connecting to DynamoDB or table '{table_name}': {e}")
        print("Please ensure DynamoDB local is running and the table exists.")
        return # Exit the function if connection fails

    # --- Test 1: Query all messages for a specific conversation ---
    test_conversation_id = "CONV#ChatGPT-New_chat" # Use a realistic ID
    print(f"\n--- Test 1: Querying all messages for ConversationID: {test_conversation_id} ---")
    try:
        response = table.query(
            KeyConditionExpression=Key('ConversationID').eq(test_conversation_id)
        )
        items = response.get('Items', [])
        print(f"Found {len(items)} messages for {test_conversation_id}:")
        if items:
            for i, item in enumerate(items):
                print(f"  - Item {i+1}: SK={item.get('ItemType')}, Author={item.get('Author')}, Content='{item.get('Content', '')[:50]}...'")
        else:
            print("  No items found for this ConversationID.")
    except Exception as e:
        print(f"Error querying ConversationID {test_conversation_id}: {e}")

    # --- Test 2: Get a single specific message ---
    # Adjust PK/SK to something potentially in the default table if testing standalone
    # Using an example from previous successful integration tests
    test_item_pk = "CONV#ChatGPT-New_chat"
    # This SK might only exist after the *updated* run in the integration test.
    # If running standalone against a fresh table, this get_item will likely fail.
    test_item_sk = "MSG#d6b59f7f-0cbf-43d8-ac9a-0fd02b3db969" # Example from updated message
    print(f"\n--- Test 2: Getting specific item with PK={test_item_pk}, SK={test_item_sk} ---")
    try:
        response = table.get_item(
            Key={
                'ConversationID': test_item_pk,
                'ItemType': test_item_sk
            }
        )
        item = response.get('Item')
        if item:
            print("Found specific item:")
            print(f"  - ConversationID: {item.get('ConversationID')}")
            print(f"  - ItemType (SK): {item.get('ItemType')}")
            print(f"  - Author: {item.get('Author')}")
            print(f"  - Timestamp: {item.get('Timestamp')}")
            print(f"  - Content: '{item.get('Content', '')}'")
        else:
            print("  Specific item not found.")
    except Exception as e:
        print(f"Error getting specific item (PK={test_item_pk}, SK={test_item_sk}): {e}")

    # --- Test 3: Scan a sample of items from the table ---
    scan_limit = 5
    print(f"\n--- Test 3: Scanning table '{table_name}' for a sample ({scan_limit} items) ---")
    try:
        response = table.scan(Limit=scan_limit)
        items = response.get('Items', [])
        print(f"Found {len(items)} sample items (limit was {scan_limit}):")
        if items:
            for i, item in enumerate(items):
                print(f"  - Sample {i+1}: PK={item.get('ConversationID')}, SK={item.get('ItemType')}, Author={item.get('Author')}")
        else:
            print("  Scan returned no items (table might be empty).")
        
        if 'LastEvaluatedKey' in response:
            print("  (Scan results truncated, more items exist in the table)")
            
    except Exception as e:
        print(f"Error scanning table {table_name}: {e}")

    print("\nQuery tests finished.")

if __name__ == "__main__":
    print("Running DB Query Script Standalone...")
    # Use default table and endpoint when run directly
    run_all_queries(DEFAULT_TABLE_NAME, DYNAMODB_ENDPOINT) 