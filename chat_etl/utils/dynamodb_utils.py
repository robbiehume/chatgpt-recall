import boto3
import time
from botocore.exceptions import ClientError
import sys # Added for __main__ exit

# --- Configuration ---
DYNAMODB_ENDPOINT = "http://localhost:8000"
DEFAULT_TABLE_NAME = "ChatConversations" # Renamed from TABLE_NAME
REGION_NAME = 'us-east-2' # Or configure as needed

# --- Helper: Get Boto3 Client/Resource ---
def _get_dynamodb_resource(endpoint_url):
    return boto3.resource('dynamodb',
                          region_name=REGION_NAME,
                          endpoint_url=endpoint_url)

def _get_dynamodb_client(endpoint_url):
    return boto3.client('dynamodb',
                        region_name=REGION_NAME,
                        endpoint_url=endpoint_url)

# --- Table Management Functions ---
def create_test_table(table_name, endpoint_url=DYNAMODB_ENDPOINT, verbose=True):
    """Creates a DynamoDB table with the required schema for testing."""
    dynamodb = _get_dynamodb_resource(endpoint_url)
    client = _get_dynamodb_client(endpoint_url)
    
    if verbose:
        print(f"Attempting to create table '{table_name}' at {endpoint_url}...")
        
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'ConversationID', # Partition key
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'ItemType', # Sort key (e.g., MSG#<uuid>, METADATA#<type>)
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'ConversationID',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'ItemType',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        # Wait until the table exists.
        if verbose:
            print("  Waiting for table to become active...")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        if verbose:
            print(f"Table '{table_name}' created successfully.")
        return table # Return the table resource object
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            if verbose:
                print(f"Table '{table_name}' already exists. Using existing table.")
            # Return the existing table object
            return dynamodb.Table(table_name)
        else:
            if verbose:
                print(f"Error creating table '{table_name}': {e}")
            raise # Re-raise other errors

def delete_test_table(table_name, endpoint_url=DYNAMODB_ENDPOINT, verbose=True):
    """Deletes the specified DynamoDB table."""
    dynamodb = _get_dynamodb_resource(endpoint_url)
    client = _get_dynamodb_client(endpoint_url)
    table = dynamodb.Table(table_name)
    
    if verbose:
        print(f"Attempting to delete table '{table_name}'...")
    
    try:
        table.delete()
        # Wait until the table is deleted
        if verbose:
             print("  Waiting for table to be deleted...")
        client.get_waiter('table_not_exists').wait(TableName=table_name)
        if verbose:
            print(f"Table '{table_name}' deleted successfully.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            if verbose:
                print(f"Table '{table_name}' not found, nothing to delete.")
            return True # Considered success if it doesn't exist
        else:
            if verbose:
                print(f"Error deleting table '{table_name}': {e}")
            return False
            
# --- Data Query/Clear Functions ---

def get_message_ids_from_db(table_name, conversation_pk, endpoint_url=DYNAMODB_ENDPOINT, verbose=False):
    """Fetches all existing message SKs (ItemType starting with MSG#) for a given ConversationID."""
    dynamodb = _get_dynamodb_resource(endpoint_url)
    client = _get_dynamodb_client(endpoint_url)
    existing_ids = set()
    
    try:
        paginator = client.get_paginator('query')
        page_iterator = paginator.paginate(
            TableName=table_name,
            KeyConditionExpression="ConversationID = :pk AND begins_with(ItemType, :sk_prefix)",
            ExpressionAttributeValues={
                ":pk": {'S': conversation_pk},
                ":sk_prefix": {'S': "MSG#"}
            },
            ProjectionExpression="ItemType" # Only fetch the sort key
        )

        for page in page_iterator:
            for item in page.get('Items', []):
                item_type_value = item.get('ItemType') 
                sk = None
                if isinstance(item_type_value, dict):
                    sk = item_type_value.get('S')
                elif isinstance(item_type_value, str):
                    # Handle potential inconsistency with DynamoDB Local/boto3
                    if verbose:
                         print(f"Warning (get_message_ids): Unexpected string format for ItemType in query result for {conversation_pk}: {item_type_value}")
                    sk = item_type_value

                if sk and sk.startswith("MSG#"):
                    existing_ids.add(sk[4:]) # Add the MessageID part (strip prefix)
        if verbose:
            print(f"  Found {len(existing_ids)} existing message IDs in DB for {conversation_pk}")
        return existing_ids
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
             if verbose:
                 print(f"Table '{table_name}' not found during query for {conversation_pk}. Returning empty set.")
             return set() # Table doesn't exist, so no messages
        else:
             if verbose:
                 print(f"Error querying existing items for {conversation_pk} in table {table_name}: {e}")
             raise # Re-raise other errors
             
def clear_dynamodb_table(endpoint_url=DYNAMODB_ENDPOINT, table_name=DEFAULT_TABLE_NAME, verbose=True):
    """
    Connects to DynamoDB, scans the specified table, and deletes all items.
    NOTE: Less useful now that tests create/delete tables, but kept for standalone use.

    Args:
        endpoint_url (str): The endpoint URL for DynamoDB.
        table_name (str): The name of the table to clear.
        verbose (bool): If True, print status messages. Defaults to True.

    Returns:
        int: The total number of items deleted. Returns -1 if connection/scan fails.
    """
    if verbose:
        print(f"Attempting to connect to DynamoDB at {endpoint_url}...")
    try:
        dynamodb = _get_dynamodb_resource(endpoint_url)
        table = dynamodb.Table(table_name)
        table.load() # Confirm table exists
        if verbose:
            print(f"Successfully connected to table '{table_name}'.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
             if verbose:
                 print(f"Table '{table_name}' not found. Cannot clear.")
             return 0 # Nothing to clear
        else:
            if verbose:
                print(f"Error connecting to DynamoDB or table '{table_name}': {e}")
                print("Please ensure DynamoDB local is running.")
            return -1 # Indicate connection/other failure
    except Exception as e: # Catch other potential connection errors
        if verbose:
            print(f"Error connecting to DynamoDB: {e}")
        return -1

    if verbose:
        print(f"Starting to clear all items from table '{table_name}'...")
    
    delete_count = 0
    scan_kwargs = {}
    items_exist = True

    while items_exist:
        try:
            # Scan requires the table resource object
            response = table.scan(**scan_kwargs)
        except ClientError as e:
            if verbose:
                print(f"Error during scan operation: {e}")
            return -1 # Indicate scan failure
        except Exception as e:
            if verbose:
                print(f"Unexpected error during scan: {e}")
            return -1

        items = response.get('Items', [])
        if not items:
            items_exist = False
            if verbose:
                print("No items found to delete in this scan.")
            break # Exit loop if no items in the current page

        if verbose:
            print(f"Found batch of {len(items)} items to delete...")
        try:
            # Batch writer requires the table resource object
            with table.batch_writer() as batch:
                for item in items:
                    key = {
                        'ConversationID': item['ConversationID'],
                        'ItemType': item['ItemType']
                    }
                    batch.delete_item(Key=key)
                    delete_count += 1
            if verbose:
                print(f"  -> Deleted batch. Total deleted so far: {delete_count}")
        except ClientError as e:
            if verbose:
                print(f"Error during batch delete operation: {e}")
                print("Stopping deletion process due to batch error.")
            # Indicate failure, but some items might have been deleted
            return -1 
        except Exception as e:
             if verbose:
                 print(f"Unexpected error during batch delete: {e}")
             return -1

        last_evaluated_key = response.get('LastEvaluatedKey')
        if last_evaluated_key:
            if verbose:
                print("  -> More items exist, continuing scan...")
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            time.sleep(0.1) # Small delay
        else:
            items_exist = False # No more pages
            if verbose:
                print("Scan complete.")

    if verbose:
        print(f"\nFinished clearing table '{table_name}'. Total items deleted: {delete_count}")
    return delete_count

# --- Main Execution Block (for standalone use: clearing DEFAULT table) ---
if __name__ == "__main__":
    print("--- DynamoDB Utilities (Standalone Mode) ---")
    print(f"This script, when run directly, will offer to clear the DEFAULT table: '{DEFAULT_TABLE_NAME}'.")
    print("The table creation/deletion functions are intended for programmatic use.")
    
    confirm = input(f"\nDo you want to clear ALL items from table '{DEFAULT_TABLE_NAME}' at {DYNAMODB_ENDPOINT}? (yes/no): ")
    if confirm.lower() == 'yes':
        deleted_count = clear_dynamodb_table(table_name=DEFAULT_TABLE_NAME, endpoint_url=DYNAMODB_ENDPOINT)
        if deleted_count == -1:
            print("Clearing failed. Check errors above.")
            sys.exit(1)
        else:
            print(f"Operation completed. {deleted_count} items were deleted from '{DEFAULT_TABLE_NAME}'.")
    else:
        print("Operation cancelled by user.") 