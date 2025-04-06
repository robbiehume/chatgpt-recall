import boto3
import json
import os
import sys
from decimal import Decimal
from botocore.exceptions import ClientError

# Import helpers using relative path within the package
try:
    from .utils.dynamodb_utils import _get_dynamodb_resource, _get_dynamodb_client, DEFAULT_TABLE_NAME, DYNAMODB_ENDPOINT
except ImportError as e:
    print(f"Error importing from .utils.dynamodb_utils: {e}. Make sure it exists.")
    # Provide default values to potentially allow parts of the script to function if utils are missing
    # This isn't ideal, but prevents immediate crash on import failure.
    DEFAULT_TABLE_NAME = "ChatConversations"
    DYNAMODB_ENDPOINT = "http://localhost:8000"
    # Define dummy functions if needed, or let subsequent code fail
    def _get_dynamodb_resource(endpoint_url):
        print("Warning: .utils.dynamodb_utils not found, creating direct boto3 resource.")
        return boto3.resource('dynamodb', region_name='us-east-2', endpoint_url=endpoint_url)
    def _get_dynamodb_client(endpoint_url):
        print("Warning: .utils.dynamodb_utils not found, creating direct boto3 client.")
        return boto3.client('dynamodb', region_name='us-east-2', endpoint_url=endpoint_url)

# --- Helper Functions (File Loading, Data Conversion - No DB Interaction) ---

def convert_floats_to_decimal(obj):
    """Recursively convert floats in a data structure to Decimals."""
    if isinstance(obj, float):
        # Ensure conversion is accurate, especially for scientific notation
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj

def load_messages_from_file(file_path):
    """Load JSON message list from a parsed file."""
    try:
        # If the parser skips writing empty files, this will handle FileNotFoundError
        if not os.path.exists(file_path):
             print(f"Info: Parsed file {file_path} not found, likely no new messages. Skipping.")
             return []
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        else:
            print(f"Warning: Expected a list in {file_path}, found {type(data)}. Skipping.")
            return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {file_path}. Skipping.")
        return []
    except Exception as e:
        print(f"Error reading file {file_path}: {e}. Skipping.")
        return []

# --- DynamoDB Interaction Functions (Parameterized) ---

def get_existing_message_ids(conversation_pk, table_name, endpoint_url):
    """Fetch all existing message SKs (ItemType) for a given ConversationID from the specified table."""
    client = _get_dynamodb_client(endpoint_url) # Get client using helper
    existing_ids = set()
    try:
        paginator = client.get_paginator('query')
        page_iterator = paginator.paginate(
            TableName=table_name,
            KeyConditionExpression="ConversationID = :pk AND begins_with(ItemType, :sk_prefix)",
            ExpressionAttributeValues={
                ":pk": {'S': conversation_pk}, # Query API uses explicit types
                ":sk_prefix": {'S': "MSG#"}
            },
            ProjectionExpression="ItemType"
        )

        for page in page_iterator:
            for item in page.get('Items', []):
                item_type_value = item.get('ItemType')
                sk = None
                if isinstance(item_type_value, dict):
                    sk = item_type_value.get('S')
                elif isinstance(item_type_value, str):
                    # Log a warning if this path is ever taken - might indicate issue with env/boto3/db
                    # print(f"Warning: Unexpected string format for ItemType in query result for {conversation_pk}: {item_type_value}")
                    sk = item_type_value
                
                if sk and sk.startswith("MSG#"):
                    existing_ids.add(sk[4:])
                    
        print(f"  Found {len(existing_ids)} existing message IDs in DB ({table_name}) for {conversation_pk}")
        return existing_ids
        
    except ClientError as e:
        # Handle table not found gracefully, as it might occur in tests before creation
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"Warning: Table '{table_name}' not found while querying for {conversation_pk}. Returning empty set.")
            return set()
        else:
            print(f"Error querying existing items for {conversation_pk} in {table_name}: {e}")
            raise # Re-raise other client errors
    except Exception as e:
        print(f"Unexpected error in get_existing_message_ids for {conversation_pk} in {table_name}: {e}")
        raise # Re-raise unexpected errors

def sync_conversation_messages(canonical_messages, conversation_id, table_name, endpoint_url):
    """Syncs a specific table in DynamoDB with the canonical message list for a conversation."""
    dynamodb_resource = _get_dynamodb_resource(endpoint_url) # Get resource using helper
    table = dynamodb_resource.Table(table_name)
    conversation_pk = f"CONV#{conversation_id}"
    puts_count = 0
    deletes_count = 0

    # 1. Get existing message IDs from DB (pass table_name and endpoint_url)
    try:
        db_message_ids = get_existing_message_ids(conversation_pk, table_name, endpoint_url)
    except Exception as e: # Catch errors raised by get_existing_message_ids
        print(f"Failed to get existing messages for {conversation_pk} from {table_name}. Aborting sync. Error: {e}")
        return 0, 0 # Return 0 puts, 0 deletes

    # 2. Get canonical message IDs from the parsed file data
    canonical_message_ids = set()
    valid_canonical_messages = []
    for msg in canonical_messages:
        msg_id = msg.get("MessageID")
        if msg_id:
            canonical_message_ids.add(msg_id)
            valid_canonical_messages.append(msg)
        else:
            print(f"Warning: Skipping message in {conversation_id} due to missing MessageID: {msg.get('Content', '')[:50]}...")
    print(f"  Found {len(canonical_message_ids)} canonical message IDs in parsed file for {conversation_pk}")

    # 3. Calculate Diff
    ids_to_delete = db_message_ids - canonical_message_ids
    print(f"  Calculated: {len(ids_to_delete)} items to DELETE, {len(valid_canonical_messages)} items to PUT/UPDATE.")

    # 4. Execute Batch Puts/Deletes
    if not ids_to_delete and not valid_canonical_messages:
        print(f"  No changes needed for {conversation_pk} in {table_name}.")
        return 0, 0
        
    try:
        # Use the table object obtained earlier
        with table.batch_writer() as batch:
            # Deletions
            for msg_id_to_delete in ids_to_delete:
                delete_key = {
                    'ConversationID': conversation_pk,
                    'ItemType': f"MSG#{msg_id_to_delete}"
                }
                batch.delete_item(Key=delete_key)
                deletes_count += 1

            # Puts (Inserts/Updates)
            for message in valid_canonical_messages:
                message_id = message["MessageID"]
                item = {
                    "ConversationID": conversation_pk,
                    "ItemType": f"MSG#{message_id}",
                    "Timestamp": message.get("Timestamp", 0),
                    "Author": message.get("Author", "Unknown"),
                    "Content": message.get("Content", "")
                }
                item = convert_floats_to_decimal(item)
                batch.put_item(Item=item)
                puts_count += 1
        
        print(f"  Batch operation successful for {conversation_pk} in {table_name}. Puts: {puts_count}, Deletes: {deletes_count}")
        return puts_count, deletes_count

    except ClientError as e:
        print(f"Error during batch write for {conversation_pk} in {table_name}: {e}")
        # Depending on the error, partial writes might have occurred.
        # For simplicity, report 0 changes on error.
        return 0, 0 
    except Exception as e:
        print(f"Unexpected error during batch write for {conversation_pk} in {table_name}: {e}")
        return 0, 0

# --- Directory Processing (Main Entry Point) ---

def process_directory(directory, table_name, endpoint_url):
    """Processes parsed files in the directory, syncing each conversation to the specified table."""
    total_puts = 0
    total_deletes = 0
    processed_files_count = 0

    print(f"\nStarting sync process for directory: '{directory}' -> Table: '{table_name}' ({endpoint_url})")

    if not os.path.isdir(directory):
        print(f"Error: Input directory '{directory}' not found.")
        # Raise error so orchestrator knows it failed
        raise FileNotFoundError(f"Input directory not found: {directory}")

    for filename in os.listdir(directory):
        if filename.endswith('_parsed.json'):
            file_path = os.path.join(directory, filename)
            base_name = filename[:-len('_parsed.json')]

            if not base_name:
                print(f"Warning: Skipping file {filename} due to unexpected naming format.")
                continue

            print(f"\nProcessing sync for: {filename} (ConversationID Base: {base_name})")
            canonical_messages = load_messages_from_file(file_path)
            
            # Pass table_name and endpoint_url down
            puts, deletes = sync_conversation_messages(canonical_messages, base_name, table_name, endpoint_url)
            total_puts += puts
            total_deletes += deletes
            processed_files_count += 1
            
    print(f"\nFinished sync process for directory '{directory}'.")
    print(f"Processed {processed_files_count} parsed conversation files for table '{table_name}'.")
    print(f"Total Puts/Updates: {total_puts}, Total Deletes: {total_deletes}")


if __name__ == "__main__":
    # Standalone execution uses defaults
    DEFAULT_INPUT_DIR = "output_json" # Assumes relative to where script is run
    print("Running ingest_convos module directly...")
    # Note: Running this directly might be less common now
    # Requires output_json to exist relative to the execution path
    # It's better to run via the orchestrator: python -m chat_etl.orchestrator
    if not os.path.isdir(DEFAULT_INPUT_DIR):
         print(f"Error: Default input directory '{DEFAULT_INPUT_DIR}' not found relative to current path.")
         sys.exit(1)
    try:
        process_directory(DEFAULT_INPUT_DIR, DEFAULT_TABLE_NAME, DYNAMODB_ENDPOINT)
    except Exception as e:
        print(f"Ingestion failed during standalone run: {e}")
        sys.exit(1)
