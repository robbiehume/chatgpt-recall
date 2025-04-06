import boto3
import json
import os
import sys
from decimal import Decimal
from botocore.exceptions import ClientError
import openai
from openai import OpenAI
import weaviate
from weaviate.util import generate_uuid5
from weaviate.classes.config import Configure, Property, DataType, Vectorizers

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

# Weaviate Configuration (Constants)
WEAVIATE_ENDPOINT = os.getenv("WEAVIATE_ENDPOINT", "http://localhost:8080")
WEAVIATE_CLASS_NAME = "Message"

# --- Weaviate Helper Functions ---

# Global Weaviate client instance (initialize later)
weaviate_client = None

def _get_weaviate_client():
    """Initializes and returns a Weaviate v4 client instance."""
    global weaviate_client
    if weaviate_client is None:
        try:
            # v4 Initialization
            weaviate_client = weaviate.connect_to_local(
                host="localhost",
                port=8080,
                grpc_port=50051
            )
            # Perform a simple check to ensure connection
            if not weaviate_client.is_ready():
                raise ConnectionError("Weaviate is not ready.")
            print(f"Weaviate v4 client connected to: {WEAVIATE_ENDPOINT}")
        except Exception as e:
            print(f"Error initializing Weaviate v4 client: {e}")
            weaviate_client = None # Reset on failure
            raise # Re-raise to prevent proceeding without a client
    return weaviate_client

def create_weaviate_schema(client: weaviate.WeaviateClient):
    """Creates the 'Message' class schema in Weaviate v4 if it doesn't exist."""
    
    try:
        # Check if collection exists (v4 uses collections)
        if not client.collections.exists(WEAVIATE_CLASS_NAME):
            # Define collection properties using v4 config classes
            properties = [
                Property(name="messageId", data_type=DataType.TEXT),
                Property(name="conversationId", data_type=DataType.TEXT),
                Property(name="timestamp", data_type=DataType.NUMBER),
                Property(name="author", data_type=DataType.TEXT),
                Property(name="content", data_type=DataType.TEXT),
            ]
            
            client.collections.create(
                name=WEAVIATE_CLASS_NAME,
                description="Stores individual messages from ChatGPT conversations",
                vectorizer_config=Configure.Vectorizer.none(), # Correct usage
                properties=properties
            )
            print(f"Created Weaviate collection: '{WEAVIATE_CLASS_NAME}'")
        else:
            print(f"Weaviate collection '{WEAVIATE_CLASS_NAME}' already exists.")
    except Exception as e:
        print(f"Error creating/checking Weaviate schema: {e}")
        raise # Re-raise schema creation errors

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
    """Syncs messages to DynamoDB and Weaviate v4 for a single conversation."""
    # Get clients
    try:
        dynamodb_resource = _get_dynamodb_resource(endpoint_url)
        w_client: weaviate.WeaviateClient = _get_weaviate_client() # Get Weaviate client
    except Exception as e:
        print(f"Error obtaining DB clients for {conversation_id}: {e}. Aborting sync.")
        return 0, 0

    table = dynamodb_resource.Table(table_name)
    conversation_pk = f"CONV#{conversation_id}"
    puts_count = 0
    deletes_count = 0
    weaviate_puts_count = 0
    weaviate_deletes_count = 0

    # Wrap main logic in a try block
    try:
        # --- Steps 1-3: Get existing IDs, Prepare messages, Calculate Diff (Same as before) ---
        # 1. Get existing message IDs from DynamoDB
        # Note: Errors during get_existing_message_ids are already caught inside it and re-raised
        db_message_ids = get_existing_message_ids(conversation_pk, table_name, endpoint_url)

        # 2. Prepare list of valid messages
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
        # We need to know which messages are genuinely new vs updated for Weaviate deletion logic
        # For simplicity now, we will delete based on DynamoDB diff and re-add all `valid_canonical_messages` to Weaviate.
        # A more robust approach might query Weaviate for existing IDs too.
        print(f"  Calculated (DynamoDB): {len(ids_to_delete)} items to DELETE, {len(valid_canonical_messages)} items to PUT/UPDATE.")

        # --- Step 4: Embedding Generation (Same as before) ---
        embeddings_map = {} # Map MessageID to its Decimal vector (for DynamoDB)
        float_embeddings_map = {} # Map MessageID to its float vector (for Weaviate)
        contents_to_embed = []
        indices_of_messages_to_embed = []
        for i, msg in enumerate(valid_canonical_messages):
            content = msg.get('Content')
            if content:
                contents_to_embed.append(content)
                indices_of_messages_to_embed.append(i)

        if contents_to_embed:
            print(f"  Attempting to generate embeddings for {len(contents_to_embed)} messages...")
            try:
                openai_client = OpenAI()
                response = openai_client.embeddings.create(
                    model="text-embedding-3-small",
                    input=contents_to_embed
                )
                if response.data and len(response.data) == len(contents_to_embed):
                    for i, embedding_data in enumerate(response.data):
                        original_message_index = indices_of_messages_to_embed[i]
                        message_id = valid_canonical_messages[original_message_index]['MessageID']
                        vector = embedding_data.embedding
                        decimal_vector = [Decimal(str(f)) for f in vector]
                        embeddings_map[message_id] = decimal_vector
                        float_embeddings_map[message_id] = vector # Store float version for Weaviate
                    print(f"  Successfully generated {len(embeddings_map)} embeddings.")
                else:
                     print(f"Warning: OpenAI response length mismatch or empty data for {conversation_pk}. Expected {len(contents_to_embed)}, got {len(response.data) if response.data else 0}.")
            except openai.APIError as e:
                print(f"Warning: OpenAI API returned an API Error for {conversation_pk}: {e}. Skipping embeddings for this conversation.")
            except openai.APIConnectionError as e:
                # Handle connection error here
                print(f"Warning: Failed to connect to OpenAI API for {conversation_pk}: {e}. Skipping embeddings for this conversation.")
            except openai.RateLimitError as e:
                # Handle rate limit error (we recommend using exponential backoff)
                print(f"Warning: OpenAI API request exceeded rate limit for {conversation_pk}: {e}. Skipping embeddings for this conversation.")
            except Exception as e:
                print(f"Warning: An unexpected error occurred during embedding generation for {conversation_pk}: {e}. Skipping embeddings for this conversation.")
        else:
            print("  No messages with content found to embed.")

        # --- Step 5: Execute Batch Writes/Deletes (DynamoDB and Weaviate) ---
        if not ids_to_delete and not valid_canonical_messages:
            print(f"  No changes needed for {conversation_pk} in {table_name}.")
            return 0, 0

        # Get the Weaviate collection object
        messages_collection = w_client.collections.get(WEAVIATE_CLASS_NAME)
        
        # Weaviate Deletions (Using v4 batch delete)
        # Need to generate UUIDs for items to delete
        uuids_to_delete = [
            generate_uuid5(f"{conversation_id}_{msg_id}") for msg_id in ids_to_delete
        ]
        
        if uuids_to_delete:
            print(f"  Attempting Weaviate batch deletions for {len(uuids_to_delete)} UUIDs...")
            # v4 batch delete returns results object
            delete_results = messages_collection.data.delete_many(
                where=wvc.Filter.by_id().contains_any(uuids_to_delete)
            )
            print(f"  Weaviate delete operation attempted. Successful: {delete_results.successful}, Failed: {delete_results.failed}")
            # Note: This deletes based on UUID match, actual count might differ if UUIDs didn't exist
            weaviate_deletes_count = delete_results.successful # Track successful deletes

        # Process DynamoDB Batch and Prepare Weaviate Batch
        # Use v4 context manager for batching data objects
        with messages_collection.batch.dynamic() as weaviate_batch:
            try: # Inner try for DynamoDB
                with table.batch_writer() as dynamo_batch:
                    # DynamoDB Deletions
                    for msg_id_to_delete in ids_to_delete:
                        delete_key = {
                            'ConversationID': conversation_pk,
                            'ItemType': f"MSG#{msg_id_to_delete}"
                        }
                        dynamo_batch.delete_item(Key=delete_key)
                        deletes_count += 1

                    # Puts (DynamoDB and Weaviate)
                    for message in valid_canonical_messages:
                        message_id = message["MessageID"]
                        
                        # Prepare DynamoDB Item
                        dynamo_item = {
                            "ConversationID": conversation_pk,
                            "ItemType": f"MSG#{message_id}",
                            "Timestamp": message.get("Timestamp", 0),
                            "Author": message.get("Author", "Unknown"),
                            "Content": message.get("Content", "")
                        }
                        if message_id in embeddings_map:
                            dynamo_item["ContentEmbedding"] = embeddings_map[message_id]
                        final_dynamo_item = convert_floats_to_decimal(dynamo_item)
                        dynamo_batch.put_item(Item=final_dynamo_item)
                        puts_count += 1

                        # Prepare Weaviate Data Object (Properties)
                        weaviate_properties = {
                            "messageId": message_id,
                            "conversationId": conversation_id,
                            "timestamp": float(message.get("Timestamp", 0)),
                            "author": message.get("Author", "Unknown"),
                            "content": message.get("Content", "")
                        }
                        # Get Weaviate UUID
                        weaviate_uuid = generate_uuid5(f"{conversation_id}_{message_id}")
                        # Get float vector for Weaviate
                        vector = float_embeddings_map.get(message_id)
                        
                        # Add object to Weaviate v4 batch
                        weaviate_batch.add_object(
                            properties=weaviate_properties,
                            uuid=weaviate_uuid,
                            vector=vector
                        )
                        weaviate_puts_count += 1 # Increment count for items added to batch

                # DynamoDB batch completes here automatically when context exits
                print(f"  DynamoDB batch successful. Puts: {puts_count}, Deletes: {deletes_count}")
            
            except ClientError as e: # Catch errors specifically from the DynamoDB batch write
                print(f"Error during DynamoDB batch write for {conversation_pk}: {e}")
                # If Dynamo fails, maybe don't proceed with Weaviate?
                # For now, we let Weaviate batch run, but might lead to inconsistency.
                raise # Re-raise DynamoDB batch errors
            
            # Weaviate v4 batch will execute automatically when its context manager exits
            # Check for Weaviate batch errors (more robust error handling needed here)
            if weaviate_batch.number_errors > 0:
                 print(f"Warning: {weaviate_batch.number_errors} errors occurred during Weaviate batch import for {conversation_pk}.")
                 # print(weaviate_batch.errors) # Can print detailed errors if needed
            print(f"  Added/Updated {weaviate_puts_count} objects in Weaviate batch for {conversation_pk}.")

    # Outer exception handler for the whole sync process (steps 1-5, including Weaviate batch execution)
    except Exception as e:
        print(f"Error during sync process for {conversation_pk}: {e}")
        # Depending on where the error occurred, DBs might be inconsistent.
        # Return 0 for simplicity on unexpected errors in this outer block.
        return 0, 0
    
    # Rough estimate - actual success depends on Weaviate batch execution results
    # If we reach here, the main try block succeeded
    return puts_count, deletes_count 

# --- Directory Processing (Main Entry Point) ---

def process_directory(directory, table_name, endpoint_url):
    """Processes parsed files, syncing each conversation to DynamoDB and Weaviate."""
    total_puts = 0
    total_deletes = 0
    processed_files_count = 0

    # --- Initialize Weaviate Client and Schema ---
    try:
        w_client = _get_weaviate_client()
        if w_client:
             create_weaviate_schema(w_client)
        else:
             print("Error: Weaviate client not available. Cannot proceed with ingestion involving Weaviate.")
             raise ConnectionAbortedError("Failed to initialize Weaviate client")
    except Exception as e:
         print(f"Halting: Failed during Weaviate initialization or schema creation: {e}")
         sys.exit(1) # Exit if Weaviate setup fails
    # --- End Initialization ---

    print(f"\nStarting sync process for directory: '{directory}' -> Table: '{table_name}', Weaviate: '{WEAVIATE_ENDPOINT}'")

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
