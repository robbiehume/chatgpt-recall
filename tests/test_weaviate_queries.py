import weaviate
import weaviate.classes as wvc
from weaviate.classes.query import MetadataQuery, Filter
from weaviate.util import generate_uuid5
import os
import sys
from openai import OpenAI

# --- Configuration ---
WEAVIATE_ENDPOINT = os.getenv("WEAVIATE_ENDPOINT", "http://localhost:8080")
WEAVIATE_CLASS_NAME = "Message"
# Example IDs for testing (use IDs known to be ingested)
TEST_CONVERSATION_ID = "ChatGPT-New_chat" 
# Assuming the 'hi' message ID from previous tests
TEST_MESSAGE_ID = "3bd19574-99f5-4c90-9508-002d3a0e2003" 
TEST_UUID = generate_uuid5(f"{TEST_CONVERSATION_ID}_{TEST_MESSAGE_ID}")
TEST_QUERY = "hello" # Simple query for vector search

def run_weaviate_queries():
    """Connects to Weaviate and runs verification queries."""
    print("Running Weaviate Query Script...")

    # --- Initialize Clients ---
    try:
        # Weaviate Client
        w_client = weaviate.connect_to_local(
            host="localhost",
            port=8080,
            grpc_port=50051
        )
        if not w_client.is_ready():
            raise ConnectionError("Weaviate is not ready.")
        print(f"Weaviate client connected to: {WEAVIATE_ENDPOINT}")
        
        # OpenAI Client (for query embedding)
        # Relies on OPENAI_API_KEY env var
        openai_client = OpenAI()
        
    except Exception as e:
        print(f"Error initializing clients: {e}")
        sys.exit(1)

    # Get Weaviate collection
    try:
        messages_collection = w_client.collections.get(WEAVIATE_CLASS_NAME)
        print(f"Accessed Weaviate collection: '{WEAVIATE_CLASS_NAME}'")
    except Exception as e:
        print(f"Error accessing Weaviate collection '{WEAVIATE_CLASS_NAME}': {e}")
        w_client.close()
        sys.exit(1)

    # --- Test 1: Fetch Object by UUID ---
    print(f"\n--- Test 1: Fetching object by known UUID: {TEST_UUID} ---")
    try:
        obj = messages_collection.query.fetch_object_by_id(
            uuid=TEST_UUID,
            include_vector=True # Include vector to verify its presence
        )
        if obj:
            print("  Found object:")
            print(f"    UUID: {obj.uuid}")
            print(f"    Properties: {obj.properties}")
            # v4 access to vector (assuming default unnamed vector)
            if obj.vector and 'default' in obj.vector:
                 vector = obj.vector['default'] # Access the default vector
                 print(f"    Vector: Present (Length: {len(vector)} First 3: {vector[:3]}...)") 
            else:
                 print("    Vector: Not Present")
        else:
            print("  Object not found.")
    except Exception as e:
        print(f"  Error fetching object by UUID: {e}")

    # --- Test 2: Vector Search (Near Vector) ---
    print(f"\n--- Test 2: Performing vector search for query: '{TEST_QUERY}' ---")
    try:
        # Get embedding for the test query
        print(f"  Generating embedding for '{TEST_QUERY}'...")
        response = openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=[TEST_QUERY]
        )
        query_vector = response.data[0].embedding
        print(f"  Query vector generated (Length: {len(query_vector)}).")

        # Perform nearVector search
        print("  Performing nearVector search...")
        search_results = messages_collection.query.near_vector(
            near_vector=query_vector,
            limit=3, # Get top 3 results
            return_metadata=MetadataQuery(distance=True), # Use v4 MetadataQuery class
            return_properties=["messageId", "conversationId", "author", "content"] # Specify props
        )
        
        print(f"  Found {len(search_results.objects)} results:")
        for i, result_obj in enumerate(search_results.objects):
            print(f"    Result {i+1}:")
            print(f"      UUID: {result_obj.uuid}")
            print(f"      Properties: {result_obj.properties}")
            print(f"      Distance: {result_obj.metadata.distance:.4f}")

    except Exception as e:
        print(f"  Error during vector search: {e}")

    # --- Test 3: Fetch Objects with Filter ---
    print(f"\n--- Test 3: Fetching objects with filter: conversationId == '{TEST_CONVERSATION_ID}' ---")
    try:
        filter_results = messages_collection.query.fetch_objects(
            limit=3,
            filters=Filter.by_property("conversationId").equal(TEST_CONVERSATION_ID) # Use v4 Filter class
        )
        print(f"  Found {len(filter_results.objects)} objects matching filter:")
        for i, result_obj in enumerate(filter_results.objects):
            print(f"    Result {i+1}:")
            print(f"      UUID: {result_obj.uuid}")
            print(f"      Properties: {result_obj.properties}")
            
    except Exception as e:
        print(f"  Error fetching objects with filter: {e}")

    # --- Cleanup ---
    print("\nClosing Weaviate client...")
    w_client.close()
    print("Query script finished.")

if __name__ == "__main__":
    # Check for OpenAI API Key before running
    if not os.getenv("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable not set.")
        print("Please set it to run the vector search test.")
        sys.exit(1)
    run_weaviate_queries() 