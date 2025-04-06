import os
import shutil
import sys
import time
import uuid
import json
import pytest # Import pytest

# Import functions from other modules using absolute paths from project root
try:
    from chat_etl.utils.dynamodb_utils import (
        create_test_table, 
        delete_test_table, 
        get_message_ids_from_db, 
        DYNAMODB_ENDPOINT 
    )
    from chat_etl.orchestrator import run_etl_workflow
    # Import parser function to get expected IDs from test files
    from chat_etl.parse_convos import extract_canonical_messages 
except ImportError as e:
    print(f"Error importing required functions: {e}")
    print("Make sure chat_etl package exists and required modules are present.")
    sys.exit(1)

# --- Configuration ---
# Get the directory containing the test file itself
TEST_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
# PROJECT_ROOT is usually not needed by pytest when run from root, but keep if used elsewhere
# PROJECT_ROOT = os.path.dirname(os.path.dirname(TEST_FILE_DIR)) # Example if needed

# Update paths to be relative to the test file's directory or project root
TEST_DATA_DIR = os.path.join(TEST_FILE_DIR, "test_data") # test_data is now in the same dir as the test script
ORIGINAL_DATA_DIR = os.path.join(TEST_DATA_DIR, "original")
UPDATED_DATA_DIR = os.path.join(TEST_DATA_DIR, "updated")
TEST_OUTPUT_DIR = os.path.join(TEST_DATA_DIR, "output_parsed") # Dedicated output for test runs

TEST_FILENAME = "ChatGPT-New_chat.json" 
TEST_CONVO_ID_BASE = "ChatGPT-New_chat" # Base name used for ConversationID PK
TEST_CONVO_PK = f"CONV#{TEST_CONVO_ID_BASE}"

# --- Pytest Fixture for Temp Table ---
@pytest.fixture(scope="function") # Run once per test function
def temp_dynamodb_table():
    """Pytest fixture to create and automatically delete a temporary DynamoDB table."""
    table_name = f"test-chat-convos-{uuid.uuid4()}"
    print(f"\n--- [Fixture Setup] Creating Temp Table: {table_name} ---")
    try:
        # Need to ensure utils/orchestrator use correct path if they rely on PROJECT_ROOT
        # For now, assuming DYNAMODB_ENDPOINT is sufficient and utils/orchestrator don't need PROJECT_ROOT
        create_test_table(table_name, endpoint_url=DYNAMODB_ENDPOINT)
        print(f"--- [Fixture Setup] Table {table_name} Created ---")
        yield table_name # Provide the table name to the test function
    finally:
        print(f"\n--- [Fixture Teardown] Deleting Temp Table: {table_name} ---")
        if not delete_test_table(table_name, endpoint_url=DYNAMODB_ENDPOINT):
             print(f"Warning: Failed to delete temporary table '{table_name}' during teardown.")
        else:
             print(f"--- [Fixture Teardown] Table {table_name} Deleted ---")

# --- Helper Functions ---
def clear_directory(directory):
    """Removes all files and subdirectories within a given directory."""
    if not os.path.isdir(directory):
        print(f"Info: Directory '{directory}' does not exist, nothing to clear.")
        return True
    print(f"Clearing directory: {directory}")
    try:
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
                return False
        print(f"Directory '{directory}' cleared.")
        return True
    except Exception as e:
        print(f"Error clearing directory '{directory}': {e}")
        return False

def run_orchestrator_step(step_name, input_dir, output_dir, table_name, endpoint_url):
    """Runs the orchestrator workflow function for a specific table and handles errors."""
    print(f"\n--- Running Step: {step_name} --- ")
    print(f"  Input: {input_dir}")
    print(f"  Output: {output_dir}")
    print(f"  Table: {table_name}")
    try:
        # Pass table_name and endpoint_url to the workflow
        run_etl_workflow(input_dir, output_dir, table_name, endpoint_url)
        print(f"--- Step '{step_name}' Completed Successfully ---")
        return True
    except Exception as e:
        print(f"Error during step '{step_name}': {e}")
        print(f"--- Step '{step_name}' Failed --- ")
        return False

def get_expected_ids(json_file_path):
    """Loads a raw JSON file, parses it, and returns the set of canonical message IDs."""
    print(f"Parsing expected IDs from: {os.path.basename(json_file_path)}")
    try:
        with open(json_file_path, 'r', encoding='utf-8') as infile:
            raw_data = json.load(infile)
        
        if isinstance(raw_data, list):
            conversation_data = raw_data[0] if raw_data else {}
        elif isinstance(raw_data, dict):
            conversation_data = raw_data
        else:
            print(f"  Error: Unexpected JSON format in {json_file_path}")
            return None # Indicate failure
            
        extracted_messages = extract_canonical_messages(conversation_data)
        expected_ids = {msg['MessageID'] for msg in extracted_messages if 'MessageID' in msg}
        print(f"  -> Found {len(expected_ids)} expected canonical message IDs.")
        return expected_ids
        
    except FileNotFoundError:
        print(f"  Error: Expected JSON file not found: {json_file_path}")
        return None
    except (json.JSONDecodeError, Exception) as e:
        print(f"  Error reading/parsing expected JSON file {json_file_path}: {e}")
        return None

# --- Test Function(s) ---
def test_diff_delete_cycle(temp_dynamodb_table): # Fixture provides table name
    """Tests the full ETL cycle with original and updated data, verifying DB state."""
    print(f"===== Running Test: Diff and Delete Cycle (Table: {temp_dynamodb_table}) ====")

    # Define test file paths
    original_test_file = os.path.join(ORIGINAL_DATA_DIR, TEST_FILENAME)
    updated_test_file = os.path.join(UPDATED_DATA_DIR, TEST_FILENAME)
    
    # Ensure input files exist (pytest doesn't run if file missing, but good practice)
    assert os.path.isfile(original_test_file), f"Missing test file: {original_test_file}"
    assert os.path.isfile(updated_test_file), f"Missing test file: {updated_test_file}"

    # --- Run 1: Process Original Data ---
    print("\n--- Test Step 1: Processing Original Data ---")
    assert clear_directory(TEST_OUTPUT_DIR), "Failed to clear test output directory before Run 1"
    
    assert run_orchestrator_step(
        "Orchestrator Run 1 (Original Data)", 
        ORIGINAL_DATA_DIR, 
        TEST_OUTPUT_DIR, 
        temp_dynamodb_table, 
        DYNAMODB_ENDPOINT
    ), "Orchestrator failed during Run 1"
        
    # --- Verification 1 ---
    print("\n--- Test Step 1: Verification --- ")
    expected_ids_1 = get_expected_ids(original_test_file)
    assert expected_ids_1 is not None, "Failed to get expected IDs for Run 1"
    
    actual_ids_1 = get_message_ids_from_db(
        temp_dynamodb_table, 
        TEST_CONVO_PK, 
        endpoint_url=DYNAMODB_ENDPOINT,
        verbose=False # Less verbose query
    )
    
    print(f"Verifying DB state after Run 1 (Expected {len(expected_ids_1)}, Got {len(actual_ids_1)})...")
    assert actual_ids_1 == expected_ids_1, (
        f"Mismatch after Run 1! Expected {len(expected_ids_1)} IDs, got {len(actual_ids_1)}. "
        f"Diff: {actual_ids_1.symmetric_difference(expected_ids_1)}"
    )
    print("Verification for Run 1 PASSED.")
    
    # --- Run 2: Process Updated Data ---
    print("\n--- Test Step 2: Processing Updated Data ---")
    assert clear_directory(TEST_OUTPUT_DIR), "Failed to clear test output directory before Run 2"
    
    assert run_orchestrator_step(
        "Orchestrator Run 2 (Updated Data)", 
        UPDATED_DATA_DIR, 
        TEST_OUTPUT_DIR, 
        temp_dynamodb_table, 
        DYNAMODB_ENDPOINT
    ), "Orchestrator failed during Run 2"
    
    # --- Verification 2 ---
    print("\n--- Test Step 2: Verification --- ")
    expected_ids_2 = get_expected_ids(updated_test_file)
    assert expected_ids_2 is not None, "Failed to get expected IDs for Run 2"
        
    actual_ids_2 = get_message_ids_from_db(
        temp_dynamodb_table, 
        TEST_CONVO_PK, 
        endpoint_url=DYNAMODB_ENDPOINT,
        verbose=False # Less verbose query
    )
    
    print(f"Verifying DB state after Run 2 (Expected {len(expected_ids_2)}, Got {len(actual_ids_2)})...")
    assert actual_ids_2 == expected_ids_2, (
        f"Mismatch after Run 2! Expected {len(expected_ids_2)} IDs, got {len(actual_ids_2)}. "
        f"Diff: {actual_ids_2.symmetric_difference(expected_ids_2)}"
    )
    print("Verification for Run 2 PASSED.")
    
    print("\n===== Test: Diff and Delete Cycle Completed Successfully ====")

# Note: No __main__ block needed. Pytest finds and runs functions starting with `test_`. 

# Note: Need to ensure pytest can still import the main modules (dynamodb_utils, etc.) 
# from the parent directory. This usually works if the project root is added to PYTHONPATH 
# or if pytest is run from the project root directory. 