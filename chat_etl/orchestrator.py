import sys
import os
import shutil
import glob
# Use relative imports within the package
from . import parse_convos 
from . import ingest_convos

# Import defaults from utils submodule
try:
    from .utils.dynamodb_utils import DEFAULT_TABLE_NAME, DYNAMODB_ENDPOINT
except ImportError:
    print("Warning: Could not import defaults from .utils.dynamodb_utils. Using hardcoded fallback values.")
    DEFAULT_TABLE_NAME = "ChatConversations"
    DYNAMODB_ENDPOINT = "http://localhost:8000"

# Define default directories (relative to project root where script is run via -m)
DEFAULT_RAW_CONVERSATIONS_DIR = "chatgpt-export-json"
DEFAULT_PARSED_CONVERSATIONS_DIR = "output_json"
DEFAULT_PARSED_ARCHIVE_DIR = "parsed_archive"

def _prepare_directories(output_dir, archive_dir):
    """
    Prepares output and archive directories for the ETL run.
    Ensures directories exist, clears the archive directory, and moves
    previous parsed files from the output directory to the archive directory.
    """
    print(f"Preparing directories: Clearing '{archive_dir}', Archiving previous run's files from '{output_dir}'...")
    try:
        os.makedirs(archive_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        # Clear the archive directory
        archive_files = glob.glob(os.path.join(archive_dir, '*'))
        for f in archive_files:
            try:
                if os.path.isfile(f) or os.path.islink(f):
                    os.unlink(f) # Use unlink for files/links
                elif os.path.isdir(f):
                    shutil.rmtree(f) # Use rmtree for directories
            except Exception as e:
                print(f"  Warning: Could not remove {f} from archive: {e}")
        print(f"  Cleared archive directory '{archive_dir}'.")

        # Move files from output directory to archive directory
        files_to_move = glob.glob(os.path.join(output_dir, '*_parsed.json'))
        moved_count = 0
        if not files_to_move:
            print(f"  No previous parsed files found in '{output_dir}' to archive.")
        else:
            for src_path in files_to_move:
                try:
                    filename = os.path.basename(src_path)
                    dest_path = os.path.join(archive_dir, filename)
                    shutil.move(src_path, dest_path)
                    moved_count += 1
                except Exception as e:
                    print(f"  Warning: Could not move {src_path} to archive: {e}")
            print(f"  Archived {moved_count} previous parsed files from '{output_dir}' to '{archive_dir}'.")
        
        print("Directory preparation complete.")

    except Exception as e:
        print(f"Error during directory preparation: {e}")
        raise # Re-raise the exception to halt execution if preparation fails

def run_etl_workflow(raw_input_dir, parsed_output_dir, table_name, endpoint_url, parsed_archive_dir):
    """
    Runs the full ETL workflow: Prepare directories, parse raw files, and ingest parsed files.

    Args:
        raw_input_dir (str): Path to the directory containing raw conversation JSON files.
        parsed_output_dir (str): Path to the directory where parsed JSON files will be written.
        table_name (str): The name of the DynamoDB table to ingest into.
        endpoint_url (str): The endpoint URL for DynamoDB.
        parsed_archive_dir (str): Path to the directory for archiving previous parsed files.
    """
    print("Starting ETL workflow...")
    print(f"  Raw Input Directory: {raw_input_dir}")
    print(f"  Parsed Output Directory: {parsed_output_dir}")
    print(f"  Parsed Archive Directory: {parsed_archive_dir}")
    print(f"  Target Table: {table_name} ({endpoint_url})")

    # --- Step 0: Prepare Directories ---
    print("\n--- Preparing Directories ---")
    try:
        _prepare_directories(parsed_output_dir, parsed_archive_dir)
        print("Directory preparation step completed.")
    except Exception as e:
        # Error already printed in _prepare_directories, just exit.
        print("Halting workflow due to directory preparation error.")
        raise 

    # --- Step 1: Parse Raw Conversations ---
    print("\n--- Running Parser ---")
    try:
        # Call functions from imported modules
        parse_convos.process_raw_directory(raw_input_dir, parsed_output_dir)
        print("Parser step completed.")
    except Exception as e:
        print(f"Error during parsing: {e}")
        raise 

    # --- Step 2: Ingest Parsed Conversations into DynamoDB ---
    print("\n--- Running Ingestion ---")
    try:
        # Call functions from imported modules
        ingest_convos.process_directory(parsed_output_dir, table_name, endpoint_url)
        print("Ingestion step completed.")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise 

    print("\nETL workflow completed successfully.")


if __name__ == "__main__":
    # This block is primarily useful if running the module directly, 
    # but standard execution is now 'python -m chat_etl.orchestrator'
    print("Running orchestrator module directly...")
    # When run directly, use the default directories and default table/endpoint
    try:
        run_etl_workflow(
            DEFAULT_RAW_CONVERSATIONS_DIR,
            DEFAULT_PARSED_CONVERSATIONS_DIR,
            DEFAULT_TABLE_NAME,
            DYNAMODB_ENDPOINT,
            DEFAULT_PARSED_ARCHIVE_DIR
        )
    except Exception as e:
        print(f"ETL workflow failed: {e}")
        sys.exit(1)
