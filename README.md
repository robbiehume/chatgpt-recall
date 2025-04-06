# ChatGPT Conversation History ETL Pipeline

## Overview

This project provides a Python-based ETL (Extract, Transform, Load) pipeline designed to process conversation history exported from OpenAI's ChatGPT (in JSON format). It extracts the canonical message thread from each conversation, transforms it into a structured format, and loads it into a DynamoDB table. A key feature is its ability to efficiently handle updates and edits in the source JSON by comparing the current canonical messages with the existing records in DynamoDB and performing necessary insertions, updates, and deletions (Diff and Delete strategy).

The project emphasizes modularity, testability, and adherence to standard Python project structuring and testing practices.

## Features

- **Parses ChatGPT JSON Exports:** Handles the official JSON structure provided by ChatGPT data exports.
- **Canonical Thread Extraction:** Intelligently traverses the conversation node mapping to extract only the messages belonging to the main (canonical) conversation thread, ignoring alternative branches created by edits.
- **Robust Parsing:** Handles variations in the export format (e.g., top-level list or dictionary).
- **DynamoDB Integration:** Loads extracted messages into a specified DynamoDB table, storing conversation ID, message ID, author, timestamp, and content.
- **Efficient Updates (Diff and Delete):** When re-processing, compares the newly parsed canonical messages against existing messages for that conversation in DynamoDB. It then performs a batch operation to:
  - `DELETE` messages that are no longer part of the canonical thread.
  - `PUT` new or updated messages (DynamoDB's `put_item` handles both inserts and updates).
- **Modular Design:** Code is organized into logical components:
  - `parse_convos.py`: Handles JSON parsing and canonical thread extraction.
  - `ingest_convos.py`: Handles interaction with DynamoDB (fetching existing IDs, batch writing puts/deletes).
  - `orchestrator.py`: Coordinates the ETL workflow, calling parser and ingestor.
  - `utils/dynamodb_utils.py`: Provides helper functions for interacting with DynamoDB, including table creation/deletion for tests.
- **Testability:**
  - Core components (`orchestrator`, `ingestor`) are parameterized to accept directory paths and table names, facilitating testing.
  - Uses `pytest` for testing.
  - Includes a robust integration test (`tests/test_etl_workflow.py`) that:
    - Uses a temporary, isolated DynamoDB table for each test run (created and torn down automatically via fixtures).
    - Runs the full ETL workflow on sample original and updated data.
    - Performs automated verification of the database state after each step using assertions.
  - Includes **unit tests** (`tests/test_parse_convos.py`) for key parsing logic.
  - Uses `pytest.ini` to filter external library warnings for cleaner test output.
- **Standard Project Structure:** Follows common Python conventions using a core application package (`chat_etl/`) and dedicated `utils/` and `tests/` directories.

## Technology Stack

- **Python 3.x**
- **Boto3:** AWS SDK for Python, used for DynamoDB interaction.
- **Pytest:** Testing framework.
- **DynamoDB:** NoSQL database (designed for AWS DynamoDB, uses DynamoDB Local for development and testing).

## Setup

Before running the application or tests, you need a local DynamoDB instance running.

1.  **Install Docker:** Ensure you have Docker installed and running on your system.
2.  **Run DynamoDB Local Container:** Open your terminal and run the following command to download and start the official DynamoDB Local container:
    ```bash
    docker run -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -sharedDb
    ```
    - `-p 8000:8000`: Maps port 8000 on your host to port 8000 inside the container, which is the default port for DynamoDB Local.
    - `-sharedDb`: Ensures that DynamoDB uses a single database file, making it behave more like the web service, especially regarding table creation/persistence across container restarts (if a volume isn't used).
      Keep this terminal window open; the container runs in the foreground.
3.  **Create the Main Table (First Time Only):** The main application script (`orchestrator.py`) expects the `ChatConversations` table to exist in your local DynamoDB instance. The automated tests create their own temporary tables, but for regular runs, you need to create the main table once.
    - You can use the AWS CLI configured for the local endpoint:
      ```bash
      aws dynamodb create-table \
          --table-name ChatConversations \
          --attribute-definitions AttributeName=ConversationID,AttributeType=S AttributeName=ItemType,AttributeType=S \
          --key-schema AttributeName=ConversationID,KeyType=HASH AttributeName=ItemType,KeyType=RANGE \
          --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
          --endpoint-url http://localhost:8000
      ```
    - Alternatively, you could adapt the `create_table` function from `chat_etl/utils/dynamodb_utils.py` into a small standalone script to create the table if you prefer not to use the AWS CLI.
4.  **Install Dependencies:** Set up a Python virtual environment and install the required packages:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

## 5. Project Structure

```
.
├── chat_etl/             # Core application package
│   ├── __init__.py
│   ├── ingest_convos.py
│   ├── orchestrator.py
│   ├── parse_convos.py
│   └── utils/
│       ├── __init__.py
│       └── dynamodb_utils.py
├── chatgpt-export-json/  # Default directory for raw ChatGPT JSON exports
├── output_json/          # Directory for parsed JSON files from the current run
├── parsed_archive/       # Archive of parsed JSON files from the previous run
├── tests/                # Contains all test code and data
│   ├── __init__.py
│   ├── test_data/        # Sample data for testing
│   │   ├── original/
│   │   ├── updated/
│   │   └── output_parsed/ # Temporary output during test runs
│   ├── test_db_queries.py # Manual query script
│   ├── test_etl_workflow.py # Pytest integration tests
│   └── test_parse_convos.py # Pytest unit tests
├── .gitignore
├── pytest.ini            # Pytest configuration (e.g., warning filters)
├── README.md             # This file
└── requirements.txt      # Project dependencies
```

## 6. Workflow & Component Breakdown

The main workflow is coordinated by `orchestrator.py`:

1.  **Parsing (`parse_convos.py`)**:
    - Reads each raw `.json` file from the specified input directory (e.g., `chatgpt-export-json/`).
    - Loads the JSON data.
    - Calls `extract_canonical_messages` to traverse the node `mapping` backwards from the `current_node` until a root node is reached.
    - Collects messages (ID, timestamp, author, content) belonging only to this canonical path.
    - Saves the extracted list of message objects as a new `_parsed.json` file in the specified output directory (e.g., `output_json/` or `tests/test_data/output_parsed/`).
2.  **Ingestion (`ingest_convos.py`)**:
    - Iterates through the `_parsed.json` files in the specified parsed output directory.
    - For each file (representing one conversation):
      - Constructs the `ConversationID` primary key (e.g., `CONV#<base_filename>`).
      - Calls `get_existing_message_ids` to query DynamoDB for all current `ItemType` sort keys starting with `MSG#` for that `ConversationID`. This returns a set of existing message IDs.
      - Loads the list of canonical messages from the `_parsed.json` file and extracts their message IDs into a set.
      - Calculates the difference:
        - `ids_to_delete = db_message_ids - canonical_message_ids`
      - Uses DynamoDB's `batch_writer` to efficiently:
        - Delete items whose `ItemType` corresponds to an ID in `ids_to_delete`.
        - Put items for every message in the current `canonical_messages` list (this performs inserts for new messages and updates for existing ones).

## 7. Running the Application

1.  Place your exported ChatGPT JSON file(s) (e.g., `conversations.json` or multiple files) into the `chatgpt-export-json/` directory.
2.  Ensure DynamoDB Local is running.
3.  Activate your virtual environment (`source venv/bin/activate`).
4.  Run the orchestrator **as a module** from the project root directory:
    ```bash
    python -m chat_etl.orchestrator
    ```
    This will process files in `chatgpt-export-json/`, save parsed results to `output_json/`, and ingest/sync data into the `ChatConversations` table in DynamoDB Local. Re-running the script will apply the diff-and-delete logic for any changes found in the source files.

## 8. Running Tests

The project includes integration tests using `pytest` that verify the end-to-end ETL process, including the diff-and-delete logic.

1.  **Prerequisites:**
    - Ensure DynamoDB Local is running.
    - Ensure dependencies (including `pytest`) are installed (`pip install -r requirements.txt`).
2.  **Run Tests:**
    Activate your virtual environment and run `pytest` from the project root directory. Using `-sv` provides verbose output and shows print statements:
    ```bash
    python -m pytest -sv
    ```
    Pytest will automatically discover and run the tests in the `tests/` directory, including both integration and unit tests. The integration tests will create temporary tables in DynamoDB Local, run the workflow, verify results, and clean up the tables automatically.

## Security Note

The `.gitignore` file is configured to prevent accidental commits of the default input (`chatgpt-export-json/`) and output (`output_json/`) directories, which may contain sensitive conversation data. The sample data included in the `tests/test_data/` directory has been sanitized.

## Future Enhancements

- **Dockerization with Compose:** Containerize the Python application (`parser`, `ingestor`, etc.) and use `Docker Compose` to manage it alongside the already-used DynamoDB Local container. This provides a fully integrated development/testing environment started with a single command (`docker-compose up`) and simplifies potential deployment.
- **Configuration File:** Manage directory paths, table names, and AWS credentials/endpoints via a configuration file (e.g., YAML, `.env`).
- **Enhanced Error Handling:** More specific error catching and reporting throughout the pipeline.
- **Logging:** Implement structured logging instead of just using `print`.
- **Schema Validation:** Add validation for incoming JSON data and outgoing DynamoDB items.
- **Metadata Storage:** Store conversation metadata (title, create time, etc.) as a separate item type in DynamoDB (e.g., `ItemType` = `METADATA`).
- **CLI Arguments:** Add more command-line arguments (e.g., using `argparse`) to control directories, table names, etc., for `orchestrator.py`.
- **Unit Tests:** Add more granular unit tests for functions like `extract_canonical_messages` and the diff logic, potentially using mocking.
