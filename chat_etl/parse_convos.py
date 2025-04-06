import json
import os
import argparse
from collections import deque

# Known root node ID patterns or null parents that signify the start
ROOT_INDICATORS = {"client-created-root", None, ""}

def extract_canonical_messages(data):
    """Extracts the list of messages belonging to the canonical conversation thread."""
    if not isinstance(data, dict):
        print(f"Warning: Expected top-level data to be a dict, got {type(data)}.")
        return []

    mapping = data.get('mapping')
    current_node_id = data.get('current_node')

    if not mapping or not current_node_id:
        print("Warning: Missing 'mapping' or 'current_node' in conversation data.")
        return []

    canonical_messages = deque() # Use deque for efficient prepending

    while current_node_id not in ROOT_INDICATORS:
        node = mapping.get(current_node_id)
        if not node:
            print(f"Warning: Node ID '{current_node_id}' not found in mapping. Stopping traversal.")
            break # Node not found, break the loop

        message_data = node.get('message')
        # Process only nodes that have actual message data
        if message_data:
            author = message_data.get('author', {}).get('role')
            # Filter out non-user/assistant messages if desired (or system/tool messages)
            # Also check for hidden flags if needed: message_data.get('metadata', {}).get('is_visually_hidden_from_conversation')
            if author in {"user", "assistant"}:
                msg_id = message_data.get('id')
                timestamp = message_data.get('create_time') or message_data.get('update_time')
                parts = message_data.get('content', {}).get('parts', [])
                # Ensure parts contains strings before joining
                content = " ".join(part.strip() for part in parts if isinstance(part, str) and part and part.strip())

                # Add to list only if it has essential info
                if msg_id and timestamp is not None and content:
                    msg_obj = {
                        "MessageID": msg_id,
                        "Timestamp": timestamp,
                        "Author": author,
                        "Content": content
                    }
                    canonical_messages.appendleft(msg_obj) # Prepend to maintain order while traversing backwards

        # Move to the parent node
        current_node_id = node.get('parent')

    return list(canonical_messages) # Convert deque back to list


def parse_single_file(input_path, output_path):
    """Loads a single raw JSON file, parses the canonical thread, and saves the extracted messages."""
    try:
        print(f"Parsing: {input_path}")
        with open(input_path, 'r', encoding='utf-8') as infile:
            # Load the JSON. It might be a list or a dict.
            raw_data = json.load(infile)
            
            # Determine the main conversation data object
            if isinstance(raw_data, list):
                if len(raw_data) > 0 and isinstance(raw_data[0], dict):
                    conversation_data = raw_data[0]
                else:
                    print(f"  -> Error: Expected a list containing one conversation dict in {os.path.basename(input_path)}, but list was empty or first item wasn't a dict.")
                    return False
            elif isinstance(raw_data, dict):
                conversation_data = raw_data # It's directly the conversation object
            else:
                print(f"  -> Error: Unexpected top-level JSON type ({type(raw_data)}) in {os.path.basename(input_path)}.")
                return False

        extracted_messages = extract_canonical_messages(conversation_data)

        if not extracted_messages:
            print(f"  -> No valid canonical messages extracted from {os.path.basename(input_path)}.")
            # Decide if we should write an empty file or skip. Skipping avoids empty files.
            # If ingestor needs to know about empty parses, writing `[]` might be better.
            # Let's skip writing for now if empty.
            return True # Treat as success (processed, but nothing to output)

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as outfile:
            json.dump(extracted_messages, outfile, indent=2)
        print(f"  -> Saved {len(extracted_messages)} canonical messages to {output_path}")
        return True
    except json.JSONDecodeError:
        print(f"  -> Error: Invalid JSON in file {os.path.basename(input_path)}. Skipping.")
        return False
    except Exception as e:
        print(f"  -> Error processing file {os.path.basename(input_path)}: {e}. Skipping.")
        return False

def process_raw_directory(input_dir, output_dir):
    """Processes all JSON files in the input directory and saves parsed results to the output directory."""
    print(f"Starting parsing process from '{input_dir}' to '{output_dir}'")
    success_count = 0
    fail_count = 0

    if not os.path.isdir(input_dir):
        print(f"Error: Input directory '{input_dir}' not found.")
        return

    os.makedirs(output_dir, exist_ok=True) # Ensure output directory exists

    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            input_path = os.path.join(input_dir, filename)
            base_name = os.path.splitext(filename)[0]
            # Save parsed file with a distinct name in the output dir
            output_filename = f"{base_name}_parsed.json"
            output_path = os.path.join(output_dir, output_filename)

            if parse_single_file(input_path, output_path):
                success_count += 1
            else:
                fail_count += 1

    print(f"Parsing complete. Successfully processed (incl. empty): {success_count}, Failed: {fail_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse canonical threads from raw ChatGPT JSON export files.")
    parser.add_argument("--input-dir", required=True, help="Directory containing raw ChatGPT JSON files.")
    parser.add_argument("--output-dir", required=True, help="Directory to save the parsed JSON message lists.")
    args = parser.parse_args()

    process_raw_directory(args.input_dir, args.output_dir)

