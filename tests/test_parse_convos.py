# Unit tests for parse_convos.py

import pytest
# Use absolute import from package
from chat_etl.parse_convos import extract_canonical_messages, ROOT_INDICATORS

# --- Test Cases for extract_canonical_messages ---

def test_empty_input():
    """Test with empty or invalid input data."""
    assert extract_canonical_messages(None) == []
    assert extract_canonical_messages({}) == []
    assert extract_canonical_messages({"mapping": {}, "current_node": None}) == []
    assert extract_canonical_messages({"mapping": {"a": {}}, "current_node": "a"}) == [] # Missing parent
    assert extract_canonical_messages({"mapping": {"a": {"parent": None}}, "current_node": "b"}) == [] # Current node not in mapping
    assert extract_canonical_messages("Not a dict") == []
    assert extract_canonical_messages(["Not a dict"]) == [] # Test case from parse_single_file logic

def test_simple_linear_conversation():
    """Test a basic linear conversation without edits."""
    data = {
        "current_node": "node3",
        "mapping": {
            "node1": {
                "id": "node1",
                "message": {
                    "id": "msg1", "author": {"role": "user"}, "create_time": 1, 
                    "content": {"parts": ["Hello"]}
                },
                "parent": None, # Root node
                "children": ["node2"]
            },
            "node2": {
                "id": "node2",
                "message": {
                    "id": "msg2", "author": {"role": "assistant"}, "create_time": 2,
                    "content": {"parts": ["Hi there!"]}
                },
                "parent": "node1",
                "children": ["node3"]
            },
            "node3": {
                "id": "node3",
                "message": {
                    "id": "msg3", "author": {"role": "user"}, "create_time": 3,
                    "content": {"parts": ["How are you?"]}
                },
                "parent": "node2",
                "children": []
            }
        }
    }
    expected = [
        {"MessageID": "msg1", "Timestamp": 1, "Author": "user", "Content": "Hello"},
        {"MessageID": "msg2", "Timestamp": 2, "Author": "assistant", "Content": "Hi there!"},
        {"MessageID": "msg3", "Timestamp": 3, "Author": "user", "Content": "How are you?"}
    ]
    assert extract_canonical_messages(data) == expected

def test_conversation_with_edit():
    """Test a conversation where an assistant message was edited."""
    data = {
        # current_node points to the *final* user message after the *edited* assistant response
        "current_node": "node4", 
        "mapping": {
            "node1": {
                "id": "node1",
                "message": {
                    "id": "msg1", "author": {"role": "user"}, "create_time": 1, 
                    "content": {"parts": ["Make a rhyme"]}
                },
                "parent": "", # Root node (empty string)
                "children": ["node2_orig"]
            },
            "node2_orig": { # Original assistant response (not on canonical path)
                "id": "node2_orig", 
                "message": {"id": "msg2_orig", "author": {"role": "assistant"}, "create_time": 2, "content": {"parts": ["cat hat"]}},
                "parent": "node1",
                "children": [] # Orphaned by edit
            },
            "node3_edited": { # Edited assistant response (on canonical path)
                "id": "node3_edited", 
                "message": {"id": "msg3_edited", "author": {"role": "assistant"}, "create_time": 3, "content": {"parts": ["dog log frog"]}},
                "parent": "node1", # Parent is still the original user message
                "children": ["node4"]
            },
            "node4": { # Final user response
                "id": "node4",
                "message": {"id": "msg4", "author": {"role": "user"}, "create_time": 4, "content": {"parts": ["Good one!"]}},
                "parent": "node3_edited", 
                "children": []
            }
        }
    }
    expected = [
        {"MessageID": "msg1", "Timestamp": 1, "Author": "user", "Content": "Make a rhyme"},
        # Should include the EDITED assistant message, not the original
        {"MessageID": "msg3_edited", "Timestamp": 3, "Author": "assistant", "Content": "dog log frog"},
        {"MessageID": "msg4", "Timestamp": 4, "Author": "user", "Content": "Good one!"}
    ]
    actual = extract_canonical_messages(data)
    print("Actual output:", actual) # Debug print
    assert actual == expected

def test_skips_messages_without_content():
    """Test that messages missing essential parts (like content) are skipped."""
    data = {
        "current_node": "node3",
        "mapping": {
            "node1": {
                "id": "node1",
                "message": {"id": "msg1", "author": {"role": "user"}, "create_time": 1, "content": {"parts": ["User message"]}},
                "parent": "client-created-root", # Root node type
                "children": ["node2"]
            },
            "node2": { # Simulate a system message or one without standard content
                "id": "node2",
                "message": {"id": "msg2_sys", "author": {"role": "tool"}, "create_time": 2, "content": {"content_type": "system_message"}}, # No 'parts'
                "parent": "node1",
                "children": ["node3"]
            },
            "node3": {
                "id": "node3",
                "message": {"id": "msg3", "author": {"role": "assistant"}, "create_time": 3, "content": {"parts": ["Assistant response"]}},
                "parent": "node2",
                "children": []
            }
        }
    }
    expected = [
        {"MessageID": "msg1", "Timestamp": 1, "Author": "user", "Content": "User message"},
        # node2 message should be skipped
        {"MessageID": "msg3", "Timestamp": 3, "Author": "assistant", "Content": "Assistant response"}
    ]
    assert extract_canonical_messages(data) == expected

def test_handles_missing_timestamps():
    """Test if create_time is missing but update_time exists."""
    data = {
        "current_node": "node2",
        "mapping": {
            "node1": {
                "id": "node1",
                "message": {"id": "msg1", "author": {"role": "user"}, "update_time": 1.5, "content": {"parts": ["Hi"]}},
                "parent": None,
                "children": ["node2"]
            },
            "node2": {
                "id": "node2",
                "message": {"id": "msg2", "author": {"role": "assistant"}, "create_time": 2, "content": {"parts": ["Hello"]}},
                "parent": "node1",
                "children": []
            }
        }
    }
    expected = [
        {"MessageID": "msg1", "Timestamp": 1.5, "Author": "user", "Content": "Hi"},
        {"MessageID": "msg2", "Timestamp": 2, "Author": "assistant", "Content": "Hello"}
    ]
    assert extract_canonical_messages(data) == expected

# TODO: Add test cases for extract_canonical_messages 