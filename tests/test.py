import requests
import json
import os

def get_bearer_token():
    """Get your bearer token from the browser."""
    # You need to get this manually from browser dev tools
    return "eyJhbGciOiJSUzI1NiIsImtpZCI6IjE5MzQ0ZTY1LWJiYzktNDRkMS1hOWQwLWY5NTdiMDc5YmQwZSIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS92MSJdLCJhenAiOiJUZEpJY2JlMTZXb1RIdE45NW55eXdoNUU0eU9vNkl0RyIsImNsaWVudF9pZCI6ImFwcF9YOHpZNnZXMnBROXRSM2RFN25LMWpMNWdIIiwiZXhwIjoxNzQ0NDcyMTgzLCJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsicG9pZCI6Im9yZy1SUkpVcTRlV05BNHM0ZGRRaDZxSEU0MlYiLCJ1c2VyX2lkIjoidXNlci01TTJtcUlET1R0MHN6d2xnTGZrcHZ1eWMifSwiaHR0cHM6Ly9hcGkub3BlbmFpLmNvbS9wcm9maWxlIjp7ImVtYWlsIjoicm9iYmllaHVtZTRAZ21haWwuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWV9LCJpYXQiOjE3NDM2MDgxODMsImlzcyI6Imh0dHBzOi8vYXV0aC5vcGVuYWkuY29tIiwianRpIjoiYzRhZmIyOWQtMTYyYi00OGMwLTgxMDQtZmRjMGVmMzUxZjgxIiwibmJmIjoxNzQzNjA4MTgzLCJwd2RfYXV0aF90aW1lIjoxNzQzNjA4MTgyNTE1LCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIHBob25lIGVtYWlsX2NvZGVfdmVyaWZpY2F0aW9uIiwic2NwIjpbIm9wZW5pZCIsImVtYWlsIiwicHJvZmlsZSIsIm9mZmxpbmVfYWNjZXNzIiwibW9kZWwucmVxdWVzdCIsIm1vZGVsLnJlYWQiLCJvcmdhbml6YXRpb24ucmVhZCIsIm9yZ2FuaXphdGlvbi53cml0ZSJdLCJzZXNzaW9uX2lkIjoiYXV0aHNlc3NfVnRjemNFUm9KdVNkckdOOGllSm5UdGZjIiwic3ViIjoiYXV0aDB8NjBkYTJhNWJhN2IzMDMwMDcwMDg2NWZkIn0.BThZKPtLYJ4UjW66GXtSd2didT4o2J2Mzm0nfrOpNgV8lag8l3b4eWa2nnJ3UpZhzyaLo49BgQ_eM6Ihg0NS4iVnzwkfXkdtW6kBQXwVYtjA7LRucCWlL31HxiqtWnNKJ7Dekul1rz8bbhyxhXkNOVOD76xNcG5iFwe1b6Nxawd_5E5kkWmmuN65B2iWnZWQZ9fxVwS8wd-gatXgRXcs_7NP42-W1PftGO97XjmCPtSLYzfwrHomk_M3WXe8lzEu0dRNOtJf_FwJzBn3nEjUn2XO0Ampu2btPwHMzfw7vULoqVCF9ibQYhEkt4Xqxy4MixNZbLgocoZBDsARB0QKbF6V6iSgcDMOv2kDdGTu4afFKazfvGVOe-9s9hikjUhtrQzVVzI2u6vrCEkdn2NKjKbSJsce2LEzxh2xHnHOYj__T_KQCcsmz4v8McwTUzaAAPxRAb-oV34JJJqujcxvGaEkjEB_IoG39_mEDzz0mV0w8md7mQF6jmBVnX_kA9ebPy61IfnYe3fxZQCQJknHJJEtD9y0hEXBMeS2JQTyosr_FD8mVCSS2kdEx0WecAKTXUlKt2cv2V3QWlaOpCBFluzHnl0FnneD4zqFnTe8-ZSKWnuyFtZPGb3QlHd79poy4xUahyWLXT3e-LgIQe-8lPljOkPEo0uvRb_deV2cMf4"

def fetch_conversations(offset=0, limit=100):
    """Fetch conversations from ChatGPT backend API."""
    url = f"https://chat.openai.com/backend-api/conversations?offset={offset}&limit={limit}"
    headers = {
        "Authorization": f"Bearer {get_bearer_token()}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def fetch_conversation_details(conversation_id):
    """Fetch details for a specific conversation."""
    url = f"https://chat.openai.com/backend-api/conversation/{conversation_id}"
    headers = {
        "Authorization": f"Bearer {get_bearer_token()}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

def save_all_conversations(output_dir="./chatgpt_conversations"):
    """Save all conversations to individual JSON files."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Get conversation list
    conversations = fetch_conversations()
    if not conversations:
        return
    
    # Fetch and save each conversation
    for convo in conversations["items"]:
        convo_id = convo["id"]
        convo_title = convo["title"]
        
        # Get full conversation details
        conversation = fetch_conversation_details(convo_id)
        if conversation:
            # Save to file
            filename = os.path.join(output_dir, f"{convo_id}.json")
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(conversation, f, indent=2)
            print(f"Saved conversation: {convo_title}")

if __name__ == "__main__":
    save_all_conversations()

