
import requests
import os
import urllib3
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

API_URL = os.getenv("EXCHANGE_API_URL", "").rstrip("/")
API_KEY = os.getenv("EXCHANGE_API_KEY")
ACCOUNT_ID = os.getenv("EXCHANGE_ACCOUNT_ID", 1)

headers = {"X-API-KEY": API_KEY}

print(f"Testing Exchange API at: {API_URL}")
print(f"Account ID: {ACCOUNT_ID}")

def test_list_and_detail():
    print("\n--- Testing List & Detail ---")
    # 获取收件箱邮件列表
    params = {
        "account_id": ACCOUNT_ID,
        "folder": "INBOX",
        "limit": 5, 
        "unread_only": True
    }
    
    try:
        list_url = f"{API_URL}/list"
        print(f"Requesting List: {list_url}")
        response = requests.get(list_url, params=params, headers=headers, verify=False)
        
        if response.status_code != 200:
            print(f"List failed: {response.status_code} - {response.text}")
            return

        data = response.json()
        emails = data.get("data", {}).get("items", [])
        
        if not emails:
            print("No emails found in INBOX.")
            return

        print(f"Found {len(emails)} emails.")
        for email in emails:
            print(f"[{email.get('id')}] {email.get('subject')} - {email.get('sender')}")
            # Check if body is significantly present in list
            if email.get("body"):
                print("  (Body present in list response)")

        # 获取邮件详情 (Test first one)
        if emails:
            email_id = emails[0]["id"]
            print(f"\nFetching detail for ID: {email_id}")
            
            # 1. Try RAW ID (User snippet style)
            detail_url = f"{API_URL}/{email_id}"
            print(f"Detail URL (Raw): {detail_url}")
            
            detail_resp = requests.get(
                detail_url,
                params={"account_id": ACCOUNT_ID},
                headers=headers,
                verify=False
            )
            
            if detail_resp.status_code == 200:
                detail = detail_resp.json()
                body = detail.get("data", {}).get("body", "")
                print("Success with RAW ID! Body preview:")
                print(body[:100] + "..." if body else "(Empty body)")
            else:
                print(f"Detail (Raw) failed: {detail_resp.status_code}")
                # print(detail_resp.text) # Reduce noise
                
                # 2. Try Encoded ID
                if '/' in email_id or '+' in email_id:
                    print("Note: ID contains special characters. Trying percent-encoding...")
                    from urllib.parse import quote
                    encoded_id = quote(email_id, safe='')
                    detail_url_enc = f"{API_URL}/{encoded_id}"
                    print(f"Encoded URL: {detail_url_enc}")
                    detail_resp_enc = requests.get(
                        detail_url_enc,
                        params={"account_id": ACCOUNT_ID},
                        headers=headers,
                        verify=False
                    )
                    if detail_resp_enc.status_code == 200:
                         print("Success with ENCODED ID!")
                         body = detail_resp_enc.json().get("data", {}).get("body", "")
                         print(body[:100] + "..." if body else "(Empty body)")
                    else:
                         print(f"Encoded detail also failed: {detail_resp_enc.status_code}")

    except Exception as e:
        print(f"List/Detail Exception: {e}")

def test_search():
    print("\n--- Testing Search ---")
    search_url = f"{API_URL}/search"
    
    # 搜索最近7天包含"报告"的邮件
    data = {
        "account_id": ACCOUNT_ID,
        "query": "报告",
        "folder": "INBOX",
        "date_from": (datetime.now() - timedelta(days=7)).isoformat(),
        "limit": 10
    }
    
    # User snippet had 'headers' including Content-Type
    headers_post = headers.copy()
    headers_post["Content-Type"] = "application/json"

    try:
        print(f"Requesting Search: {search_url}")
        response = requests.post(search_url, json=data, headers=headers_post, verify=False)
        
        if response.status_code != 200:
            print(f"Search failed: {response.status_code} - {response.text}")
            return

        results = response.json().get("data", {}).get("items", [])
        print(f"Found {len(results)} search results.")
        for email in results:
            print(f"{email.get('received_time')} - {email.get('subject')}")

    except Exception as e:
        print(f"Search Exception: {e}")

if __name__ == "__main__":
    test_list_and_detail()
    test_search()
