import httpx
import os
import re
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class ExchangeClient:
    """
    Exchange 接口客户端，封装 HTTP 调用逻辑。
    """
    def __init__(self, settings=None):
        if settings is None:
            from src.config import get_settings
            settings = get_settings()
            
        self.api_url = settings.EXCHANGE_API_URL.rstrip("/")
        self.api_key = settings.EXCHANGE_API_KEY
        self.account_id = settings.EXCHANGE_ACCOUNT_ID
        self.ssl_verify = settings.EXCHANGE_SSL_VERIFY

        if not self.api_url:
            self.api_url = "http://localhost:8000/mock/exchange"

    async def get_recent_emails(self, limit: int = 10, exclude_ids: List[str] = None) -> List[Dict[str, Any]]:
        """
        从接口获取未读邮件列表及其详情。
        """
        if exclude_ids is None:
            exclude_ids = []
            
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}

        # 严格对齐示例代码的参数设置
        params = {
            "account_id": self.account_id,
            "folder": "INBOX",
            "limit": limit,
            "unread_only": "True"  # 尝试字符串 "True" 以匹配 requests 的行为
        }

        # Use configured SSL verification
        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                # 1. 获取列表
                list_url = f"{self.api_url}/list"
                print(f"正在拉取邮件列表: {list_url} (params: {params})")

                response = await client.get(list_url, params=params, headers=headers, timeout=10.0)
                if response.status_code != 200:
                    print(f"列表获取失败: {response.status_code} - {response.text}")
                    return []

                data = response.json()
                # 打印原始数据结构以供调试
                print(f"列表接口返回数据状态: {data.get('code')}, 消息: {data.get('message')}")

                items = data.get("data", {}).get("items", [])
                if not items:
                    print("目前没有未读邮件。")

                full_emails = []
                from urllib.parse import quote
                # 2. 获取每封邮件的详情
                for item in items:
                    email_id = item.get("id")
                    
                    if email_id in exclude_ids:
                        # Skip if already processed in this session
                        continue
                    
                    # 优化：如果列表接口已经返回了 body，则直接使用，不再请求详情
                    # FIX: 列表返回的 body 可能不完整，强制获取详情
                    # if item.get("body"):
                    #      print(f"列表已包含邮件内容，跳过详情请求 (ID: {email_id})")
                    #      full_emails.append(item)
                    #      continue

                    # URL encode the ID to handle special characters like '/'
                    encoded_id = quote(email_id, safe='')
                    detail_url = f"{self.api_url}/{encoded_id}"

                    try:
                        print(f"正在请求详情: {detail_url}")
                        detail_resp = await client.get(
                            detail_url,
                            params={"account_id": self.account_id},
                            headers=headers,
                            timeout=10.0
                        )
                        if detail_resp.status_code == 200:
                            detail_data = detail_resp.json().get("data", {})
                            if detail_data:
                                # 确保包含 ID，以便后续跟踪
                                if "id" not in detail_data:
                                    detail_data["id"] = email_id
                                full_emails.append(detail_data)
                            else:
                                print(f"警告: 邮件详情为空 (ID: {email_id})")
                        else:
                            print(f"详情获取失败 (ID: {email_id}): {detail_resp.status_code} - {detail_resp.text}")
                    except Exception as detail_err:
                        print(f"请求详情异常 (ID: {email_id}): {detail_err}")

                if full_emails:
                    print(f"成功获取 {len(full_emails)} 封邮件的完整详情")
                return full_emails
            except Exception as e:
                print(f"获取邮件异常: {e}")
                return []

    async def sync_emails(self, sync_state: str = None, folder: str = "INBOX", limit: int = 50) -> Dict[str, Any]:
        """
        Sync emails using the incremental sync API.
        Returns a dict containing 'sync_state' and 'items'.
        """
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        endpoint = f"{self.api_url}/sync"
        
        payload = {
            "account_id": self.account_id,
            "folder": folder,
            "limit": limit,
            "sync_state": sync_state
        }

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                # print(f"Syncing emails with state: {sync_state}")
                response = await client.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=30.0 
                )
                
                if response.status_code == 200:
                    print("DEBUG: Sync request successful (200 OK). Starting response.json()...")
                    try:
                        data = response.json()
                        print("DEBUG: response.json() successful.")
                    except Exception as json_err:
                        print(f"DEBUG: response.json() failed: {json_err}")
                        return {}
                    
                    if data.get('code') == 200:
                        sync_data = data.get('data', {})
                        print(f"DEBUG: Sync data received. Sync state present: {'sync_state' in sync_data}, Items count: {len(sync_data.get('items', [])) if sync_data else 'N/A'}")
                        return sync_data
                    else:
                        print(f"Sync API failed: {data.get('msg')}")
                else:
                    print(f"Sync request failed: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"Sync exception: {e}")
        
        return {}

    async def send_email(self, to: str, subject: str, body: str) -> bool:
        """
        调用接口发送邮件。
        """
        return await self._send_payload(to, subject, body, is_draft=False)

    async def create_draft(self, to: str, subject: str, body: str) -> bool:
        """
        调用接口创建草稿。
        Ref: User provided specific implementation for /emails/drafts
        """
        # Clean the 'to' address
        clean_to = to
        if "email_address=" in to:
            match = re.search(r"email_address='([^']*)'", to)
            if match:
                clean_to = match.group(1)

        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        # Assuming api_url is .../exchange/emails, and new endpoint is .../exchange/emails/drafts
        endpoint = f"{self.api_url}/drafts"
        
        payload = {
            "account_id": self.account_id,
            "to": [clean_to],
            "subject": subject,
            "body": body,
            "body_type": "html",
            "folder": "Drafts"
        }

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                print(f"正在请求保存草稿接口: {endpoint}")
                response = await client.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=10.0
                )
                response.raise_for_status()
                # Optional: log result
                # print(f"Draft saved response: {response.json()}")
                return True
            except Exception as e:
                print(f"保存草稿失败: {e}")
                if hasattr(e, 'response') and e.response:
                    print(f"Server response: {e.response.text}")
                return False

    async def _send_payload(self, to: str, subject: str, body: str, is_draft: bool = False) -> bool:
        """
        Internal: Send email or draft payload. 
        Assuming server supports 'save_only' or similar flag, otherwise directing to /drafts endpoint if exists.
        For now, I will use a hypothetical '/drafts' endpoint for drafts, and '/send' for sending.
        """
        # Clean the 'to' address
        clean_to = to
        if "email_address=" in to:
            match = re.search(r"email_address='([^']*)'", to)
            if match:
                clean_to = match.group(1)
        
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        payload = {
            "account_id": self.account_id,
            "to": [clean_to],
            "subject": subject,
            "body": body
        }

        # Select endpoint based on action
        if is_draft:
            # Assuming a CREATE endpoint for drafts exists or using a query param on send.
            # Let's try a standard REST pattern for creating resources in a 'drafts' collection if possible,
            # OR just assuming the server has a create_draft capability.
            # Given the user context provided no API docs, I'll try generating a request to `/drafts` 
            # If that 404s, we might need to adjust.
            endpoint = f"{self.api_url}/drafts"
        else:
            endpoint = f"{self.api_url}/send"

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                action = "保存草稿" if is_draft else "发送邮件"
                print(f"正在请求{action}接口: {endpoint}")
                response = await client.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=10.0
                )
                if response.status_code == 404 and is_draft:
                    # Fallback: maybe specific param on send?
                    # For now, let's just log and fail gracefully or try 'save=true' on send endpoint if known.
                    # But distinct endpoint is cleaner design to assume first.
                    print(f"Draft endpoint 404. {response.text}")
                    return False
                    
                response.raise_for_status()
                return True
            except Exception as e:
                print(f"{action}失败: {e}")
                return False

    async def mark_as_read(self, email_id: str, is_read: bool = True) -> bool:
        """
        Mark an email as read/unread using the new API.
        """
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        # Make sure to URL encode the ID if needed, though requests/httpx usually handle params well,
        # but the ID is in the path here.
        from urllib.parse import quote
        encoded_id = quote(email_id, safe='')
        endpoint = f"{self.api_url}/{encoded_id}/read"
        
        # Based on test_prod_sync.py, it uses PUT with query params
        params = {
            "account_id": self.account_id,
            "is_read": is_read
        }

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                response = await client.put(endpoint, params=params, headers=headers, timeout=10.0)
                if response.status_code == 200:
                    data = response.json()
                    return data.get('code') == 200
                else:
                    # Log as warning rather than error to avoid panic if object ID is stale
                    print(f"WARNING: Mark as read failed (ID: {email_id}): {response.status_code} - {response.text}")
                    return False
            except Exception as e:
                print(f"Failed to mark email {email_id} as read: {e}")
                return False

    async def move_email(self, email_id: str, folder_id: str) -> bool:
        """
        Move an email to a specific folder.
        """
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        endpoint = f"{self.api_url}/{email_id}/move"
        payload = {"folder_id": folder_id}

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                response = await client.post(endpoint, json=payload, headers=headers, timeout=5.0)
                return response.status_code == 200
            except Exception as e:
                print(f"Failed to move email {email_id} to {folder_id}: {e}")
                return False

    async def delete_email(self, email_id: str) -> bool:
        """
        Delete an email involved in freeing up quota.
        """
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        endpoint = f"{self.api_url}/{email_id}"
        
        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                response = await client.delete(endpoint, headers=headers, timeout=5.0)
                return response.status_code == 200
            except Exception as e:
                print(f"Failed to delete email {email_id}: {e}")
                return False

    async def get_email(self, email_id: str, account_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch full details for a specific email by ID.
        """
        from urllib.parse import quote
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        encoded_id = quote(email_id, safe='')
        endpoint = f"{self.api_url}/{encoded_id}"
        target_account_id = account_id if account_id is not None else self.account_id
        
        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                response = await client.get(
                    endpoint, 
                    params={"account_id": target_account_id},
                    headers=headers, 
                    timeout=20.0
                )
                if response.status_code == 200:
                    return response.json().get("data", {})
                else:
                    print(f"Failed to get email details for {email_id}: {response.status_code}")
            except Exception as e:
                print(f"Exception getting email {email_id}: {e}")
        return {}

    async def reply_email(self, email_id: str, body: str, to: List[str] = None, cc: List[str] = None) -> bool:
        """
        New Interface: Reply to an existing email.
        """
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        endpoint = f"{self.api_url}/reply"
        
        payload = {
            "account_id": self.account_id,
            "reference_item_id": email_id,
            "body": body,
            "body_type": "html"
        }
        if to:
            payload["to"] = to
        if cc:
            payload["cc"] = cc

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                print(f"正在请求回复接口: {endpoint}")
                response = await client.post(endpoint, json=payload, headers=headers, timeout=15.0)
                if response.status_code == 200:
                    return response.json().get("code") == 200
                else:
                    print(f"Reply failed: {response.status_code} - {response.text}")
                    return False
            except Exception as e:
                print(f"Reply exception: {e}")
                return False

    async def resolve_contact(self, query: str) -> Optional[str]:
        """
        通过 Exchange 通讯录查询联系人名称。
        优先搜索个人通讯录，未找到则回退到全局地址列表 (GAL)。
        
        Args:
            query: 查询关键词（邮箱地址、姓名或别名）
            
        Returns:
            联系人显示名称，未找到则返回 None
        """
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        
        # Derive contacts endpoint from emails endpoint
        # e.g. https://host/api/v1/exchange/emails -> https://host/api/v1/exchange/contacts/resolve
        import re as _re
        base_url = _re.sub(r'/emails/?$', '', self.api_url)
        endpoint = f"{base_url}/contacts/resolve"
        
        params = {
            "q": query,
            "account_id": self.account_id
        }

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                response = await client.get(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=10.0
                )
                if response.status_code == 200:
                    data = response.json()
                    if data.get("success") and data.get("data"):
                        # Return the first match's name
                        return data["data"][0].get("name")
                else:
                    print(f"Contact resolve failed for '{query}': {response.status_code}")
            except Exception as e:
                print(f"Contact resolve exception for '{query}': {e}")
        
        return None

    async def forward_email(self, email_id: str, to: List[str], body: str) -> bool:
        """
        New Interface: Forward an existing email.
        """
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        endpoint = f"{self.api_url}/forward"
        
        payload = {
            "account_id": self.account_id,
            "reference_item_id": email_id,
            "to": to,
            "body": body,
            "body_type": "html"
        }

        async with httpx.AsyncClient(verify=self.ssl_verify) as client:
            try:
                print(f"正在请求转发接口: {endpoint}")
                response = await client.post(endpoint, json=payload, headers=headers, timeout=15.0)
                if response.status_code == 200:
                    return response.json().get("code") == 200
                else:
                    print(f"Forward failed: {response.status_code} - {response.text}")
                    return False
            except Exception as e:
                print(f"Forward exception: {e}")
                return False
