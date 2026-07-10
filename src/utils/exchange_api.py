import httpx
import re
import logging
from typing import List, Dict, Any, Optional
logger = logging.getLogger("ExchangeClient")

SENTITEMS_FOLDER_ALIASES = {"已发送邮件", "已发送", "sent items", "sentitems", "sent"}
DRAFTS_FOLDER_ALIASES = {"草稿", "drafts", "draft"}


def _normalize_folder_name(name: str | None) -> str:
    return re.sub(r"[\s_-]+", "", (name or "").strip().casefold())

class ExchangeClient:
    """
    Exchange 接口客户端，封装 HTTP 调用逻辑。
    Uses a shared httpx.AsyncClient with connection pooling for performance.
    """
    def __init__(self, settings=None):
        if settings is None:
            from src.config import get_settings
            settings = get_settings()
            
        self.api_url = settings.EXCHANGE_API_URL.rstrip("/")
        from src.config import resolve_secret
        self.api_key = resolve_secret(settings.EXCHANGE_API_KEY)
        self.account_id = settings.EXCHANGE_ACCOUNT_ID
        self.ssl_verify = settings.EXCHANGE_SSL_VERIFY

        if not self.api_url:
            self.api_url = "http://localhost:8000/mock/exchange"

        self._http_client: httpx.AsyncClient | None = None

        # --- Folder cache for webhook routing ---
        self._folder_cache: dict | None = None
        self._folder_tree: dict | None = None
        self._folder_policies: dict | None = None
        self.sentitems_folder_id: str | None = None
        self.drafts_folder_id: str | None = None
        self._sentitems_name = getattr(settings, "EXCHANGE_FOLDER_SENTITEMS", "已发送邮件")
        self._drafts_name = getattr(settings, "EXCHANGE_FOLDER_DRAFTS", "草稿")

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazy-initialized shared HTTP client with connection pooling."""
        if self._http_client is None or self._http_client.is_closed:
            headers = {"X-API-KEY": self.api_key} if self.api_key else {}
            self._http_client = httpx.AsyncClient(
                verify=self.ssl_verify,
                headers=headers,
                timeout=httpx.Timeout(20.0, connect=10.0),
            )
        return self._http_client

    async def close(self):
        """Close the shared HTTP client."""
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
            self._http_client = None

    async def get_all_folders(self, force_refresh: bool = False) -> dict:
        """
        Fetch all folders and build in-memory cache/tree.

        Endpoint:
            GET {api_url}/folders/all?account_id=...
        """
        if self._folder_cache is not None and not force_refresh:
            return self._folder_cache

        base_url = re.sub(r"/emails/?$", "", self.api_url)
        endpoints = [
            f"{self.api_url}/folders/all",
            f"{base_url}/folders/all",
        ]
        params = {"account_id": self.account_id}

        client = self.http_client
        try:
            for endpoint in dict.fromkeys(endpoints):
                response = await client.get(
                    endpoint,
                    params=params,
                    timeout=15.0,
                )
                if response.status_code == 200:
                    folders = response.json().get("data", {}).get("folders", [])
                    self._build_folder_cache(folders)
                    logger.info(
                        "Folder cache loaded from %s: %s folders. sentitems=%s, drafts=%s",
                        endpoint,
                        len(self._folder_cache),
                        self.sentitems_folder_id,
                        self.drafts_folder_id,
                    )
                    return self._folder_cache
                logger.warning(
                    "Folder endpoint returned %s: %s",
                    response.status_code,
                    endpoint,
                )
            logger.warning(
                "Failed to get folders from all candidate endpoints. "
                "Routing will use safe fallback mode."
            )
        except Exception as e:
            logger.error("Exception getting folders: %s", e)

        self._folder_cache = {}
        self._folder_tree = {}
        return self._folder_cache

    def _build_folder_cache(self, folders: list) -> None:
        """Build folder_id->name mapping and parent-child tree."""
        self._folder_cache = {}
        self._folder_tree = {}
        self.sentitems_folder_id = None
        self.drafts_folder_id = None

        for folder in folders:
            folder_id = folder.get("id")
            folder_name = folder.get("name", "")
            parent_id = folder.get("parent_id")
            if not folder_id:
                continue

            self._folder_cache[folder_id] = folder_name
            self._folder_tree[folder_id] = {
                "name": folder_name,
                "parent_id": parent_id,
                "children": [],
                "folder_class": folder.get("folder_class", ""),
            }

            if self._is_sentitems_folder(folder_name):
                self.sentitems_folder_id = folder_id
            elif self._is_drafts_folder(folder_name):
                self.drafts_folder_id = folder_id

        for folder_id, node in self._folder_tree.items():
            parent_id = node["parent_id"]
            if parent_id and parent_id in self._folder_tree:
                self._folder_tree[parent_id]["children"].append(folder_id)

    def _is_sentitems_folder(self, folder_name: str) -> bool:
        aliases = {self._sentitems_name, *SENTITEMS_FOLDER_ALIASES}
        return _normalize_folder_name(folder_name) in {
            _normalize_folder_name(alias) for alias in aliases
        }

    def _is_drafts_folder(self, folder_name: str) -> bool:
        aliases = {self._drafts_name, *DRAFTS_FOLDER_ALIASES}
        return _normalize_folder_name(folder_name) in {
            _normalize_folder_name(alias) for alias in aliases
        }

    def compute_folder_policies(
        self,
        folders_full: set[str],
        folders_archive: set[str],
    ) -> dict[str, str]:
        """
        Compute per-folder policy with explicit override + ancestor inheritance.
        """
        if not self._folder_tree:
            return {}

        policies: dict[str, str] = {}

        def _ancestor_names(folder_id: str) -> list[str]:
            names = []
            current = folder_id
            visited = set()
            while current and current in self._folder_tree and current not in visited:
                visited.add(current)
                parent_id = self._folder_tree[current]["parent_id"]
                if parent_id and parent_id in self._folder_tree:
                    names.append(self._folder_tree[parent_id]["name"])
                current = parent_id
            return names

        for folder_id, node in self._folder_tree.items():
            name = node["name"]

            if name in folders_archive:
                policies[folder_id] = "archive"
                continue
            if name in folders_full:
                policies[folder_id] = "full"
                continue

            inherited = "ignore"
            for ancestor_name in _ancestor_names(folder_id):
                if ancestor_name in folders_full:
                    inherited = "full"
                    break
                if ancestor_name in folders_archive:
                    inherited = "archive"
                    break

            policies[folder_id] = inherited

        return policies

    def init_folder_policies(self, folders_full: set[str], folders_archive: set[str]) -> None:
        """Precompute and cache folder policy map."""
        self._folder_policies = self.compute_folder_policies(folders_full, folders_archive)
        full_count = sum(1 for v in self._folder_policies.values() if v == "full")
        archive_count = sum(1 for v in self._folder_policies.values() if v == "archive")
        logger.info(
            "Folder policies computed: %s full, %s archive, %s ignore",
            full_count,
            archive_count,
            len(self._folder_policies) - full_count - archive_count,
        )

    def get_folder_policy(self, folder_id: str | None) -> str:
        """Get precomputed policy for folder_id, fallback to ignore."""
        if not folder_id or not self._folder_policies:
            return "ignore"
        return self._folder_policies.get(folder_id, "ignore")

    def get_folder_name(self, folder_id: str | None) -> str | None:
        """Resolve folder name from cache by folder_id."""
        if not folder_id or not self._folder_cache:
            return None
        return self._folder_cache.get(folder_id)

    async def get_recent_emails(self, limit: int = 10, exclude_ids: List[str] = None) -> List[Dict[str, Any]]:
        """
        从接口获取未读邮件列表及其详情。
        """
        if exclude_ids is None:
            exclude_ids = []

        # 严格对齐示例代码的参数设置
        params = {
            "account_id": self.account_id,
            "folder": "INBOX",
            "limit": limit,
            "unread_only": "True"  # 尝试字符串 "True" 以匹配 requests 的行为
        }

        client = self.http_client
        try:
            # 1. 获取列表
            list_url = f"{self.api_url}/list"
            logger.info("正在拉取邮件列表: %s (params: %s)", list_url, params)

            response = await client.get(list_url, params=params, timeout=10.0)
            if response.status_code != 200:
                logger.error("列表获取失败: %s - %s", response.status_code, response.text)
                return []

            data = response.json()
            # 打印原始数据结构以供调试
            logger.info(
                "列表接口返回数据状态: %s, 消息: %s",
                data.get("code"),
                data.get("message"),
            )

            items = data.get("data", {}).get("items", [])
            if not items:
                logger.info("目前没有未读邮件。")

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
                    logger.info("正在请求详情: %s", detail_url)
                    detail_resp = await client.get(
                        detail_url,
                        params={"account_id": self.account_id},
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
                            logger.warning("邮件详情为空 (ID: %s)", email_id)
                    else:
                        logger.error(
                            "详情获取失败 (ID: %s): %s - %s",
                            email_id,
                            detail_resp.status_code,
                            detail_resp.text,
                        )
                except Exception as detail_err:
                    logger.error("请求详情异常 (ID: %s): %s", email_id, detail_err)

            if full_emails:
                logger.info("成功获取 %s 封邮件的完整详情", len(full_emails))
            return full_emails
        except Exception as e:
            logger.error("获取邮件异常: %s", e)
            return []

    async def send_email(self, to: str, subject: str, body: str) -> bool:
        """
        调用接口发送邮件。
        """
        return await self._send_payload(to, subject, body, is_draft=False)

    async def create_draft(self, to: List[str], subject: str, body: str, cc: List[str] = None) -> bool:
        """
        调用接口创建草稿。
        """
        endpoint = f"{self.api_url}/drafts"
        
        payload = {
            "account_id": self.account_id,
            "to": to,
            "cc": cc or [],
            "subject": subject,
            "body": body,
            "body_type": "html",
            "folder": "Drafts"
        }

        client = self.http_client
        try:
            logger.info("正在请求保存草稿接口: %s", endpoint)
            response = await client.post(
                endpoint,
                json=payload,
                timeout=10.0
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error("保存草稿失败: %s", e)
            if hasattr(e, 'response') and e.response:
                logger.error("Server response: %s", e.response.text)
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
        
        payload = {
            "account_id": self.account_id,
            "to": [clean_to],
            "subject": subject,
            "body": body
        }

        # Select endpoint based on action
        if is_draft:
            endpoint = f"{self.api_url}/drafts"
        else:
            endpoint = f"{self.api_url}/send"

        client = self.http_client
        try:
            action = "保存草稿" if is_draft else "发送邮件"
            logger.info("正在请求%s接口: %s", action, endpoint)
            response = await client.post(
                endpoint,
                json=payload,
                timeout=10.0
            )
            if response.status_code == 404 and is_draft:
                logger.warning("Draft endpoint 404. %s", response.text)
                return False

            response.raise_for_status()
            return True
        except Exception as e:
            logger.error("%s失败: %s", action, e)
            return False

    async def mark_as_read(self, email_id: str, is_read: bool = True) -> bool:
        """
        Mark an email as read/unread using the new API.
        """
        from urllib.parse import quote
        encoded_id = quote(email_id, safe='')
        endpoint = f"{self.api_url}/{encoded_id}/read"
        
        params = {
            "account_id": self.account_id,
            "is_read": is_read
        }

        client = self.http_client
        try:
            response = await client.put(endpoint, params=params, timeout=10.0)
            if response.status_code == 200:
                data = response.json()
                return data.get('code') == 200
            else:
                logger.warning(
                    "Mark as read failed (ID: %s): %s - %s",
                    email_id,
                    response.status_code,
                    response.text,
                )
                return False
        except Exception as e:
            logger.error("Failed to mark email %s as read: %s", email_id, e)
            return False

    async def move_email(self, email_id: str, folder_id: str) -> bool:
        """
        Move an email to a specific folder.
        """
        endpoint = f"{self.api_url}/{email_id}/move"
        payload = {"folder_id": folder_id}

        client = self.http_client
        try:
            response = await client.post(endpoint, json=payload, timeout=5.0)
            return response.status_code == 200
        except Exception as e:
            logger.error("Failed to move email %s to %s: %s", email_id, folder_id, e)
            return False

    async def delete_email(self, email_id: str) -> bool:
        """
        Delete an email involved in freeing up quota.
        """
        endpoint = f"{self.api_url}/{email_id}"

        client = self.http_client
        try:
            response = await client.delete(endpoint, timeout=5.0)
            return response.status_code == 200
        except Exception as e:
            logger.error("Failed to delete email %s: %s", email_id, e)
            return False

    async def get_email(self, email_id: str, account_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch full details for a specific email by ID.
        """
        from urllib.parse import quote
        encoded_id = quote(email_id, safe='')
        endpoint = f"{self.api_url}/{encoded_id}"
        target_account_id = account_id if account_id is not None else self.account_id

        client = self.http_client
        try:
            response = await client.get(
                endpoint,
                params={"account_id": target_account_id},
            )
            if response.status_code == 200:
                return response.json().get("data", {})
            else:
                logger.error(
                    "Failed to get email details for %s: %s",
                    email_id,
                    response.status_code,
                )
        except Exception as e:
            logger.error("Exception getting email %s: %s", email_id, e)
        return {}

    async def reply_email(self, email_id: str, body: str, to: List[str] = None, cc: List[str] = None) -> bool:
        """
        New Interface: Reply to an existing email.
        """
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

        client = self.http_client
        try:
            logger.info("正在请求回复接口: %s", endpoint)
            response = await client.post(endpoint, json=payload, timeout=15.0)
            if response.status_code == 200:
                return response.json().get("code") == 200
            else:
                logger.error("Reply failed: %s - %s", response.status_code, response.text)
                return False
        except Exception as e:
            logger.error("Reply exception: %s", e)
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
        # Derive contacts endpoint from emails endpoint
        # e.g. https://host/api/v1/exchange/emails -> https://host/api/v1/exchange/contacts/resolve
        import re as _re
        base_url = _re.sub(r'/emails/?$', '', self.api_url)
        endpoint = f"{base_url}/contacts/resolve"
        
        params = {
            "q": query,
            "account_id": self.account_id
        }

        client = self.http_client
        try:
            response = await client.get(
                endpoint,
                params=params,
                timeout=10.0
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    return data["data"][0].get("name")
            else:
                logger.warning(
                    "Contact resolve failed for '%s': %s",
                    query,
                    response.status_code,
                )
        except Exception as e:
            logger.error("Contact resolve exception for '%s': %s", query, e)
        
        return None

    async def forward_email(self, email_id: str, to: List[str], body: str) -> bool:
        """
        New Interface: Forward an existing email.
        """
        endpoint = f"{self.api_url}/forward"
        
        payload = {
            "account_id": self.account_id,
            "reference_item_id": email_id,
            "to": to,
            "body": body,
            "body_type": "html"
        }

        client = self.http_client
        try:
            logger.info("正在请求转发接口: %s", endpoint)
            response = await client.post(endpoint, json=payload, timeout=15.0)
            if response.status_code == 200:
                return response.json().get("code") == 200
            else:
                logger.error("Forward failed: %s - %s", response.status_code, response.text)
                return False
        except Exception as e:
            logger.error("Forward exception: %s", e)
            return False
