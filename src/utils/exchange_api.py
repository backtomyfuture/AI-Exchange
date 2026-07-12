import httpx
import re
import logging
from typing import List, Dict, Any, Optional
from src.safety.http_response import read_json_limited
from src.safety.input_limits import input_limits_from_settings
from src.security.redaction import fingerprint_identifier
logger = logging.getLogger("ExchangeClient")

SENTITEMS_FOLDER_ALIASES = {"已发送邮件", "已发送", "sent items", "sentitems", "sent"}
DRAFTS_FOLDER_ALIASES = {"草稿", "drafts", "draft"}


def _normalize_folder_name(name: str | None) -> str:
    return re.sub(r"[\s_-]+", "", (name or "").strip().casefold())


def _resolve_system_folder(
    folders: list[tuple[str, str]],
    *,
    configured_name: str,
    aliases: set[str],
) -> tuple[str | None, frozenset[str]]:
    """Resolve one system folder without depending on response order."""
    configured_normalized = _normalize_folder_name(configured_name)
    configured_matches = {
        folder_id
        for folder_id, folder_name in folders
        if _normalize_folder_name(folder_name) == configured_normalized
    }
    if len(configured_matches) > 1:
        return None, frozenset(configured_matches)
    if len(configured_matches) == 1:
        return next(iter(configured_matches)), frozenset(configured_matches)

    normalized_aliases = {_normalize_folder_name(alias) for alias in aliases}
    alias_matches = {
        folder_id
        for folder_id, folder_name in folders
        if _normalize_folder_name(folder_name) in normalized_aliases
    }
    if len(alias_matches) == 1:
        return next(iter(alias_matches)), frozenset(alias_matches)
    return None, frozenset(alias_matches)

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
        ssl_verify = bool(settings.EXCHANGE_SSL_VERIFY)
        ca_file = str(getattr(settings, "EXCHANGE_CA_FILE", "") or "").strip()
        self.ssl_verify: bool | str = ca_file if ssl_verify and ca_file else ssl_verify
        self._input_limits = input_limits_from_settings(settings)

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
                        "Folder cache loaded: count=%d sentitems=%s drafts=%s",
                        len(self._folder_cache),
                        bool(self.sentitems_folder_id),
                        bool(self.drafts_folder_id),
                    )
                    return self._folder_cache
                logger.warning(
                    "Folder endpoint returned non-success: status=%s",
                    response.status_code,
                )
            logger.warning(
                "Failed to get folders from all candidate endpoints. "
                "Routing will use safe fallback mode."
            )
        except Exception as exc:
            logger.error(
                "Exception getting folders: error_type=%s",
                type(exc).__name__,
            )

        self._folder_cache = {}
        self._folder_tree = {}
        return self._folder_cache

    def _build_folder_cache(self, folders: list) -> None:
        """Build folder_id->name mapping and parent-child tree."""
        self._folder_cache = {}
        self._folder_tree = {}
        self.sentitems_folder_id = None
        self.drafts_folder_id = None
        system_folder_candidates: list[tuple[str, str]] = []

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
            system_folder_candidates.append((folder_id, folder_name))

        self.sentitems_folder_id, sentitems_candidates = _resolve_system_folder(
            system_folder_candidates,
            configured_name=self._sentitems_name,
            aliases=SENTITEMS_FOLDER_ALIASES,
        )
        self.drafts_folder_id, drafts_candidates = _resolve_system_folder(
            system_folder_candidates,
            configured_name=self._drafts_name,
            aliases=DRAFTS_FOLDER_ALIASES,
        )
        if sentitems_candidates & drafts_candidates:
            self.sentitems_folder_id = None
            self.drafts_folder_id = None

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
            logger.info("Fetching Exchange email list")

            async with client.stream(
                "GET",
                list_url,
                params=params,
                timeout=10.0,
            ) as response:
                if response.status_code != 200:
                    logger.error("列表获取失败: status=%s", response.status_code)
                    return []
                data = await read_json_limited(
                    response,
                    max_bytes=self._input_limits.exchange_response_bytes,
                )
            # 打印原始数据结构以供调试
            logger.info("Exchange email list returned: code=%s", data.get("code"))

            list_data = data.get("data")
            if not isinstance(list_data, dict):
                logger.warning("列表接口 data 字段不是对象")
                return []
            items = list_data.get("items", [])
            if not isinstance(items, list):
                logger.warning("列表接口 items 字段不是数组")
                return []
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
                    logger.info(
                        "Fetching Exchange email detail: email=%s",
                        fingerprint_identifier(email_id, namespace="email"),
                    )
                    async with client.stream(
                        "GET",
                        detail_url,
                        params={"account_id": self.account_id},
                        timeout=10.0,
                    ) as detail_resp:
                        if detail_resp.status_code == 200:
                            detail_payload = await read_json_limited(
                                detail_resp,
                                max_bytes=self._input_limits.exchange_response_bytes,
                            )
                            detail_data = detail_payload.get("data")
                            if isinstance(detail_data, dict) and detail_data:
                                # 确保包含 ID，以便后续跟踪
                                if "id" not in detail_data:
                                    detail_data["id"] = email_id
                                full_emails.append(detail_data)
                            else:
                                logger.warning(
                                    "Exchange email detail was empty: email=%s",
                                    fingerprint_identifier(email_id, namespace="email"),
                                )
                        else:
                            logger.error(
                                "Exchange email detail failed: email=%s status=%s",
                                fingerprint_identifier(email_id, namespace="email"),
                                detail_resp.status_code,
                            )
                except Exception as detail_err:
                    logger.error(
                        "Exchange email detail raised: email=%s error_type=%s",
                        fingerprint_identifier(email_id, namespace="email"),
                        type(detail_err).__name__,
                    )

            if full_emails:
                logger.info("成功获取 %s 封邮件的完整详情", len(full_emails))
            return full_emails
        except Exception as e:
            logger.error("获取邮件异常: error_type=%s", type(e).__name__)
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
            logger.info("Calling Exchange create-draft endpoint")
            response = await client.post(
                endpoint,
                json=payload,
                timeout=10.0
            )
            response.raise_for_status()
            return True
        except Exception as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            logger.error(
                "保存草稿失败: status=%s error_type=%s",
                status_code,
                type(exc).__name__,
            )
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
            logger.info("Calling Exchange draft endpoint: action=%s", action)
            response = await client.post(
                endpoint,
                json=payload,
                timeout=10.0
            )
            if response.status_code == 404 and is_draft:
                logger.warning("Draft endpoint rejected request: status=404")
                return False

            response.raise_for_status()
            return True
        except Exception as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            logger.error(
                "%s失败: status=%s error_type=%s",
                action,
                status_code,
                type(exc).__name__,
            )
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
                    "Mark as read failed: status=%s",
                    response.status_code,
                )
                return False
        except Exception as exc:
            logger.error(
                "Failed to mark email as read: error_type=%s",
                type(exc).__name__,
            )
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
        except Exception as exc:
            logger.error(
                "Failed to move Exchange email: email=%s folder=%s error_type=%s",
                fingerprint_identifier(email_id, namespace="email"),
                fingerprint_identifier(folder_id, namespace="exchange_folder"),
                type(exc).__name__,
            )
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
        except Exception as exc:
            logger.error(
                "Failed to delete Exchange email: email=%s error_type=%s",
                fingerprint_identifier(email_id, namespace="email"),
                type(exc).__name__,
            )
            return False

    async def get_email(
        self,
        email_id: str,
        account_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch full details for a specific email by ID.
        """
        from urllib.parse import quote
        encoded_id = quote(email_id, safe='')
        endpoint = f"{self.api_url}/{encoded_id}"
        target_account_id = account_id if account_id is not None else self.account_id

        client = self.http_client
        try:
            async with client.stream(
                "GET",
                endpoint,
                params={"account_id": target_account_id},
            ) as response:
                if response.status_code == 200:
                    payload = await read_json_limited(
                        response,
                        max_bytes=self._input_limits.exchange_response_bytes,
                    )
                    email_data = payload.get("data")
                    return email_data if isinstance(email_data, dict) and email_data else None
                logger.error(
                    "Failed to get Exchange email details: email=%s status=%s",
                    fingerprint_identifier(email_id, namespace="email"),
                    response.status_code,
                )
        except Exception as exc:
            logger.error(
                "Exception getting Exchange email: email=%s error_type=%s",
                fingerprint_identifier(email_id, namespace="email"),
                type(exc).__name__,
            )
        return None

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
            logger.info("Calling Exchange reply endpoint")
            response = await client.post(endpoint, json=payload, timeout=15.0)
            if response.status_code == 200:
                return response.json().get("code") == 200
            else:
                logger.error("Reply failed: status=%s", response.status_code)
                return False
        except Exception as exc:
            logger.error("Reply exception: error_type=%s", type(exc).__name__)
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
                    "Exchange contact resolution failed: query=%s status=%s",
                    fingerprint_identifier(query, namespace="contact_query"),
                    response.status_code,
                )
        except Exception as exc:
            logger.error(
                "Exchange contact resolution raised: query=%s error_type=%s",
                fingerprint_identifier(query, namespace="contact_query"),
                type(exc).__name__,
            )
        
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
            logger.info("Calling Exchange forward endpoint")
            response = await client.post(endpoint, json=payload, timeout=15.0)
            if response.status_code == 200:
                return response.json().get("code") == 200
            else:
                logger.error("Forward failed: status=%s", response.status_code)
                return False
        except Exception as exc:
            logger.error("Forward exception: error_type=%s", type(exc).__name__)
            return False
