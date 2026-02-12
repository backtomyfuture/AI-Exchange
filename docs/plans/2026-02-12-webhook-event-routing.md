# Webhook 事件分流 + 文件夹策略路由 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 Exchange webhook 事件处理从"所有事件走统一管线"重构为"基于事件类型 + 文件夹的分流路由"，实现：
1. NewMailEvent：按文件夹白名单决定走完整 AI 管线还是仅归档
2. CreatedEvent：仅监控已发送邮件进行归档，忽略草稿和文件夹创建
3. 修复 item_id 嵌套对象解析 bug

**Architecture:** 在现有 webhook worker 之上增加事件路由层，不改变 LangGraph 主管线。改动集中在 exchange_service.py 的入队逻辑和 exchange_api.py 的 folder 能力。

**Tech Stack:** Python 3.10, FastAPI, httpx, asyncio

**前置依赖:** Exchange 服务端已提供 `GET /api/v1/exchange/emails/folders/all` 接口。

**文件夹策略模型: 递归继承 + 显式覆盖**

配置文件夹名称到 `EXCHANGE_FOLDERS_FULL` 或 `EXCHANGE_FOLDERS_ARCHIVE`。子文件夹自动继承父级策略，但可被显式配置覆盖。

示例：
```env
EXCHANGE_FOLDERS_FULL=收件箱
EXCHANGE_FOLDERS_ARCHIVE=项目A日报
```
- 收件箱 → FULL（显式）
- 收件箱/VIP → FULL（继承自 收件箱）
- 收件箱/项目A日报 → ARCHIVE（显式覆盖）
- 已发送邮件 → ARCHIVE（CreatedEvent 硬编码）
- 草稿 → IGNORE（CreatedEvent 硬编码）
- 其他 → IGNORE（默认）

**已发送/草稿识别:** 通过可配置的 `EXCHANGE_FOLDER_SENTITEMS` / `EXCHANGE_FOLDER_DRAFTS` 名称匹配（默认 "已发送邮件" / "草稿"），支持中英文 Exchange 环境。

---

## Task 1: ExchangeClient 新增 get_all_folders() + 文件夹树 + 策略预计算

**问题:** 当前 `ExchangeClient` 没有获取文件夹信息的能力。webhook payload 中只有 `parent_folder_id`（不可读的 ID），无法判断邮件所属文件夹。需要在启动时：
1. 拉取全量文件夹列表
2. 利用 `parent_id` 构建文件夹树
3. 根据配置的白名单 + 递归继承规则，预计算每个 `folder_id` 的处理策略（`full` / `archive` / `ignore`）

**Files:**
- Modify: `src/utils/exchange_api.py`
- Modify: `src/config.py`
- Test: `tests/unit/test_folder_cache.py`

### Step 1: Write the failing test

```python
# tests/unit/test_folder_cache.py
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


# --- Shared mock data matching real Exchange API response ---
MOCK_FOLDERS_RESPONSE = {
    "code": 200,
    "msg": "success",
    "data": {
        "folders": [
            {"id": "ROOT_ID", "name": "Top of Information Store", "parent_id": None,
             "folder_class": "IPF.Note", "total_count": 0, "unread_count": 0, "child_folder_count": 5},
            {"id": "INBOX_ID", "name": "收件箱", "parent_id": "ROOT_ID",
             "folder_class": "IPF.Note", "total_count": 1502, "unread_count": 5, "child_folder_count": 2},
            {"id": "VIP_ID", "name": "VIP邮件", "parent_id": "INBOX_ID",
             "folder_class": "IPF.Note", "total_count": 30, "unread_count": 1, "child_folder_count": 0},
            {"id": "DAILY_ID", "name": "项目A日报", "parent_id": "INBOX_ID",
             "folder_class": "IPF.Note", "total_count": 20, "unread_count": 0, "child_folder_count": 0},
            {"id": "SENT_ID", "name": "已发送邮件", "parent_id": "ROOT_ID",
             "folder_class": "IPF.Note", "total_count": 890, "unread_count": 0, "child_folder_count": 0},
            {"id": "DRAFTS_ID", "name": "草稿", "parent_id": "ROOT_ID",
             "folder_class": "IPF.Note", "total_count": 10, "unread_count": 0, "child_folder_count": 0},
            {"id": "CAL_ID", "name": "日历", "parent_id": "ROOT_ID",
             "folder_class": "IPF.Appointment", "total_count": 50, "unread_count": 0, "child_folder_count": 0},
        ]
    }
}


def _make_client():
    mock_settings = MagicMock()
    mock_settings.EXCHANGE_API_URL = "http://mock/api/v1/exchange/emails"
    mock_settings.EXCHANGE_API_KEY = "test-key"
    mock_settings.EXCHANGE_ACCOUNT_ID = 1
    mock_settings.EXCHANGE_SSL_VERIFY = False
    mock_settings.EXCHANGE_FOLDER_SENTITEMS = "已发送邮件"
    mock_settings.EXCHANGE_FOLDER_DRAFTS = "草稿"

    from src.utils.exchange_api import ExchangeClient
    return ExchangeClient(settings=mock_settings)


def _mock_httpx(response_data):
    """Return a context manager patch for httpx.AsyncClient."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data

    mock_instance = AsyncMock()
    mock_instance.get = AsyncMock(return_value=mock_response)
    mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
    mock_instance.__aexit__ = AsyncMock(return_value=False)

    return patch("httpx.AsyncClient", return_value=mock_instance), mock_instance


@pytest.mark.asyncio
async def test_get_all_folders_returns_id_name_mapping():
    """get_all_folders should return dict mapping folder_id -> folder_name."""
    client = _make_client()
    patcher, mock_inst = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        result = await client.get_all_folders()

    assert isinstance(result, dict)
    assert result["INBOX_ID"] == "收件箱"
    assert result["SENT_ID"] == "已发送邮件"
    assert result["VIP_ID"] == "VIP邮件"


@pytest.mark.asyncio
async def test_get_all_folders_caches_result():
    """Calling get_all_folders twice should reuse cached result (1 API call)."""
    client = _make_client()
    patcher, mock_inst = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        r1 = await client.get_all_folders()
        r2 = await client.get_all_folders()

    assert r1 is r2
    assert mock_inst.get.call_count == 1


@pytest.mark.asyncio
async def test_sentitems_and_drafts_identified_by_name():
    """sentitems/drafts folder IDs should be identified by configured names."""
    client = _make_client()
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        await client.get_all_folders()

    assert client.sentitems_folder_id == "SENT_ID"
    assert client.drafts_folder_id == "DRAFTS_ID"


@pytest.mark.asyncio
async def test_compute_folder_policies_recursive_inheritance():
    """
    Policy computation should support recursive inheritance:
    - 收件箱 in FOLDERS_FULL → VIP邮件 (child) inherits 'full'
    - 项目A日报 explicitly in FOLDERS_ARCHIVE → overrides parent's 'full'
    """
    client = _make_client()
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        await client.get_all_folders()

    folders_full = {"收件箱"}
    folders_archive = {"项目A日报"}
    policies = client.compute_folder_policies(folders_full, folders_archive)

    assert policies["INBOX_ID"] == "full"       # Explicit
    assert policies["VIP_ID"] == "full"          # Inherited from 收件箱
    assert policies["DAILY_ID"] == "archive"     # Explicit override
    assert policies.get("SENT_ID") == "ignore"   # Not in any list
    assert policies.get("CAL_ID") == "ignore"    # Non-mail folder


@pytest.mark.asyncio
async def test_get_folder_policy_after_init():
    """get_folder_policy should return precomputed policy for a folder_id."""
    client = _make_client()
    patcher, _ = _mock_httpx(MOCK_FOLDERS_RESPONSE)

    with patcher:
        await client.get_all_folders()

    client.init_folder_policies(
        folders_full={"收件箱"},
        folders_archive={"项目A日报"},
    )

    assert client.get_folder_policy("INBOX_ID") == "full"
    assert client.get_folder_policy("VIP_ID") == "full"
    assert client.get_folder_policy("DAILY_ID") == "archive"
    assert client.get_folder_policy("UNKNOWN_ID") == "ignore"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_folder_cache.py -v`
Expected: FAIL — `get_all_folders`, `compute_folder_policies`, `get_folder_policy` 均不存在。

### Step 3: Implement folder cache + tree + policy computation

在 `src/utils/exchange_api.py` 的 `ExchangeClient` 类中：

1. 在 `__init__` 中初始化属性：

```python
def __init__(self, settings=None):
    # ... 现有代码 ...
    # --- Folder cache ---
    self._folder_cache: dict | None = None           # folder_id -> folder_name
    self._folder_tree: dict | None = None            # folder_id -> {"name", "parent_id", "children": []}
    self._folder_policies: dict | None = None        # folder_id -> "full" | "archive" | "ignore"
    self.sentitems_folder_id: str | None = None
    self.drafts_folder_id: str | None = None
    
    # Configurable well-known folder names (supports Chinese/English Exchange)
    self._sentitems_name = getattr(settings, "EXCHANGE_FOLDER_SENTITEMS", "已发送邮件")
    self._drafts_name = getattr(settings, "EXCHANGE_FOLDER_DRAFTS", "草稿")
```

2. 新增 `get_all_folders` 方法（对齐真实 API）：

```python
async def get_all_folders(self, force_refresh: bool = False) -> dict:
    """
    获取所有文件夹并构建缓存。
    
    调用: GET {api_url}/folders/all?account_id=...
    响应: {"data": {"folders": [{"id", "name", "parent_id", "folder_class", ...}]}}
    
    构建:
      - _folder_cache:  folder_id -> folder_name
      - _folder_tree:   folder_id -> {name, parent_id, children: [child_id, ...]}
      - sentitems_folder_id / drafts_folder_id (按名称匹配)
    
    Returns:
        dict: {folder_id: folder_name}
    """
    if self._folder_cache is not None and not force_refresh:
        return self._folder_cache

    headers = {"X-API-KEY": self.api_key} if self.api_key else {}
    endpoint = f"{self.api_url}/folders/all"
    params = {"account_id": self.account_id}

    async with httpx.AsyncClient(verify=self.ssl_verify) as client:
        try:
            response = await client.get(
                endpoint, params=params, headers=headers, timeout=15.0
            )
            if response.status_code == 200:
                folders = response.json().get("data", {}).get("folders", [])
                self._build_folder_cache(folders)
                logger.info(
                    f"Folder cache loaded: {len(self._folder_cache)} folders. "
                    f"sentitems={self.sentitems_folder_id}, "
                    f"drafts={self.drafts_folder_id}"
                )
                return self._folder_cache
            else:
                logger.error(f"Failed to get folders: {response.status_code}")
        except Exception as e:
            logger.error(f"Exception getting folders: {e}")

    self._folder_cache = {}
    self._folder_tree = {}
    return self._folder_cache


def _build_folder_cache(self, folders: list) -> None:
    """Build folder_id->name mapping and parent-child tree from flat folder list."""
    self._folder_cache = {}
    self._folder_tree = {}
    
    # Pass 1: Build flat maps
    for f in folders:
        fid = f.get("id")
        fname = f.get("name", "")
        parent_id = f.get("parent_id")
        if not fid:
            continue
        
        self._folder_cache[fid] = fname
        self._folder_tree[fid] = {
            "name": fname,
            "parent_id": parent_id,
            "children": [],
            "folder_class": f.get("folder_class", ""),
        }
        
        # Identify well-known folders by name
        if fname == self._sentitems_name:
            self.sentitems_folder_id = fid
        elif fname == self._drafts_name:
            self.drafts_folder_id = fid
    
    # Pass 2: Link children
    for fid, node in self._folder_tree.items():
        pid = node["parent_id"]
        if pid and pid in self._folder_tree:
            self._folder_tree[pid]["children"].append(fid)


def compute_folder_policies(
    self, folders_full: set[str], folders_archive: set[str]
) -> dict[str, str]:
    """
    Compute per-folder processing policy using recursive inheritance.
    
    Rules (in priority order):
    1. Folder name explicitly in folders_archive → "archive"
    2. Folder name explicitly in folders_full → "full"
    3. Any ancestor's name in folders_full → "full" (inherited)
    4. Any ancestor's name in folders_archive → "archive" (inherited)
    5. Default → "ignore"
    
    Returns:
        dict: {folder_id: "full" | "archive" | "ignore"}
    """
    if not self._folder_tree:
        return {}
    
    policies = {}
    
    def _get_ancestors(fid: str) -> list[str]:
        """Walk up parent chain, return list of ancestor names."""
        ancestors = []
        current = fid
        visited = set()
        while current and current in self._folder_tree and current not in visited:
            visited.add(current)
            parent_id = self._folder_tree[current]["parent_id"]
            if parent_id and parent_id in self._folder_tree:
                ancestors.append(self._folder_tree[parent_id]["name"])
            current = parent_id
        return ancestors
    
    for fid, node in self._folder_tree.items():
        name = node["name"]
        
        # Rule 1: Explicit archive
        if name in folders_archive:
            policies[fid] = "archive"
            continue
        
        # Rule 2: Explicit full
        if name in folders_full:
            policies[fid] = "full"
            continue
        
        # Rule 3-4: Check ancestors (closest ancestor wins)
        ancestor_names = _get_ancestors(fid)
        inherited = "ignore"
        for anc_name in ancestor_names:  # closest ancestor first
            if anc_name in folders_full:
                inherited = "full"
                break
            if anc_name in folders_archive:
                inherited = "archive"
                break
        
        policies[fid] = inherited
    
    return policies


def init_folder_policies(self, folders_full: set[str], folders_archive: set[str]) -> None:
    """Compute and cache folder policies. Called after get_all_folders()."""
    self._folder_policies = self.compute_folder_policies(folders_full, folders_archive)
    full_count = sum(1 for v in self._folder_policies.values() if v == "full")
    archive_count = sum(1 for v in self._folder_policies.values() if v == "archive")
    logger.info(f"Folder policies computed: {full_count} full, {archive_count} archive, "
                f"{len(self._folder_policies) - full_count - archive_count} ignore")


def get_folder_policy(self, folder_id: str) -> str:
    """Get precomputed policy for a folder. Returns 'ignore' if unknown."""
    if not self._folder_policies:
        return "ignore"
    return self._folder_policies.get(folder_id, "ignore")


def get_folder_name(self, folder_id: str) -> str | None:
    """Get folder name from cache."""
    if not self._folder_cache:
        return None
    return self._folder_cache.get(folder_id)
```

3. 在 `exchange_api.py` 顶部添加 logger（若不存在）：

```python
import logging
logger = logging.getLogger("ExchangeClient")
```

### Step 4: Add config fields to config.py

在 `src/config.py` 的 `Settings` 类中新增：

```python
    # --- Webhook Event Routing ---
    # Folder names that trigger full AI pipeline (comma-separated)
    # Subfolders inherit parent policy unless explicitly overridden
    EXCHANGE_FOLDERS_FULL: str = "收件箱"
    # Folder names for Qdrant-only archival (comma-separated, overrides inheritance)
    EXCHANGE_FOLDERS_ARCHIVE: str = ""
    # Well-known folder name matching (supports Chinese/English Exchange)
    EXCHANGE_FOLDER_SENTITEMS: str = "已发送邮件"
    EXCHANGE_FOLDER_DRAFTS: str = "草稿"
```

### Step 5: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_folder_cache.py -v`
Expected: PASS

### Step 6: Run existing tests for regression

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_exchange_api.py -v --tb=short`
Expected: All previously passing tests still pass.

### Step 7: Commit

```bash
git add src/utils/exchange_api.py src/config.py tests/unit/test_folder_cache.py
git commit -m "feat: add folder cache with tree-based policy computation

New capabilities on ExchangeClient:
- get_all_folders(): fetch from /emails/folders/all, build id->name cache + tree
- compute_folder_policies(): recursive inheritance with explicit override
- get_folder_policy(): O(1) lookup of precomputed policy per folder_id
- Identify sentitems/drafts by configurable folder names (Chinese/English)

Config: EXCHANGE_FOLDERS_FULL, EXCHANGE_FOLDERS_ARCHIVE,
        EXCHANGE_FOLDER_SENTITEMS, EXCHANGE_FOLDER_DRAFTS"
```

---

## Task 2: 启动时初始化文件夹缓存 + 策略预计算

**问题:** `get_all_folders()` 和 `init_folder_policies()` 需要在应用启动时调用，确保事件路由时策略表已就绪。需要在 `setup_async` 中按顺序执行：获取文件夹 → 解析配置白名单 → 预计算策略。

**Files:**
- Modify: `src/init_app.py`
- Test: `tests/unit/test_init_app_folders.py`

### Step 1: Write the failing test

```python
# tests/unit/test_init_app_folders.py
import pytest
import inspect

def test_setup_async_calls_get_all_folders():
    """setup_async should call exchange_client.get_all_folders()."""
    from src.init_app import AppContext
    source = inspect.getsource(AppContext.setup_async)
    assert "get_all_folders" in source, \
        "setup_async should call exchange_client.get_all_folders() during startup"

def test_setup_async_calls_init_folder_policies():
    """setup_async should call exchange_client.init_folder_policies()."""
    from src.init_app import AppContext
    source = inspect.getsource(AppContext.setup_async)
    assert "init_folder_policies" in source, \
        "setup_async should call exchange_client.init_folder_policies() during startup"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_init_app_folders.py -v`
Expected: FAIL — `setup_async` 中没有相关调用。

### Step 3: Add folder init to setup_async

在 `src/init_app.py` 的 `setup_async` 方法中，在 DB pool open 之后、graph build 之前添加：

```python
async def setup_async(self):
    # ... existing DB pool open ...
    
    # Initialize folder cache and compute routing policies
    try:
        await self.exchange_client.get_all_folders()
        
        # Parse whitelist from config
        settings = get_settings()
        folders_full = {f.strip() for f in settings.EXCHANGE_FOLDERS_FULL.split(",") if f.strip()}
        folders_archive = {f.strip() for f in settings.EXCHANGE_FOLDERS_ARCHIVE.split(",") if f.strip()}
        
        self.exchange_client.init_folder_policies(folders_full, folders_archive)
        logger.info("Exchange folder cache and routing policies initialized.")
    except Exception as e:
        logger.warning(f"Failed to initialize folder cache (will use safe defaults): {e}")
    
    # ... existing graph build ...
```

**注意：** folder cache 失败不阻止服务启动。事件路由在策略表为空时会走安全降级路径（Task 3 处理）。

### Step 4: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_init_app_folders.py -v`
Expected: PASS

### Step 5: Commit

```bash
git add src/init_app.py tests/unit/test_init_app_folders.py
git commit -m "feat: initialize folder cache and routing policies at startup

Call get_all_folders() + init_folder_policies() during setup_async.
Parses EXCHANGE_FOLDERS_FULL/ARCHIVE config and precomputes per-folder
routing strategy (full/archive/ignore) with recursive inheritance.
Gracefully degrades if folder API is unavailable."
```

---

## Task 3: 重构 enqueue_webhook_event — 事件路由核心

**问题:** 当前 `enqueue_webhook_event` 对所有事件类型和文件夹一视同仁，全部以 `skip_analysis=False` 入队。需要：
1. 修复 `item_id` 嵌套对象解析 bug（当前把 dict 当作 email_id 使用）
2. 实现基于 event_type + parent_folder_id 的分流路由
3. 支持文件夹白名单配置

**Files:**
- Modify: `src/exchange_service.py`
- Test: `tests/unit/test_event_routing.py`

### Step 1: Write the failing tests

```python
# tests/unit/test_event_routing.py
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

def _make_exchange_client_mock(sentitems_id="SENT_FOLDER_ID", drafts_id="DRAFTS_FOLDER_ID"):
    """Create a mock ExchangeClient with folder cache populated."""
    client = MagicMock()
    client._folder_cache = {
        "INBOX_FOLDER_ID": "Inbox",
        sentitems_id: "Sent Items",
        drafts_id: "Drafts",
        "VIP_FOLDER_ID": "VIP",
        "NOTIF_FOLDER_ID": "通知",
    }
    client.sentitems_folder_id = sentitems_id
    client.drafts_folder_id = drafts_id
    client.inbox_folder_id = "INBOX_FOLDER_ID"
    client.get_folder_name = lambda fid: client._folder_cache.get(fid)
    return client


@pytest.mark.asyncio
async def test_newmail_inbox_full_pipeline():
    """NewMailEvent in Inbox folder should enqueue with skip_analysis=False."""
    from src.exchange_service import enqueue_webhook_event, start_worker, stop_worker, _webhook_queue

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), \
         patch("src.exchange_service._webhook_queue", asyncio.Queue()) as q, \
         patch("src.exchange_service.get_settings") as mock_settings:

        mock_settings.return_value.EXCHANGE_FOLDERS_FULL = "Inbox,VIP"
        mock_settings.return_value.EXCHANGE_FOLDERS_ARCHIVE = "通知"

        result = await enqueue_webhook_event({
            "event_type": "NewMailEvent",
            "item_id": {"id": "EMAIL_001", "changekey": "CQ=="},
            "parent_folder_id": {"id": "INBOX_FOLDER_ID", "changekey": "AQ=="},
        })

        assert result["queued"] is True
        assert result["route"] == "full"
        email_data, skip = await q.get()
        assert email_data["id"] == "EMAIL_001"
        assert skip is False


@pytest.mark.asyncio
async def test_newmail_unknown_folder_ignored():
    """NewMailEvent in unconfigured folder should be ignored."""
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), \
         patch("src.exchange_service._webhook_queue", asyncio.Queue()) as q, \
         patch("src.exchange_service.get_settings") as mock_settings:

        mock_settings.return_value.EXCHANGE_FOLDERS_FULL = "Inbox"
        mock_settings.return_value.EXCHANGE_FOLDERS_ARCHIVE = ""

        result = await enqueue_webhook_event({
            "event_type": "NewMailEvent",
            "item_id": {"id": "EMAIL_002", "changekey": "CQ=="},
            "parent_folder_id": {"id": "UNKNOWN_FOLDER_ID", "changekey": "AQ=="},
        })

        assert result["queued"] is False
        assert result.get("reason") == "folder_not_in_whitelist"


@pytest.mark.asyncio
async def test_created_sentitems_archive_only():
    """CreatedEvent in Sent Items should enqueue with skip_analysis=True."""
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), \
         patch("src.exchange_service._webhook_queue", asyncio.Queue()) as q, \
         patch("src.exchange_service.get_settings") as mock_settings:

        mock_settings.return_value.EXCHANGE_FOLDERS_FULL = "Inbox"
        mock_settings.return_value.EXCHANGE_FOLDERS_ARCHIVE = ""

        result = await enqueue_webhook_event({
            "event_type": "CreatedEvent",
            "item_id": {"id": "SENT_EMAIL_001", "changekey": "CQ=="},
            "parent_folder_id": {"id": "SENT_FOLDER_ID", "changekey": "AQ=="},
        })

        assert result["queued"] is True
        assert result["route"] == "archive"
        email_data, skip = await q.get()
        assert email_data["id"] == "SENT_EMAIL_001"
        assert skip is True


@pytest.mark.asyncio
async def test_created_drafts_ignored():
    """CreatedEvent in Drafts should be ignored."""
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), \
         patch("src.exchange_service._webhook_queue", asyncio.Queue()) as q, \
         patch("src.exchange_service.get_settings") as mock_settings:

        mock_settings.return_value.EXCHANGE_FOLDERS_FULL = "Inbox"
        mock_settings.return_value.EXCHANGE_FOLDERS_ARCHIVE = ""

        result = await enqueue_webhook_event({
            "event_type": "CreatedEvent",
            "item_id": {"id": "DRAFT_001", "changekey": "CQ=="},
            "parent_folder_id": {"id": "DRAFTS_FOLDER_ID", "changekey": "AQ=="},
        })

        assert result["queued"] is False
        assert result.get("reason") == "drafts_ignored"


@pytest.mark.asyncio
async def test_no_item_id_ignored():
    """Event with no item_id (folder creation) should be ignored."""
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), \
         patch("src.exchange_service._webhook_queue", asyncio.Queue()), \
         patch("src.exchange_service.get_settings") as mock_settings:

        mock_settings.return_value.EXCHANGE_FOLDERS_FULL = "Inbox"
        mock_settings.return_value.EXCHANGE_FOLDERS_ARCHIVE = ""

        result = await enqueue_webhook_event({
            "event_type": "CreatedEvent",
            "item_id": None,
            "folder_id": {"id": "NEW_FOLDER_ID", "changekey": "AQ=="},
            "parent_folder_id": {"id": "INBOX_FOLDER_ID", "changekey": "AQ=="},
        })

        assert result["queued"] is False
        assert result.get("reason") == "no_item_id"


@pytest.mark.asyncio
async def test_newmail_archive_folder():
    """NewMailEvent in archive-only folder should enqueue with skip_analysis=True."""
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = _make_exchange_client_mock()

    with patch("src.exchange_service._worker_ctx", mock_ctx), \
         patch("src.exchange_service._webhook_queue", asyncio.Queue()) as q, \
         patch("src.exchange_service.get_settings") as mock_settings:

        mock_settings.return_value.EXCHANGE_FOLDERS_FULL = "Inbox"
        mock_settings.return_value.EXCHANGE_FOLDERS_ARCHIVE = "通知"

        result = await enqueue_webhook_event({
            "event_type": "NewMailEvent",
            "item_id": {"id": "NOTIF_001", "changekey": "CQ=="},
            "parent_folder_id": {"id": "NOTIF_FOLDER_ID", "changekey": "AQ=="},
        })

        assert result["queued"] is True
        assert result["route"] == "archive"
        email_data, skip = await q.get()
        assert skip is True


@pytest.mark.asyncio
async def test_folder_cache_empty_fallback():
    """If folder cache is empty, NewMailEvent should fallback to full pipeline."""
    from src.exchange_service import enqueue_webhook_event

    mock_ctx = MagicMock()
    mock_ctx.exchange_client = MagicMock()
    mock_ctx.exchange_client._folder_cache = {}  # Empty cache
    mock_ctx.exchange_client.sentitems_folder_id = None
    mock_ctx.exchange_client.drafts_folder_id = None
    mock_ctx.exchange_client.get_folder_name = lambda fid: None

    with patch("src.exchange_service._worker_ctx", mock_ctx), \
         patch("src.exchange_service._webhook_queue", asyncio.Queue()) as q, \
         patch("src.exchange_service.get_settings") as mock_settings:

        mock_settings.return_value.EXCHANGE_FOLDERS_FULL = "Inbox"
        mock_settings.return_value.EXCHANGE_FOLDERS_ARCHIVE = ""

        result = await enqueue_webhook_event({
            "event_type": "NewMailEvent",
            "item_id": {"id": "EMAIL_003", "changekey": "CQ=="},
            "parent_folder_id": {"id": "UNKNOWN_ID", "changekey": "AQ=="},
        })

        # Fallback: when cache is empty and cannot resolve folder,
        # NewMailEvent should default to full pipeline (safe default)
        assert result["queued"] is True
        assert result["route"] == "full"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_event_routing.py -v`
Expected: FAIL — 当前 `enqueue_webhook_event` 没有路由逻辑。

### Step 3: Rewrite enqueue_webhook_event

重写 `src/exchange_service.py` 中的 `enqueue_webhook_event`。

核心变化：使用 `exchange_client.get_folder_policy(folder_id)` 查预计算策略表（O(1)），不在运行时解析白名单。

```python
def _extract_id(raw) -> str | None:
    """Safely extract ID from nested EWS object or plain string."""
    if isinstance(raw, dict):
        return raw.get("id")
    if isinstance(raw, str):
        return raw
    return None


async def enqueue_webhook_event(payload: dict, header_event: str | None = None) -> dict:
    """
    Event router: classify webhook event and decide processing strategy.
    
    Uses precomputed folder policies (from init_folder_policies at startup):
    - "full"    → complete AI pipeline (skip_analysis=False)
    - "archive" → Qdrant ingest only (skip_analysis=True)
    - "ignore"  → drop event
    
    Special handling for CreatedEvent:
    - sentitems folder → always archive (regardless of policy)
    - drafts folder → always ignore
    - other → ignore
    """
    if _webhook_queue is None:
        raise RuntimeError("Exchange worker is not running")

    event_type = header_event or payload.get("event_type") or payload.get("event")
    
    # --- Step 1: Extract IDs from nested EWS objects ---
    email_id = _extract_id(payload.get("item_id"))
    parent_folder_id = _extract_id(payload.get("parent_folder_id"))
    
    if not email_id:
        logger.debug(f"Ignoring event {event_type}: no item_id (folder creation or empty)")
        return {"queued": False, "reason": "no_item_id", "event_type": event_type}

    if event_type not in {"NewMailEvent", "CreatedEvent"}:
        return {"queued": False, "reason": "unsupported_event", "event_type": event_type}

    # --- Step 2: Resolve folder info ---
    exchange_client = _worker_ctx.exchange_client if _worker_ctx else None
    folder_name = exchange_client.get_folder_name(parent_folder_id) if exchange_client else None
    
    # --- Step 3: Route ---
    skip_analysis = False
    route = None

    if event_type == "NewMailEvent":
        if exchange_client and exchange_client._folder_policies:
            policy = exchange_client.get_folder_policy(parent_folder_id)
        else:
            # Cache not loaded (startup race) → safe default: full pipeline
            policy = "full"
            logger.warning(f"Folder policies not loaded, defaulting {email_id} to full")
        
        if policy == "full":
            skip_analysis = False
            route = "full"
        elif policy == "archive":
            skip_analysis = True
            route = "archive"
        else:  # "ignore"
            logger.info(f"Ignoring NewMailEvent {email_id}: folder '{folder_name or parent_folder_id}' policy=ignore")
            return {"queued": False, "reason": "folder_not_in_whitelist",
                    "folder": folder_name or parent_folder_id}

    elif event_type == "CreatedEvent":
        if exchange_client and parent_folder_id == exchange_client.sentitems_folder_id:
            skip_analysis = True
            route = "archive"
        elif exchange_client and parent_folder_id == exchange_client.drafts_folder_id:
            logger.debug(f"Ignoring CreatedEvent in Drafts: {email_id}")
            return {"queued": False, "reason": "drafts_ignored"}
        else:
            logger.debug(f"Ignoring CreatedEvent {email_id}: folder '{folder_name or parent_folder_id}'")
            return {"queued": False, "reason": "created_other_ignored",
                    "folder": folder_name or parent_folder_id}

    # --- Step 4: Assemble minimal email_data and enqueue ---
    email_data = payload.get("item") if isinstance(payload.get("item"), dict) else {}
    email_data.setdefault("id", email_id)
    email_data.setdefault("subject", payload.get("subject", ""))
    email_data.setdefault("sender", payload.get("sender", ""))
    email_data.setdefault("received_at", payload.get("received_time", ""))
    email_data["_parent_folder_id"] = parent_folder_id
    email_data["_parent_folder_name"] = folder_name
    email_data["_event_type"] = event_type

    await _webhook_queue.put((email_data, skip_analysis))
    logger.info(
        f"Enqueued {event_type} [{route}]: {email_id} "
        f"(folder={folder_name or 'unknown'}, skip_analysis={skip_analysis})"
    )
    return {
        "queued": True,
        "email_id": email_id,
        "route": route,
        "folder": folder_name,
        "queue_size": _webhook_queue.qsize(),
    }
```

### Step 4: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_event_routing.py -v`
Expected: All PASS

### Step 5: Run existing webhook tests for regression

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_exchange_webhook.py -v --tb=short`
Expected: May need updates — existing tests use old payload format (flat `item_id` strings). Update if needed.

### Step 6: Commit

```bash
git add src/exchange_service.py tests/unit/test_event_routing.py
git commit -m "feat: implement webhook event routing by event type and folder

Replace one-size-fits-all event handling with folder-aware routing:
- NewMailEvent: whitelist-based (FOLDERS_FULL → AI pipeline, FOLDERS_ARCHIVE → Qdrant only)
- CreatedEvent: sentitems → archive, drafts → ignore, other → ignore
- Fix item_id nested object parsing bug (dict.id extraction)
- Add _parent_folder_id/_name metadata for downstream traceability"
```

---

## Task 4: 细化 process_and_archive_email 的归档路径

**问题:** 当前 `skip_analysis=True` 路径仍会执行附件上传和标记已读，这对已发送邮件和归档邮件是不必要的（已发送邮件不需要附件上传到飞书，也不需要标记已读）。

**Files:**
- Modify: `src/exchange_service.py`
- Test: `tests/unit/test_archive_path.py`

### Step 1: Write the failing test

```python
# tests/unit/test_archive_path.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

@pytest.mark.asyncio
async def test_skip_analysis_skips_attachment_upload():
    """When skip_analysis=True, should NOT upload attachments to Lark."""
    from src.exchange_service import process_and_archive_email

    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    mock_ctx.email_processor = MagicMock()
    mock_ctx.email_processor.process_email = MagicMock()
    mock_ctx.exchange_client = AsyncMock()
    mock_ctx.exchange_client.mark_as_read = AsyncMock(return_value=True)

    email_data = {
        "id": "SENT_001",
        "subject": "Test sent email",
        "sender": "me@example.com",
        "body": "<p>Hello</p>",
        "attachments": [{"name": "file.pdf", "content": "base64data"}],
        "_event_type": "CreatedEvent",
    }

    with patch("src.exchange_service._upload_attachments_to_lark") as mock_upload, \
         patch("src.exchange_service._ingest_to_qdrant") as mock_ingest, \
         patch("src.exchange_service._mark_email_read") as mock_read:
        
        mock_ingest.return_value = None
        mock_read.return_value = None
        
        await process_and_archive_email(email_data, mock_ctx, skip_analysis=True)

        # Attachment upload should NOT be called for archive-only path
        mock_upload.assert_not_called()
        # Qdrant ingest SHOULD be called
        mock_ingest.assert_called_once()
        # Mark as read should NOT be called for sent items
        mock_read.assert_not_called()


@pytest.mark.asyncio
async def test_full_pipeline_uploads_attachments():
    """When skip_analysis=False, should upload attachments and mark as read."""
    from src.exchange_service import process_and_archive_email

    mock_ctx = MagicMock()
    mock_ctx.db_manager = AsyncMock()
    mock_ctx.db_manager.log_initial_email = AsyncMock(return_value=True)
    mock_ctx.db_manager.update_status = AsyncMock()
    mock_ctx.email_processor = MagicMock()
    mock_ctx.email_processor.process_email = MagicMock()
    mock_ctx.exchange_client = AsyncMock()
    mock_ctx.graph = AsyncMock()

    email_data = {
        "id": "INBOX_001",
        "subject": "Test incoming email",
        "sender": "someone@example.com",
        "body": "<p>Hello</p>",
        "attachments": [],
        "_event_type": "NewMailEvent",
    }

    with patch("src.exchange_service._upload_attachments_to_lark") as mock_upload, \
         patch("src.exchange_service._ingest_to_qdrant") as mock_ingest, \
         patch("src.exchange_service._run_ai_pipeline", return_value={"classification": {"need_reply": False}}) as mock_ai, \
         patch("src.exchange_service._dispatch_notification") as mock_notify, \
         patch("src.exchange_service._mark_email_read") as mock_read:

        mock_ingest.return_value = None
        mock_read.return_value = None

        await process_and_archive_email(email_data, mock_ctx, skip_analysis=False)

        # Full pipeline should upload attachments and mark as read
        mock_upload.assert_called_once()
        mock_ingest.assert_called_once()
        mock_ai.assert_called_once()
        mock_read.assert_called_once()
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_archive_path.py -v`
Expected: FAIL — 当前 `skip_analysis=True` 仍调用 `_upload_attachments_to_lark` 和 `_mark_email_read`。

### Step 3: Refine process_and_archive_email

重写 `src/exchange_service.py` 的 `process_and_archive_email`：

```python
async def process_and_archive_email(email_data, ctx, skip_analysis: bool = False):
    """
    Process a single email based on routing decision.
    
    - skip_analysis=False (full pipeline): upload → ingest → AI → notify → mark_read
    - skip_analysis=True  (archive only):  ingest → DB archived
    """
    thread_id = email_data.get("id", str(time.time()))
    config = {"configurable": {"thread_id": thread_id}}
    event_type = email_data.get("_event_type", "unknown")
    folder_name = email_data.get("_parent_folder_name", "unknown")
    
    logger.info(
        f"Processing email: {thread_id} - {email_data.get('subject')} "
        f"(event={event_type}, folder={folder_name}, skip_analysis={skip_analysis})"
    )

    # Dedup check
    is_new = await ctx.db_manager.log_initial_email(email_data)
    if not is_new:
        logger.info(f"Email {thread_id} already exists in DB, skipping.")
        if not skip_analysis:
            await _mark_email_read(thread_id, ctx)
        return

    logger.info(f"Email {thread_id} logged to DB as 'pending'.")

    if skip_analysis:
        # === Archive-only path ===
        # Ingest to Qdrant for RAG context, then mark as archived.
        # No attachment upload, no AI pipeline, no Lark notification, no mark_read.
        await _ingest_to_qdrant(thread_id, email_data, ctx)
        await ctx.db_manager.update_status(thread_id, "archived")
        logger.info(f"Email {thread_id} archived (Qdrant only, event={event_type}).")
    else:
        # === Full AI pipeline path ===
        await _upload_attachments_to_lark(email_data)
        await _ingest_to_qdrant(thread_id, email_data, ctx)

        pipeline_result = await _run_ai_pipeline(thread_id, email_data, ctx, config)
        if pipeline_result is not None:
            await _dispatch_notification(thread_id, pipeline_result, ctx, config)

        await _mark_email_read(thread_id, ctx)
```

### Step 4: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_archive_path.py -v`
Expected: PASS

### Step 5: Run all exchange service tests for regression

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_exchange_service_refactor.py tests/unit/test_exchange_webhook.py -v --tb=short`
Expected: All PASS (may need minor updates to existing tests)

### Step 6: Commit

```bash
git add src/exchange_service.py tests/unit/test_archive_path.py
git commit -m "feat: differentiate archive-only and full pipeline paths

skip_analysis=True (sent items, archive folders):
- Only ingests to Qdrant + marks DB as archived
- Skips attachment upload, AI pipeline, Lark notification, mark_as_read

skip_analysis=False (inbox, important folders):
- Full pipeline: upload → ingest → AI → notify → mark_read"
```

---

## Task 5: 清理 exchange_api.py 遗留问题

**问题:** `exchange_api.py` 仍有大量 sync 时代遗留：
1. 全文使用 `print()` 而非 `logger`
2. `get_recent_emails()` 是 sync 时代的轮询方法，webhook 模式下不再使用
3. 顶部有 `from dotenv import load_dotenv; load_dotenv()` 副作用

**Files:**
- Modify: `src/utils/exchange_api.py`
- Test: `tests/unit/test_exchange_api_cleanup.py`

### Step 1: Write the failing test

```python
# tests/unit/test_exchange_api_cleanup.py
import ast
import pytest

def test_no_print_in_exchange_api():
    """exchange_api.py should use logger, not print()."""
    with open("src/utils/exchange_api.py", "r") as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "print":
                pytest.fail(f"exchange_api.py: print() found at line {node.lineno}")


def test_no_load_dotenv_in_exchange_api():
    """exchange_api.py should not call load_dotenv() at module level."""
    import inspect
    import src.utils.exchange_api as mod
    source = inspect.getsource(mod)
    assert "load_dotenv()" not in source, \
        "exchange_api.py should not have load_dotenv() side effect"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_exchange_api_cleanup.py -v`
Expected: FAIL — 大量 `print()` 和 `load_dotenv()`。

### Step 3: Clean up exchange_api.py

1. 在文件顶部添加 `logger`（如果 Task 1 中未添加）：
```python
import logging
logger = logging.getLogger("ExchangeClient")
```

2. 删除顶部的：
```python
from dotenv import load_dotenv
load_dotenv()
```

3. 全局替换所有 `print(...)` 为适当的 `logger.info/warning/error(...)`:
   - 调试/状态输出 → `logger.info`
   - 错误信息 → `logger.error`
   - 警告（如 404）→ `logger.warning`

4. 考虑是否保留 `get_recent_emails()`：
   - 如果仍有脚本或测试依赖它 → 保留但标记 `# DEPRECATED: sync-era method`
   - 如果完全没有引用 → 删除

### Step 4: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_exchange_api_cleanup.py tests/unit/test_exchange_api.py -v --tb=short`
Expected: PASS

### Step 5: Commit

```bash
git add src/utils/exchange_api.py tests/unit/test_exchange_api_cleanup.py
git commit -m "chore: clean up exchange_api.py legacy code

Replace all print() with logger. Remove load_dotenv() module-level
side effect. Mark sync-era get_recent_emails() as deprecated."
```

---

## Task 6: 更新 .env.example 和现有测试适配

**问题:** 新增了配置字段，需要更新 `.env.example` 和适配现有测试中的 payload 格式变化。

**Files:**
- Modify: `.env.example`
- Modify: `tests/unit/test_exchange_webhook.py` (适配嵌套 item_id 格式)
- Modify: `tests/test_consolidation.py` (如有引用变更方法)

### Step 1: Update .env.example

在 `.env.example` 中添加：

```env
# --- Webhook Event Routing ---
# Folders that trigger full AI pipeline (comma-separated folder names)
EXCHANGE_FOLDERS_FULL=Inbox
# Folders that only get Qdrant archival (comma-separated, optional)
EXCHANGE_FOLDERS_ARCHIVE=
```

### Step 2: Update test_exchange_webhook.py

更新现有 webhook 测试中的 payload 格式，使用嵌套 `item_id` 对象：

```python
# 旧格式:
# payload = {"event_type": "NewMailEvent", "item_id": "EMAIL_001", ...}

# 新格式:
payload = {
    "event_type": "NewMailEvent",
    "item_id": {"id": "EMAIL_001", "changekey": "CQ=="},
    "parent_folder_id": {"id": "INBOX_ID", "changekey": "AQ=="},
}
```

同时需要 mock `_worker_ctx.exchange_client` 的 folder cache。

### Step 3: Run full test suite

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/ -v --tb=short -x`
Expected: All PASS

### Step 4: Commit

```bash
git add .env.example tests/
git commit -m "chore: update .env.example and adapt tests for nested item_id format

Add EXCHANGE_FOLDERS_FULL/ARCHIVE config examples.
Update webhook test payloads to use EWS nested object format."
```

---

## 执行顺序与依赖关系

```
Task 1 (Folder Cache)         ← 独立，最高优先
Task 2 (Startup Init)         ← 依赖 Task 1
Task 3 (Event Router)         ← 依赖 Task 1 + Task 2
Task 4 (Archive Path)         ← 依赖 Task 3（使用新路由的 skip_analysis）
Task 5 (API Cleanup)          ← 独立，可与 Task 2-4 并行
Task 6 (Tests & Config)       ← 最后，适配所有变更
```

**推荐执行路径：**
- 第一批: Task 1 + Task 5 (并行)
- 第二批: Task 2 → Task 3 → Task 4 (顺序)
- 第三批: Task 6 (全量验证)

---

## 降级与容错策略

| 场景 | 行为 |
|:-----|:-----|
| Exchange folders API 不可用 | 启动 warning，缓存为空 |
| 缓存为空 + NewMailEvent | 默认走 full pipeline（安全降级） |
| 缓存为空 + CreatedEvent | 无法判断 sentitems，忽略事件 |
| parent_folder_id 不在缓存中 | 按 folder 名 lookup 失败，走白名单外路径 |
| 白名单配置为空 | `EXCHANGE_FOLDERS_FULL=""` → 所有 NewMailEvent 被忽略（需至少配置一个） |

---

## 验证清单（全部完成后）

```bash
# 1. 全量测试
python -m pytest tests/ -v --tb=short

# 2. 事件路由测试
python -m pytest tests/unit/test_event_routing.py tests/unit/test_archive_path.py -v

# 3. 文件夹缓存测试
python -m pytest tests/unit/test_folder_cache.py -v

# 4. 健康检查（部署后）
curl http://localhost:8000/health

# 5. 日志验证（Docker）
docker-compose logs -f | grep -E "(Enqueued|Ignoring|Folder cache)"
```

---

## Exchange 服务端接口（已就绪）

```
GET /api/v1/exchange/emails/folders/all?account_id=1
Headers: X-API-KEY

Response:
{
  "code": 200,
  "msg": "success",
  "data": {
    "folders": [
      {"id": "AQMk...", "name": "收件箱", "parent_id": "ROOT_ID",
       "folder_class": "IPF.Note", "total_count": 1502, "unread_count": 5, "child_folder_count": 1},
      {"id": "AQMk...", "name": "已发送邮件", "parent_id": "ROOT_ID",
       "folder_class": "IPF.Note", "total_count": 890, "unread_count": 0, "child_folder_count": 0},
      ...
    ]
  }
}
```

关键字段：
- `id`: 文件夹 ID（与 webhook 的 `parent_folder_id.id` 匹配）
- `name`: 文件夹名称（用于白名单匹配）
- `parent_id`: 父文件夹 ID（用于构建树结构、递归继承策略）
- `folder_class`: `IPF.Note` = 邮件文件夹, `IPF.Appointment` = 日历, etc.
