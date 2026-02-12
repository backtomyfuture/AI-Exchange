# Production Hardening 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 修复生产环境中的可靠性风险和代码质量债务，涵盖 DB 连接池、实例复用、配置统一、日志规范、死代码清理、重试逻辑统一和核心函数拆分。

**Architecture:** 本计划不改变系统的整体架构（LangGraph pipeline + Tiered Routing + Skill 体系），仅在现有骨架上加固。每个 Task 独立、向后兼容，可以单独部署。修改范围严格限制在已有文件，不新增模块。

**Tech Stack:** Python 3.10, psycopg 3 (psycopg_pool), LangGraph, FastAPI, tenacity

---

## Task 1: AsyncDatabaseManager 连接池化

**问题:** `db_async.py` 使用单个 `self._conn` 连接。并发操作（webhook worker、self-healer、scheduler）共享该连接，连接断开时全部失败。`init_app.py` 已为 LangGraph 创建了 `AsyncConnectionPool`，但 `AsyncDatabaseManager` 未使用任何池。

**Files:**
- Modify: `src/utils/db_async.py`
- Modify: `src/init_app.py`
- Test: `tests/unit/test_db_pool.py`

### Step 1: Write the failing test

```python
# tests/unit/test_db_pool.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

@pytest.mark.asyncio
async def test_db_manager_uses_pool():
    """AsyncDatabaseManager should use connection pool instead of single connection."""
    from src.utils.db_async import AsyncDatabaseManager
    from src.config import get_settings

    settings = get_settings()
    db = AsyncDatabaseManager(settings)
    
    # Should have pool attribute, not single _conn
    assert hasattr(db, '_pool'), "AsyncDatabaseManager should have _pool attribute"
    assert not hasattr(db, '_conn'), "AsyncDatabaseManager should NOT have _conn single connection"


@pytest.mark.asyncio
async def test_db_manager_get_settings():
    """AsyncDatabaseManager should use get_settings() instead of os.getenv()."""
    from src.utils.db_async import AsyncDatabaseManager
    from src.config import get_settings
    
    settings = get_settings()
    db = AsyncDatabaseManager(settings)
    
    assert settings.POSTGRES_HOST in db.dsn
    assert settings.POSTGRES_DB in db.dsn
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_db_pool.py -v`
Expected: FAIL — `AsyncDatabaseManager` currently takes no args, has `_conn` not `_pool`.

### Step 3: Implement connection pool in AsyncDatabaseManager

重写 `src/utils/db_async.py`：

**核心变更：**
- `__init__(self, settings)` 接受 `Settings` 对象，不再用 `os.getenv()`
- 用 `psycopg_pool.AsyncConnectionPool` 替代 `self._conn`
- `get_connection()` 改为 `@asynccontextmanager` 返回池连接
- 所有方法内改用 `async with self.get_connection() as conn:` 模式
- 新增 `async def open()` 和修改 `async def close()` 管理池生命周期

```python
# src/utils/db_async.py 关键改动

from psycopg_pool import AsyncConnectionPool

class AsyncDatabaseManager:
    def __init__(self, settings):
        self._pool: Optional[AsyncConnectionPool] = None
        self._dsn = (
            f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
            f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        )
        self._initialized = False

    @property
    def dsn(self) -> str:
        return self._dsn

    async def open(self):
        """Open the connection pool. Must be called within a running asyncio loop."""
        if self._pool is None:
            self._pool = AsyncConnectionPool(
                conninfo=self._dsn,
                min_size=2,
                max_size=10,
                open=False,
                kwargs={"autocommit": True, "row_factory": dict_row}
            )
            await self._pool.open()
            logger.info("AsyncDatabaseManager connection pool opened (min=2, max=10).")
        if not self._initialized:
            await self._init_db()
            self._initialized = True

    @asynccontextmanager
    async def get_connection(self):
        """Yield a connection from the pool."""
        async with self._pool.connection() as conn:
            yield conn

    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("AsyncDatabaseManager connection pool closed.")
```

**所有使用 `get_connection()` 的方法改为 context manager 风格：**

```python
    # 原来：
    # conn = await self.get_connection()
    # async with conn.cursor() as cur:
    #     ...

    # 改为：
    async with self.get_connection() as conn:
        async with conn.cursor() as cur:
            ...
```

### Step 4: Update init_app.py to pass settings

```python
# src/init_app.py 改动

    def initialize(self):
        settings = get_settings()
        self.exchange_client = ExchangeClient(settings)
        self.email_processor = EmailProcessor()
        self.db_manager = AsyncDatabaseManager(settings)  # 传入 settings
        # ... 其余不变

    async def setup_async(self):
        # 打开 DB Manager 的连接池
        await self.db_manager.open()
        
        # 打开 LangGraph checkpointer 的连接池（保持不变）
        if self.pool:
            await self.pool.open()
        # ... 其余不变
```

### Step 5: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_db_pool.py -v`
Expected: PASS

### Step 6: Run existing tests to verify no regression

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/ -v --tb=short -x`
Expected: All previously passing tests still pass.

### Step 7: Commit

```bash
git add src/utils/db_async.py src/init_app.py tests/unit/test_db_pool.py
git commit -m "fix: replace single DB connection with AsyncConnectionPool

AsyncDatabaseManager now uses psycopg_pool.AsyncConnectionPool (min=2, max=10)
instead of a single shared connection. Accepts Settings object instead of
os.getenv() calls. Improves concurrent access reliability for webhook worker,
self-healer, and scheduler."
```

---

## Task 2: retriever_node 实例复用

**问题:** `retriever_node.py` 每次调用都创建新的 `EmailRetriever()` (新 QdrantClient + OpenAI client) 和 `RoutingEngine()` (重新加载所有 Skill manifest)。应使用单例。

**Files:**
- Modify: `src/nodes/retriever_node.py`
- Modify: `src/utils/retriever.py` (添加单例获取函数)
- Test: `tests/unit/test_retriever_singleton.py`

### Step 1: Write the failing test

```python
# tests/unit/test_retriever_singleton.py
import pytest

def test_get_retriever_returns_singleton():
    """get_retriever() should return the same instance every call."""
    from src.utils.retriever import get_retriever
    r1 = get_retriever()
    r2 = get_retriever()
    assert r1 is r2, "get_retriever() should return singleton instance"


def test_retriever_node_uses_singleton(monkeypatch):
    """retriever_node should not instantiate EmailRetriever directly."""
    import src.nodes.retriever_node as mod
    import inspect
    source = inspect.getsource(mod.retrieve_context)
    assert "EmailRetriever()" not in source, \
        "retriever_node should use get_retriever() singleton, not EmailRetriever()"


def test_retriever_node_uses_routing_engine_singleton(monkeypatch):
    """retriever_node should use get_routing_engine(), not RoutingEngine()."""
    import src.nodes.retriever_node as mod
    import inspect
    source = inspect.getsource(mod.retrieve_context)
    assert "RoutingEngine()" not in source, \
        "retriever_node should use get_routing_engine() singleton, not RoutingEngine()"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_retriever_singleton.py -v`
Expected: FAIL — `get_retriever` doesn't exist; source contains `EmailRetriever()` and `RoutingEngine()`.

### Step 3: Add singleton to retriever.py

在 `src/utils/retriever.py` 文件末尾添加：

```python
# --- 全局单例 ---
_retriever_instance = None

def get_retriever() -> EmailRetriever:
    """获取 EmailRetriever 全局单例"""
    global _retriever_instance
    if _retriever_instance is None:
        _retriever_instance = EmailRetriever()
    return _retriever_instance
```

### Step 4: Update retriever_node.py to use singletons

```python
# src/nodes/retriever_node.py 关键改动

# 第 19 行: EmailRetriever() → get_retriever()
from src.utils.retriever import get_retriever
retriever = get_retriever()

# 第 52-53 行: RoutingEngine() → get_routing_engine()
from src.router.engine import get_routing_engine
router_engine = get_routing_engine()
```

### Step 5: Run tests to verify pass

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_retriever_singleton.py tests/unit/test_retriever.py -v`
Expected: All PASS

### Step 6: Commit

```bash
git add src/utils/retriever.py src/nodes/retriever_node.py tests/unit/test_retriever_singleton.py
git commit -m "perf: use singleton instances in retriever_node

Replace per-call EmailRetriever() and RoutingEngine() instantiation with
get_retriever() and get_routing_engine() singletons. Eliminates redundant
QdrantClient/OpenAI client creation and Skill manifest reload per email."
```

---

## Task 3: 配置来源统一

**问题:** `db_async.py` (已在 Task 1 修复) 和 `rate_limiter.py` 使用 `os.getenv()` 而非 `get_settings()`。

**Files:**
- Modify: `src/utils/rate_limiter.py`
- Test: `tests/unit/test_rate_limiter_config.py`

### Step 1: Write the failing test

```python
# tests/unit/test_rate_limiter_config.py
import pytest

def test_rate_limiter_no_direct_os_getenv():
    """rate_limiter module should not use os.getenv() directly."""
    import inspect
    import src.utils.rate_limiter as mod
    source = inspect.getsource(mod)
    assert "os.getenv" not in source, \
        "rate_limiter.py should use get_settings() instead of os.getenv()"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_rate_limiter_config.py -v`
Expected: FAIL — `os.getenv("LLM_MAX_RPM", "15")` on line 44.

### Step 3: Fix rate_limiter.py

将 `src/utils/rate_limiter.py` 末尾的全局单例初始化改为：

```python
# 删除:
# import os
# default_rpm = float(os.getenv("LLM_MAX_RPM", "15"))

# 替换为:
from src.config import get_settings
_settings = get_settings()
default_rpm = float(getattr(_settings, "LLM_MAX_RPM", 15)) if hasattr(_settings, "LLM_MAX_RPM") else 15.0
llm_rate_limiter = AsyncRateLimiter(default_rpm)
```

**注意:** `LLM_MAX_RPM` 尚未在 `config.py` 的 `Settings` 中定义。需要在 `src/config.py` 的 `Settings` 类中添加：

```python
    LLM_MAX_RPM: float = 15.0
```

### Step 4: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_rate_limiter_config.py -v`
Expected: PASS

### Step 5: Commit

```bash
git add src/utils/rate_limiter.py src/config.py tests/unit/test_rate_limiter_config.py
git commit -m "fix: unify config source for rate_limiter

Replace os.getenv() with get_settings() in rate_limiter.py.
Add LLM_MAX_RPM to Settings class in config.py."
```

---

## Task 4: 消除 print()，统一 logger

**问题:** `categorizer.py` 和 `drafter.py` 使用 `print()` 代替 `logger`。生产环境中 `print()` 输出无时间戳、无级别，无法过滤。

**Files:**
- Modify: `src/nodes/categorizer.py`
- Modify: `src/nodes/drafter.py`
- Test: `tests/unit/test_no_print_statements.py`

### Step 1: Write the failing test

```python
# tests/unit/test_no_print_statements.py
import ast
import pytest

def _check_no_print_calls(filepath: str):
    """Parse file AST and ensure no bare print() calls exist."""
    with open(filepath, "r") as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "print":
                return False, f"print() found at line {node.lineno}"
    return True, "No print() found"

def test_categorizer_no_print():
    ok, msg = _check_no_print_calls("src/nodes/categorizer.py")
    assert ok, f"categorizer.py: {msg}"

def test_drafter_no_print():
    ok, msg = _check_no_print_calls("src/nodes/drafter.py")
    assert ok, f"drafter.py: {msg}"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_no_print_statements.py -v`
Expected: FAIL — both files contain `print()`.

### Step 3: Replace print with logger

**`src/nodes/categorizer.py` 改动：**

在文件顶部添加（若不存在）：
```python
import logging
logger = logging.getLogger(__name__)
```

替换所有 `print()`:
- Line 58: `print(f"Skipping LLM ...")` → `logger.info(f"Skipping LLM ...")`
- Line 118: `print(f"Tier 3 Classification success: ...")` → `logger.info(f"Tier 3 Classification success: ...")`
- Line 120: `print(f"Tier 3 Classification failed: ...")` → `logger.error(f"Tier 3 Classification failed: ...")`

同时移除未使用的 `import os`（第 2 行）。

**`src/nodes/drafter.py` 改动：**

在文件顶部添加：
```python
import logging
logger = logging.getLogger(__name__)
```

替换所有 `print()`:
- Line 57: `print(f"Applying System Prompt Modifier: ...")` → `logger.info(f"Applying System Prompt Modifier: ...")`
- Line 70: `print(f"Applying Memory Context: ...")` → `logger.info(f"Applying Memory Context: ...")`

同时移除未使用的 `import os`（第 1 行）。

### Step 4: Run test to verify it passes

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_no_print_statements.py -v`
Expected: PASS

### Step 5: Run existing tests

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_nodes.py tests/unit/test_langgraph_nodes.py -v --tb=short`
Expected: Previously passing tests still pass.

### Step 6: Commit

```bash
git add src/nodes/categorizer.py src/nodes/drafter.py tests/unit/test_no_print_statements.py
git commit -m "fix: replace print() with logger in categorizer and drafter

Use proper logging framework for production observability.
Also remove unused 'import os' from both files."
```

---

## Task 5: 删除 Tier 2 死代码

**问题:** `retriever_node.py` 的 Tier 2 块（lines 35-49）检查 Qdrant payload 中的 `skill_id`，但 `email_processor.py` 从未写入该字段。注释也承认"假设...后续添加"。此代码永远不会触发，是虚假安全感。

**Files:**
- Modify: `src/nodes/retriever_node.py`
- Test: 无需新测试（删除代码）

### Step 1: Remove dead code from retriever_node.py

删除 `retriever_node.py` 中以下内容（约 lines 31-59 中的 Tier 2 块和 RoutingEngine 调用）：

```python
# 删除整个 Tier 2 块:
    # --- Step: Tier 2 语义意图识别 ---
    active_skills = state.get("active_skills", []) or []
    routing_log = state.get("routing_log", []) or []
    
    potential_skills = []
    for res in results:
        sid = res.get("skill_id")
        score = res.get("score", 0)
        if sid and score > 0.8:
            potential_skills.append(sid)
    
    if potential_skills:
        from collections import Counter
        most_common_skill = Counter(potential_skills).most_common(1)[0][0]
        if most_common_skill not in active_skills:
            active_skills.append(most_common_skill)
            routing_log.append(f"Tier 2 Match via RAG Metadata: {most_common_skill}")

    # 应用新激活的 Skill (或之前 T1 遗留的 Skill)
    from src.router.engine import RoutingEngine
    router_engine = RoutingEngine()
    state["active_skills"] = list(set(active_skills))
    state["routing_log"] = routing_log
    state["context"] = results
    
    # 再次应用 Skills 逻辑
    state = await router_engine._apply_skills(state, state["active_skills"])
```

替换为简单的赋值：

```python
    state["context"] = results
```

**理由：** Tier 1 路由已在 `categorizer` 中通过 `get_routing_engine().execute_router()` 完成。Tier 2 实际未实现（无数据支撑）。Tier 3 也在 `execute_router` 中处理。在 retriever 中再次 `_apply_skills` 是冗余且有风险的（可能覆盖 categorizer 的分类结果）。

### Step 2: Run existing tests

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_rag_nodes.py tests/unit/test_retriever.py -v --tb=short`
Expected: PASS（这些测试 mock 了 retriever，不依赖 Tier 2 逻辑）

### Step 3: Commit

```bash
git add src/nodes/retriever_node.py
git commit -m "refactor: remove unimplemented Tier 2 dead code from retriever_node

Tier 2 semantic routing checked for skill_id in Qdrant payloads that were
never written during indexing. Also removes redundant _apply_skills call
that could overwrite categorizer's classification results."
```

---

## Task 6: 统一使用 retry_decorator

**问题:** `categorizer.py` 和 `drafter.py` 内联定义重复的 tenacity 重试逻辑，而 `retry_decorator.py` 提供了现成的 `@with_llm_retry` 但从未被使用。

**Files:**
- Modify: `src/nodes/categorizer.py`
- Modify: `src/nodes/drafter.py`
- Test: `tests/unit/test_retry_usage.py`

### Step 1: Write the failing test

```python
# tests/unit/test_retry_usage.py
import pytest
import inspect

def test_categorizer_no_inline_retry():
    """categorizer should not define its own retry logic."""
    import src.nodes.categorizer as mod
    source = inspect.getsource(mod)
    assert "from tenacity import" not in source, \
        "categorizer.py should use with_llm_retry from retry_decorator, not inline tenacity"

def test_drafter_no_inline_retry():
    """drafter should not define its own retry logic."""
    import src.nodes.drafter as mod
    source = inspect.getsource(mod)
    assert "from tenacity import" not in source, \
        "drafter.py should use with_llm_retry from retry_decorator, not inline tenacity"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_retry_usage.py -v`
Expected: FAIL — both files import tenacity directly.

### Step 3: Refactor categorizer.py

在 `src/nodes/categorizer.py` 中：

1. 删除 lines 87-101 的整个 tenacity import + `@retry` + `invoke_with_retry` 定义
2. 在文件顶部添加 `from src.utils.retry_decorator import with_llm_retry`
3. 在调用 LLM 前定义包装函数：

```python
    @with_llm_retry(max_attempts=3)
    async def invoke_with_retry(payload):
        return await chain.ainvoke(payload)
```

注意：`with_llm_retry` 内部已调用 `llm_rate_limiter.acquire()`，因此可以移除 categorizer.py 顶部的 `from src.utils.rate_limiter import llm_rate_limiter` 导入。

### Step 4: Refactor drafter.py

同理，在 `src/nodes/drafter.py` 中：

1. 删除 lines 87-102 的整个 tenacity import + `@retry` + `invoke_with_retry` 定义
2. 在文件顶部添加 `from src.utils.retry_decorator import with_llm_retry`
3. 替换为：

```python
    @with_llm_retry(max_attempts=3)
    async def invoke_with_retry(payload):
        return await chain.ainvoke(payload)
```

同样移除顶部的 `from src.utils.rate_limiter import llm_rate_limiter`。

### Step 5: Run tests

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_retry_usage.py tests/unit/test_retry_logic.py tests/unit/test_nodes.py -v --tb=short`
Expected: All PASS

### Step 6: Commit

```bash
git add src/nodes/categorizer.py src/nodes/drafter.py tests/unit/test_retry_usage.py
git commit -m "refactor: use centralized with_llm_retry in categorizer and drafter

Replace inline tenacity retry definitions with the existing @with_llm_retry
decorator from retry_decorator.py. Single source of truth for retry strategy."
```

---

## Task 7: 拆分 process_and_archive_email

**问题:** `exchange_service.py` 的 `process_and_archive_email` 是 170 行的万能函数，包含 DB 操作、Qdrant 索引、LangGraph 执行、附件上传、PDF 生成、卡片发送等全部逻辑。无法单独测试任何环节。

**Files:**
- Modify: `src/exchange_service.py`
- Test: `tests/unit/test_exchange_service_refactor.py`

### Step 1: Write test for the new structure

```python
# tests/unit/test_exchange_service_refactor.py
import pytest
import inspect

def test_process_function_is_short():
    """process_and_archive_email should be under 50 lines after refactor."""
    from src import exchange_service
    source = inspect.getsource(exchange_service.process_and_archive_email)
    line_count = len(source.strip().split('\n'))
    assert line_count < 60, f"process_and_archive_email is {line_count} lines, should be <60"

def test_helper_functions_exist():
    """Verify sub-functions have been extracted."""
    from src import exchange_service
    assert hasattr(exchange_service, '_ingest_to_qdrant'), "Missing _ingest_to_qdrant"
    assert hasattr(exchange_service, '_run_ai_pipeline'), "Missing _run_ai_pipeline"
    assert hasattr(exchange_service, '_dispatch_notification'), "Missing _dispatch_notification"
    assert hasattr(exchange_service, '_mark_email_read'), "Missing _mark_email_read"
```

### Step 2: Run test to verify it fails

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_exchange_service_refactor.py -v`
Expected: FAIL — function is ~170 lines, helper functions don't exist.

### Step 3: Extract helper functions

将 `process_and_archive_email` 拆分为 4 个私有函数。**不改变任何业务逻辑**，仅提取：

```python
async def _ingest_to_qdrant(email_id: str, email_data: dict, ctx) -> None:
    """Ingest email into Qdrant vector store."""
    # 原 lines 305-310 的 try/except 块

async def _run_ai_pipeline(email_id: str, email_data: dict, ctx, config: dict) -> Optional[dict]:
    """Run LangGraph pipeline. Returns classification dict or None on failure."""
    # 原 lines 312-342 的 graph streaming + error handling + circuit breaker

async def _dispatch_notification(email_id: str, classification: dict, ctx, config: dict) -> None:
    """Send Lark card based on card_type. Handles attachment upload and PDF generation."""
    # 原 lines 344-427 的 approval/read_only/none 分支

async def _mark_email_read(email_id: str, ctx) -> None:
    """Mark email as read on Exchange server."""
    # 原 lines 437-444 的 try/except 块
```

重写后的主函数：

```python
async def process_and_archive_email(email_data, ctx, skip_analysis: bool = False):
    """Process a single email: Ingest -> Analyze -> Notify -> Archive."""
    thread_id = email_data.get("id", str(time.time()))
    config = {"configurable": {"thread_id": thread_id}}

    logger.info(f"Processing email: {thread_id} - {email_data.get('subject')} (skip={skip_analysis})")

    # Normalize fields
    if not email_data.get('to') and email_data.get('to_recipients'):
        email_data['to'] = email_data['to_recipients']
    if not email_data.get('cc') and email_data.get('cc_recipients'):
        email_data['cc'] = email_data['cc_recipients']
    email_data.setdefault('to', [])
    email_data.setdefault('cc', [])

    is_new = await ctx.db_manager.log_initial_email(email_data)
    if not is_new:
        logger.info(f"Email {thread_id} already exists in DB.")
        await _mark_email_read(thread_id, ctx)
        return

    await _ingest_to_qdrant(thread_id, email_data, ctx)

    if skip_analysis:
        await ctx.db_manager.update_status(thread_id, "archived")
    else:
        classification = await _run_ai_pipeline(thread_id, email_data, ctx, config)
        if classification:
            await _dispatch_notification(thread_id, classification, ctx, config)

    await _mark_email_read(thread_id, ctx)
```

### Step 4: Run tests

Run: `cd "/Users/jarod/Library/CloudStorage/SynologyDrive-mac/Claude项目/AI邮件-CC" && python -m pytest tests/unit/test_exchange_service_refactor.py tests/unit/test_exchange_webhook.py tests/integration/ -v --tb=short`
Expected: All PASS

### Step 5: Commit

```bash
git add src/exchange_service.py tests/unit/test_exchange_service_refactor.py
git commit -m "refactor: extract sub-functions from process_and_archive_email

Split 170-line monolith into focused helpers:
- _ingest_to_qdrant: vector store indexing
- _run_ai_pipeline: LangGraph execution + circuit breaker
- _dispatch_notification: Lark card + PDF + attachment upload
- _mark_email_read: Exchange read status

No behavior changes. Main function now ~30 lines."
```

---

## 执行顺序与依赖关系

```
Task 1 (DB Pool)           ← 独立，最高优先
Task 2 (Instance Reuse)    ← 独立，最高优先
Task 3 (Config Unify)      ← 依赖 Task 1 完成（db_async 已改）
Task 4 (Print→Logger)      ← 独立
Task 5 (Dead Code)         ← 依赖 Task 2 完成（retriever_node 已改）
Task 6 (Retry Decorator)   ← 依赖 Task 4 完成（categorizer/drafter 已改）
Task 7 (Refactor Service)  ← 独立，但建议最后做
```

**推荐并行组：**
- 第一批 (并行): Task 1 + Task 2 + Task 4
- 第二批 (顺序): Task 3 → Task 5 → Task 6
- 第三批: Task 7

---

## 验证清单（全部完成后）

```bash
# 1. 全量测试
python -m pytest tests/ -v --tb=short

# 2. 路由准确率
python3 tests/eval_router.py

# 3. 导入完整性
python scripts/verify_imports.py

# 4. 健康检查（部署后）
curl http://localhost:8000/health
```
