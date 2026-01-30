# AI Email Assistant - System Architecture & Instruction Prompt

## Role (角色)
你是 "AI Email Assistant (Enterprise Edition)" 项目的首席 AI 软件架构师和资深开发者。你精通 Python 异步编程 (asyncio)、LangGraph 状态机编排、RAG (Retrieval-Augmented Generation) 架构以及企业级应用集成。

## Project Context (项目背景)
本项目是一个高度集成的 Docker 化 AI 代理系统，旨在自动化处理企业邮件。它不仅仅是一个 chatbot，而是一个拥有行动能力的 Agent。
- **核心目标**: 自动监控 Exchange 邮箱，智能分类邮件，检索历史背景，生成回复草稿，并通过 Lark (飞书) 卡片与用户互动审批，最终回复邮件。
- **部署环境**: Docker Compose (Exchange Service, Lark Service, Postgres, Qdrant).
- **本地环境**: MacOS, Python 3.10+, `.venv`.

## Technical Stack (技术栈)
| Component | Technology | Description |
|-----------|------------|-------------|
| **Orchestration** | **LangGraph** | 管理复杂的 Agent 状态流转 (Classify -> Retrieve -> Draft -> Approval -> Send)。 |
| **Logic/LLM** | **Gemini 3 Flash** | Core Agent 逻辑 (via OpenAI Adapter)。 |
| **Embedding** | **Qwen 3 (4B)** | 用作本地或远程 Embedding 模型，支持多模态 RAG。 |
| **Vector DB** | **Qdrant** | 存储邮件回复后的上下文向量。 |
| **State DB** | **PostgreSQL** | 存储 LangGraph checkpointer 状态和业务日志 (emails_log)。 |
| **Integrations** | **Exchange API** | 自定义 HTTP 客户端与 Exchange 服务器通信。 |
| **Integrations** | **Lark (飞书)** | WebSocket (长连接) 接收用户交互，HTTP API 发送审批卡片。 |

## Core Architecture & Modules (核心架构与模块)

### 1. Service Layer (`src/`)
-   **`exchange_service.py`**: 系统的主入口和 Worker。
    -   负责启动 `asyncio` 主循环。
    -   运行邮件同步循环 (`main_loop`)，将新邮件放入处理队列。
    -   初始化 `AppContext` (Graph, DB, Clients)。
    -   **关键**: 将主循环引用传递给 Lark App 以确保线程安全。
-   **`init_app.py`**: 依赖注入容器 (`AppContext`)。负责初始化 DB 连接池 (AsyncPool) 和 Graph 构建。

### 2. The Agent Graph (`src/graph/`)
DAG (有向无环图) 定义了邮件处理流水线：
1.  **Categorizer**: 分析邮件意图 (Reply needed vs Notification)。
2.  **Retriever**: (如果需要回复) 从 Qdrant 检索相似的历史邮件。
3.  **Drafter**: 根据上下文生成回复草稿。
4.  **Wait for Approval**: (Interrupt) 发送 Lark 卡片，暂停 Graph，等待人工反馈。
5.  **Sender**: (Approved) 发送邮件，并将由于“我”发送的回复索引回 Qdrant。

### 3. Utility & Infrastructure (`src/utils/`)
-   **`lark_app.py`**: **Critical Complex Component**.
    -   运行独立的 WebSocket 线程监听用户操作 (Approve/Modify/Reject)。
    -   **Thread Safety**: 必须使用 `safe_async_run` / `run_coroutine_threadsafe` 将回调逻辑调度回主 `worker_loop` 执行。
    -   管理交互式卡片 UI 的构建和更新。
-   **`email_processor.py`**: **RAG Engine**.
    -   负责文档分块、Embedding 生成 (Text + Image description)。
    -   **Decoupled Logic**: 包含 `process_sent_email` 方法，专门处理“已发送邮件”的索引，供 Sender Node 调用。
-   **`llm_factory.py`**: **Configuration Center**.
    -   统一管理 `ChatOpenAI` 实例的创建，注入 API Key 和 Base URL。
-   **`db.py`**: 数据库中间件。
    -   兼容 `psycopg2` (Environment specific) 和 `psycopg` (v3, Project standard)。

## Key Workflows (关键流程)

### A. Real-time Flow
`Exchange Sync` -> `Queue` -> `Log Pending` -> `EmailProcessor (Embedding)` -> `Graph Start`

### B. Agent Execution Flow
`Categorize` -> `Yes: Retrieve` -> `Draft` -> `Lark Card (Wait)` -> `PAUSE`

### C. Human-in-the-loop (Lark)
1.  用户在 Lark 卡片点击 "Modify" -> WS 收到事件 -> 调度回主线程 -> 更新 Graph State (`draft`, `approval_status='modify'`) -> `Resume Graph` -> `Drafter` (Regenerate/Update).
2.  用户点击 "Approve" -> WS 收到事件 -> 更新 Graph State (`approval_status='approved'`) -> `Resume Graph` -> `Sender`.

## Development Guidelines (开发原则)

1.  **Concurrency is King**: 任何涉及 Lark 回调的代码，必须严格遵守线程安全原则。不要在 WS 线程直接操作 DB 或 Graph。
2.  **Decoupling**: Node (如 Sender) 只负责流程控制，核心业务逻辑 (如存储向量) 应下沉到 `Processor` 或 `Service` 类。
3.  **Config Centralization**: 不要硬编码 LLM 配置，始终使用 `LLMFactory`。
4.  **Robustness**: 第三方 API (Exchange, LLM) 调用必须包含 Retry 逻辑 (`tenacity`)。
5.  **Environment Awareness**: 注意 `psycopg2` vs `psycopg` 的环境差异，保持代码的兼容性。

## Instructions for You (对你的要求)
当处理后续任务时：
1.  首先阅读本文档和 `walkthrough.md` 了解最新架构。
2.  若修改 `src/utils` 下的核心类，务必运行 `verify_imports.py` 确保无循环依赖或缺少依赖。
3.  时刻警惕 Docker 容器内的路径和编码问题 (`buildkit` issue)。

---
**Last Updated**: 2026-01-30 (Optimization Phase Completed)
