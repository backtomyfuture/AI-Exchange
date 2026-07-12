# AI Email Assistant (Enterprise Edition)

这是一个为企业环境设计的高级 AI 邮件处理系统。它利用 Gemini 3 Flash 进行智能推理，Qdrant 作为向量数据库（RAG），并使用 LangGraph 编排复杂的 Agent 工作流。系统通过飞书 (Lark) 与用户进行审批互动。

## 核心功能

1.  **自动化同步**: 自动轮询 Exchange 邮箱，抓取新邮件。
2.  **智能分类**: 使用 LLM 识别邮件意图，判断是否需要回复及其紧急程度。
3.  **多模态 RAG**: 支持提取邮件文本和图片描述，并存储在 Qdrant 中作为历史背景。
4.  **人机协作 (Human-in-the-Loop)**:
    -   系统生成回复草稿并通过飞书交互式卡片推送给用户。
    -   用户可以点击“通过”、“拒绝”或“修改”建议。
5.  **自动回复 & 归档**: 审批通过后，系统自动发送邮件并将回复内容索引回 Qdrant。

## 系统架构

-   **编排层**: `LangGraph` (基于 Postgres 的状态持久化)。
-   **向量数据库**: `Qdrant` (存储邮件嵌入向量，支持语义搜索)。
-   **推理引擎**: `Gemini 3 Flash` (通过 OpenAI Adapter 调用)。
-   **集成端口**:
    -   `Exchange API`: 自定义适配器，处理邮件收发和状态更新。
    -   `Lark (飞书)`: 采用 WebSocket (长连接) 监听回调，HTTP API 发送交互卡片。

## 分离式服务

项目在部署时分为两个核心容器服务：
-   **`exchange-service`**: 负责邮件同步循环、初步分类、RAG 检索和生成初始草稿并推送卡片。
-   **`lark-service`**: 运行 WebSocket 监听器，接收用户在飞书卡片上的操作，并根据反馈恢复/更新 LangGraph 状态机。

## 快速开始

### 1. 环境准备

安装依赖（MacOS 环境）:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置环境变量

直接在本机运行 Python 时，复制 `.env.runtime.example` 到 `.env.runtime`；
使用生产 Compose 时，复制 `.env.example` 到 `.env`，并将 migration-owner
完整 DSN 单独写入 `MIGRATION_DATABASE_URL_FILE` 指向的 0600 文件。不要把
admin 或 migration 凭据放入运行时配置。随后填入：
-   Exchange API 认证信息
-   飞书 App ID & Secret
-   Gemini API Key & Base URL
-   Postgres & Qdrant 连接信息

### 3. 运行系统

使用 Docker Compose 启动完整环境：
```bash
# 禁止在 catalog role verifier（Task 1B0-B）完成前执行下一行。
# 角色创建和 ownership 转移也必须先由独立 DBA checkpoint 完成。
docker compose --profile migration run --rm database-bootstrap
docker compose up -d
```

或者本地分进程启动：
-   运行主同步服务: `python -m src.exchange_service`
-   运行飞书监听服务: `python -m src.lark_service`

## 目录结构说明

-   `src/graph`: 定义 LangGraph 状态机和工作流逻辑。
-   `src/nodes`: 工作流中的各个功能节点（分类、检索、草稿、发送）。
-   `src/utils`: 核心组件（Exchange 客户端、飞书应用、数据库管理器、RAG 引擎）。
-   `src/scripts`: 辅助脚本（模型检查、Exchange API 测试等）。

## 运维建议

-   **监控日志**: 核心日志输出在控制台，可通过 Docker logs 查看。
-   **数据库管理**: Postgres 存储了所有的 LangGraph checkpoints，支持在服务重启后恢复处理中的任务。
-   **模型切换**: 推荐使用 Gemini 3 Flash 以获得最佳的性价比平衡。

---
**Last Updated**: 2026-01-30
