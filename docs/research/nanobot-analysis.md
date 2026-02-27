# HKUDS/nanobot — Comprehensive Architecture Analysis

> **Repository**: https://github.com/HKUDS/nanobot
> **Author**: HKUDS (Data Intelligence Lab, University of Hong Kong)
> **License**: MIT
> **Language**: Python 96.6% | TypeScript 1.6% | Shell 1.3%
> **Stars**: 25,700+ | **Forks**: 4,060+
> **Latest Release**: v0.1.4.post2 (February 24, 2026)
> **Research Date**: February 27, 2026

---

## 1. What is nanobot? What Problem Does It Solve?

nanobot is an **ultra-lightweight personal AI assistant framework** that delivers core agent functionality in approximately **3,932 lines of verified Python code** — 99% smaller than OpenClaw's 430,000+ line codebase. It was released on February 2, 2026.

### Problem Statement

Most AI agent frameworks (OpenClaw, Open Interpreter, etc.) are massive, opaque systems where developers cannot easily understand, audit, or modify the agent's decision-making pipeline. nanobot takes the opposite approach: **radical minimalism and transparency**.

### Core Philosophy

> "What is the simplest agent that actually works?"

Inspired by Bruce Lee's philosophy of "removing the unessential," nanobot prioritizes three principles:

1. **Readability** — The entire pipeline can be read end-to-end in an afternoon
2. **Modifiability** — The minimal skeleton makes it easy to understand and change
3. **Controllability** — You can track when tools get called, which files are modified, and why

### Key Metrics

| Metric | nanobot | OpenClaw |
|:-------|:--------|:---------|
| Core code | ~3,932 lines | ~430,000 lines |
| Startup time | 0.8s | 5.98s |
| RAM usage | ~100MB / 45MB | ~1.52GB |
| Language | Python | TypeScript |

---

## 2. Core Architecture & Design Patterns

### 2.1 The Agentic Loop (Heart of the System)

The entire agent is a straightforward while-loop in `nanobot/agent/loop.py`:

```
Message arrives → Context assembly → LLM decision → Tool execution → Result backfill → Response returns
```

The `AgentLoop._run_agent_loop()` method implements this:

```python
while iteration < self.max_iterations:
    iteration += 1
    response = await self.provider.chat(messages, tools, model, temperature, max_tokens)

    if response.has_tool_calls:
        # Execute each tool call, append results to messages
        for tool_call in response.tool_calls:
            result = await self.tools.execute(tool_call.name, tool_call.arguments)
            messages = self.context.add_tool_result(messages, tool_call.id, tool_call.name, result)
    else:
        # No tool calls = final response
        final_content = response.content
        break
```

**Key design decisions**:
- Maximum 40 iterations per turn (configurable)
- Tool results are truncated to 500 chars in session storage (but full results are used in-conversation)
- `<think>` blocks from reasoning models (DeepSeek-R1, etc.) are stripped from output
- Progress streaming sends tool execution hints to the user in real-time

### 2.2 Message Bus Architecture

nanobot uses an **async message bus** (`nanobot/bus/queue.py`) to decouple channels from the agent core:

```
[Telegram] ──┐
[Discord]  ──┤──→ MessageBus (inbound queue) ──→ AgentLoop ──→ MessageBus (outbound queue) ──→ [ChannelManager]
[WhatsApp] ──┤                                                                                     ├──→ [Telegram]
[CLI]      ──┘                                                                                     ├──→ [Discord]
                                                                                                   └──→ [WhatsApp]
```

The bus uses two `asyncio.Queue` instances:
- **Inbound**: channels push messages; agent consumes
- **Outbound**: agent pushes responses; `ChannelManager._dispatch_outbound()` routes to the correct channel

This is elegantly simple — no message broker, no Redis, no external dependencies. Just Python's built-in async queues.

### 2.3 Event Types

Two dataclasses define all communication:

```python
@dataclass
class InboundMessage:
    channel: str          # "telegram", "discord", "cli", "system"
    sender_id: str
    chat_id: str
    content: str
    media: list[str]      # Image URLs
    metadata: dict        # Channel-specific (e.g., message_id for replies)
    session_key_override: str | None  # For thread-scoped sessions

@dataclass
class OutboundMessage:
    channel: str
    chat_id: str
    content: str
    reply_to: str | None
    media: list[str]
    metadata: dict        # _progress, _tool_hint flags for streaming
```

---

## 3. File/Directory Structure

```
nanobot/
├── agent/                    # Core agent engine (1,285 lines)
│   ├── loop.py               # The agentic loop — message processing, tool execution
│   ├── context.py            # System prompt builder (identity, memory, skills, bootstrap files)
│   ├── memory.py             # Two-layer memory: MEMORY.md + HISTORY.md
│   ├── skills.py             # Skills loader (workspace + builtin skills)
│   ├── subagent.py           # Background sub-agent manager
│   └── tools/                # Built-in tool implementations (1,166 lines)
│       ├── base.py           # Abstract Tool class with JSON Schema validation
│       ├── registry.py       # ToolRegistry — register, execute, get definitions
│       ├── filesystem.py     # ReadFile, WriteFile, EditFile, ListDir tools
│       ├── shell.py          # ExecTool — shell command execution
│       ├── web.py            # WebSearch (Brave API), WebFetch tools
│       ├── message.py        # MessageTool — send to specific channel/chat
│       ├── spawn.py          # SpawnTool — launch background sub-agents
│       ├── cron.py           # CronTool — schedule/manage recurring tasks
│       └── mcp.py            # MCP server connection and tool bridge
│
├── bus/                      # Message bus (88 lines)
│   ├── events.py             # InboundMessage, OutboundMessage dataclasses
│   └── queue.py              # MessageBus — async inbound/outbound queues
│
├── channels/                 # Platform integrations (excluded from core count)
│   ├── base.py               # BaseChannel abstract class
│   ├── manager.py            # ChannelManager — init, route, start/stop
│   ├── telegram.py           # Telegram Bot API
│   ├── discord.py            # Discord gateway (raw WebSocket)
│   ├── whatsapp.py           # WhatsApp via bridge WebSocket
│   ├── feishu.py             # Feishu/Lark WebSocket
│   ├── dingtalk.py           # DingTalk Stream
│   ├── slack.py              # Slack Socket Mode
│   ├── email.py              # IMAP email polling
│   ├── qq.py                 # QQ Bot
│   ├── mochat.py             # Mochat
│   └── matrix.py             # Matrix/Element (E2EE support)
│
├── config/                   # Configuration system (469 lines)
│   ├── schema.py             # Pydantic models (camelCase ↔ snake_case)
│   └── loader.py             # Load from ~/.nanobot/config.json
│
├── providers/                # LLM provider abstraction (excluded from core count)
│   ├── base.py               # LLMProvider ABC, LLMResponse, ToolCallRequest
│   ├── registry.py           # ProviderSpec dataclass + PROVIDERS tuple (15+ providers)
│   ├── litellm_provider.py   # LiteLLM-based provider (handles most providers)
│   ├── custom_provider.py    # Direct OpenAI-compatible endpoint
│   ├── openai_codex_provider.py  # OAuth-based Codex provider
│   └── transcription.py      # Voice-to-text (Groq Whisper)
│
├── cron/                     # Scheduling system (432 lines)
│   ├── service.py            # CronService — timer-based job scheduler
│   └── types.py              # CronJob, CronSchedule, CronPayload dataclasses
│
├── heartbeat/                # Periodic task checker (178 lines)
│   └── service.py            # HeartbeatService — reads HEARTBEAT.md, asks LLM to decide
│
├── session/                  # Session management (217 lines)
│   └── manager.py            # JSONL-based session storage with consolidation pointer
│
├── skills/                   # Built-in skills (SKILL.md files)
│   ├── memory/               # Memory management instructions
│   ├── github/               # GitHub CLI integration
│   ├── weather/              # Weather lookups
│   ├── summarize/            # URL/file/YouTube summarization
│   ├── tmux/                 # tmux session control (with shell scripts)
│   ├── clawhub/              # Skill registry search/install
│   ├── cron/                 # Cron scheduling instructions
│   └── skill-creator/        # Create new skills
│
├── templates/                # Default workspace files
│   ├── AGENTS.md             # Agent instructions
│   ├── SOUL.md               # Personality definition
│   ├── USER.md               # User profile template
│   ├── TOOLS.md              # Tool usage guide
│   ├── HEARTBEAT.md          # Heartbeat task definitions
│   └── memory/MEMORY.md      # Initial memory template
│
├── utils/                    # Shared utilities (83 lines)
│   └── helpers.py            # ensure_dir, safe_filename, etc.
│
├── cli/                      # CLI entry point
│   └── commands.py           # Typer app — agent, gateway, onboard, status commands
│
├── __init__.py               # Version, logo
└── __main__.py               # python -m nanobot support

tests/                        # Test suite
├── test_tool_validation.py
├── test_cron_service.py
├── test_heartbeat_service.py
├── test_memory_consolidation_types.py
├── test_context_prompt_cache.py
├── test_message_tool.py
├── test_commands.py
└── ...

bridge/                       # WhatsApp bridge (TypeScript)
case/                         # Example configurations
pyproject.toml                # Package definition (hatchling build)
Dockerfile                    # Container deployment
docker-compose.yml            # One-command deployment
core_agent_lines.sh           # Line count verification script
```

---

## 4. Key Technical Features

### 4.1 Tool System

The tool system is remarkably clean. Every tool extends `Tool` (ABC):

```python
class Tool(ABC):
    @property
    def name(self) -> str: ...
    @property
    def description(self) -> str: ...
    @property
    def parameters(self) -> dict: ...      # JSON Schema
    async def execute(self, **kwargs) -> str: ...
```

The `ToolRegistry` manages registration and execution:
- Tools self-describe using OpenAI function-calling format
- Parameter validation uses recursive JSON Schema checking (built-in, no external library)
- Tool results always return strings
- Errors include a `[Analyze the error above and try a different approach.]` hint to guide the LLM

**Default tools** (registered in `AgentLoop.__init__`):
1. `read_file` — Read file contents
2. `write_file` — Write file
3. `edit_file` — Patch/edit file
4. `list_dir` — List directory
5. `exec` — Execute shell commands (configurable timeout, optional workspace restriction)
6. `web_search` — Brave Search API
7. `web_fetch` — Fetch and parse web pages (readability extraction)
8. `message` — Send to specific channel/chat
9. `spawn` — Launch background sub-agents
10. `cron` — Schedule/manage recurring tasks

### 4.2 Memory System ("Less is More")

The memory system was explicitly designed to avoid complexity. From [GitHub Discussion #566](https://github.com/HKUDS/nanobot/discussions/566):

**Two files, zero dependencies:**

| File | Purpose | Loaded into context? |
|:-----|:--------|:--------------------|
| `memory/MEMORY.md` | Long-term facts (identity, preferences, project context) | Always (in system prompt) |
| `memory/HISTORY.md` | Append-only timestamped event log | On-demand via `grep` |

**Why `grep` over RAG:**
1. **Composable** — works in any shell, OS, or context
2. **Zero cost** — no embedding API calls, database hosting, or index maintenance
3. **Auditable** — human-readable files instead of opaque vector databases
4. **Deterministic** — same query always produces same results

**Auto-consolidation flow** (when `unconsolidated >= memory_window`):
1. Extract old messages beyond the consolidation pointer
2. Send to LLM with a `save_memory` tool call
3. LLM returns: `history_entry` (appended to HISTORY.md) + `memory_update` (replaces MEMORY.md)
4. Advance the consolidation pointer

The `memory_window` defaults to 100 messages. The consolidation keeps the most recent `memory_window / 2` messages intact.

### 4.3 Session Management

Sessions are stored as **JSONL files** in `{workspace}/sessions/`:
- First line: metadata (key, timestamps, `last_consolidated` pointer)
- Subsequent lines: individual messages

Key design: **Messages are append-only** for LLM prompt cache efficiency. Consolidation writes to MEMORY.md/HISTORY.md but does NOT modify the messages list. The `last_consolidated` pointer tracks which messages have been processed.

### 4.4 Provider Registry System

Adding a new LLM provider requires exactly 2 steps:
1. Add a `ProviderSpec` dataclass to `PROVIDERS` tuple in `providers/registry.py`
2. Add a config field to `ProvidersConfig` in `config/schema.py`

The `ProviderSpec` is a frozen dataclass with:
- Identity fields (name, keywords, env_key)
- LiteLLM prefix mapping (e.g., `deepseek-chat` → `deepseek/deepseek-chat`)
- Gateway/local detection (API key prefix, base URL keyword matching)
- Per-model parameter overrides (e.g., Kimi K2.5 requires `temperature >= 1.0`)
- OAuth flag for Codex/Copilot providers
- Prompt caching support flag

**Currently supported providers** (15+):
- Gateways: OpenRouter, AiHubMix, SiliconFlow, VolcEngine
- Standard: Anthropic, OpenAI, DeepSeek, Gemini, Zhipu, DashScope, Moonshot, MiniMax
- OAuth: OpenAI Codex, GitHub Copilot
- Local: vLLM
- Auxiliary: Groq (mainly for Whisper transcription)

### 4.5 Channel/Gateway System

Each channel extends `BaseChannel` (ABC):

```python
class BaseChannel(ABC):
    name: str
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, msg: OutboundMessage) -> None: ...
    def is_allowed(self, sender_id: str) -> bool: ...
```

Channels are lazily imported — if a dependency isn't installed, the channel is silently skipped.

**Access control**: Each channel has an `allow_from` list. Empty list = allow everyone. This is checked in `BaseChannel._handle_message()` before forwarding to the bus.

**Supported platforms** (12):
Telegram, Discord, WhatsApp, Feishu/Lark, DingTalk, Slack, Email (IMAP), QQ, Mochat, Matrix/Element

### 4.6 Sub-Agent System

The `SubagentManager` spawns background agents for complex tasks:

- Sub-agents run in `asyncio.create_task()` — non-blocking
- They get a subset of tools: filesystem, shell, web (NO message tool, NO spawn — prevents recursion)
- Limited to 15 iterations (vs 40 for the main agent)
- When complete, results are announced via the bus as a "system" message
- Session-scoped: can be cancelled with `/stop`

The announce mechanism is clever — the result is injected as an `InboundMessage` with `channel="system"`, which triggers the main agent to summarize and deliver the result naturally.

### 4.7 Cron/Scheduling System

The cron system is self-contained (no external scheduler daemon):

- Jobs stored as JSON in `{workspace}/cron_jobs.json`
- Supports three schedule types:
  - `at` — one-time execution at a specific timestamp
  - `every` — recurring at fixed intervals
  - `cron` — standard cron expressions (uses `croniter`)
- Timer-based execution: computes next wake time, sets `asyncio.sleep`
- Timezone-aware cron expressions (ZoneInfo)
- Jobs can auto-delete after execution (`delete_after_run`)

### 4.8 Heartbeat System

A periodic wake-up mechanism that reads `HEARTBEAT.md` every 30 minutes:

1. **Phase 1 (Decision)**: Sends HEARTBEAT.md content to LLM with a virtual `heartbeat` tool
2. LLM calls `heartbeat(action="skip")` or `heartbeat(action="run", tasks="...")`
3. **Phase 2 (Execution)**: If `run`, executes tasks through the full agent loop
4. Delivers results to configured channel

This replaces brittle regex-based task detection with structured tool-call output.

### 4.9 MCP (Model Context Protocol) Support

Native MCP integration connects external tool servers:
- Configured in `config.json` under `mcp_servers`
- Lazy connection on first message (`_connect_mcp()`)
- MCP tools are registered into the same `ToolRegistry` as built-in tools
- Transparent to the LLM — MCP tools appear alongside native tools

### 4.10 Skills System

Skills are markdown files (`SKILL.md`) that teach the agent how to use specific tools:

- **Workspace skills** (`{workspace}/skills/`) — highest priority
- **Built-in skills** (`nanobot/skills/`) — shipped with the package
- Skills have YAML frontmatter with metadata (name, description, requirements, `always` flag)
- Skills marked `always: true` are always loaded into the system prompt
- Other skills are listed as summaries; the agent reads them on-demand via `read_file`
- Requirements checking: skills can require CLI binaries (`bins`) and env vars (`env`)
- Compatible with OpenClaw's skill format

### 4.11 Context Building

The `ContextBuilder` assembles the system prompt from multiple layers:

```
System Prompt = Identity + Bootstrap Files + Memory + Always-Skills + Skills Summary
```

1. **Identity**: Runtime info (OS, Python version, workspace path), core guidelines
2. **Bootstrap files**: `AGENTS.md`, `SOUL.md`, `USER.md`, `TOOLS.md`, `IDENTITY.md` (from workspace)
3. **Memory**: MEMORY.md content (long-term facts)
4. **Always-skills**: Full content of skills marked `always: true`
5. **Skills summary**: XML-formatted list of all available skills (name, description, path, availability)

Additionally, a **runtime context** block is injected before the user message with:
- Current timestamp and timezone
- Channel and chat ID

### 4.12 Prompt Caching

The system supports Anthropic-style prompt caching:
- Providers with `supports_prompt_caching=True` (Anthropic, OpenRouter) get cache breakpoints
- The system prompt and early messages are marked with `cache_control: {"type": "ephemeral"}`
- This significantly reduces token costs for long conversations

---

## 5. Configuration

Configuration lives in `~/.nanobot/config.json` (JSON, supports both camelCase and snake_case):

```json
{
  "providers": {
    "openrouter": { "apiKey": "sk-or-..." },
    "anthropic": { "apiKey": "sk-ant-..." }
  },
  "agents": {
    "default": { "model": "anthropic/claude-opus-4-5" }
  },
  "channels": {
    "telegram": { "enabled": true, "token": "...", "allowFrom": ["12345"] },
    "discord": { "enabled": true, "token": "..." }
  },
  "tools": {
    "braveApiKey": "BSA..."
  }
}
```

The Pydantic schema (`config/schema.py`) provides:
- Type validation
- Default values
- CamelCase ↔ snake_case alias generation

---

## 6. How It Differs from Other Agent Frameworks

### vs. OpenClaw (430K lines, TypeScript)

| Aspect | nanobot | OpenClaw |
|:-------|:--------|:---------|
| Code size | ~4K lines | ~430K lines |
| Language | Python | TypeScript |
| Architecture | Single agentic loop | Complex orchestration |
| Memory | 2 text files + grep | Vector DB + RAG |
| Extensibility | Skills (markdown) | Plugins (npm packages) |
| Deployment | `pip install` | `npm install` |
| Resource usage | ~100MB RAM | ~1.5GB RAM |
| Channel plugins | 12 built-in | 38+ via plugin system |

### vs. PicoClaw (Go, embedded)

PicoClaw optimizes for edge devices (Raspberry Pi, NanoKVM) with a single Go binary under 10MB. nanobot sits between PicoClaw's extreme minimalism and OpenClaw's full-featured approach.

### Key Differentiators

1. **Transparency**: The entire agent pipeline is auditable. No black-box decisions.
2. **No vector DB**: Memory uses plain text files + grep. Zero external infrastructure.
3. **Provider-agnostic**: 15+ LLM providers via a simple registry pattern.
4. **Research-friendly**: Clean enough to use as a teaching tool for AI agent architecture.
5. **Progressive skill loading**: Skills are summarized in context but loaded on-demand.
6. **OpenClaw compatibility**: Skills follow OpenClaw format — skills can be shared.

---

## 7. Notable Design Decisions

### 7.1 Text Files Over Databases

The entire persistence layer is flat files:
- Sessions: JSONL files
- Memory: Markdown files
- Cron jobs: JSON file
- Configuration: JSON file

No database, no migrations, no ORM. This makes deployment trivial and debugging easy.

### 7.2 Append-Only Sessions

Session messages are never modified or deleted. The `last_consolidated` pointer tracks which messages have been summarized to memory. This preserves LLM prompt cache efficiency — the beginning of the conversation never changes.

### 7.3 LLM-as-Decision-Engine Pattern

Both the heartbeat system and memory consolidation use the same pattern: send context to the LLM with a structured tool call, parse the tool call arguments. This avoids brittle regex/token parsing of free-text LLM output.

### 7.4 Lazy Imports for Channels

Channel dependencies (telegram, discord, etc.) are imported inside `if enabled:` blocks. This means you only need the dependencies for the channels you actually use. Missing a dependency silently skips that channel.

### 7.5 No Framework Lock-in

nanobot doesn't use LangChain, LlamaIndex, or any agent framework. It implements the agentic loop directly using the OpenAI function-calling format. This keeps the dependency tree small and the behavior predictable.

### 7.6 Sub-Agent Sandboxing

Sub-agents cannot send messages to users or spawn other sub-agents. This prevents recursive agent spawning and unexpected user communication from background tasks.

---

## 8. Dependencies

Core dependencies (from `pyproject.toml`):

| Package | Purpose |
|:--------|:--------|
| `litellm` | Multi-provider LLM routing |
| `pydantic` + `pydantic-settings` | Configuration validation |
| `typer` + `rich` | CLI interface |
| `loguru` | Logging |
| `httpx` | HTTP client |
| `websockets` + `websocket-client` | Channel connections |
| `croniter` | Cron expression parsing |
| `readability-lxml` | Web page content extraction |
| `json-repair` | Malformed LLM JSON response repair |
| `mcp` | Model Context Protocol client |
| `prompt-toolkit` | Interactive CLI input |
| Channel SDKs | python-telegram-bot, lark-oapi, slack-sdk, etc. |

Dev dependencies: `pytest`, `pytest-asyncio`, `ruff`

---

## 9. Testing

Tests are in `/tests/` and use `pytest` with `pytest-asyncio`:
- `asyncio_mode = "auto"` (no manual event loop management)
- Tests cover: tool validation, cron service, heartbeat, memory consolidation, context building, CLI commands, channel integrations

---

## 10. Deployment Options

1. **PyPI**: `pip install nanobot-ai`
2. **uv**: `uv tool install nanobot-ai`
3. **Source**: `git clone && pip install -e .`
4. **Docker**: `docker compose up` (Dockerfile + docker-compose.yml included)

---

## 11. Relevance to Our Project (AI Email Assistant)

Several nanobot patterns could improve our AI Email Assistant:

### Applicable Patterns

1. **Provider Registry**: Our `llm_factory.py` could adopt the `ProviderSpec` pattern for cleaner multi-provider support
2. **Memory system simplification**: Our Qdrant-based RAG could be supplemented with a simple `MEMORY.md` for user preferences/rules
3. **Message Bus decoupling**: The async queue pattern could clean up our Lark ↔ Exchange coupling
4. **Skill system**: Our `skills_registry/` already follows a similar pattern — nanobot's markdown-based approach is simpler
5. **Session management**: JSONL sessions with consolidation pointers are simpler than database-backed sessions
6. **Heartbeat pattern**: LLM-as-decision-engine for periodic tasks (our self-healing could use this)

### Key Differences

Our system is fundamentally different in scope:
- **nanobot**: General-purpose personal assistant (chat-driven)
- **Our system**: Domain-specific email processing pipeline (event-driven)

nanobot's architecture assumes interactive conversations with a user. Our system processes emails autonomously through a LangGraph pipeline. The patterns above can be borrowed, but the overall architecture should remain pipeline-oriented.

---

## 12. External Resources

- **GitHub**: https://github.com/HKUDS/nanobot
- **Website**: https://nanobot.club
- **Architecture Teardown (Medium)**: https://jinlow.medium.com/nanobot-architecture-teardown-4-000-lines-achieving-openclaw-capability-3f242113ccbc
- **Memory Design Discussion**: https://github.com/HKUDS/nanobot/discussions/566
- **DeepWiki Documentation**: https://deepwiki.com/HKUDS/nanobot
- **Comparison Article**: https://medium.com/@somanathtv/openclaw-vs-nanobot-vs-picoclaw-a-brief-technical-comparison-for-ai-agent-builders-9d19089a414b
- **Tutorial**: https://blogs.nionee.com/build-an-agent-with-nanobot-lighter-replacement-for-openclaw/
