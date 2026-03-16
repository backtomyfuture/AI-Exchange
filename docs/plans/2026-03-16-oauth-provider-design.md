# OAuth Provider 层增强设计

**日期：** 2026-03-16
**动机：** 当前 ai-exchange 的 LLM provider 只支持 API Key 认证，无法接入 OAuth 模型（OpenAI Codex、Gemini CLI）。借鉴 nanobot 的 provider 架构，增强灵活性。

## 痛点

1. **快速切换模型** — per-role 配置已有，但缺少 OAuth 类模型支持
2. **OAuth 模型接入** — OpenAI Codex 和 Gemini CLI 使用 OAuth token，当前无法接入
3. **动态添加 provider** — 新增 provider 应尽量只改注册表，不改业务代码

## 借鉴来源

nanobot 的 `providers/` 模块：
- `ProviderSpec` 的 `is_oauth` / `is_direct` 标志
- `OpenAICodexProvider` 的 OAuth token 获取 + SSE 流处理
- `oauth_cli_kit` 库的 token 管理

## 设计

### 1. ProviderSpec 扩展

在 `src/providers/registry.py` 的 `ProviderSpec` 增加字段：

```python
is_oauth: bool = False    # 使用 OAuth 流而非 API Key
is_direct: bool = False   # 绕过 ChatOpenAI，使用自定义 BaseChatModel 实现
```

新增注册条目：

```python
ProviderSpec(
    name="openai_codex",
    display_name="OpenAI Codex",
    keywords=("openai-codex",),
    env_key="",
    default_base_url="https://chatgpt.com/backend-api/codex/responses",
    is_oauth=True,
    is_direct=True,
)

ProviderSpec(
    name="gemini_cli",
    display_name="Gemini CLI",
    keywords=("gemini-cli",),
    env_key="",
    default_base_url="https://generativelanguage.googleapis.com/v1beta/openai",
    is_oauth=True,
    is_direct=True,
)
```

### 2. OAuth Provider 基类

新建 `src/providers/oauth_base.py`：

```python
class OAuthChatModel(BaseChatModel):
    """LangChain 兼容的 OAuth chat model 基类。

    继承 BaseChatModel 而非绕过 LangChain，原因：
    - 所有节点通过 ChatModel 接口调用 LLM
    - 无缝替换 ChatOpenAI，节点代码零改动

    子类实现：
    - _get_token() → 获取/刷新 OAuth token
    - _call_api() → 调用模型 API
    """
```

### 3. 具体实现

**`src/providers/codex_provider.py`** — 移植 nanobot 的实现：
- 使用 `oauth_cli_kit.get_token` 获取 Codex OAuth token
- SSE 流处理 Responses API
- 消息格式转换（OpenAI Chat → Codex input items）
- SSL 证书容错

**`src/providers/gemini_cli_provider.py`** — Gemini CLI OAuth：
- 读取 `~/.gemini/oauth_creds.json` 中的 refresh token
- 标准 Google OAuth token 刷新
- 调用 OpenAI-compatible 端点（比 Codex 简单）

### 4. Factory 层改造

修改 `src/providers/factory.py` 的 `get_llm()`：

```python
def get_llm(...) -> BaseChatModel:  # 返回类型从 ChatOpenAI 改为 BaseChatModel
    spec = match_provider(model, ...)

    if spec and spec.is_oauth:
        return _create_oauth_model(spec, model, temperature, **kwargs)

    return ChatOpenAI(**final_kwargs)  # 现有逻辑不变
```

`_create_oauth_model()` 通过注册表映射延迟导入对应 provider 类。

### 5. 配置方式

```bash
# .env — 通过 model name 前缀自动匹配 OAuth provider
LLM_DRAFTER_MODEL=openai-codex/gpt-5.1-codex
LLM_CATEGORIZER_MODEL=gemini-cli/gemini-2.5-flash
LLM_MODEL=gemini-3-flash  # 其他角色不受影响
```

### 6. 依赖

```toml
[project.optional-dependencies]
codex = ["oauth-cli-kit>=0.1.3"]
```

Gemini CLI token 刷新只需 httpx（已有依赖）。

## 改动清单

| 文件 | 改动类型 | 说明 |
|------|----------|------|
| `src/providers/registry.py` | 修改 | ProviderSpec 加字段 + 2 个新条目 |
| `src/providers/oauth_base.py` | 新建 | OAuth BaseChatModel 基类 |
| `src/providers/codex_provider.py` | 新建 | 移植 nanobot Codex 实现 |
| `src/providers/gemini_cli_provider.py` | 新建 | Gemini CLI OAuth 实现 |
| `src/providers/factory.py` | 修改 | 加 OAuth 分支，返回类型改 BaseChatModel |
| `pyproject.toml` | 修改 | 加可选依赖 |

## 不改动

- 所有 graph 节点（categorizer、drafter、reviewer、sender）
- LangGraph 状态机
- 现有 API Key provider 的行为
