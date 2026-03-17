"""
LLM Provider Registry — single source of truth for provider metadata.

Ported from nanobot's provider registry pattern. Adding a new provider:
  1. Add a ProviderSpec to PROVIDERS below.
  2. (Optional) Add an API key field to Settings.
  Done. Model auto-matching, env resolution, and status display all derive from here.

Order matters — it controls match priority. Gateways first.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ProviderSpec:
    """Metadata for one LLM provider."""

    name: str
    display_name: str = ""
    keywords: tuple[str, ...] = ()
    env_key: str = ""
    default_base_url: str = ""
    default_model: str = ""

    is_gateway: bool = False
    is_local: bool = False
    detect_by_key_prefix: str = ""
    detect_by_base_keyword: str = ""

    supports_json_mode: bool = False
    supports_vision: bool = False
    supports_function_calling: bool = True

    model_overrides: tuple[tuple[str, dict[str, Any]], ...] = ()

    is_oauth: bool = False
    is_direct: bool = False

    @property
    def label(self) -> str:
        return self.display_name or self.name.title()

    def get_model_kwargs(self, model: str) -> dict[str, Any]:
        """Return provider- and model-specific parameter overrides."""
        for pattern, kwargs in self.model_overrides:
            if pattern in model:
                return dict(kwargs)
        return {}


PROVIDERS: tuple[ProviderSpec, ...] = (

    # === Gateways (route any model, checked first) ========================

    ProviderSpec(
        name="openrouter",
        display_name="OpenRouter",
        keywords=(),
        env_key="OPENROUTER_API_KEY",
        default_base_url="https://openrouter.ai/api/v1",
        is_gateway=True,
        detect_by_key_prefix="sk-or-",
        detect_by_base_keyword="openrouter",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="aihubmix",
        display_name="AiHubMix",
        keywords=(),
        env_key="AIHUBMIX_API_KEY",
        default_base_url="https://aihubmix.com/v1",
        is_gateway=True,
        detect_by_base_keyword="aihubmix",
        supports_json_mode=True,
        supports_vision=True,
    ),

    # === OAuth Providers (use OAuth flow, bypass ChatOpenAI) ================
    # 必须在 Major Cloud Providers 之前，避免 openai-codex 被 openai 关键字拦截

    ProviderSpec(
        name="openai_codex",
        display_name="OpenAI Codex",
        keywords=("openai-codex",),
        env_key="",
        default_base_url="https://chatgpt.com/backend-api/codex/responses",
        is_oauth=True,
        is_direct=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="gemini_cli",
        display_name="Gemini CLI",
        keywords=("gemini-cli",),
        env_key="",
        default_base_url="https://generativelanguage.googleapis.com/v1beta/openai",
        is_oauth=True,
        is_direct=True,
        supports_json_mode=True,
        supports_vision=True,
    ),

    # === Major Cloud Providers ============================================

    ProviderSpec(
        name="openai",
        display_name="OpenAI",
        keywords=("gpt-", "o1-", "o3-", "o4-", "chatgpt-"),
        env_key="OPENAI_API_KEY",
        default_base_url="https://api.openai.com/v1",
        default_model="gpt-4o",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="anthropic",
        display_name="Anthropic",
        keywords=("claude-",),
        env_key="ANTHROPIC_API_KEY",
        default_base_url="https://api.anthropic.com/v1",
        default_model="claude-4-sonnet",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="google",
        display_name="Google Gemini",
        keywords=("gemini-",),
        env_key="GOOGLE_API_KEY",
        default_base_url="https://generativelanguage.googleapis.com/v1beta/openai",
        default_model="gemini-2.5-flash",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="deepseek",
        display_name="DeepSeek",
        keywords=("deepseek-",),
        env_key="DEEPSEEK_API_KEY",
        default_base_url="https://api.deepseek.com/v1",
        default_model="deepseek-chat",
        supports_json_mode=True,
        supports_vision=False,
        model_overrides=(
            ("deepseek-reasoner", {"temperature": 1.0}),
        ),
    ),

    ProviderSpec(
        name="xai",
        display_name="xAI (Grok)",
        keywords=("grok-",),
        env_key="XAI_API_KEY",
        default_base_url="https://api.x.ai/v1",
        default_model="grok-3",
        supports_json_mode=True,
        supports_vision=True,
    ),

    # === Chinese Cloud Providers ==========================================

    ProviderSpec(
        name="dashscope",
        display_name="Alibaba (Qwen)",
        keywords=("qwen-", "qwen2", "qwen3"),
        env_key="DASHSCOPE_API_KEY",
        default_base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        default_model="qwen-plus",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="zhipu",
        display_name="Zhipu (GLM)",
        keywords=("glm-",),
        env_key="ZHIPUAI_API_KEY",
        default_base_url="https://open.bigmodel.cn/api/paas/v4",
        default_model="glm-4-plus",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="moonshot",
        display_name="Moonshot (Kimi)",
        keywords=("moonshot-", "kimi-"),
        env_key="MOONSHOT_API_KEY",
        default_base_url="https://api.moonshot.cn/v1",
        default_model="moonshot-v1-auto",
        supports_json_mode=True,
        supports_vision=False,
        model_overrides=(
            ("kimi-k2", {"temperature": 1.0}),
        ),
    ),

    ProviderSpec(
        name="minimax",
        display_name="MiniMax",
        keywords=("minimax-", "abab-", "MiniMax-"),
        env_key="MINIMAX_API_KEY",
        default_base_url="https://api.minimax.chat/v1",
        default_model="MiniMax-Text-01",
        supports_json_mode=True,
        supports_vision=False,
    ),

    ProviderSpec(
        name="baichuan",
        display_name="Baichuan",
        keywords=("baichuan-",),
        env_key="BAICHUAN_API_KEY",
        default_base_url="https://api.baichuan-ai.com/v1",
        default_model="Baichuan4-Turbo",
        supports_json_mode=True,
        supports_vision=False,
    ),

    ProviderSpec(
        name="stepfun",
        display_name="Stepfun",
        keywords=("step-",),
        env_key="STEPFUN_API_KEY",
        default_base_url="https://api.stepfun.com/v1",
        default_model="step-2-16k",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="volcengine",
        display_name="Volcengine (Doubao)",
        keywords=("doubao-", "ep-"),
        env_key="VOLCENGINE_API_KEY",
        default_base_url="https://ark.cn-beijing.volces.com/api/v3",
        default_model="doubao-1.5-pro-32k",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="spark",
        display_name="iFlytek Spark",
        keywords=("spark-", "generalv"),
        env_key="SPARK_API_KEY",
        default_base_url="https://spark-api-open.xf-yun.com/v1",
        default_model="spark-max",
        supports_json_mode=True,
        supports_vision=False,
    ),

    ProviderSpec(
        name="siliconflow",
        display_name="SiliconFlow",
        keywords=(),
        env_key="SILICONFLOW_API_KEY",
        default_base_url="https://api.siliconflow.cn/v1",
        is_gateway=True,
        detect_by_base_keyword="siliconflow",
        supports_json_mode=True,
        supports_vision=True,
    ),

    # === International Cloud Providers ====================================

    ProviderSpec(
        name="mistral",
        display_name="Mistral",
        keywords=("mistral-", "codestral-", "pixtral-"),
        env_key="MISTRAL_API_KEY",
        default_base_url="https://api.mistral.ai/v1",
        default_model="mistral-large-latest",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="groq",
        display_name="Groq",
        keywords=(),
        env_key="GROQ_API_KEY",
        default_base_url="https://api.groq.com/openai/v1",
        detect_by_base_keyword="groq",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="together",
        display_name="Together AI",
        keywords=(),
        env_key="TOGETHER_API_KEY",
        default_base_url="https://api.together.xyz/v1",
        detect_by_base_keyword="together",
        supports_json_mode=True,
        supports_vision=False,
    ),

    ProviderSpec(
        name="fireworks",
        display_name="Fireworks",
        keywords=(),
        env_key="FIREWORKS_API_KEY",
        default_base_url="https://api.fireworks.ai/inference/v1",
        detect_by_base_keyword="fireworks",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="cohere",
        display_name="Cohere",
        keywords=("command-",),
        env_key="COHERE_API_KEY",
        default_base_url="https://api.cohere.com/compatibility/v1",
        default_model="command-a-03-2025",
        supports_json_mode=True,
        supports_vision=False,
    ),

    ProviderSpec(
        name="cerebras",
        display_name="Cerebras",
        keywords=(),
        env_key="CEREBRAS_API_KEY",
        default_base_url="https://api.cerebras.ai/v1",
        detect_by_base_keyword="cerebras",
        supports_json_mode=True,
        supports_vision=False,
    ),

    # === Local / Self-hosted ==============================================

    ProviderSpec(
        name="ollama",
        display_name="Ollama",
        keywords=(),
        env_key="",
        default_base_url="http://localhost:11434/v1",
        is_local=True,
        detect_by_base_keyword="11434",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="vllm",
        display_name="vLLM",
        keywords=(),
        env_key="",
        default_base_url="http://localhost:8001/v1",
        is_local=True,
        detect_by_base_keyword="vllm",
        supports_json_mode=True,
        supports_vision=True,
    ),

    ProviderSpec(
        name="lmstudio",
        display_name="LM Studio",
        keywords=(),
        env_key="",
        default_base_url="http://localhost:1234/v1",
        is_local=True,
        detect_by_base_keyword="1234",
        supports_json_mode=True,
        supports_vision=True,
    ),
)

_PROVIDER_MAP: dict[str, ProviderSpec] = {p.name: p for p in PROVIDERS}


def get_provider(name: str) -> ProviderSpec | None:
    return _PROVIDER_MAP.get(name)


def match_provider(
    model: str,
    api_key: str = "",
    api_base: str = "",
) -> ProviderSpec | None:
    """Auto-detect provider from model name, API key prefix, or base URL."""
    model_lower = model.lower()
    base_lower = (api_base or "").lower()

    # Pass 1: gateway detection by key prefix or base URL keyword
    for spec in PROVIDERS:
        if spec.detect_by_key_prefix and api_key.startswith(spec.detect_by_key_prefix):
            return spec
        if spec.is_gateway and spec.detect_by_base_keyword and spec.detect_by_base_keyword in base_lower:
            return spec

    # Pass 2: keyword matching on model name
    for spec in PROVIDERS:
        if spec.is_gateway:
            continue
        for kw in spec.keywords:
            if kw.lower() in model_lower:
                return spec

    # Pass 3: local provider detection by base URL
    for spec in PROVIDERS:
        if spec.is_local and spec.detect_by_base_keyword and spec.detect_by_base_keyword in base_lower:
            return spec

    return None


def list_providers_status(configured_keys: dict[str, str]) -> list[dict]:
    """Return provider status list for diagnostics."""
    result = []
    for spec in PROVIDERS:
        has_key = bool(configured_keys.get(spec.env_key)) if spec.env_key else spec.is_local
        result.append({
            "name": spec.name,
            "display_name": spec.label,
            "configured": has_key,
            "env_key": spec.env_key,
            "default_model": spec.default_model,
            "is_gateway": spec.is_gateway,
            "is_local": spec.is_local,
        })
    return result
