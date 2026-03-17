"""
LLM Factory — creates LangChain chat models with provider auto-detection.

Replaces the old LLMFactory static method with provider-aware creation
that supports per-role model selection and multi-provider API keys.
同时支持 API Key provider（通过 ChatOpenAI）和 OAuth provider
（通过 OAuthChatModel 子类）。
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_openai import ChatOpenAI

from src.config import get_settings, resolve_secret
from src.providers.registry import ProviderSpec, match_provider

logger = logging.getLogger(__name__)

# Role name → Settings field suffix mapping
_ROLE_MODEL_FIELDS = {
    "categorizer": "LLM_CATEGORIZER_MODEL",
    "drafter": "LLM_DRAFTER_MODEL",
    "reviewer": "LLM_REVIEWER_MODEL",
    "router": "LLM_ROUTER_MODEL",
    "summary": "LLM_SUMMARY_MODEL",
    "consolidator": "LLM_CONSOLIDATOR_MODEL",
}

# OAuth provider name → (module_path, class_name)
_OAUTH_PROVIDERS: dict[str, tuple[str, str]] = {
    "openai_codex": ("src.providers.codex_provider", "CodexChatModel"),
    "gemini_cli": ("src.providers.gemini_cli_provider", "GeminiCliChatModel"),
}


def _resolve_model_for_role(role: str) -> str:
    """Resolve the model name for a given role, falling back to default."""
    settings = get_settings()
    if role and role in _ROLE_MODEL_FIELDS:
        override = getattr(settings, _ROLE_MODEL_FIELDS[role], "")
        if override:
            return override
    return settings.LLM_MODEL


def _resolve_provider_credentials(
    spec: ProviderSpec | None,
    model: str,
) -> tuple[str, str]:
    """Resolve API key and base URL for a provider.

    Priority:
      1. Provider-specific env var (e.g. ANTHROPIC_API_KEY)
      2. Global OPENAI_API_KEY / OPENAI_API_BASE (backward compatible)
      3. Provider default base URL
    """
    settings = get_settings()
    import os

    api_key = ""
    base_url = ""

    if spec:
        if spec.env_key:
            api_key = os.environ.get(spec.env_key, "")
        if not api_key and spec.name != "custom":
            provider_setting = f"{spec.name.upper()}_API_KEY"
            setting_val = getattr(settings, provider_setting, None)
            if setting_val:
                api_key = resolve_secret(setting_val)

        base_url = spec.default_base_url

    if not api_key:
        api_key = resolve_secret(settings.OPENAI_API_KEY)
    if settings.OPENAI_API_BASE:
        base_url = settings.OPENAI_API_BASE

    return api_key, base_url


def _create_oauth_model(
    spec: ProviderSpec,
    model: str,
    temperature: float,
    **kwargs: Any,
) -> BaseChatModel:
    """通过延迟导入创建 OAuth-based chat model。"""
    if spec.name not in _OAUTH_PROVIDERS:
        raise ValueError(f"未注册 OAuth provider 类: '{spec.name}'")

    module_path, class_name = _OAUTH_PROVIDERS[spec.name]
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)

    logger.info("创建 OAuth LLM: model=%s, provider=%s", model, spec.label)
    return cls(model_name=model, temperature=temperature, **kwargs)


def get_llm(
    model: str | None = None,
    temperature: float = 0.7,
    role: str = "",
    **kwargs: Any,
) -> BaseChatModel:
    """创建 chat model，自动检测 provider。

    API Key provider 返回 ChatOpenAI；OAuth provider（Codex、Gemini CLI）
    返回对应的 OAuthChatModel 子类。

    Args:
        model: 模型名称。None 时从 role 或默认配置解析。
        temperature: 采样温度。
        role: 功能角色（categorizer、drafter 等），用于 per-role 模型覆盖。
        **kwargs: 透传给 ChatOpenAI 或 OAuthChatModel 的额外参数。
    """
    settings = get_settings()

    if model is None:
        model = _resolve_model_for_role(role)

    api_key = resolve_secret(settings.OPENAI_API_KEY)
    base_url = settings.OPENAI_API_BASE

    spec = match_provider(model, api_key=api_key, api_base=base_url)

    # OAuth provider 不走 ChatOpenAI
    if spec and spec.is_oauth:
        return _create_oauth_model(spec, model, temperature, **kwargs)

    resolved_key, resolved_base = _resolve_provider_credentials(spec, model)

    if not resolved_key:
        logger.warning(
            "No API key found for model '%s' (provider=%s). LLM calls will likely fail.",
            model,
            spec.name if spec else "unknown",
        )

    model_kwargs = {}
    if spec:
        model_kwargs = spec.get_model_kwargs(model)
        if model_kwargs.get("temperature") is not None:
            temperature = model_kwargs.pop("temperature")

    final_kwargs = {
        "model": model,
        "temperature": temperature,
        "base_url": resolved_base or None,
        "api_key": resolved_key or "not-set",
        "max_retries": 2,
        "timeout": 60,
        **model_kwargs,
        **kwargs,
    }

    if spec:
        logger.debug(
            "Creating LLM: model=%s, provider=%s, base_url=%s",
            model, spec.label, resolved_base,
        )

    return ChatOpenAI(**final_kwargs)


def get_llm_for_role(
    role: str,
    temperature: float = 0.7,
    **kwargs: Any,
) -> BaseChatModel:
    """Shorthand: create an LLM for a specific functional role."""
    return get_llm(model=None, temperature=temperature, role=role, **kwargs)
