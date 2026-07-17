import json
from dataclasses import dataclass
from typing import Any, Iterable

from src.safety.input_limits import InputLimitExceeded


@dataclass(frozen=True)
class TokenBudget:
    max_input_tokens: int
    max_output_tokens: int
    max_total_tokens: int


class ModelInputTooLarge(InputLimitExceeded):
    def __init__(self, role: str) -> None:
        self.role = role
        super().__init__("model_input_tokens")


def conservative_token_upper_bound(value: str) -> int:
    return len(value.encode("utf-8"))


def enforce_model_input_budget(
    role: str,
    value: str,
    *,
    budget: TokenBudget,
) -> None:
    actual = conservative_token_upper_bound(value)
    if (
        actual > budget.max_input_tokens
        or actual + budget.max_output_tokens > budget.max_total_tokens
    ):
        raise ModelInputTooLarge(role)


def _positive_integer_setting(settings: Any, name: str, default: int) -> int:
    value = getattr(settings, name, None)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return default


def token_budget_from_settings(settings: Any) -> TokenBudget:
    return TokenBudget(
        max_input_tokens=_positive_integer_setting(
            settings,
            "LLM_MAX_INPUT_TOKENS",
            122_880,
        ),
        max_output_tokens=_positive_integer_setting(
            settings,
            "LLM_MAX_OUTPUT_TOKENS",
            8_192,
        ),
        max_total_tokens=_positive_integer_setting(
            settings,
            "LLM_MAX_TOTAL_TOKENS",
            131_072,
        ),
    )


def rendered_messages_for_budget(messages: Iterable[Any]) -> str:
    rendered: list[str] = []
    for message in messages:
        role = getattr(message, "type", message.__class__.__name__)
        content = getattr(message, "content", message)
        if not isinstance(content, str):
            try:
                content = json.dumps(content, ensure_ascii=False, sort_keys=True)
            except (TypeError, ValueError):
                content = str(content)
        rendered.append(f"{role}: {content}")
    return "\n".join(rendered)
