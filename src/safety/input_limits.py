from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class InputLimits:
    webhook_bytes: int = 1_048_576
    exchange_response_bytes: int = 67_108_864
    body_bytes: int = 10_485_760
    attachment_count: int = 20
    attachment_single_bytes: int = 26_214_400
    attachment_total_bytes: int = 52_428_800


class InputLimitExceeded(ValueError):
    def __init__(self, category: str) -> None:
        self.category = category
        super().__init__(category)


def _integer_setting(settings: Any, name: str, default: int) -> int:
    value = getattr(settings, name, None)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return default


def input_limits_from_settings(settings: Any) -> InputLimits:
    defaults = InputLimits()
    return InputLimits(
        webhook_bytes=_integer_setting(
            settings,
            "WEBHOOK_MAX_BYTES",
            defaults.webhook_bytes,
        ),
        exchange_response_bytes=_integer_setting(
            settings,
            "EXCHANGE_RESPONSE_MAX_BYTES",
            defaults.exchange_response_bytes,
        ),
        body_bytes=_integer_setting(
            settings,
            "EMAIL_BODY_MAX_BYTES",
            defaults.body_bytes,
        ),
        attachment_count=_integer_setting(
            settings,
            "EMAIL_ATTACHMENT_MAX_COUNT",
            defaults.attachment_count,
        ),
        attachment_single_bytes=_integer_setting(
            settings,
            "EMAIL_ATTACHMENT_SINGLE_MAX_BYTES",
            defaults.attachment_single_bytes,
        ),
        attachment_total_bytes=_integer_setting(
            settings,
            "EMAIL_ATTACHMENT_TOTAL_MAX_BYTES",
            defaults.attachment_total_bytes,
        ),
    )


def _attachment_size_upper_bound(attachment: Mapping[str, Any]) -> int:
    encoded = attachment.get("content")
    if isinstance(encoded, (str, bytes)) and encoded:
        return len(encoded) * 3 // 4

    declared_size = attachment.get("size")
    if (
        isinstance(declared_size, int)
        and not isinstance(declared_size, bool)
        and declared_size >= 0
    ):
        return declared_size

    return 0


def validate_email_input(email: Mapping[str, Any], limits: InputLimits) -> None:
    body = email.get("body") or ""
    if not isinstance(body, str):
        body = str(body)
    if len(body.encode("utf-8")) > limits.body_bytes:
        raise InputLimitExceeded("body_bytes")

    attachments = email.get("attachments") or []
    if len(attachments) > limits.attachment_count:
        raise InputLimitExceeded("attachment_count")

    sizes = [
        _attachment_size_upper_bound(attachment)
        for attachment in attachments
        if isinstance(attachment, Mapping)
    ]
    if sum(sizes) > limits.attachment_total_bytes:
        raise InputLimitExceeded("attachment_total_bytes")

    if any(size > limits.attachment_single_bytes for size in sizes):
        raise InputLimitExceeded("attachment_single_bytes")
