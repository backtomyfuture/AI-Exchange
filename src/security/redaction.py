"""Small source-level redaction helpers for security-sensitive log paths."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Collection


_SAFE_NAMESPACE = re.compile(r"[^a-zA-Z0-9_.-]+")
_SAFE_METADATA = re.compile(r"[a-zA-Z0-9_.:-]+")
_MAX_METADATA_LENGTH = 64


def fingerprint_identifier(value: object, *, namespace: str = "id") -> str:
    """Return a stable, single-line fingerprint without retaining the value."""

    safe_namespace = _SAFE_NAMESPACE.sub("_", str(namespace).strip())[:24] or "id"
    raw = "" if value is None else str(value)
    digest = hashlib.sha256(
        safe_namespace.encode("utf-8") + b"\0" + raw.encode("utf-8")
    ).hexdigest()[:16]
    return f"{safe_namespace}:{digest}"


def exception_type(exc: BaseException) -> str:
    """Return only the exception class name, never its potentially sensitive text."""

    return type(exc).__name__


def safe_log_metadata(
    value: object,
    *,
    allowed_values: Collection[str],
    fallback: str = "other",
    max_length: int = _MAX_METADATA_LENGTH,
) -> str:
    """Return only a bounded, explicitly allowlisted log metadata token."""

    limit = min(max(int(max_length), 1), _MAX_METADATA_LENGTH)
    candidate = value if isinstance(value, str) else ""
    allowed = {
        item
        for item in allowed_values
        if isinstance(item, str)
        and len(item) <= limit
        and _SAFE_METADATA.fullmatch(item)
    }
    if (
        candidate in allowed
        and len(candidate) <= limit
        and _SAFE_METADATA.fullmatch(candidate)
    ):
        return candidate

    safe_fallback = fallback if isinstance(fallback, str) else "other"
    if (
        len(safe_fallback) > limit
        or not _SAFE_METADATA.fullmatch(safe_fallback)
    ):
        safe_fallback = "other"
    return safe_fallback
