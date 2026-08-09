from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass


logger = logging.getLogger(__name__)

_OPEN_ID_RE = re.compile(r"^open_id\s*=\s*(?P<value>.+)$", re.IGNORECASE)
_MAILBOX_EMAIL_RE = re.compile(
    r"email_address\s*=\s*(?P<quote>['\"])(?P<value>.*?)(?P=quote)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ResolvedRecipients:
    """The one normalized recipient projection used by send and draft-save."""

    to: tuple[str, ...]
    cc: tuple[str, ...]


def normalize_recipient_address(value: object) -> str | None:
    """Return one bounded mailbox address, rejecting ambiguous input."""
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if (
        not candidate
        or len(candidate.encode("utf-8")) > 320
        or any(ord(character) < 32 or ord(character) == 127 for character in candidate)
        or any(character in candidate for character in (",", ";"))
    ):
        return None

    if "<" in candidate or ">" in candidate:
        display_match = re.fullmatch(r"[^<>]*<([^<>]+)>", candidate)
        if not display_match:
            return None
        address = display_match.group(1).strip()
    else:
        address = candidate

    if (
        address.count("@") != 1
        or any(character.isspace() for character in address)
        or any(character in address for character in "()<>[],:;\\\"")
    ):
        return None
    local_part, domain = address.rsplit("@", 1)
    if (
        not local_part
        or not domain
        or len(local_part.encode("utf-8")) > 64
        or len(domain.encode("utf-8")) > 253
        or local_part.startswith(".")
        or local_part.endswith(".")
        or domain.startswith(".")
        or domain.endswith(".")
        or ".." in local_part
        or ".." in domain
    ):
        return None
    allowed_local_punctuation = frozenset(".!#$%&'*+/=?^_`{|}~-")
    if any(
        not character.isalnum() and character not in allowed_local_punctuation
        for character in local_part
    ):
        return None
    for label in domain.split("."):
        if (
            not label
            or len(label.encode("utf-8")) > 63
            or label.startswith("-")
            or label.endswith("-")
            or any(
                not character.isalnum() and character != "-"
                for character in label
            )
        ):
            return None
    return address


def _deduplicate(values: Sequence[str]) -> tuple[str, ...]:
    """Preserve input order while removing repeated normalized addresses."""
    return tuple(dict.fromkeys(values))


async def _resolve_lark_open_id(
    open_id: str,
    *,
    lark_client: object | None,
) -> str | None:
    if not open_id or lark_client is None:
        return None
    try:
        from lark_oapi.api.contact.v3 import GetUserRequest

        request = (
            GetUserRequest.builder()
            .user_id(open_id)
            .user_id_type("open_id")
            .build()
        )
        response = await asyncio.to_thread(
            lark_client.contact.v3.user.get,
            request,
        )
        if response.success() and response.data and response.data.user:
            user = response.data.user
            return normalize_recipient_address(
                getattr(user, "enterprise_email", None)
                or getattr(user, "email", None)
            )
    except Exception as exc:
        logger.error(
            "Recipient contact lookup failed: error_type=%s",
            type(exc).__name__,
        )
    return None


async def resolve_recipient(
    recipient: object,
    *,
    lark_client: object | None = None,
) -> str | None:
    """Resolve one approval recipient to a validated Exchange address.

    Accepted values are a normal address, RFC display-name syntax, the
    serialized Exchange ``Mailbox(..., email_address=...)`` form, or a
    ``open_id=...`` value resolved through the supplied Lark client. Any
    malformed or unavailable value fails closed.
    """
    if recipient is None:
        return None
    try:
        value = str(recipient).strip()
    except Exception as exc:
        logger.error(
            "Recipient conversion failed: error_type=%s",
            type(exc).__name__,
        )
        return None
    if not value:
        return None

    open_id_match = _OPEN_ID_RE.fullmatch(value)
    if open_id_match:
        return await _resolve_lark_open_id(
            open_id_match.group("value").strip(),
            lark_client=lark_client,
        )

    mailbox_match = _MAILBOX_EMAIL_RE.search(value)
    if mailbox_match:
        value = mailbox_match.group("value").strip()
    return normalize_recipient_address(value)


def _recipient_values(value: object) -> list[object] | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return None


async def resolve_recipients(
    raw_to: object,
    raw_cc: object,
    *,
    lark_client: object | None = None,
) -> ResolvedRecipients | None:
    """Resolve and validate a complete To/CC projection, failing closed.

    ``To`` must contain at least one valid address. ``Cc`` may be empty, but
    every supplied entry must resolve successfully. The result is immutable so
    send and save-draft callers cannot accidentally diverge after resolution.
    """
    to_values = _recipient_values(raw_to)
    cc_values = _recipient_values(raw_cc)
    if to_values is None or cc_values is None:
        return None

    resolved_to = [
        await resolve_recipient(value, lark_client=lark_client)
        for value in to_values
    ]
    resolved_cc = [
        await resolve_recipient(value, lark_client=lark_client)
        for value in cc_values
    ]
    if (
        not resolved_to
        or any(value is None for value in resolved_to)
        or any(value is None for value in resolved_cc)
    ):
        return None

    final_to = _deduplicate([value for value in resolved_to if value is not None])
    final_cc = _deduplicate([value for value in resolved_cc if value is not None])
    if not final_to:
        return None
    return ResolvedRecipients(to=final_to, cc=final_cc)


__all__ = [
    "ResolvedRecipients",
    "normalize_recipient_address",
    "resolve_recipient",
    "resolve_recipients",
]
