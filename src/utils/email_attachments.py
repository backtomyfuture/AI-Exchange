"""Classify inbound email attachments at the human-review boundary."""

from collections.abc import Mapping
from typing import Any


def is_inline_attachment(
    attachment: Mapping[str, Any],
    *,
    body: str = "",
) -> bool:
    """Return whether an attachment belongs inside the rendered email body."""
    explicit_inline = attachment.get("is_inline")
    if explicit_inline is True:
        return True
    if explicit_inline is False:
        return False

    raw_content_id = attachment.get("content_id")
    if not isinstance(raw_content_id, str):
        return False
    content_id = raw_content_id.strip().strip("<>").casefold()
    if not content_id:
        return False

    normalized_body = body.casefold()
    return (
        f"cid:{content_id}" in normalized_body
        or f"cid:<{content_id}>" in normalized_body
    )


def select_business_attachments(email_data: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return standalone files that should be surfaced with a Feishu delivery."""
    attachments = email_data.get("attachments")
    if not isinstance(attachments, list):
        return []
    body = email_data.get("body")
    normalized_body = body if isinstance(body, str) else ""
    return [
        attachment
        for attachment in attachments
        if isinstance(attachment, dict)
        and not is_inline_attachment(attachment, body=normalized_body)
    ]
