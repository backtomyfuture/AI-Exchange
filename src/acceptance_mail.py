"""One-shot production acceptance mail with a closed recipient boundary."""

from __future__ import annotations

import json
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path


ACCEPTANCE_RECIPIENT = "q-fu@tianjin-air.com"


class AcceptanceMailRejected(RuntimeError):
    pass


def _claim_single_attempt(marker: Path, *, recipient: str) -> None:
    marker.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload = json.dumps(
        {
            "recipient": recipient,
            "claimed_at": datetime.now(UTC).isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    try:
        descriptor = os.open(
            marker,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
    except FileExistsError:
        raise AcceptanceMailRejected("acceptance_mail_already_attempted") from None
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


async def send_acceptance_mail_once(
    *,
    recipient: str,
    marker: Path,
    send: Callable[[str, str, str], Awaitable[bool]],
) -> None:
    """Issue at most one send request, and only to the authorized mailbox."""
    if type(recipient) is not str or recipient != ACCEPTANCE_RECIPIENT:
        raise AcceptanceMailRejected("acceptance_recipient_rejected")
    _claim_single_attempt(marker, recipient=recipient)
    accepted = await send(
        recipient,
        "[AI-Exchange 验收] 绿地部署自测",
        (
            "<p>这是一封 AI-Exchange 绿地部署验收邮件。</p>"
            "<p>请验证：Durable Inbox、规范路由决策、飞书卡片及已读闭环。</p>"
        ),
    )
    if accepted is not True:
        raise AcceptanceMailRejected("acceptance_send_not_accepted")


__all__ = [
    "ACCEPTANCE_RECIPIENT",
    "AcceptanceMailRejected",
    "send_acceptance_mail_once",
]
