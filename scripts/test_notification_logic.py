#!/usr/bin/env python3
"""Smoke-test notification routing against the current Task 7 contract."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# Allow direct execution from any working directory.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.exchange_service import _dispatch_notification

logger = logging.getLogger(__name__)


async def run_notification_smoke() -> list[dict[str, object]]:
    """Exercise approval/read-only/skip routing without production services."""
    ctx = SimpleNamespace(
        db_manager=SimpleNamespace(update_status=AsyncMock()),
        email_processor=SimpleNamespace(update_email_labels=MagicMock()),
    )
    cases = [
        {
            "classification": {
                "priority": "P1",
                "intent": "通知",
                "need_reply": False,
            },
            "email": {"subject": "Urgent Notice"},
            "draft": "",
            "context": [],
        },
        {
            "classification": {
                "priority": "P2",
                "intent": "通知",
                "need_reply": False,
            },
            "email": {
                "subject": "General Notification",
                "to": ["notification-smoke@example.test"],
            },
        },
        {
            "classification": {
                "priority": "P3",
                "intent": "垃圾邮件",
                "need_reply": False,
            },
            "email": {"subject": "Spam"},
        },
    ]

    with patch(
        "src.utils.notification_policy.get_settings",
        return_value=SimpleNamespace(
            EXCHANGE_ACCOUNT_EMAIL="notification-smoke@example.test",
            LEADER_SENDERS="",
        ),
    ), patch(
        "src.exchange_service.lark_app.generate_and_upload_pdf",
        new=AsyncMock(return_value=None),
    ), patch(
        "src.exchange_service.lark_app.send_read_only_card",
        return_value=True,
    ), patch(
        "src.exchange_service.lark_app.send_approval_card",
        return_value=True,
    ):
        return [
            await _dispatch_notification(
                f"notification-smoke-{index}",
                pipeline_result,
                ctx,
                {},
            )
            for index, pipeline_result in enumerate(cases, start=1)
        ]


async def main() -> None:
    results = await run_notification_smoke()
    expected = ["read_only", "read_only", "skipped"]
    actual = [result["kind"] for result in results]
    if actual != expected or not all(result["delivered"] for result in results):
        raise SystemExit(f"notification smoke failed: {actual}")
    logger.info("Notification smoke passed: kinds=%s", actual)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
