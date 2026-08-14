from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from src.maintenance.approval_sla import (
    APPROVAL_SLA,
    ApprovalExpiryService,
    ExpiredApproval,
)


class FakeRepository:
    def __init__(self, rows: list[ExpiredApproval] | None = None) -> None:
        self.rows = list(rows or [])
        self.claimed: list[str] = []

    async def list_expired_approvals(self, *, older_than: datetime, limit: int):
        return [row for row in self.rows if row.waiting_since < older_than][:limit]

    async def expire_approval(
        self,
        email_id: str,
        *,
        error_code: str,
        inbox_id: str | None = None,
        expected_version: int | None = None,
    ) -> bool:
        self.claimed.append(email_id)
        remaining = [row for row in self.rows if row.email_id != email_id]
        claimed = len(remaining) != len(self.rows)
        self.rows = remaining
        return claimed and error_code == "approval_expired"


@pytest.mark.asyncio
async def test_approval_sla_expires_waiting_rows_and_records_metrics():
    now = datetime(2026, 8, 14, tzinfo=UTC)
    expired = ExpiredApproval(
        email_id="mail-old",
        waiting_since=now - timedelta(hours=25),
        payload_revision=3,
    )
    current = ExpiredApproval(
        email_id="mail-fresh",
        waiting_since=now - timedelta(hours=2),
        payload_revision=1,
    )
    repository = FakeRepository([expired, current])
    notifier = AsyncMock(return_value=True)
    recorded: list[object] = []

    service = ApprovalExpiryService(
        repository=repository,
        notify=notifier,
        record=lambda event: recorded.append(event),
        now=lambda: now,
    )
    expired_count = await service.expire_due()

    assert expired_count == 1
    assert repository.claimed == ["mail-old"]
    notifier.assert_awaited_once()
    assert recorded[0].kind == "expired"
    assert recorded[0].count == 1


@pytest.mark.asyncio
async def test_approval_sla_skips_rows_that_lose_the_cas_race():
    now = datetime(2026, 8, 14, tzinfo=UTC)
    repository = FakeRepository(
        [
            ExpiredApproval(
                email_id="mail-raced",
                waiting_since=now - APPROVAL_SLA - timedelta(minutes=1),
                payload_revision=2,
            )
        ]
    )
    repository.expire_approval = AsyncMock(return_value=False)  # type: ignore[method-assign]
    notifier = AsyncMock()

    service = ApprovalExpiryService(
        repository=repository,
        notify=notifier,
        now=lambda: now,
    )
    expired_count = await service.expire_due()

    assert expired_count == 0
    notifier.assert_not_awaited()
