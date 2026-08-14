"""Expire stale draft-approval rows without coupling to checkpoint cleanup."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from src.observability.metrics import record_approval_expiry
from src.safety.manual_review import normalize_manual_review_code


logger = logging.getLogger(__name__)

APPROVAL_SLA = timedelta(hours=24)
APPROVAL_EXPIRY_CODE = "approval_expired"
_DEFAULT_TICK_SECONDS = 60.0
_DEFAULT_BATCH_SIZE = 50


@dataclass(frozen=True, slots=True)
class ExpiredApproval:
    email_id: str
    waiting_since: datetime
    payload_revision: int | None = None
    inbox_id: str | None = None
    handoff_version: int | None = None
    route_decision: object | None = None
    classification: object | None = None
    original_draft: str | None = None
    final_draft: str | None = None


@dataclass(frozen=True, slots=True)
class ApprovalExpiryEvent:
    kind: str
    count: int
    oldest_seconds: float = 0.0


class ApprovalExpiryRepository(Protocol):
    async def list_expired_approvals(
        self,
        *,
        older_than: datetime,
        limit: int,
    ) -> list[ExpiredApproval]: ...

    async def expire_approval(
        self,
        email_id: str,
        *,
        error_code: str,
        inbox_id: str | None = None,
        expected_version: int | None = None,
    ) -> bool: ...


NotifyExpired = Callable[[ExpiredApproval], Awaitable[bool] | bool]
RecordExpiry = Callable[[ApprovalExpiryEvent], None]


class ApprovalExpiryService:
    """Atomically move stale waiting_approval rows to manual_review."""

    def __init__(
        self,
        *,
        repository: ApprovalExpiryRepository,
        notify: NotifyExpired | None = None,
        record: RecordExpiry | None = None,
        now: Callable[[], datetime] | None = None,
        sla: timedelta = APPROVAL_SLA,
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> None:
        self._repository = repository
        self._notify = notify
        self._record = record or record_approval_expiry
        self._now = now or (lambda: datetime.now(UTC))
        self._sla = sla
        self._batch_size = batch_size

    async def expire_due(self) -> int:
        now = self._now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        cutoff = now - self._sla
        expired = await self._repository.list_expired_approvals(
            older_than=cutoff,
            limit=self._batch_size,
        )
        claimed = 0
        oldest_seconds = 0.0
        for row in expired:
            waiting_since = row.waiting_since
            if waiting_since.tzinfo is None:
                waiting_since = waiting_since.replace(tzinfo=UTC)
            oldest_seconds = max(oldest_seconds, (now - waiting_since).total_seconds())
            if not await self._repository.expire_approval(
                row.email_id,
                error_code=normalize_manual_review_code(APPROVAL_EXPIRY_CODE),
                inbox_id=row.inbox_id,
                expected_version=row.handoff_version,
            ):
                continue
            claimed += 1
            if self._notify is not None:
                result = self._notify(row)
                if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
                    await result
        if claimed:
            self._record(ApprovalExpiryEvent("expired", claimed, oldest_seconds))
        if oldest_seconds:
            self._record(ApprovalExpiryEvent("pending_oldest", 0, oldest_seconds))
        return claimed


class ApprovalSlaScheduler:
    """Periodic runner that keeps approval cards from hanging forever."""

    def __init__(
        self,
        *,
        service: ApprovalExpiryService,
        tick_seconds: float = _DEFAULT_TICK_SECONDS,
    ) -> None:
        self._service = service
        self._tick_seconds = tick_seconds
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run_loop(), name="approval-sla")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run_loop(self) -> None:
        while True:
            try:
                await self._service.expire_due()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Approval SLA pass failed safely: error_type=%s",
                    type(exc).__name__,
                )
            await asyncio.sleep(self._tick_seconds)


class DatabaseApprovalExpiryRepository:
    """emails_log-backed repository for stale waiting_approval rows."""

    def __init__(self, database) -> None:
        self._database = database

    async def list_expired_approvals(
        self,
        *,
        older_than: datetime,
        limit: int,
    ) -> list[ExpiredApproval]:
        list_fn = getattr(self._database, "list_expired_approvals", None)
        if callable(list_fn):
            rows = await list_fn(older_than=older_than, limit=limit)
            return [
                row if isinstance(row, ExpiredApproval) else ExpiredApproval(
                    email_id=str(row["id"]),
                    waiting_since=row["updated_at"],
                    payload_revision=row.get("payload_revision"),
                    inbox_id=row.get("inbox_id"),
                    handoff_version=row.get("handoff_version"),
                    route_decision=row.get("decision_json") or row.get("route_decision"),
                    classification=row.get("classification"),
                    original_draft=row.get("original_draft"),
                    final_draft=row.get("final_draft"),
                )
                for row in rows
            ]
        return []

    async def expire_approval(
        self,
        email_id: str,
        *,
        error_code: str,
        inbox_id: str | None = None,
        expected_version: int | None = None,
    ) -> bool:
        if inbox_id is not None and expected_version is not None:
            transition = getattr(self._database, "transition_handoff_manual_review", None)
            if callable(transition):
                await transition(inbox_id=inbox_id, expected_version=int(expected_version))
        return await self._database.compare_and_set_manual_review(
            email_id,
            expected=frozenset({"waiting_approval"}),
            error_code=error_code,
        )


__all__ = [
    "APPROVAL_SLA",
    "ApprovalExpiryEvent",
    "ApprovalExpiryService",
    "ApprovalSlaScheduler",
    "DatabaseApprovalExpiryRepository",
    "ExpiredApproval",
]
