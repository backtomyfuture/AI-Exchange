"""Durable, plain-text daily email-operations digest delivery.

The legacy daily summary was an in-memory LLM task.  This module deliberately
does something narrower and more auditable: it snapshots durable Inbox facts
for a fixed 18:00--18:00 Asia/Shanghai reporting window, stores the exact
plain-text messages before sending, and records confirmation per message part.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Mapping, Protocol
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.utils.lark_messaging import (
    LarkTextDelivery,
    LarkTextReconciliationUnavailable,
    find_daily_digest_headers,
    send_daily_digest_text,
)


logger = logging.getLogger(__name__)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_REPORT_HOUR = 18
_DEFAULT_TICK_SECONDS = 30.0
_BACKFILL_GRACE = timedelta(minutes=5)
_KNOWN_REJECTION_RETRY = timedelta(minutes=1)
_UNRESOLVED_STATUSES = frozenset(
    {
        "approved",
        "accepted",
        "dead_letter",
        "delivery_failed",
        "ingested",
        "leased",
        "manual_review",
        "pending",
        "processing",
        "retry_wait",
        "saving_draft",
        "send_unknown",
        "send_failed",
        "send_queued",
        "sending",
        "waiting_approval",
    }
)
_BACKLOG_STATUSES = _UNRESOLVED_STATUSES - {"waiting_approval"}


class _DatabasePort(Protocol):
    def get_connection(self): ...


@dataclass(frozen=True, slots=True)
class DailyDigestWindow:
    """A half-open reporting interval represented in UTC."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if (
            self.start.tzinfo is None
            or self.end.tzinfo is None
            or self.start.astimezone(UTC) >= self.end.astimezone(UTC)
        ):
            raise ValueError("invalid_daily_digest_window")

    @classmethod
    def latest_completed(cls, now: datetime | None = None) -> "DailyDigestWindow":
        """Return the most recently completed 18:00 Shanghai reporting window."""

        instant = _as_utc(now or datetime.now(UTC))
        local_now = instant.astimezone(_SHANGHAI)
        local_boundary = local_now.replace(
            hour=_REPORT_HOUR,
            minute=0,
            second=0,
            microsecond=0,
        )
        if local_now < local_boundary:
            local_boundary -= timedelta(days=1)
        return cls(
            start=(local_boundary - timedelta(days=1)).astimezone(UTC),
            end=local_boundary.astimezone(UTC),
        )

    @property
    def label(self) -> str:
        start = self.start.astimezone(_SHANGHAI)
        end = self.end.astimezone(_SHANGHAI)
        return f"{start:%Y-%m-%d %H:%M}~{end:%Y-%m-%d %H:%M}"


@dataclass(frozen=True, slots=True)
class DigestEmailItem:
    received_at: datetime
    sender: str
    subject: str
    status: str

    @property
    def display_status(self) -> str:
        return _status_display(self.status)[0]

    @property
    def next_action(self) -> str:
        return _status_display(self.status)[1]

    @property
    def needs_attention(self) -> bool:
        return _normal_status(self.status) in _UNRESOLVED_STATUSES

    def compact_line(self, *, historical: bool = False) -> str:
        timestamp = self.received_at.astimezone(_SHANGHAI).strftime("%m-%d %H:%M")
        prefix = "历史积压 | " if historical else ""
        return (
            f"- {prefix}收 {timestamp} | {_safe_text(self.sender, 96)} | "
            f"{_safe_text(self.subject, 192)} | {self.display_status} | "
            f"{self.next_action}"
        )


@dataclass(frozen=True, slots=True)
class DailyDigestSnapshot:
    window: DailyDigestWindow
    emails: tuple[DigestEmailItem, ...]
    historical_backlog: tuple[DigestEmailItem, ...]
    missed_windows: tuple[DailyDigestWindow, ...]
    processing_active: bool
    polling_active: bool
    polling_cursor_ready: bool
    ready: bool

    @property
    def processed_count(self) -> int:
        return sum(not item.needs_attention for item in self.emails)

    @property
    def waiting_approval_count(self) -> int:
        return sum(
            _normal_status(item.status) == "waiting_approval" for item in self.emails
        )

    @property
    def attention_items(self) -> tuple[DigestEmailItem, ...]:
        return tuple(item for item in self.emails if item.needs_attention)

    @property
    def failure_or_backlog_count(self) -> int:
        return sum(
            _normal_status(item.status) in _BACKLOG_STATUSES for item in self.emails
        ) + len(self.historical_backlog)


@dataclass(frozen=True, slots=True)
class DailyDigestExecution:
    account_id: int
    delivery_scope_hash: str
    window: DailyDigestWindow
    state: str
    is_backfill: bool
    parts: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class ClaimedDigestPart:
    index: int
    header: str
    text: str
    request_uuid: str


@dataclass(frozen=True, slots=True)
class ReconciliationCandidates:
    headers: frozenset[str]
    not_before: datetime


def _as_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("daily_digest_timestamp_must_be_aware")
    return value.astimezone(UTC)


def _normal_status(value: object) -> str:
    if not isinstance(value, str):
        return "unknown"
    normalized = value.strip().lower()
    return normalized or "unknown"


def _status_display(status: object) -> tuple[str, str]:
    normalized = _normal_status(status)
    mapping = {
        "waiting_approval": ("待审批", "请在飞书完成审批"),
        "approved": ("待发送", "正在发送或等待重试"),
        "send_queued": ("待发送", "等待发送"),
        "sending": ("发送中", "等待发送结果"),
        "accepted": ("已受理", "等待最终送达结果"),
        "send_failed": ("发送失败", "请人工处理"),
        "delivery_failed": ("投递失败", "请人工处理"),
        "send_unknown": ("发送待核实", "请人工核实发送结果"),
        "sent": ("已发送", "已处理"),
        "draft_saved": ("草稿已保存", "已处理"),
        "saving_draft": ("保存草稿中", "等待保存结果"),
        "rejected": ("已拒绝", "已处理"),
        "manual_review": ("人工复核", "请人工检查"),
        "dead_letter": ("处理失败", "请人工处理"),
        "retry_wait": ("等待重试", "系统将自动重试"),
        "pending": ("待处理", "系统处理中"),
        "leased": ("处理中", "等待处理完成"),
        "processing": ("处理中", "等待处理完成"),
        "ingested": ("已入队", "等待处理"),
        "completed": ("已完成", "已处理"),
        "no_action": ("无需处理", "已处理"),
        "notified_readonly": ("已通知", "已处理"),
        "skipped": ("已跳过", "已处理"),
        "archived": ("已归档", "已处理"),
        "cancelled": ("已取消", "已处理"),
        "expired": ("已过期", "已处理"),
    }
    return mapping.get(normalized, ("状态待确认", "请人工检查"))


def _safe_text(value: object, max_bytes: int) -> str:
    if not isinstance(value, str):
        value = ""
    compact = " ".join(value.replace("\r", " ").replace("\n", " ").split())
    if not compact:
        compact = "未知"
    encoded = compact.encode("utf-8")
    if len(encoded) <= max_bytes:
        return compact
    suffix = "…"
    clipped = encoded[: max(0, max_bytes - len(suffix.encode("utf-8")))].decode(
        "utf-8", errors="ignore"
    )
    return f"{clipped}{suffix}"


def delivery_scope_hash(chat_id: str) -> str:
    """Use a stable scope identity without storing the chat identifier itself."""

    if not isinstance(chat_id, str) or not chat_id:
        raise ValueError("daily_digest_chat_id_invalid")
    return hashlib.sha256(chat_id.encode("utf-8")).hexdigest()


def digest_header(window: DailyDigestWindow, part: int | None = None, total: int | None = None) -> str:
    header = f"【邮件日报 {window.label}】"
    if part is not None and total is not None:
        header += f"（第 {part}/{total} 部分）"
    return header


def _chunk_lines(lines: list[str], budget: int) -> list[list[str]]:
    if budget < 256:
        raise ValueError("daily_digest_message_limit_too_small")
    chunks: list[list[str]] = [[]]
    size = 0
    for raw_line in lines:
        line = _safe_text(raw_line, max(32, budget - 8))
        line_size = len(line.encode("utf-8")) + (1 if chunks[-1] else 0)
        if chunks[-1] and size + line_size > budget:
            chunks.append([])
            size = 0
            line_size = len(line.encode("utf-8"))
        chunks[-1].append(line)
        size += line_size
    return [chunk for chunk in chunks if chunk]


def render_daily_digest(
    snapshot: DailyDigestSnapshot,
    *,
    is_backfill: bool,
    max_bytes: int,
) -> tuple[tuple[str, str], ...]:
    """Return ordered ``(header, text)`` plain-text messages for a snapshot."""

    if not isinstance(max_bytes, int) or not 1_024 <= max_bytes <= 64_000:
        raise ValueError("daily_digest_message_limit_invalid")

    service_status = "正常" if (
        snapshot.ready
        and snapshot.processing_active
        and snapshot.polling_active
        and snapshot.polling_cursor_ready
    ) else "需关注"
    lines = []
    if is_backfill:
        lines.extend((f"补发：原报告窗口 {snapshot.window.label}", ""))
    lines.extend(
        (
            "今日概况",
            f"- 收到邮件：{len(snapshot.emails)}",
            f"- 已处理：{snapshot.processed_count}",
            f"- 待审批：{snapshot.waiting_approval_count}",
            f"- 失败或积压：{snapshot.failure_or_backlog_count}",
            (
                "- 服务状态："
                f"{service_status}（处理{'运行中' if snapshot.processing_active else '未运行'}，"
                f"轮询{'就绪' if snapshot.polling_active and snapshot.polling_cursor_ready else '未就绪'}）"
            ),
            "",
            "需关注事项",
        )
    )
    attention_lines: list[str] = []
    attention_lines.extend(
        f"- 历史漏发日报：{window.label}" for window in snapshot.missed_windows
    )
    attention_lines.extend(item.compact_line() for item in snapshot.attention_items)
    attention_lines.extend(
        item.compact_line(historical=True) for item in snapshot.historical_backlog
    )
    lines.extend(attention_lines or ["- 无需关注"])
    lines.extend(("", "邮件清单"))
    lines.extend(item.compact_line() for item in snapshot.emails)
    if not snapshot.emails:
        lines.append("- 今日无新邮件")

    single_header = digest_header(snapshot.window)
    single_text = f"{single_header}\n" + "\n".join(lines)
    if len(single_text.encode("utf-8")) <= max_bytes:
        return ((single_header, single_text),)

    # Reserve enough bytes for the widest practical part marker so every
    # persisted message remains under the configured transport limit.
    reserve_header = digest_header(snapshot.window, 999, 999)
    chunks = _chunk_lines(lines, max_bytes - len(reserve_header.encode("utf-8")) - 1)
    if len(chunks) > 999:
        raise ValueError("daily_digest_too_many_parts")
    total = len(chunks)
    return tuple(
        (
            digest_header(snapshot.window, index, total),
            f"{digest_header(snapshot.window, index, total)}\n" + "\n".join(chunk),
        )
        for index, chunk in enumerate(chunks, start=1)
    )


class DailyDigestRepository:
    """Persisted execution and read-only source projection for the digest."""

    def __init__(self, database: _DatabasePort, *, account_id: int, scope_hash: str) -> None:
        if not isinstance(account_id, int) or account_id <= 0:
            raise ValueError("daily_digest_account_invalid")
        if not isinstance(scope_hash, str) or len(scope_hash) != 64:
            raise ValueError("daily_digest_scope_invalid")
        self._database = database
        self._account_id = account_id
        self._scope_hash = scope_hash

    async def build_snapshot(
        self,
        window: DailyDigestWindow,
        *,
        health: object,
    ) -> DailyDigestSnapshot:
        async with self._database.get_connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    """
                    WITH digest_rows AS (
                        SELECT DISTINCT ON (inbox.external_email_id)
                            inbox.external_email_id,
                            inbox.received_at,
                            COALESCE(log.sender, '未知发件人') AS sender,
                            COALESCE(log.subject, '无主题') AS subject,
                            COALESCE(email.status, inbox.status, 'unknown') AS status
                        FROM public.event_inbox AS inbox
                        LEFT JOIN public.emails AS email
                          ON email.account_id = inbox.account_id
                         AND email.external_email_id = inbox.external_email_id
                        LEFT JOIN public.emails_log AS log
                          ON log.id = inbox.external_email_id
                        WHERE inbox.account_id = %s
                          AND inbox.change_kind = 'create'
                          AND inbox.received_at >= %s
                          AND inbox.received_at < %s
                        ORDER BY inbox.external_email_id, inbox.received_at ASC
                    )
                    SELECT external_email_id, received_at, sender, subject, status
                    FROM digest_rows
                    ORDER BY received_at ASC, external_email_id ASC
                    """,
                    (self._account_id, window.start, window.end),
                )
                email_rows = await cursor.fetchall()
                await cursor.execute(
                    """
                    WITH backlog_rows AS (
                        SELECT DISTINCT ON (inbox.external_email_id)
                            inbox.external_email_id,
                            inbox.received_at,
                            COALESCE(log.sender, '未知发件人') AS sender,
                            COALESCE(log.subject, '无主题') AS subject,
                            COALESCE(email.status, inbox.status, 'unknown') AS status
                        FROM public.event_inbox AS inbox
                        LEFT JOIN public.emails AS email
                          ON email.account_id = inbox.account_id
                         AND email.external_email_id = inbox.external_email_id
                        LEFT JOIN public.emails_log AS log
                          ON log.id = inbox.external_email_id
                        WHERE inbox.account_id = %s
                          AND inbox.change_kind = 'create'
                          AND inbox.received_at < %s
                        ORDER BY inbox.external_email_id, inbox.received_at ASC
                    )
                    SELECT external_email_id, received_at, sender, subject, status
                    FROM backlog_rows
                    ORDER BY received_at ASC, external_email_id ASC
                    """,
                    (self._account_id, window.start),
                )
                backlog_rows = await cursor.fetchall()
                await cursor.execute(
                    """
                    SELECT window_start, window_end
                    FROM public.daily_digest_executions
                    WHERE account_id = %s
                      AND delivery_scope_hash = %s
                      AND state = 'missed'
                      AND missed_reported_at IS NULL
                      AND window_end <= %s
                    ORDER BY window_end ASC
                    """,
                    (self._account_id, self._scope_hash, window.start),
                )
                missed_rows = await cursor.fetchall()

        emails = tuple(_email_item_from_row(row) for row in email_rows)
        historical_backlog = tuple(
            item
            for item in (_email_item_from_row(row) for row in backlog_rows)
            if item.needs_attention
        )
        missed_windows = tuple(
            DailyDigestWindow(_as_utc(row["window_start"]), _as_utc(row["window_end"]))
            for row in missed_rows
        )
        return DailyDigestSnapshot(
            window=window,
            emails=emails,
            historical_backlog=historical_backlog,
            missed_windows=missed_windows,
            ready=bool(getattr(health, "ready", False)),
            processing_active=bool(getattr(health, "processing_active", False)),
            polling_active=bool(getattr(health, "polling_active", False)),
            polling_cursor_ready=bool(getattr(health, "polling_cursor_ready", False)),
        )

    async def get_execution(
        self, window: DailyDigestWindow
    ) -> DailyDigestExecution | None:
        async with self._database.get_connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    """
                    SELECT account_id, delivery_scope_hash, window_start, window_end,
                           state, is_backfill, delivery_parts
                    FROM public.daily_digest_executions
                    WHERE account_id = %s
                      AND delivery_scope_hash = %s
                      AND window_start = %s
                      AND window_end = %s
                    """,
                    (self._account_id, self._scope_hash, window.start, window.end),
                )
                row = await cursor.fetchone()
        return _execution_from_row(row) if row else None

    async def ensure_execution(
        self,
        snapshot: DailyDigestSnapshot,
        *,
        is_backfill: bool,
        max_bytes: int,
    ) -> DailyDigestExecution:
        messages = render_daily_digest(
            snapshot,
            is_backfill=is_backfill,
            max_bytes=max_bytes,
        )
        parts = [
            {
                "header": header,
                "text": text,
                "request_uuid": str(
                    uuid5(
                        NAMESPACE_URL,
                        (
                            "ai-exchange-daily-digest:"
                            f"{self._scope_hash}:{snapshot.window.start.isoformat()}:"
                            f"{snapshot.window.end.isoformat()}:{index}"
                        ),
                    )
                ),
                "state": "pending",
                "attempts": 0,
            }
            for index, (header, text) in enumerate(messages)
        ]
        async with self._database.get_connection() as connection:
            async with connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(
                        """
                        INSERT INTO public.daily_digest_executions (
                            account_id, delivery_scope_hash, window_start, window_end,
                            state, is_backfill, delivery_parts
                        ) VALUES (%s, %s, %s, %s, 'pending', %s, %s)
                        ON CONFLICT (account_id, delivery_scope_hash, window_start, window_end)
                        DO NOTHING
                        RETURNING account_id, delivery_scope_hash, window_start, window_end,
                                  state, is_backfill, delivery_parts
                        """,
                        (
                            self._account_id,
                            self._scope_hash,
                            snapshot.window.start,
                            snapshot.window.end,
                            is_backfill,
                            Jsonb(parts),
                        ),
                    )
                    row = await cursor.fetchone()
                    if row is None:
                        await cursor.execute(
                            """
                            SELECT account_id, delivery_scope_hash, window_start, window_end,
                                   state, is_backfill, delivery_parts
                            FROM public.daily_digest_executions
                            WHERE account_id = %s
                              AND delivery_scope_hash = %s
                              AND window_start = %s
                              AND window_end = %s
                            FOR UPDATE
                            """,
                            (
                                self._account_id,
                                self._scope_hash,
                                snapshot.window.start,
                                snapshot.window.end,
                            ),
                        )
                        row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("daily_digest_execution_unavailable")
        return _execution_from_row(row)

    async def mark_expired_executions_missed(self, cutoff: datetime) -> int:
        """Close unresolved earlier windows before a new 18:00 window begins."""

        instant = _as_utc(cutoff)
        async with self._database.get_connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE public.daily_digest_executions
                    SET state = 'missed',
                        missed_at = COALESCE(missed_at, CURRENT_TIMESTAMP),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE account_id = %s
                      AND delivery_scope_hash = %s
                      AND state = 'pending'
                      AND window_end <= %s
                    """,
                    (self._account_id, self._scope_hash, instant),
                )
                return int(cursor.rowcount or 0)

    async def reconciliation_candidates(
        self,
        window: DailyDigestWindow,
        *,
        now: datetime,
        delay: timedelta,
    ) -> ReconciliationCandidates | None:
        execution = await self.get_execution(window)
        if execution is None or execution.state != "pending":
            return None
        instant = _as_utc(now)
        headers: set[str] = set()
        attempts: list[datetime] = []
        for part in execution.parts:
            if part.get("state") not in {"sending", "unknown"}:
                continue
            attempted_at = _json_datetime(part.get("last_attempt_at"))
            header = part.get("header")
            if (
                attempted_at is not None
                and attempted_at + delay <= instant
                and isinstance(header, str)
            ):
                headers.add(header)
                attempts.append(attempted_at)
        if not headers or not attempts:
            return None
        return ReconciliationCandidates(
            headers=frozenset(headers),
            not_before=min(attempts) - timedelta(minutes=1),
        )

    async def reconcile_parts(
        self,
        window: DailyDigestWindow,
        *,
        found_headers: set[str],
        now: datetime,
    ) -> None:
        """Apply one successful, bounded chat reconciliation under a row lock."""

        instant = _as_utc(now)
        async with self._database.get_connection() as connection:
            async with connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    row = await self._locked_execution_row(cursor, window)
                    if row is None or row["state"] != "pending":
                        return
                    parts = _parts_from_value(row["delivery_parts"])
                    changed = False
                    for part in parts:
                        if part.get("state") not in {"sending", "unknown"}:
                            continue
                        header = part.get("header")
                        if not isinstance(header, str):
                            continue
                        if header in found_headers:
                            part["state"] = "confirmed"
                            part["confirmed_at"] = instant.isoformat()
                        else:
                            part["state"] = "pending"
                            part["next_attempt_at"] = instant.isoformat()
                        changed = True
                    if changed:
                        await self._write_parts_and_refresh_state(
                            cursor,
                            window,
                            parts,
                            now=instant,
                        )

    async def claim_next_part(
        self,
        window: DailyDigestWindow,
        *,
        now: datetime,
    ) -> ClaimedDigestPart | None:
        """Claim only the first unconfirmed part, preserving bundle order."""

        instant = _as_utc(now)
        async with self._database.get_connection() as connection:
            async with connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    row = await self._locked_execution_row(cursor, window)
                    if row is None or row["state"] != "pending":
                        return None
                    parts = _parts_from_value(row["delivery_parts"])
                    for index, part in enumerate(parts):
                        state = part.get("state")
                        if state == "confirmed":
                            continue
                        if state in {"sending", "unknown"}:
                            return None
                        if state != "pending":
                            raise RuntimeError("daily_digest_part_state_invalid")
                        next_attempt = _json_datetime(part.get("next_attempt_at"))
                        if next_attempt is not None and next_attempt > instant:
                            return None
                        header = part.get("header")
                        text = part.get("text")
                        request_uuid = part.get("request_uuid")
                        if not all(
                            isinstance(value, str) and value
                            for value in (header, text, request_uuid)
                        ):
                            raise RuntimeError("daily_digest_part_invalid")
                        part["state"] = "sending"
                        part["attempts"] = int(part.get("attempts", 0)) + 1
                        part["last_attempt_at"] = instant.isoformat()
                        part.pop("next_attempt_at", None)
                        await self._write_parts(cursor, window, parts, now=instant)
                        return ClaimedDigestPart(
                            index=index,
                            header=header,
                            text=text,
                            request_uuid=request_uuid,
                        )
        return None

    async def mark_delivery_confirmed(
        self,
        window: DailyDigestWindow,
        part: ClaimedDigestPart,
        *,
        now: datetime,
    ) -> None:
        await self._finish_claimed_part(
            window,
            part,
            state="confirmed",
            now=now,
        )

    async def mark_delivery_unknown(
        self,
        window: DailyDigestWindow,
        part: ClaimedDigestPart,
        *,
        now: datetime,
    ) -> None:
        await self._finish_claimed_part(window, part, state="unknown", now=now)

    async def mark_delivery_rejected(
        self,
        window: DailyDigestWindow,
        part: ClaimedDigestPart,
        *,
        now: datetime,
    ) -> None:
        instant = _as_utc(now)
        await self._finish_claimed_part(
            window,
            part,
            state="pending",
            now=instant,
            next_attempt_at=instant + _KNOWN_REJECTION_RETRY,
        )

    async def _finish_claimed_part(
        self,
        window: DailyDigestWindow,
        claim: ClaimedDigestPart,
        *,
        state: str,
        now: datetime,
        next_attempt_at: datetime | None = None,
    ) -> None:
        instant = _as_utc(now)
        async with self._database.get_connection() as connection:
            async with connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    row = await self._locked_execution_row(cursor, window)
                    if row is None or row["state"] != "pending":
                        return
                    parts = _parts_from_value(row["delivery_parts"])
                    if not 0 <= claim.index < len(parts):
                        raise RuntimeError("daily_digest_claim_invalid")
                    part = parts[claim.index]
                    if (
                        part.get("state") != "sending"
                        or part.get("request_uuid") != claim.request_uuid
                    ):
                        return
                    part["state"] = state
                    if state == "confirmed":
                        part["confirmed_at"] = instant.isoformat()
                    elif next_attempt_at is not None:
                        part["next_attempt_at"] = _as_utc(next_attempt_at).isoformat()
                    await self._write_parts_and_refresh_state(
                        cursor,
                        window,
                        parts,
                        now=instant,
                    )

    async def _locked_execution_row(self, cursor: Any, window: DailyDigestWindow):
        await cursor.execute(
            """
            SELECT state, delivery_parts
            FROM public.daily_digest_executions
            WHERE account_id = %s
              AND delivery_scope_hash = %s
              AND window_start = %s
              AND window_end = %s
            FOR UPDATE
            """,
            (self._account_id, self._scope_hash, window.start, window.end),
        )
        return await cursor.fetchone()

    async def _write_parts(
        self,
        cursor: Any,
        window: DailyDigestWindow,
        parts: list[dict[str, object]],
        *,
        now: datetime,
    ) -> None:
        await cursor.execute(
            """
            UPDATE public.daily_digest_executions
            SET delivery_parts = %s,
                attempt_count = attempt_count + 1,
                last_attempt_at = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE account_id = %s
              AND delivery_scope_hash = %s
              AND window_start = %s
              AND window_end = %s
            """,
            (
                Jsonb(parts),
                _as_utc(now),
                self._account_id,
                self._scope_hash,
                window.start,
                window.end,
            ),
        )

    async def _write_parts_and_refresh_state(
        self,
        cursor: Any,
        window: DailyDigestWindow,
        parts: list[dict[str, object]],
        *,
        now: datetime,
    ) -> None:
        all_confirmed = bool(parts) and all(
            part.get("state") == "confirmed" for part in parts
        )
        await cursor.execute(
            """
            UPDATE public.daily_digest_executions
            SET delivery_parts = %s,
                state = CASE WHEN %s THEN 'confirmed' ELSE state END,
                confirmed_at = CASE
                    WHEN %s THEN COALESCE(confirmed_at, %s)
                    ELSE confirmed_at
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE account_id = %s
              AND delivery_scope_hash = %s
              AND window_start = %s
              AND window_end = %s
            """,
            (
                Jsonb(parts),
                all_confirmed,
                all_confirmed,
                _as_utc(now),
                self._account_id,
                self._scope_hash,
                window.start,
                window.end,
            ),
        )
        if all_confirmed:
            await cursor.execute(
                """
                UPDATE public.daily_digest_executions
                SET missed_reported_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account_id = %s
                  AND delivery_scope_hash = %s
                  AND state = 'missed'
                  AND missed_reported_at IS NULL
                  AND window_end <= %s
                """,
                (self._account_id, self._scope_hash, window.start),
            )


def _email_item_from_row(row: Mapping[str, object]) -> DigestEmailItem:
    timestamp = row.get("received_at")
    if not isinstance(timestamp, datetime):
        raise RuntimeError("daily_digest_source_timestamp_invalid")
    return DigestEmailItem(
        received_at=_as_utc(timestamp),
        sender=_safe_text(row.get("sender"), 96),
        subject=_safe_text(row.get("subject"), 192),
        status=_normal_status(row.get("status")),
    )


def _parts_from_value(value: object) -> list[dict[str, object]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            raise RuntimeError("daily_digest_parts_invalid") from None
    if not isinstance(value, list):
        raise RuntimeError("daily_digest_parts_invalid")
    parts: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            raise RuntimeError("daily_digest_parts_invalid")
        parts.append(dict(item))
    if not parts:
        raise RuntimeError("daily_digest_parts_invalid")
    return parts


def _execution_from_row(row: Mapping[str, object]) -> DailyDigestExecution:
    return DailyDigestExecution(
        account_id=int(row["account_id"]),
        delivery_scope_hash=str(row["delivery_scope_hash"]),
        window=DailyDigestWindow(
            _as_utc(row["window_start"]),  # type: ignore[arg-type]
            _as_utc(row["window_end"]),  # type: ignore[arg-type]
        ),
        state=str(row["state"]),
        is_backfill=bool(row["is_backfill"]),
        parts=tuple(_parts_from_value(row["delivery_parts"])),
    )


def _json_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


class DailyDigestScheduler:
    """Run one durable digest report for each completed 18:00 window."""

    def __init__(
        self,
        *,
        database: _DatabasePort,
        account_id: int,
        chat_id: str,
        health_snapshot: Callable[[], object],
        enabled: bool = True,
        max_message_bytes: int = 12_000,
        reconciliation_delay_seconds: int = 900,
        tick_seconds: float = _DEFAULT_TICK_SECONDS,
        sender: Callable[..., LarkTextDelivery] = send_daily_digest_text,
        reconciler: Callable[..., set[str]] = find_daily_digest_headers,
    ) -> None:
        if not isinstance(max_message_bytes, int) or not 1_024 <= max_message_bytes <= 64_000:
            raise ValueError("daily_digest_message_limit_invalid")
        if not isinstance(reconciliation_delay_seconds, int) or reconciliation_delay_seconds < 1:
            raise ValueError("daily_digest_reconciliation_delay_invalid")
        if not isinstance(tick_seconds, (int, float)) or tick_seconds <= 0:
            raise ValueError("daily_digest_tick_invalid")
        self._repository = DailyDigestRepository(
            database,
            account_id=account_id,
            scope_hash=delivery_scope_hash(chat_id),
        )
        self._chat_id = chat_id
        self._health_snapshot = health_snapshot
        self._enabled = enabled
        self._max_message_bytes = max_message_bytes
        self._reconciliation_delay = timedelta(seconds=reconciliation_delay_seconds)
        self._tick_seconds = float(tick_seconds)
        self._sender = sender
        self._reconciler = reconciler
        self._task: asyncio.Task[None] | None = None
        self._run_lock = asyncio.Lock()

    async def start(self) -> None:
        if not self._enabled or self._task is not None:
            return
        self._task = asyncio.create_task(
            self._run_loop(),
            name="daily-email-operations-digest",
        )

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
                await self.run_due()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Daily digest pass failed safely: error_type=%s",
                    type(exc).__name__,
                )
            await asyncio.sleep(self._tick_seconds)

    async def run_due(self, *, now: datetime | None = None) -> None:
        """Run/recover the most recently completed reporting window once."""

        if not self._enabled:
            return
        instant = _as_utc(now or datetime.now(UTC))
        async with self._run_lock:
            window = DailyDigestWindow.latest_completed(instant)
            await self._repository.mark_expired_executions_missed(window.start)
            execution = await self._repository.get_execution(window)
            if execution is None:
                health = self._health_snapshot()
                snapshot = await self._repository.build_snapshot(window, health=health)
                execution = await self._repository.ensure_execution(
                    snapshot,
                    is_backfill=instant > window.end + _BACKFILL_GRACE,
                    max_bytes=self._max_message_bytes,
                )
            if execution.state == "pending":
                await self._deliver_pending_execution(window)

    async def _deliver_pending_execution(self, window: DailyDigestWindow) -> None:
        # A bundle is ordered.  Keep sending confirmed parts in one pass, but
        # stop on a rejection/unknown outcome so retries are paced and safe.
        while True:
            instant = datetime.now(UTC)
            candidates = await self._repository.reconciliation_candidates(
                window,
                now=instant,
                delay=self._reconciliation_delay,
            )
            if candidates is not None:
                try:
                    found = await self._call_reconciler(candidates, now=instant)
                except LarkTextReconciliationUnavailable:
                    return
                await self._repository.reconcile_parts(
                    window,
                    found_headers=found,
                    now=datetime.now(UTC),
                )
                continue
            claim = await self._repository.claim_next_part(window, now=instant)
            if claim is None:
                return
            outcome = await self._call_sender(claim)
            if outcome.accepted:
                await self._repository.mark_delivery_confirmed(
                    window,
                    claim,
                    now=datetime.now(UTC),
                )
                continue
            if outcome.outcome_known:
                await self._repository.mark_delivery_rejected(
                    window,
                    claim,
                    now=datetime.now(UTC),
                )
            else:
                await self._repository.mark_delivery_unknown(
                    window,
                    claim,
                    now=datetime.now(UTC),
                )
            return

    async def _call_sender(self, part: ClaimedDigestPart) -> LarkTextDelivery:
        if inspect.iscoroutinefunction(self._sender):
            result = await self._sender(
                part.text,
                request_uuid=part.request_uuid,
                chat_id=self._chat_id,
            )
        else:
            result = await asyncio.to_thread(
                self._sender,
                part.text,
                request_uuid=part.request_uuid,
                chat_id=self._chat_id,
            )
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, LarkTextDelivery):
            return LarkTextDelivery(accepted=False, outcome_known=False)
        return result

    async def _call_reconciler(
        self,
        candidates: ReconciliationCandidates,
        *,
        now: datetime,
    ) -> set[str]:
        kwargs = {
            "not_before": candidates.not_before,
            "not_after": now,
            "chat_id": self._chat_id,
        }
        if inspect.iscoroutinefunction(self._reconciler):
            result = await self._reconciler(set(candidates.headers), **kwargs)
        else:
            result = await asyncio.to_thread(
                self._reconciler,
                set(candidates.headers),
                **kwargs,
            )
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, set) or not all(isinstance(item, str) for item in result):
            raise LarkTextReconciliationUnavailable("invalid_reconciliation_result")
        return result
