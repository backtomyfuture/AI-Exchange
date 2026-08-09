"""Narrow ``sync_state`` ingress that feeds the existing Durable Inbox.

This module owns only the Gateway polling boundary.  It does not process mail,
start a Worker, construct a graph, or invoke Feishu.  The runtime supplies the
same policy snapshot and Inbox-backed cursor store used by the main pipeline.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout

from src.domain.errors import SyncTransientError
from src.ingestion.models import (
    MAX_SYNC_CHANGES_PER_BATCH,
    POSTGRES_BIGINT_MAX,
    IngressSource,
    NormalizedIngressEvent,
    SyncBatch,
)
from src.ingestion.normalization import normalize_sync_change
from src.ingestion.policy import FolderScope, PolicySnapshot, ProcessingPolicyResolver
from src.ingestion.runtime_authority import (
    RuntimeInstanceLease,
    RuntimeInstanceLifecycle,
)


logger = logging.getLogger(__name__)


class PollingIngressOutcome(StrEnum):
    """Safe result of one complete-delta polling attempt."""

    BASELINED = "baselined"
    COMMITTED = "committed"


@dataclass(frozen=True, slots=True)
class PollingCursorCheckpoint:
    """Opaque cursor snapshot used for one compare-and-swap commit."""

    cursor: str | None
    version: int

    def __post_init__(self) -> None:
        if self.cursor is not None and (
            type(self.cursor) is not str
            or not self.cursor
            or self.cursor != self.cursor.strip()
            or len(self.cursor) > 8192
        ):
            raise ValueError("cursor must be an exact bounded string or None")
        if type(self.version) is not int or not 0 <= self.version <= POSTGRES_BIGINT_MAX:
            raise ValueError("version must be a nonnegative PostgreSQL BIGINT")


class PollingPageClient(Protocol):
    """Current Gateway complete-delta adapter."""

    async def sync_polling(
        self,
        account_id: int,
        folder: str,
        sync_state: str | None,
        limit: int,
        *,
        discard_items: bool = False,
    ) -> SyncBatch: ...


class PollingCursorStore(Protocol):
    """Durable cursor and Inbox commit boundary for one configured folder."""

    async def load(
        self,
        account_id: int,
        folder: str,
    ) -> PollingCursorCheckpoint: ...

    async def commit_activation_boundary(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
    ) -> None: ...

    async def commit_delta(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
        events: Sequence[NormalizedIngressEvent],
    ) -> None: ...


class PollingCursorConflict(RuntimeError):
    """A concurrent cursor transition won this polling attempt."""

    safe_code = "polling.cursor_conflict"

    def __init__(self) -> None:
        super().__init__(self.safe_code)


class PollingCursorUnavailable(RuntimeError):
    """The durable cursor is not in a state safe for automatic polling."""

    safe_code = "polling.cursor_unavailable"

    def __init__(self) -> None:
        super().__init__(self.safe_code)


def _row_value(row: object, key: str, index: int) -> object:
    if isinstance(row, Mapping):
        return row[key]
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return row[index]
    raise PollingCursorUnavailable()


def _checkpoint_from_row(row: object) -> tuple[PollingCursorCheckpoint, str]:
    try:
        cursor = _row_value(row, "cursor", 0)
        status = _row_value(row, "status", 1)
        version = _row_value(row, "version", 2)
        if type(status) is not str:
            raise ValueError
        checkpoint = PollingCursorCheckpoint(cursor=cursor, version=version)
    except (IndexError, KeyError, TypeError, ValueError):
        raise PollingCursorUnavailable() from None
    if status == "active" and checkpoint.cursor is not None:
        return checkpoint, status
    if status == "baselining" and checkpoint.cursor is None:
        return checkpoint, status
    raise PollingCursorUnavailable()


@dataclass(frozen=True, slots=True)
class PollingPageCommitResult:
    """The one page receipt returned by the fenced database function."""

    cursor: str
    version: int
    inserted_count: int
    duplicate_count: int

    def __post_init__(self) -> None:
        PollingCursorCheckpoint(cursor=self.cursor, version=self.version)
        if self.version < 1:
            raise ValueError("committed cursor version is invalid")
        for field in ("inserted_count", "duplicate_count"):
            value = getattr(self, field)
            if type(value) is not int or not 0 <= value <= MAX_SYNC_CHANGES_PER_BATCH:
                raise ValueError(f"{field} is invalid")


class PollingPageCommitter(Protocol):
    """Commit one complete sync page through the active web-session lease."""

    async def commit_page(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
        events: Sequence[NormalizedIngressEvent],
        *,
        activation: bool,
    ) -> PollingPageCommitResult: ...


_SYNC_PAGE_CURSOR_CONFLICTS = frozenset(
    {
        "greenfield_sync_cursor_conflict",
        "greenfield_sync_lease_conflict",
    }
)
_SYNC_PAGE_TRANSIENT_DB_ERRORS = (
    psycopg.OperationalError,
    psycopg.errors.DeadlockDetected,
    psycopg.errors.LockNotAvailable,
    psycopg.errors.QueryCanceled,
    psycopg.errors.SerializationFailure,
    PoolTimeout,
)


class GreenfieldSyncPageWriter:
    """Invoke the only runtime mutation boundary for ``sync_state`` pages.

    The database function constructs the persisted notification payload and
    derives policy from the frozen scope itself.  Python therefore never gains
    direct write access to either ``event_inbox`` or ``sync_cursors``.
    """

    def __init__(self, pool: Any) -> None:
        if not callable(getattr(pool, "connection", None)):
            raise ValueError("pool is invalid")
        self._pool = pool

    @staticmethod
    def _envelopes(
        lease: RuntimeInstanceLease,
        next_cursor: str,
        events: Sequence[NormalizedIngressEvent],
    ) -> tuple[dict[str, object], ...]:
        exact_events = tuple(events)
        envelopes: list[dict[str, object]] = []
        for event in exact_events:
            if (
                type(event) is not NormalizedIngressEvent
                or event.account_id != lease.account_id
                or event.folder != "INBOX"
                or event.source is not IngressSource.SYNC
                or event.raw_event_type != event.kind.value
                or event.source_event_at is not None
            ):
                raise PollingCursorUnavailable()
            # ``normalize_sync_change`` binds the source cursor into both the
            # payload and dedupe identity.  The function will independently
            # rebuild the stored payload from these notification-only fields.
            payload = event.payload_for_storage()
            if (
                payload.get("cursor") != next_cursor
                or payload.get("change_type") != event.kind.value
                or payload.get("id") != event.external_email_id
                or payload.get("source_version") != event.source_version
            ):
                raise PollingCursorUnavailable()
            item = payload.get("item")
            if item != {} and item is not None:
                raise PollingCursorUnavailable()
            envelopes.append(
                {
                    "external_email_id": event.external_email_id,
                    "change_kind": event.kind.value,
                    "dedupe_key": event.dedupe_key,
                    "source_version": event.source_version,
                }
            )
        return tuple(sorted(envelopes, key=lambda event: str(event["dedupe_key"])))

    @staticmethod
    def _result(
        row: object,
        *,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
        event_count: int,
    ) -> PollingPageCommitResult:
        try:
            result = PollingPageCommitResult(
                cursor=_row_value(row, "committed_cursor", 0),
                version=_row_value(row, "committed_version", 1),
                inserted_count=_row_value(row, "inserted_count", 2),
                duplicate_count=_row_value(row, "duplicate_count", 3),
            )
        except (IndexError, KeyError, TypeError, ValueError):
            raise PollingCursorUnavailable() from None
        if (
            result.cursor != next_cursor
            or result.version != checkpoint.version + 1
            or result.inserted_count + result.duplicate_count != event_count
        ):
            raise PollingCursorUnavailable()
        return result

    async def commit_page(
        self,
        lease: RuntimeInstanceLease,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
        events: Sequence[NormalizedIngressEvent],
        *,
        activation: bool,
    ) -> PollingPageCommitResult:
        if (
            type(lease) is not RuntimeInstanceLease
            or lease.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
            or type(checkpoint) is not PollingCursorCheckpoint
            or type(next_cursor) is not str
            or type(activation) is not bool
        ):
            raise PollingCursorUnavailable()
        if activation is (checkpoint.cursor is not None):
            raise PollingCursorUnavailable()
        if activation and tuple(events):
            raise PollingCursorUnavailable()
        envelopes = self._envelopes(lease, next_cursor, events)
        remaining_seconds = (lease.lease_until - datetime.now(UTC)).total_seconds()
        if remaining_seconds <= 1.0:
            raise PollingCursorConflict()
        params = (
            lease.account_id,
            lease.session_id,
            lease.lease_version,
            "INBOX",
            checkpoint.cursor,
            checkpoint.version,
            next_cursor,
            Jsonb(list(envelopes)),
            activation,
        )
        try:
            async with asyncio.timeout(remaining_seconds - 1.0):
                async with self._pool.connection() as connection:
                    async with connection.transaction():
                        cursor = await connection.execute(
                            "SELECT committed_cursor, committed_version, "
                            "inserted_count, duplicate_count "
                            "FROM public.greenfield_commit_sync_page("
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            params,
                        )
                        row = await cursor.fetchone()
            return self._result(
                row,
                checkpoint=checkpoint,
                next_cursor=next_cursor,
                event_count=len(envelopes),
            )
        except (PollingCursorConflict, PollingCursorUnavailable):
            raise
        except TimeoutError:
            raise SyncTransientError() from None
        except psycopg.errors.RaiseException as error:
            message = getattr(getattr(error, "diag", None), "message_primary", "")
            if message in _SYNC_PAGE_CURSOR_CONFLICTS:
                raise PollingCursorConflict() from None
            raise PollingCursorUnavailable() from None
        except _SYNC_PAGE_TRANSIENT_DB_ERRORS:
            raise SyncTransientError() from None
        except psycopg.Error:
            raise PollingCursorUnavailable() from None
        except (TypeError, ValueError):
            raise PollingCursorUnavailable() from None


class PostgresPollingCursorStore:
    """Read a configured cursor and delegate every mutation to one commit port."""

    def __init__(
        self,
        pool: Any,
        page_committer: PollingPageCommitter,
        *,
        account_id: int,
        folder: str,
    ) -> None:
        if type(account_id) is not int or not 1 <= account_id <= POSTGRES_BIGINT_MAX:
            raise ValueError("account_id must be a positive PostgreSQL BIGINT")
        if type(folder) is not str or not folder or len(folder) > 512:
            raise ValueError("folder must be an exact bounded string")
        if not callable(getattr(pool, "connection", None)):
            raise ValueError("pool is invalid")
        if not callable(getattr(page_committer, "commit_page", None)):
            raise ValueError("page_committer is invalid")
        self._pool = pool
        self._page_committer = page_committer
        self._account_id = account_id
        self._folder = folder

    def _require_scope(self, account_id: int, folder: str) -> None:
        if account_id != self._account_id or folder != self._folder:
            raise ValueError("cursor scope does not match store")

    async def load(
        self,
        account_id: int,
        folder: str,
    ) -> PollingCursorCheckpoint:
        self._require_scope(account_id, folder)
        async with self._pool.connection() as connection:
            result = await connection.execute(
                "SELECT cursor, status, version FROM public.sync_cursors "
                "WHERE account_id = %s AND folder_key = %s",
                (self._account_id, self._folder),
            )
            row = await result.fetchone()
        if row is None:
            return PollingCursorCheckpoint(cursor=None, version=0)
        checkpoint, _status = _checkpoint_from_row(row)
        return checkpoint

    async def commit_activation_boundary(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
    ) -> None:
        if type(checkpoint) is not PollingCursorCheckpoint or checkpoint.cursor is not None:
            raise ValueError("activation requires an empty cursor checkpoint")
        PollingCursorCheckpoint(cursor=next_cursor, version=0)
        result = await self._page_committer.commit_page(
            checkpoint,
            next_cursor,
            (),
            activation=True,
        )
        if (
            type(result) is not PollingPageCommitResult
            or result.cursor != next_cursor
            or result.version != checkpoint.version + 1
            or result.inserted_count != 0
            or result.duplicate_count != 0
        ):
            raise PollingCursorUnavailable()

    async def commit_delta(
        self,
        checkpoint: PollingCursorCheckpoint,
        next_cursor: str,
        events: Sequence[NormalizedIngressEvent],
    ) -> None:
        if type(checkpoint) is not PollingCursorCheckpoint or checkpoint.cursor is None:
            raise ValueError("delta requires an active cursor checkpoint")
        PollingCursorCheckpoint(cursor=next_cursor, version=0)
        exact_events = tuple(events)
        if any(
            type(event) is not NormalizedIngressEvent
            or event.account_id != self._account_id
            or event.folder != self._folder
            for event in exact_events
        ):
            raise ValueError("events do not belong to the cursor scope")
        result = await self._page_committer.commit_page(
            checkpoint,
            next_cursor,
            exact_events,
            activation=False,
        )
        if (
            type(result) is not PollingPageCommitResult
            or result.cursor != next_cursor
            or result.version != checkpoint.version + 1
            or result.inserted_count + result.duplicate_count != len(exact_events)
        ):
            raise PollingCursorUnavailable()


class PollingIngress:
    """Poll one configured folder and commit only post-activation deltas."""

    def __init__(
        self,
        *,
        account_id: int,
        scope: FolderScope,
        snapshot: PolicySnapshot,
        page_client: PollingPageClient,
        cursor_store: PollingCursorStore,
        page_limit: int = MAX_SYNC_CHANGES_PER_BATCH,
    ) -> None:
        if type(account_id) is not int or not 1 <= account_id <= POSTGRES_BIGINT_MAX:
            raise ValueError("account_id must be a positive PostgreSQL BIGINT")
        if type(scope) is not FolderScope:
            raise ValueError("scope must be an exact FolderScope")
        if scope.canonical_key != "INBOX" or scope.sync_folder != "INBOX":
            raise ValueError("polling requires the Gateway INBOX scope")
        if type(snapshot) is not PolicySnapshot or not snapshot.ready:
            raise ValueError("snapshot must be a ready PolicySnapshot")
        if scope not in snapshot.scopes:
            raise ValueError("scope must belong to snapshot")
        if (
            type(page_limit) is not int
            or not 1 <= page_limit <= MAX_SYNC_CHANGES_PER_BATCH
        ):
            raise ValueError("page_limit is invalid")
        if not callable(getattr(page_client, "sync_polling", None)):
            raise ValueError("page_client is invalid")
        for method in ("load", "commit_activation_boundary", "commit_delta"):
            if not callable(getattr(cursor_store, method, None)):
                raise ValueError("cursor_store is invalid")
        self._account_id = account_id
        self._scope = scope
        self._snapshot = snapshot
        self._page_client = page_client
        self._cursor_store = cursor_store
        self._page_limit = page_limit
        self._policy_resolver = ProcessingPolicyResolver()

    async def _fetch_page(
        self,
        cursor: str | None,
        *,
        discard_items: bool,
    ) -> SyncBatch:
        """Fetch the Gateway's complete delta response."""

        page = await self._page_client.sync_polling(
            self._account_id,
            self._scope.sync_folder,
            cursor,
            self._page_limit,
            discard_items=discard_items,
        )
        if type(page) is not SyncBatch:
            raise RuntimeError("polling_sync_page_invalid")
        if page.includes_last is not True:
            raise RuntimeError("polling_sync_response_incomplete")
        return page

    def _normalize_events(self, page: SyncBatch) -> tuple[NormalizedIngressEvent, ...]:
        return tuple(
            normalize_sync_change(
                self._account_id,
                self._scope.canonical_key,
                page.cursor,
                change,
                processing_policy=self._policy_resolver.resolve(
                    IngressSource.SYNC,
                    change.kind.value,
                    change.kind,
                    self._scope.sync_folder,
                    self._snapshot,
                ),
            )
            for change in page.changes
        )

    async def sync_once(self) -> PollingIngressOutcome:
        """Commit one complete delta, or establish the initial activation edge."""

        checkpoint = await self._cursor_store.load(
            self._account_id,
            self._scope.canonical_key,
        )
        if type(checkpoint) is not PollingCursorCheckpoint:
            raise RuntimeError("polling_cursor_checkpoint_invalid")

        if checkpoint.cursor is None:
            # The Gateway fully consumes its internal Exchange paging before
            # returning a final state. Fresh storage therefore creates one
            # baseline boundary from this deliberately discarded snapshot.
            page = await self._fetch_page(None, discard_items=True)
            await self._cursor_store.commit_activation_boundary(
                checkpoint,
                page.cursor,
            )
            return PollingIngressOutcome.BASELINED

        # Once active, the same complete Gateway delta is written through the
        # existing Inbox-plus-cursor transaction.
        page = await self._fetch_page(checkpoint.cursor, discard_items=False)
        await self._cursor_store.commit_delta(
            checkpoint,
            page.cursor,
            self._normalize_events(page),
        )
        return PollingIngressOutcome.COMMITTED


class _PollingIngressPort(Protocol):
    async def sync_once(self) -> PollingIngressOutcome: ...


class PollingRuntime:
    """Single-process scheduler for one or more configured polling scopes."""

    def __init__(
        self,
        ingresses: Sequence[_PollingIngressPort],
        *,
        interval_seconds: float,
    ) -> None:
        exact_ingresses = tuple(ingresses)
        if not exact_ingresses or any(
            not callable(getattr(ingress, "sync_once", None))
            for ingress in exact_ingresses
        ):
            raise ValueError("ingresses must be a non-empty polling sequence")
        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (int, float))
            or not math.isfinite(float(interval_seconds))
            or interval_seconds <= 0
        ):
            raise ValueError("interval_seconds must be a finite positive number")
        self._ingresses = exact_ingresses
        self._interval_seconds = float(interval_seconds)
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._activated = False

    @property
    def live(self) -> bool:
        task = self._task
        return bool(self._started and task is not None and not task.done())

    @property
    def ready(self) -> bool:
        return bool(self.live and self._activated)

    async def cycle_once(self) -> tuple[PollingIngressOutcome, ...]:
        outcomes: list[PollingIngressOutcome] = []
        for ingress in self._ingresses:
            outcomes.append(await ingress.sync_once())
        return tuple(outcomes)

    async def start(self) -> None:
        if self._started or self._task is not None:
            raise RuntimeError("polling_runtime_not_startable")
        self._stop_event.clear()
        self._activated = False
        self._started = True
        self._task = asyncio.create_task(
            self._run(initial_delay=0.0),
            name="exchange-sync-state-polling",
        )

    async def stop(self) -> None:
        self._started = False
        self._activated = False
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _transient_delay(self, error: SyncTransientError) -> float:
        retry_after = error.retry_after_seconds
        if type(retry_after) is int and retry_after > 0:
            return float(retry_after)
        return self._interval_seconds

    async def _run(self, initial_delay: float) -> None:
        delay = initial_delay
        while not self._stop_event.is_set():
            if delay > 0:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=delay,
                    )
                except TimeoutError:
                    pass
            if self._stop_event.is_set():
                return
            try:
                await self.cycle_once()
            except SyncTransientError as error:
                delay = self._transient_delay(error)
                logger.warning(
                    "Exchange polling activation is temporarily unavailable; retrying in %ss",
                    int(delay),
                )
            except PollingCursorConflict:
                delay = self._interval_seconds
                logger.info("Exchange polling cursor conflicted; retrying")
            except Exception as error:
                # The cursor only advances in the page commit, so retrying a
                # rejected poll is safe.  Keep the scheduler alive and take
                # readiness down instead of silently ending this task and
                # forcing the whole application into a restart loop.
                self._activated = False
                delay = self._interval_seconds
                candidate_code = getattr(type(error), "safe_code", None)
                safe_code = (
                    candidate_code
                    if type(candidate_code) is str
                    else "polling.unexpected_failure"
                )
                logger.error(
                    "Exchange polling attempt failed safely; retrying in %ss: "
                    "error_type=%s safe_code=%s",
                    int(delay),
                    type(error).__name__,
                    safe_code,
                )
            else:
                self._activated = True
                delay = self._interval_seconds


__all__ = [
    "PollingCursorCheckpoint",
    "PollingCursorConflict",
    "PollingCursorStore",
    "PollingCursorUnavailable",
    "GreenfieldSyncPageWriter",
    "PollingIngress",
    "PollingIngressOutcome",
    "PollingPageCommitResult",
    "PollingPageCommitter",
    "PollingRuntime",
    "PostgresPollingCursorStore",
]
