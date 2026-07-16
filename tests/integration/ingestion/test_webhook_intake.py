from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import HTTPException
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from starlette.requests import Request

from src.db.bootstrap import bootstrap_database
from src.ingestion.models import ChangeKind, IngressSource, ProcessingPolicy
from src.ingestion.ownership import PipelineOwnershipRepository
from src.ingestion.policy import FolderScope, PolicySnapshot, ProcessingPolicyResolver
from src.ingestion.repository import InboxRepository
from src.ingestion.webhook import WebhookIngressService
from src.server import exchange_webhook


_WEBHOOK_SECRET = "task9g-integration-secret"


@dataclass(slots=True)
class _Runtime:
    pool: AsyncConnectionPool
    ownership: PipelineOwnershipRepository


class _SnapshotProvider:
    def __init__(self, snapshot: PolicySnapshot) -> None:
        self._snapshot = snapshot

    async def get_ready_snapshot(self, account_id: int) -> PolicySnapshot:
        assert account_id == 8
        return self._snapshot


class _BarrierTransaction:
    def __init__(
        self,
        transaction: Any,
        committed: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._transaction = transaction
        self._committed = committed
        self._release = release

    async def __aenter__(self) -> object:
        return await self._transaction.__aenter__()

    async def __aexit__(self, error_type, error, traceback) -> object:
        result = await self._transaction.__aexit__(error_type, error, traceback)
        if error_type is None:
            self._committed.set()
            await self._release.wait()
        return result


class _BarrierConnection:
    def __init__(
        self,
        connection: Any,
        committed: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._connection = connection
        self._committed = committed
        self._release = release

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)

    def transaction(self) -> _BarrierTransaction:
        return _BarrierTransaction(
            self._connection.transaction(),
            self._committed,
            self._release,
        )


class _BarrierPool:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        committed: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._pool = pool
        self._committed = committed
        self._release = release

    @asynccontextmanager
    async def connection(self):
        async with self._pool.connection() as connection:
            yield _BarrierConnection(connection, self._committed, self._release)


class _CommitAcknowledgementLost(RuntimeError):
    pass


class _AckLossTransaction:
    def __init__(self, transaction: Any) -> None:
        self._transaction = transaction

    async def __aenter__(self) -> object:
        return await self._transaction.__aenter__()

    async def __aexit__(self, error_type, error, traceback) -> object:
        result = await self._transaction.__aexit__(error_type, error, traceback)
        if error_type is None:
            raise _CommitAcknowledgementLost("commit acknowledgement lost")
        return result


class _AckLossConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)

    def transaction(self) -> _AckLossTransaction:
        return _AckLossTransaction(self._connection.transaction())


class _AckLossPool:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def connection(self):
        async with self._pool.connection() as connection:
            yield _AckLossConnection(connection)


class _ForcedRollback(RuntimeError):
    pass


class _RollbackConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._raised = False

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)

    def transaction(self):
        return self._connection.transaction()

    async def execute(self, statement, params=None):
        try:
            rendered = statement.as_string(self._connection)
        except AttributeError:
            rendered = str(statement)
        cursor = await self._connection.execute(statement, params)
        if not self._raised and "INSERT INTO" in rendered and "event_inbox" in rendered:
            self._raised = True
            raise _ForcedRollback("force rollback after event insert")
        return cursor


class _RollbackPool:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def connection(self):
        async with self._pool.connection() as connection:
            yield _RollbackConnection(connection)


class _PausedInboxRepository:
    def __init__(
        self,
        repository: InboxRepository,
        entered: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._repository = repository
        self._entered = entered
        self._release = release

    async def insert(self, event, generation: int, fencing_token: int):
        self._entered.set()
        await self._release.wait()
        return await self._repository.insert(event, generation, fencing_token)


def _policy_snapshot() -> PolicySnapshot:
    matrix = {
        (IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE): (
            ProcessingPolicy.FULL
        ),
        (IngressSource.WEBHOOK, "CreatedEvent", ChangeKind.CREATE): (
            ProcessingPolicy.IGNORED
        ),
        (IngressSource.WEBHOOK, "ModifiedEvent", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.WEBHOOK, "DeletedEvent", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "create", ChangeKind.CREATE): ProcessingPolicy.FULL,
        (IngressSource.SYNC, "update", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "delete", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
    }
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("INBOX",),
                sync_folder="Inbox",
                event_policy_matrix=matrix,
            ),
        )
    )


def _service(runtime: _Runtime, inbox_repository: object) -> WebhookIngressService:
    return WebhookIngressService(
        expected_account_id=8,
        snapshot_provider=_SnapshotProvider(_policy_snapshot()),
        policy_resolver=ProcessingPolicyResolver(),
        ownership_repository=runtime.ownership,
        inbox_repository=inbox_repository,
    )


def _mail_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "account_id": 8,
        "event": "NewMailEvent",
        "timestamp": 1_752_384_245,
        "item_id": {"id": "message-1", "changekey": "version-1"},
        "parent_folder_id": {"id": "INBOX"},
        "message": "first delivery metadata",
    }
    payload.update(overrides)
    return payload


def _test_payload() -> dict[str, Any]:
    return {
        "event": "TestEvent",
        "timestamp": 1_752_384_245,
        "account_id": 8,
        "message": "Webhook test successful",
    }


def _raw(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _request(
    service: WebhookIngressService,
    payload: dict[str, Any],
    *,
    header_event: str,
) -> Request:
    body = _raw(payload)
    signature = hmac.new(
        _WEBHOOK_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    delivered = False

    async def receive() -> dict[str, object]:
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    application = SimpleNamespace(
        state=SimpleNamespace(webhook_ingress_service=service)
    )
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhooks/exchange",
            "app": application,
            "headers": [
                (b"content-type", b"application/json"),
                (b"x-exchange-event", header_event.encode("ascii")),
                (b"x-exchange-signature", signature.encode("ascii")),
            ],
        },
        receive,
    )


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        EXCHANGE_WEBHOOK_SECRET=_WEBHOOK_SECRET,
        WEBHOOK_MAX_BYTES=1_048_576,
    )


async def _count(pool: AsyncConnectionPool, statement: str, params=()) -> int:
    async with pool.connection() as connection:
        cursor = await connection.execute(statement, params)
        row = await cursor.fetchone()
    assert row is not None
    return int(next(iter(row.values())))


@pytest_asyncio.fixture
async def webhook_runtime(postgres_database_factory) -> _Runtime:
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    pool = AsyncConnectionPool(
        conninfo=schema.runtime_dsn,
        min_size=1,
        max_size=10,
        open=False,
        kwargs={"autocommit": True, "row_factory": dict_row},
    )
    await pool.open()
    ownership = PipelineOwnershipRepository(pool)
    await ownership.bootstrap(8, "durable_v1")
    try:
        yield _Runtime(pool=pool, ownership=ownership)
    finally:
        await pool.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_redelivery_commits_one_stable_inbox_identity(
    webhook_runtime: _Runtime,
) -> None:
    service = _service(webhook_runtime, InboxRepository(webhook_runtime.pool))
    payload = _mail_payload()

    with patch("src.server.get_settings", return_value=_settings()):
        responses = await asyncio.gather(
            *(
                exchange_webhook(
                    _request(service, payload, header_event="NewMailEvent")
                )
                for _ in range(2)
            )
        )

    assert {response.status_code for response in responses} == {202}
    assert {response.body for response in responses} == {b'{"status":"accepted"}'}
    assert await _count(webhook_runtime.pool, "SELECT count(*) FROM event_inbox") == 1

    changed = _mail_payload(message="changed delivery metadata")
    with patch("src.server.get_settings", return_value=_settings()):
        replay = await exchange_webhook(
            _request(service, changed, header_event="NewMailEvent")
        )
    assert replay.status_code == 202
    async with webhook_runtime.pool.connection() as connection:
        cursor = await connection.execute("SELECT payload FROM event_inbox")
        row = await cursor.fetchone()
    assert row is not None
    assert row["payload"]["message"] == "first delivery metadata"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_test_event_is_zero_write_and_unknown_folder_is_completed_ignored(
    webhook_runtime: _Runtime,
) -> None:
    service = _service(webhook_runtime, InboxRepository(webhook_runtime.pool))
    test_payload = _test_payload()

    with patch("src.server.get_settings", return_value=_settings()):
        test_response = await exchange_webhook(
            _request(service, test_payload, header_event="TestEvent")
        )
    assert test_response == {"status": "ok", "test": True}
    assert await _count(webhook_runtime.pool, "SELECT count(*) FROM event_inbox") == 0

    unknown = _mail_payload(parent_folder_id={"id": "UNKNOWN-FOLDER"})
    with patch("src.server.get_settings", return_value=_settings()):
        accepted = await exchange_webhook(
            _request(service, unknown, header_event="NewMailEvent")
        )
    assert accepted.status_code == 202
    async with webhook_runtime.pool.connection() as connection:
        cursor = await connection.execute(
            "SELECT status, processing_policy FROM event_inbox"
        )
        row = await cursor.fetchone()
    assert row == {"status": "completed", "processing_policy": "ignored"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_accept_waits_for_commit_acknowledgement_barrier(
    webhook_runtime: _Runtime,
) -> None:
    committed = asyncio.Event()
    release = asyncio.Event()
    repository = InboxRepository(_BarrierPool(webhook_runtime.pool, committed, release))
    service = _service(webhook_runtime, repository)
    payload = _mail_payload(item_id={"id": "barrier", "changekey": "v1"})

    with patch("src.server.get_settings", return_value=_settings()):
        task = asyncio.create_task(
            exchange_webhook(_request(service, payload, header_event="NewMailEvent"))
        )
        try:
            await asyncio.wait_for(committed.wait(), timeout=5)
            assert task.done() is False
            assert (
                await _count(webhook_runtime.pool, "SELECT count(*) FROM event_inbox")
                == 1
            )
        finally:
            release.set()
        response = await asyncio.wait_for(task, timeout=5)
    assert response.status_code == 202


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lost_commit_ack_never_returns_a_false_acceptance_and_replays(
    webhook_runtime: _Runtime,
) -> None:
    faulting = _service(
        webhook_runtime,
        InboxRepository(_AckLossPool(webhook_runtime.pool)),
    )
    payload = _mail_payload(item_id={"id": "ack-loss", "changekey": "v1"})

    with patch("src.server.get_settings", return_value=_settings()):
        with pytest.raises(HTTPException) as caught:
            await exchange_webhook(
                _request(faulting, payload, header_event="NewMailEvent")
            )
    assert caught.value.status_code == 503
    assert caught.value.detail == "Webhook ingress unavailable"
    assert await _count(webhook_runtime.pool, "SELECT count(*) FROM event_inbox") == 1

    healthy = _service(webhook_runtime, InboxRepository(webhook_runtime.pool))
    with patch("src.server.get_settings", return_value=_settings()):
        replay = await exchange_webhook(
            _request(healthy, payload, header_event="NewMailEvent")
        )
    assert replay.status_code == 202


@pytest.mark.integration
@pytest.mark.asyncio
async def test_insert_failure_rolls_back_and_http_returns_fixed_503(
    webhook_runtime: _Runtime,
) -> None:
    service = _service(
        webhook_runtime,
        InboxRepository(_RollbackPool(webhook_runtime.pool)),
    )
    payload = _mail_payload(item_id={"id": "rollback", "changekey": "v1"})
    audit_count = await _count(
        webhook_runtime.pool, "SELECT count(*) FROM audit_events"
    )

    with patch("src.server.get_settings", return_value=_settings()):
        with pytest.raises(HTTPException) as caught:
            await exchange_webhook(
                _request(service, payload, header_event="NewMailEvent")
            )

    assert caught.value.status_code == 503
    assert caught.value.detail == "Webhook ingress unavailable"
    assert await _count(webhook_runtime.pool, "SELECT count(*) FROM event_inbox") == 0
    assert (
        await _count(webhook_runtime.pool, "SELECT count(*) FROM audit_events")
        == audit_count
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_replay_revalidates_authority_inside_insert_transaction(
    webhook_runtime: _Runtime,
) -> None:
    payload = _mail_payload(item_id={"id": "fence-race", "changekey": "v1"})
    healthy = _service(webhook_runtime, InboxRepository(webhook_runtime.pool))
    with patch("src.server.get_settings", return_value=_settings()):
        first = await exchange_webhook(
            _request(healthy, payload, header_event="NewMailEvent")
        )
    assert first.status_code == 202

    entered = asyncio.Event()
    release = asyncio.Event()
    paused = _PausedInboxRepository(
        InboxRepository(webhook_runtime.pool),
        entered,
        release,
    )
    racing = _service(webhook_runtime, paused)
    with patch("src.server.get_settings", return_value=_settings()):
        task = asyncio.create_task(
            exchange_webhook(_request(racing, payload, header_event="NewMailEvent"))
        )
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            quiesced = await webhook_runtime.ownership.quiesce(
                8,
                1,
                1,
                "task9g-test",
                "linearize duplicate replay against authority change",
            )
            assert quiesced.state.value == "quiescing"
        finally:
            release.set()
        with pytest.raises(HTTPException) as caught:
            await asyncio.wait_for(task, timeout=5)

    assert caught.value.status_code == 503
    assert caught.value.detail == "Webhook ingress unavailable"
    assert await _count(webhook_runtime.pool, "SELECT count(*) FROM event_inbox") == 1
