import json
import logging
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import psycopg
import pytest

from src.domain.errors import DatabaseOperationError
from src.storage import ContentRef, ContentStoreReferenceError
from src.utils.db_async import AsyncDatabaseManager


class FakeCursor:
    def __init__(self, *, rowcount: int = 1, fetchone_result=None):
        self.rowcount = rowcount
        self.fetchone_result = fetchone_result
        self.executions = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def execute(self, query, params=None):
        self.executions.append((query, params))

    async def fetchone(self):
        return self.fetchone_result


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


@asynccontextmanager
async def fake_connection(cursor: FakeCursor):
    yield FakeConnection(cursor)


def _db(cursor: FakeCursor) -> AsyncDatabaseManager:
    manager = AsyncDatabaseManager(MagicMock(database_url="postgresql://test/test"))
    manager.get_connection = lambda: fake_connection(cursor)
    return manager


def _ref() -> ContentRef:
    return ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000007",
        key_version="v1",
        sha256="a" * 64,
    )


@pytest.mark.asyncio
async def test_content_ref_typed_write_and_restart_read_round_trip():
    write_cursor = FakeCursor(rowcount=1)
    await _db(write_cursor).set_content_ref("mail-1", _ref())

    query, params = write_cursor.executions[-1]
    assert "content_ref" in query
    persisted = params[0]
    if not isinstance(persisted, str):
        persisted = persisted.obj
    if isinstance(persisted, str):
        persisted = json.loads(persisted)

    read_cursor = FakeCursor(fetchone_result={"content_ref": persisted})
    loaded = await _db(read_cursor).get_content_ref("mail-1")

    assert loaded == _ref()
    assert read_cursor.executions[-1][1] == ("mail-1",)


@pytest.mark.asyncio
async def test_missing_and_malformed_content_refs_fail_closed():
    assert await _db(FakeCursor(fetchone_result=None)).get_content_ref("missing") is None

    malformed = {"account_id": 8, "object_id": "not-a-uuid"}
    with pytest.raises(ContentStoreReferenceError):
        await _db(FakeCursor(fetchone_result={"content_ref": malformed})).get_content_ref(
            "mail-1"
        )


@pytest.mark.asyncio
async def test_set_content_ref_fails_when_email_row_is_absent():
    with pytest.raises(DatabaseOperationError) as caught:
        await _db(FakeCursor(rowcount=0)).set_content_ref("missing", _ref())

    assert caught.value.operation == "set_content_ref"


@pytest.mark.asyncio
async def test_content_ref_claim_is_atomic_and_reports_winner():
    winner_cursor = FakeCursor(rowcount=1)
    loser_cursor = FakeCursor(rowcount=0)

    assert await _db(winner_cursor).set_content_ref_if_absent("mail-1", _ref()) is True
    assert await _db(loser_cursor).set_content_ref_if_absent("mail-1", _ref()) is False
    query, params = winner_cursor.executions[-1]
    assert "content_ref IS NULL" in query
    assert params[1] == "mail-1"


@pytest.mark.asyncio
async def test_content_ref_claim_wraps_database_error_without_logging_details(caplog):
    private = "PRIVATE-CONTENT-REF-DATABASE-DETAIL"
    manager = AsyncDatabaseManager(MagicMock(database_url="postgresql://test/test"))

    @asynccontextmanager
    async def failing_connection():
        raise psycopg.OperationalError(private)
        yield

    manager.get_connection = failing_connection
    caplog.set_level(logging.ERROR)

    with pytest.raises(DatabaseOperationError) as caught:
        await manager.set_content_ref_if_absent("mail-1", _ref())

    assert caught.value.operation == "set_content_ref_if_absent"
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    assert private not in str(caught.value)
    assert private not in caplog.text


@pytest.mark.asyncio
async def test_draft_store_uses_existing_row_and_never_returns_content_in_identifier():
    content = "PRIVATE-DRAFT-CONTENT"
    save_cursor = FakeCursor(rowcount=1)

    draft_id = await _db(save_cursor).save_draft("mail-1", content)

    assert draft_id == "mail-1"
    assert content not in draft_id
    query, params = save_cursor.executions[-1]
    assert "draft_content" in query
    assert params == (content, "mail-1")

    load_cursor = FakeCursor(fetchone_result={"draft_content": content})
    assert await _db(load_cursor).load_draft(draft_id) == content


@pytest.mark.asyncio
async def test_draft_store_missing_rows_fail_safely_without_logging_content(caplog):
    caplog.set_level(logging.ERROR)
    private = "PRIVATE-DRAFT-MUST-NOT-LOG"

    with pytest.raises(DatabaseOperationError) as save_error:
        await _db(FakeCursor(rowcount=0)).save_draft("missing", private)
    with pytest.raises(DatabaseOperationError) as load_error:
        await _db(FakeCursor(fetchone_result=None)).load_draft("missing")

    assert save_error.value.operation == "save_draft"
    assert load_error.value.operation == "load_draft"
    assert private not in caplog.text


@pytest.mark.asyncio
async def test_draft_store_wraps_database_errors_without_logging_draft(caplog):
    private = "PRIVATE-DATABASE-DRAFT"
    manager = AsyncDatabaseManager(MagicMock(database_url="postgresql://test/test"))

    @asynccontextmanager
    async def failing_connection():
        raise psycopg.OperationalError(private)
        yield

    manager.get_connection = failing_connection
    caplog.set_level(logging.ERROR)

    with pytest.raises(DatabaseOperationError) as caught:
        await manager.save_draft("mail-1", private)

    assert caught.value.operation == "save_draft"
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    assert private not in caplog.text
