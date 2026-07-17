from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace

import httpx
import pytest

import src.domain.errors as domain_errors
import src.utils.exchange_api as exchange_api
from src.domain.errors import ErrorKind
from src.ingestion.models import ChangeKind, SyncBatch
from src.utils.exchange_api import ExchangeClient


CONTRACT_VERSION = "exchange_sync_contract_v2"
CONTRACT_HEADER = ("X-Exchange-Sync-Contract", CONTRACT_VERSION)
VALID_ITEM = {
    "id": "message-1",
    "subject": "subject",
    "sender": "sender@example.com",
    "received_time": "2026-07-13T10:00:00",
    "is_read": False,
    "has_attachments": False,
}
VALID_CHANGE = {"change_type": "create", "id": "message-1", "item": VALID_ITEM}


def _settings(
    url: str = "https://exchange.internal/api/v1/exchange",
    *,
    max_bytes: int = 1_000_000,
) -> SimpleNamespace:
    return SimpleNamespace(
        EXCHANGE_API_URL=url,
        EXCHANGE_API_KEY="test-key",
        EXCHANGE_ACCOUNT_ID=8,
        EXCHANGE_SSL_VERIFY=False,
        EXCHANGE_CA_FILE="",
        EXCHANGE_FOLDER_SENTITEMS="Sent Items",
        EXCHANGE_FOLDER_DRAFTS="Drafts",
        EXCHANGE_RESPONSE_MAX_BYTES=max_bytes,
    )


def _wire_body(
    *,
    cursor: object = "cursor-2",
    includes_last: object = True,
    items: object = None,
) -> dict[str, object]:
    return {
        "code": 200,
        "msg": "OK",
        "data": {
            "sync_state": cursor,
            "includes_last": includes_last,
            "items": [VALID_CHANGE] if items is None else items,
        },
    }


def _json_response(
    status: int = 200,
    *,
    body: object | None = None,
    headers: list[tuple[str, str]] | None = None,
) -> httpx.Response:
    return httpx.Response(
        status,
        headers=headers if headers is not None else [CONTRACT_HEADER],
        content=json.dumps(body if body is not None else _wire_body()).encode(),
    )


class TrackingStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes | BaseException]) -> None:
        self.chunks = chunks
        self.chunks_consumed = 0
        self.closed = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.block = False

    async def __aiter__(self):
        self.started.set()
        if self.block:
            await self.release.wait()
        for chunk in self.chunks:
            self.chunks_consumed += 1
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
async def client_factory():
    clients: list[ExchangeClient] = []

    def factory(
        handler,
        *,
        url: str = "https://exchange.internal/api/v1/exchange",
        max_bytes: int = 1_000_000,
    ) -> ExchangeClient:
        client = ExchangeClient(settings=_settings(url, max_bytes=max_bytes))
        client._http_client = httpx.AsyncClient(  # noqa: SLF001 - fixture injection
            transport=httpx.MockTransport(handler),
            headers={"X-API-KEY": "test-key"},
        )
        clients.append(client)
        return client

    yield factory

    for client in clients:
        await client.close()


@pytest.fixture
def sync_error_types() -> SimpleNamespace:
    names = (
        "SyncAuthorizationError",
        "SyncCursorInvalidError",
        "SyncTransientError",
        "SyncContractError",
    )
    missing = [name for name in names if not hasattr(domain_errors, name)]
    assert missing == []
    return SimpleNamespace(**{name: getattr(domain_errors, name) for name in names})


def test_sync_errors_have_frozen_privacy_safe_machine_contract(sync_error_types) -> None:
    cases = [
        (
            sync_error_types.SyncAuthorizationError(),
            "exchange.sync.authorization_failed",
            "Exchange sync authorization failed",
            ErrorKind.AUTHENTICATION,
            False,
        ),
        (
            sync_error_types.SyncCursorInvalidError(),
            "exchange.sync.cursor_invalid",
            "Exchange sync cursor is invalid",
            ErrorKind.PERMANENT_DEPENDENCY,
            False,
        ),
        (
            sync_error_types.SyncTransientError(retry_after_seconds=12),
            "exchange.sync.transient_failure",
            "Exchange sync is temporarily unavailable",
            ErrorKind.TRANSIENT_DEPENDENCY,
            True,
        ),
        (
            sync_error_types.SyncContractError(),
            "exchange.sync.contract_invalid",
            "Exchange sync contract is invalid",
            ErrorKind.PERMANENT_DEPENDENCY,
            False,
        ),
    ]

    for error, code, summary, kind, retryable in cases:
        assert error.safe_code == code
        assert error.safe_summary == summary
        assert error.kind is kind
        assert error.retryable is retryable
        assert str(error) == summary
        assert error.args == (summary,)
        assert "TOP-SECRET" not in repr(error)
        with pytest.raises(AttributeError):
            error.retryable = not retryable
        with pytest.raises(AttributeError):
            error.secret = "TOP-SECRET"

    assert vars(cases[0][0]) == {}
    assert vars(cases[1][0]) == {}
    assert vars(cases[2][0]) == {"retry_after_seconds": 12}
    assert vars(cases[3][0]) == {}


def test_sync_errors_allow_only_python_traceback_bookkeeping(sync_error_types) -> None:
    try:
        raise RuntimeError("source")
    except RuntimeError as source:
        traceback = source.__traceback__

    error = sync_error_types.SyncContractError()
    error.__traceback__ = traceback
    assert error.__traceback__ is traceback
    error.__traceback__ = None

    with pytest.raises(AttributeError):
        error.__cause__ = RuntimeError("TOP-SECRET")
    with pytest.raises(AttributeError):
        error.__context__ = RuntimeError("TOP-SECRET")


@pytest.mark.parametrize("name", ["SyncAuthorizationError", "SyncCursorInvalidError", "SyncContractError"])
def test_non_transient_sync_errors_reject_caller_messages(
    sync_error_types,
    name: str,
) -> None:
    error_type = getattr(sync_error_types, name)

    with pytest.raises(TypeError) as caught:
        error_type("TOP-SECRET")

    assert "TOP-SECRET" not in str(caught.value)


@pytest.mark.parametrize("value", [True, -1, 3601, 1.5, "TOP-SECRET"])
def test_transient_error_rejects_invalid_retry_hint_without_retaining_value(
    sync_error_types,
    value: object,
) -> None:
    with pytest.raises(TypeError) as caught:
        sync_error_types.SyncTransientError(retry_after_seconds=value)

    assert str(caught.value) == "invalid retry_after_seconds"
    assert "TOP-SECRET" not in repr(caught.value)


def test_transient_error_rejects_int_subclass_without_rendering_it(
    sync_error_types,
) -> None:
    class SecretInt(int):
        def __repr__(self) -> str:
            return "TOP-SECRET-RETRY"

    with pytest.raises(TypeError) as caught:
        sync_error_types.SyncTransientError(
            retry_after_seconds=SecretInt(12),
        )

    assert str(caught.value) == "invalid retry_after_seconds"
    assert "TOP-SECRET-RETRY" not in repr(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configured_url", "expected_url"),
    [
        (
            "https://exchange.internal/api/v1/exchange",
            "https://exchange.internal/api/v1/exchange/emails/sync",
        ),
        (
            "https://exchange.internal:8443/api/v1/exchange/emails/",
            "https://exchange.internal:8443/api/v1/exchange/emails/sync",
        ),
    ],
)
async def test_sync_posts_exact_bounded_v2_request(
    client_factory,
    configured_url: str,
    expected_url: str,
) -> None:
    calls: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert str(request.url) == expected_url
        assert request.method == "POST"
        assert request.headers["X-API-KEY"] == "test-key"
        assert request.headers["Accept-Encoding"] == "identity"
        assert json.loads(request.content) == {
            "account_id": 8,
            "folder": "INBOX",
            "sync_state": "cursor-1",
            "limit": 500,
            "only_fields": [
                "id",
                "subject",
                "sender",
                "datetime_received",
                "is_read",
                "has_attachments",
            ],
        }
        assert request.extensions["timeout"] == {
            "connect": 10.0,
            "read": 135.0,
            "write": 10.0,
            "pool": 10.0,
        }
        return _json_response()

    client = client_factory(handler, url=configured_url)

    batch = await client.sync_emails(8, "INBOX", "cursor-1", 500)

    assert client.api_url == configured_url.rstrip("/")
    assert len(calls) == 1
    assert isinstance(batch, SyncBatch)
    assert batch.contract_version == CONTRACT_VERSION
    assert batch.cursor == "cursor-2"
    assert batch.includes_last is True
    assert [(change.kind, change.external_email_id) for change in batch.changes] == [
        (ChangeKind.CREATE, "message-1")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("account_id", "folder", "cursor", "limit"),
    [
        (1, "I", "c", 1),
        (2**63 - 1, "F" * 512, "C" * 8192, 500),
    ],
)
async def test_sync_accepts_exact_input_boundaries(
    client_factory,
    account_id: int,
    folder: str,
    cursor: str,
    limit: int,
) -> None:
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return _json_response(body=_wire_body(items=[]))

    batch = await client_factory(handler).sync_emails(
        account_id, folder, cursor, limit
    )

    assert len(requests) == 1
    assert requests[0]["account_id"] == account_id
    assert requests[0]["folder"] == folder
    assert requests[0]["sync_state"] == cursor
    assert requests[0]["limit"] == limit
    assert batch.changes == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        " https://exchange.internal/api/v1/exchange",
        "ftp://exchange.internal/api/v1/exchange",
        "https:///api/v1/exchange",
        "https://user@exchange.internal/api/v1/exchange",
        "https://exchange.internal/api/v1/exchange?secret=value",
        "https://exchange.internal/api/v1/exchange?",
        "https://exchange.internal/api/v1/exchange#fragment",
        "https://exchange.internal/api/v1/exchange#",
        "https://exchange.internal/api/v1/exch\nange",
        "https://exchange.internal/prefix/api/v1/exchange",
        "https://exchange.internal/api/v1/exchange/suffix",
        "https://exchange.internal/api/v1/%65xchange",
        "https://exchange.internal/api/v1/exchange//",
    ],
)
async def test_sync_rejects_invalid_base_url_without_request(
    client_factory,
    sync_error_types,
    url: str,
) -> None:
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _json_response()

    client = client_factory(handler, url=url)

    with pytest.raises(sync_error_types.SyncContractError):
        await client.sync_emails(8, "INBOX", "cursor-1", 500)

    assert calls == 0


@pytest.mark.asyncio
async def test_invalid_sync_url_fails_before_http_client_initialization(
    sync_error_types,
    monkeypatch,
) -> None:
    constructor_calls = 0

    def fail_if_initialized(*_args, **_kwargs):
        nonlocal constructor_calls
        constructor_calls += 1
        raise AssertionError("HTTP client must not be initialized")

    monkeypatch.setattr(exchange_api.httpx, "AsyncClient", fail_if_initialized)
    client = ExchangeClient(
        settings=_settings("https://exchange.internal/api/v1/exchange//")
    )

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client.sync_emails(8, "INBOX", "cursor-1", 500)

    assert constructor_calls == 0
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.asyncio
async def test_http_client_initialization_failure_is_safely_translated(
    sync_error_types,
    monkeypatch,
    caplog,
) -> None:
    sentinel = "TOP-SECRET-/private/ca.pem"
    constructor_calls = 0

    def fail_initialization(*_args, **_kwargs):
        nonlocal constructor_calls
        constructor_calls += 1
        raise RuntimeError(sentinel)

    monkeypatch.setattr(exchange_api.httpx, "AsyncClient", fail_initialization)
    client = ExchangeClient(settings=_settings())

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client.sync_emails(8, "INBOX", "cursor-1", 500)

    assert constructor_calls == 1
    assert client._http_client is None  # noqa: SLF001 - proves zero requests
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert sentinel not in str(caught.value)
    assert sentinel not in repr(caught.value)
    assert sentinel not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("account_id", "folder", "cursor", "limit"),
    [
        (True, "INBOX", "cursor-1", 500),
        (0, "INBOX", "cursor-1", 500),
        (2**63, "INBOX", "cursor-1", 500),
        (8, "", "cursor-1", 500),
        (8, " INBOX", "cursor-1", 500),
        (8, "INBOX\x1f", "cursor-1", 500),
        (8, "INBOX\x80", "cursor-1", 500),
        (8, "INBOX-\ud800", "cursor-1", 500),
        (8, "x" * 513, "cursor-1", 500),
        (8, "INBOX", "", 500),
        (8, "INBOX", " cursor-1", 500),
        (8, "INBOX", "cursor\x7f", 500),
        (8, "INBOX", "cursor-\ud800", 500),
        (8, "INBOX", "x" * 8193, 500),
        (8, "INBOX", "cursor-1", True),
        (8, "INBOX", "cursor-1", 0),
        (8, "INBOX", "cursor-1", 501),
    ],
)
async def test_sync_rejects_invalid_inputs_without_request(
    client_factory,
    sync_error_types,
    account_id: object,
    folder: object,
    cursor: object,
    limit: object,
) -> None:
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _json_response()

    client = client_factory(handler)

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client.sync_emails(account_id, folder, cursor, limit)

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cursor", "includes_last"),
    [("cursor-1", False), ("opaque+Boundary/%3D", False), ("cursor-2", True)],
)
async def test_empty_page_preserves_same_or_new_opaque_cursor(
    client_factory,
    cursor: str,
    includes_last: bool,
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(
            body=_wire_body(cursor=cursor, includes_last=includes_last, items=[])
        )

    batch = await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert batch.cursor == cursor
    assert batch.changes == ()
    assert batch.includes_last is includes_last


@pytest.mark.asyncio
async def test_sync_passes_each_wire_mapping_unmodified_to_shared_validator(
    client_factory,
    monkeypatch,
) -> None:
    changes = [
        VALID_CHANGE,
        {"change_type": "delete", "id": "message-2", "item": None},
    ]
    seen: list[object] = []
    real_validator = exchange_api.validate_sync_change_contract

    def spy(change):
        seen.append(change)
        return real_validator(change)

    monkeypatch.setattr(exchange_api, "validate_sync_change_contract", spy)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(body=_wire_body(items=changes))

    batch = await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert seen == changes
    assert [change.external_email_id for change in batch.changes] == [
        "message-1",
        "message-2",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {**VALID_CHANGE, "unexpected": "outer-extra"},
        {
            **VALID_CHANGE,
            "item": {**VALID_ITEM, "unexpected": "item-extra"},
        },
        {
            **VALID_CHANGE,
            "item": {**VALID_ITEM, "is_read": "false"},
        },
    ],
)
async def test_sync_passes_invalid_wire_mapping_unmodified_before_fail_closed(
    client_factory,
    sync_error_types,
    monkeypatch,
    change: dict[str, object],
) -> None:
    seen: list[object] = []
    real_validator = exchange_api.validate_sync_change_contract

    def spy(raw_change):
        seen.append(raw_change)
        return real_validator(raw_change)

    monkeypatch.setattr(exchange_api, "validate_sync_change_contract", spy)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(body=_wire_body(items=[change]))

    with pytest.raises(sync_error_types.SyncContractError):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert seen == [change]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [],
        [("X-Exchange-Sync-Contract", "legacy")],
        [("X-Exchange-Sync-Contract", "exchange_sync_contract_v3")],
        [("X-Exchange-Sync-Contract", f"{CONTRACT_VERSION}, legacy")],
        [CONTRACT_HEADER, CONTRACT_HEADER],
        [CONTRACT_HEADER, ("Content-Encoding", "gzip")],
        [CONTRACT_HEADER, ("Content-Encoding", "identity, identity")],
        [CONTRACT_HEADER, ("Content-Encoding", "identity"), ("Content-Encoding", "identity")],
    ],
)
async def test_sync_rejects_invalid_contract_or_content_encoding_before_body(
    client_factory,
    sync_error_types,
    headers: list[tuple[str, str]],
) -> None:
    stream = TrackingStream([json.dumps(_wire_body()).encode()])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=headers, stream=stream)

    with pytest.raises(sync_error_types.SyncContractError):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert stream.chunks_consumed == 0
    assert stream.closed is True


@pytest.mark.asyncio
async def test_sync_accepts_one_exact_identity_content_encoding(client_factory) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(
            headers=[CONTRACT_HEADER, ("Content-Encoding", "identity")]
        )

    batch = await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert batch.cursor == "cursor-2"


@pytest.mark.asyncio
async def test_sync_rejects_declared_oversize_200_before_body_iteration(
    client_factory,
    sync_error_types,
) -> None:
    stream = TrackingStream([b'{"never":"read"}'])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers=[CONTRACT_HEADER, ("Content-Length", "6")],
            stream=stream,
        )

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client_factory(handler, max_bytes=5).sync_emails(
            8, "INBOX", "cursor-1", 500
        )

    assert stream.chunks_consumed == 0
    assert stream.closed is True
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.asyncio
async def test_sync_rejects_incremental_oversize_at_first_crossing_chunk(
    client_factory,
    sync_error_types,
) -> None:
    stream = TrackingStream([b"123", b"456", b"TOP-SECRET-UNREAD"])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=[CONTRACT_HEADER], stream=stream)

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client_factory(handler, max_bytes=5).sync_emails(
            8, "INBOX", "cursor-1", 500
        )

    assert stream.chunks_consumed == 2
    assert stream.closed is True
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.asyncio
async def test_sync_accepts_content_length_equal_to_limit_and_closes_stream(
    client_factory,
) -> None:
    raw = json.dumps(_wire_body(items=[]), separators=(",", ":")).encode()
    stream = TrackingStream([raw[:7], raw[7:]])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers=[CONTRACT_HEADER, ("Content-Length", str(len(raw)))],
            stream=stream,
        )

    batch = await client_factory(handler, max_bytes=len(raw)).sync_emails(
        8, "INBOX", "cursor-1", 500
    )

    assert batch.changes == ()
    assert stream.chunks_consumed == 2
    assert stream.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        {"data": _wire_body()["data"]},
        {"code": 200, "msg": "OK", "data": _wire_body()["data"], "extra": 1},
        {"code": True, "msg": "OK", "data": _wire_body()["data"]},
        {"code": 201, "msg": "OK", "data": _wire_body()["data"]},
        {"code": 200, "msg": "ok", "data": _wire_body()["data"]},
        {"code": 200, "msg": "OK", "data": []},
        {"code": 200, "msg": "OK", "data": {"sync_state": "cursor-2", "items": []}},
        {
            "code": 200,
            "msg": "OK",
            "data": {
                "sync_state": "cursor-2",
                "includes_last": True,
                "items": [],
                "extra": 1,
            },
        },
        _wire_body(cursor=None),
        _wire_body(cursor=" cursor-2"),
        _wire_body(includes_last=1),
        _wire_body(items={}),
        _wire_body(
            items=[{"change_type": "read_flag_change", "id": "message-1", "item": None}]
        ),
    ],
)
async def test_sync_rejects_invalid_success_wrapper_or_data_shape(
    client_factory,
    sync_error_types,
    body: object,
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(body=body)

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.asyncio
async def test_sync_rejects_limit_plus_one_before_validating_items(
    client_factory,
    sync_error_types,
    monkeypatch,
) -> None:
    validator = pytest.fail
    monkeypatch.setattr(exchange_api, "validate_sync_change_contract", validator)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(body=_wire_body(items=[VALID_CHANGE, VALID_CHANGE]))

    with pytest.raises(sync_error_types.SyncContractError):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 1)


@pytest.mark.asyncio
async def test_sync_accepts_items_count_equal_to_requested_limit(client_factory) -> None:
    second_item = {**VALID_ITEM, "id": "message-2"}
    changes = [
        VALID_CHANGE,
        {"change_type": "update", "id": "message-2", "item": second_item},
    ]

    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(body=_wire_body(items=changes))

    batch = await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 2)

    assert [change.external_email_id for change in batch.changes] == [
        "message-1",
        "message-2",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second",
    [
        VALID_CHANGE,
        {
            "change_type": "create",
            "id": "message-1",
            "item": {**VALID_ITEM, "subject": "different"},
        },
    ],
)
async def test_sync_rejects_duplicate_page_identity(
    client_factory,
    sync_error_types,
    second: dict[str, object],
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response(body=_wire_body(items=[VALID_CHANGE, second]))

    with pytest.raises(sync_error_types.SyncContractError):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "error_name"),
    [
        (401, "SyncAuthorizationError"),
        (403, "SyncAuthorizationError"),
        *[
            (status, "SyncContractError")
            for status in range(400, 500)
            if status not in {401, 403, 408, 429}
        ],
        (199, "SyncContractError"),
        (204, "SyncContractError"),
        (302, "SyncContractError"),
        (600, "SyncContractError"),
        (408, "SyncTransientError"),
        (429, "SyncTransientError"),
        *[(status, "SyncTransientError") for status in range(500, 600)],
    ],
)
async def test_sync_classifies_status_without_reading_non_200_body(
    client_factory,
    sync_error_types,
    status: int,
    error_name: str,
) -> None:
    stream = TrackingStream([b"TOP-SECRET-RESPONSE"])
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, stream=stream)

    error_type = getattr(sync_error_types, error_name)
    with pytest.raises(error_type):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert stream.chunks_consumed == 0
    assert stream.closed is True
    assert calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [
            CONTRACT_HEADER,
            ("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2"),
        ],
    ],
)
async def test_exact_double_header_400_is_cursor_invalid(
    client_factory,
    sync_error_types,
    headers: list[tuple[str, str]],
) -> None:
    stream = TrackingStream([b"TOP-SECRET-RESPONSE"])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, headers=headers, stream=stream)

    with pytest.raises(sync_error_types.SyncCursorInvalidError):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert stream.chunks_consumed == 0
    assert stream.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2")],
        [CONTRACT_HEADER],
        [CONTRACT_HEADER, ("X-Exchange-Sync-Error", "legacy")],
        [
            ("X-Exchange-Sync-Contract", "legacy"),
            ("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2"),
        ],
        [
            ("X-Exchange-Sync-Contract", f"{CONTRACT_VERSION}, legacy"),
            ("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2"),
        ],
        [
            CONTRACT_HEADER,
            CONTRACT_HEADER,
            ("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2"),
        ],
        [
            CONTRACT_HEADER,
            ("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2, legacy"),
        ],
        [
            CONTRACT_HEADER,
            ("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2"),
            ("X-Exchange-Sync-Error", "exchange_sync_cursor_invalid_v2"),
        ],
    ],
)
async def test_ambiguous_400_never_authorizes_cursor_reset(
    client_factory,
    sync_error_types,
    headers: list[tuple[str, str]],
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, headers=headers, content=b"TOP-SECRET")

    with pytest.raises(sync_error_types.SyncContractError):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ("0", 0),
        ("12", 12),
        ("3600", 3600),
        ("3601", 3600),
        ("00012", 12),
        ("999999999999999999999999999", 3600),
        ("-1", None),
        ("+1", None),
        (" 12", None),
        ("1.5", None),
        ("12, 13", None),
        ("TOP-SECRET", None),
    ],
)
async def test_retry_after_decimal_is_bounded_and_never_retained_raw(
    client_factory,
    sync_error_types,
    raw: str | None,
    expected: int | None,
) -> None:
    headers = [] if raw is None else [("Retry-After", raw)]

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers=headers)

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == expected
    assert raw is None or raw not in repr(vars(caught.value)) or raw == str(expected)


@pytest.mark.asyncio
async def test_retry_after_http_date_uses_injected_utc_clock_and_ceil(
    client_factory,
    sync_error_types,
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 14, 1, 2, 3, 100_000, tzinfo=UTC)
    monkeypatch.setattr(exchange_api, "_utc_now", lambda: now)
    retry_date = format_datetime(now + timedelta(seconds=13.1), usegmt=True)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"Retry-After": retry_date})

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == 13


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "retry_date",
    [
        "Sun, 06 Nov 1994 08:49:37 GMT",
        "Sunday, 06-Nov-94 08:49:37 GMT",
        "Sun Nov  6 08:49:37 1994",
    ],
)
async def test_retry_after_accepts_all_three_exact_http_date_formats(
    client_factory,
    sync_error_types,
    monkeypatch,
    retry_date: str,
) -> None:
    now = datetime(1994, 11, 6, 8, 49, 30, tzinfo=UTC)
    monkeypatch.setattr(exchange_api, "_utc_now", lambda: now)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"Retry-After": retry_date})

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == 7


@pytest.mark.asyncio
async def test_retry_after_http_date_accepts_leap_second(
    client_factory,
    sync_error_types,
    monkeypatch,
) -> None:
    now = datetime(2016, 12, 31, 23, 59, 58, tzinfo=UTC)
    monkeypatch.setattr(exchange_api, "_utc_now", lambda: now)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            headers={"Retry-After": "Sat, 31 Dec 2016 23:59:60 GMT"},
        )

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_date", "expected"),
    [
        ("Wednesday, 01-Jan-76 00:00:00 GMT", 3600),
        ("Thursday, 01-Jan-76 00:00:01 GMT", 0),
    ],
)
async def test_retry_after_rfc850_uses_complete_fifty_year_boundary(
    client_factory,
    sync_error_types,
    monkeypatch,
    retry_date: str,
    expected: int,
) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(exchange_api, "_utc_now", lambda: now)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"Retry-After": retry_date})

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == expected


@pytest.mark.asyncio
async def test_retry_after_rfc850_advances_century_below_fifty_year_boundary(
    client_factory,
    sync_error_types,
    monkeypatch,
) -> None:
    now = datetime(1990, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(exchange_api, "_utc_now", lambda: now)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            headers={"Retry-After": "Saturday, 01-Jan-00 00:00:00 GMT"},
        )

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == 3600


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retry_date", "expected"),
    [
        ("Sunday, 01-Jan-40 00:00:00 GMT", 0),
        ("Thursday, 31-Dec-39 23:59:59 GMT", 3600),
    ],
)
async def test_retry_after_rfc850_uses_complete_lower_fifty_year_boundary(
    client_factory,
    sync_error_types,
    monkeypatch,
    retry_date: str,
    expected: int,
) -> None:
    now = datetime(2090, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(exchange_api, "_utc_now", lambda: now)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"Retry-After": retry_date})

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delta", "expected"),
    [
        (timedelta(seconds=-1), 0),
        (timedelta(days=2), 3600),
    ],
)
async def test_retry_after_http_date_past_and_far_future_are_bounded(
    client_factory,
    sync_error_types,
    monkeypatch,
    delta: timedelta,
    expected: int,
) -> None:
    now = datetime(2026, 7, 14, 1, 2, 3, tzinfo=UTC)
    monkeypatch.setattr(exchange_api, "_utc_now", lambda: now)
    retry_date = format_datetime(now + delta, usegmt=True)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"Retry-After": retry_date})

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [
        "not-a-date",
        "Tue, 32 Jul 2026 01:02:03 GMT",
        "Tue, 14 Jul 2026 01:02:03",
        "Tue, 14 Jul 2026 01:02:03 GMT trailing",
        "Tue, 14 Jul 2026 01:02:03 +0800",
        "Tue, 14 Jul 2026 01:02:03 GMT, Tue, 14 Jul 2026 01:02:04 GMT",
        "Sunday, 06-Nov-94 08:49:37 GMT trailing",
        "Sunday, 06-Nov-94 08:49:37 +0000",
        "Sun Nov  6 08:49:37 1994 trailing",
        "Monday, 06-Nov-94 08:49:37 GMT",
        "Mon Nov  6 08:49:37 1994",
    ],
)
async def test_invalid_retry_after_http_dates_are_ignored(
    client_factory,
    sync_error_types,
    raw: str,
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"Retry-After": raw})

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds is None


@pytest.mark.asyncio
async def test_repeated_retry_after_is_ignored(
    client_factory,
    sync_error_types,
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers=[("Retry-After", "1"), ("Retry-After", "2")])

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.retry_after_seconds is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        httpx.ReadTimeout("TOP-SECRET-TIMEOUT"),
        httpx.ConnectError("TOP-SECRET-CONNECT"),
    ],
)
async def test_network_failures_translate_outside_exception_context(
    client_factory,
    sync_error_types,
    failure: Exception,
) -> None:
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise failure

    with pytest.raises(sync_error_types.SyncTransientError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert calls == 1
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "TOP-SECRET" not in repr(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "error_name"),
    [
        (httpx.ReadError("TOP-SECRET-MIDSTREAM"), "SyncTransientError"),
        (httpx.DecodingError("TOP-SECRET-DECODING"), "SyncContractError"),
    ],
)
async def test_midstream_failures_close_stream_and_clear_context(
    client_factory,
    sync_error_types,
    failure: Exception,
    error_name: str,
) -> None:
    stream = TrackingStream([b'{"code":', failure])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=[CONTRACT_HEADER], stream=stream)

    with pytest.raises(getattr(sync_error_types, error_name)) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert stream.closed is True
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert "TOP-SECRET" not in repr(caught.value)


@pytest.mark.asyncio
async def test_json_and_validator_failures_clear_context_and_do_not_leak(
    client_factory,
    sync_error_types,
    monkeypatch,
) -> None:
    sentinel = "TOP-SECRET-VALIDATOR"

    def fail_validator(_change):
        raise RuntimeError(sentinel)

    monkeypatch.setattr(exchange_api, "validate_sync_change_contract", fail_validator)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return _json_response()

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert sentinel not in repr(caught.value)


@pytest.mark.asyncio
async def test_malformed_json_is_safely_translated_without_body_or_log_leak(
    client_factory,
    sync_error_types,
    caplog,
) -> None:
    sentinel = "TOP-SECRET-MALFORMED-BODY"
    stream = TrackingStream([f'{{"value":"{sentinel}"'.encode()])

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=[CONTRACT_HEADER], stream=stream)

    with pytest.raises(sync_error_types.SyncContractError) as caught:
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert stream.chunks_consumed == 1
    assert stream.closed is True
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert sentinel not in str(caught.value)
    assert sentinel not in repr(caught.value)
    assert sentinel not in caplog.text


@pytest.mark.asyncio
async def test_redirect_is_not_followed_and_call_is_not_retried(
    client_factory,
    sync_error_types,
    monkeypatch,
) -> None:
    calls: list[str] = []
    stream_kwargs: list[dict[str, object]] = []
    original_stream = httpx.AsyncClient.stream

    def recording_stream(client, *args, **kwargs):
        stream_kwargs.append(dict(kwargs))
        return original_stream(client, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "stream", recording_stream)

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(307, headers={"Location": "/TOP-SECRET-REDIRECT"})

    with pytest.raises(sync_error_types.SyncContractError):
        await client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)

    assert len(calls) == 1
    assert len(stream_kwargs) == 1
    assert stream_kwargs[0]["follow_redirects"] is False


@pytest.mark.asyncio
async def test_cancellation_propagates_and_closes_stream(client_factory) -> None:
    stream = TrackingStream([json.dumps(_wire_body()).encode()])
    stream.block = True

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=[CONTRACT_HEADER], stream=stream)

    task = asyncio.create_task(
        client_factory(handler).sync_emails(8, "INBOX", "cursor-1", 500)
    )
    await stream.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert stream.closed is True
