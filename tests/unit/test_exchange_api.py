import json

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from pydantic import SecretStr

from src.config import Settings
from src.domain.send_result import ExchangeSendOutcome, ExchangeSendResult
from src.utils.exchange_api import ExchangeClient


class StreamingResponse:
    def __init__(
        self,
        status_code: int,
        chunks: list[bytes],
        *,
        headers: list[tuple[str, str]] | None = None,
    ) -> None:
        self.status_code = status_code
        self._chunks = chunks
        self.chunks_consumed = 0
        self.headers = httpx.Headers(headers or [])

    async def aiter_bytes(self):
        for chunk in self._chunks:
            self.chunks_consumed += 1
            yield chunk

    @property
    def text(self) -> str:
        raise AssertionError("bounded Exchange reads must never inspect response.text")


class _StreamContext:
    def __init__(self, response: StreamingResponse) -> None:
        self._response = response

    async def __aenter__(self) -> StreamingResponse:
        return self._response

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None


class StreamingHTTPClient:
    def __init__(self, responses: list[StreamingResponse]) -> None:
        self._responses = list(responses)
        self.stream_calls: list[tuple[tuple, dict]] = []

    def stream(self, *args, **kwargs) -> _StreamContext:
        self.stream_calls.append((args, kwargs))
        return _StreamContext(self._responses.pop(0))

@pytest.mark.asyncio
async def test_get_recent_emails_empty(mock_settings):
    """Test getting recent emails when the list is empty."""
    client = ExchangeClient(settings=mock_settings)

    mock_http = StreamingHTTPClient(
        [
            StreamingResponse(
                200,
                [b'{"code":200,"data":{"items":[]},"message":"OK"}'],
            )
        ]
    )

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        emails = await client.get_recent_emails()
        assert emails == []
        assert len(mock_http.stream_calls) == 1
        assert mock_http.stream_calls[0][0][0] == "GET"


@pytest.mark.asyncio
async def test_get_recent_emails_with_items(mock_settings):
    """Test getting recent emails with items, verifying detail fetching behavior."""
    client = ExchangeClient(settings=mock_settings)

    mock_http = StreamingHTTPClient(
        [
            StreamingResponse(
                200,
                [
                    b'{"code":200,"data":{"items":['
                    b'{"id":"1","subject":"Test 1"},'
                    b'{"id":"2","subject":"Test 2","body":"Load content"}]}}'
                ],
            ),
            StreamingResponse(
                200,
                [b'{"code":200,"data":{"id":"1","subject":"Test 1","body":"Fetched content"}}'],
            ),
            StreamingResponse(
                200,
                [b'{"code":200,"data":{"id":"2","subject":"Test 2","body":"Load content"}}'],
            ),
        ]
    )

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        emails = await client.get_recent_emails()
        
        assert len(emails) == 2
        assert emails[0]["body"] == "Fetched content"
        assert emails[1]["body"] == "Load content"
        
        assert len(mock_http.stream_calls) == 3


@pytest.mark.asyncio
async def test_get_recent_emails_aborts_oversized_list_without_reading_text(mock_settings):
    mock_settings.EXCHANGE_RESPONSE_MAX_BYTES = 5
    client = ExchangeClient(settings=mock_settings)
    response = StreamingResponse(200, [b"1234", b"56", b"never-read"])
    mock_http = StreamingHTTPClient([response])

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        emails = await client.get_recent_emails()

    assert emails == []
    assert response.chunks_consumed == 2


@pytest.mark.asyncio
async def test_get_recent_emails_skips_invalid_detail_and_continues(mock_settings):
    client = ExchangeClient(settings=mock_settings)
    mock_http = StreamingHTTPClient(
        [
            StreamingResponse(
                200,
                [b'{"data":{"items":[{"id":"1"},{"id":"2"}]}}'],
            ),
            StreamingResponse(200, [b"not-json"]),
            StreamingResponse(200, [b'{"data":{"id":"2","body":"ok"}}']),
        ]
    )

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        emails = await client.get_recent_emails()

    assert emails == [{"id": "2", "body": "ok"}]
    assert len(mock_http.stream_calls) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"not-json", b'["not-an-object"]'])
async def test_get_email_returns_none_for_invalid_streamed_response(
    mock_settings,
    raw: bytes,
):
    client = ExchangeClient(settings=mock_settings)
    mock_http = StreamingHTTPClient([StreamingResponse(200, [raw])])

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        email = await client.get_email("email-id")

    assert email is None
    assert mock_http.stream_calls[0][0][0] == "GET"


@pytest.mark.asyncio
async def test_get_email_returns_none_for_oversized_response(mock_settings):
    mock_settings.EXCHANGE_RESPONSE_MAX_BYTES = 5
    client = ExchangeClient(settings=mock_settings)
    response = StreamingResponse(200, [b"1234", b"56", b"never-read"])
    mock_http = StreamingHTTPClient([response])

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        email = await client.get_email("email-id")

    assert email is None
    assert response.chunks_consumed == 2


@pytest.mark.asyncio
async def test_get_recent_emails_returns_empty_for_non_object_data(mock_settings):
    client = ExchangeClient(settings=mock_settings)
    mock_http = StreamingHTTPClient(
        [StreamingResponse(200, [b'{"data":[{"id":"unexpected"}]}'])]
    )

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        emails = await client.get_recent_emails()

    assert emails == []
    assert len(mock_http.stream_calls) == 1


@pytest.mark.asyncio
async def test_get_email_returns_none_for_non_object_data(mock_settings):
    client = ExchangeClient(settings=mock_settings)
    mock_http = StreamingHTTPClient(
        [StreamingResponse(200, [b'{"data":[{"id":"unexpected"}]}'])]
    )

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        email = await client.get_email("email-id")

    assert email is None


@pytest.mark.asyncio
async def test_get_email_adds_canonical_recipient_fields(mock_settings):
    raw_email = {
        "id": "email-id",
        "to_recipients": [
            "Mailbox(name='同名用户3', email_address='me@example.com')"
        ],
        "cc_recipients": [
            "Mailbox(name='抄送用户', email_address='cc@example.com')"
        ],
    }
    response_body = json.dumps({"data": raw_email}, ensure_ascii=False).encode()
    client = ExchangeClient(settings=mock_settings)
    mock_http = StreamingHTTPClient(
        [StreamingResponse(200, [response_body])]
    )

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        email = await client.get_email("email-id")

    assert email == {
        **raw_email,
        "to": raw_email["to_recipients"],
        "cc": raw_email["cc_recipients"],
    }


@pytest.mark.asyncio
async def test_send_email_success(mock_settings):
    """Test sending an email successfully."""
    client = ExchangeClient(settings=mock_settings)
    
    mock_http = AsyncMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_http.post.return_value = mock_resp

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        result = await client.send_email("test@example.com", "Subj", "Body")
        assert result is True
        
        mock_http.post.assert_called_once()
        args, kwargs = mock_http.post.call_args
        assert kwargs["json"]["to"] == ["test@example.com"]
        assert kwargs["json"]["subject"] == "Subj"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["reply", "forward"])
async def test_existing_email_send_returns_typed_confirmed_result(
    mock_settings,
    operation: str,
):
    client = ExchangeClient(settings=mock_settings)
    mock_http = AsyncMock()
    response = MagicMock(status_code=200)
    response.json.return_value = {"code": 200, "data": {"ignored": True}}
    mock_http.post.return_value = response

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        if operation == "reply":
            result = await client.reply_email_result(
                "mail-1",
                "body-secret",
                to=["to@example.com"],
                cc=["cc@example.com"],
            )
        else:
            result = await client.forward_email_result(
                "mail-1",
                ["to@example.com"],
                "body-secret",
            )

    assert result == ExchangeSendResult.sent()
    assert result.outcome is ExchangeSendOutcome.SENT
    assert result.confirmed_sent is True
    assert result.safe_code == "exchange.send.confirmed"
    mock_http.post.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "payload", "json_error"),
    [
        (500, {"code": 200}, None),
        (200, {"code": 500}, None),
        (200, ["not-an-object"], None),
        (200, None, ValueError("private invalid response")),
    ],
    ids=["http-error", "unconfirmed-code", "non-object", "invalid-json"],
)
async def test_existing_email_send_maps_every_non_confirmation_to_unknown(
    mock_settings,
    status_code: int,
    payload,
    json_error: Exception | None,
):
    client = ExchangeClient(settings=mock_settings)
    mock_http = AsyncMock()
    response = MagicMock(status_code=status_code)
    if json_error is None:
        response.json.return_value = payload
    else:
        response.json.side_effect = json_error
    mock_http.post.return_value = response

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        result = await client.reply_email_result(
            "mail-1",
            "body-secret",
            to=["to@example.com"],
        )

    assert result.outcome is ExchangeSendOutcome.UNKNOWN
    assert result.status_code == status_code
    assert result.confirmed_sent is False
    assert result.safe_code == "exchange.send.outcome_unknown"
    mock_http.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_existing_email_send_exception_is_unknown_without_retry(mock_settings):
    client = ExchangeClient(settings=mock_settings)
    mock_http = AsyncMock()
    mock_http.post.side_effect = httpx.ReadTimeout("private timeout detail")

    with patch.object(
        type(client),
        "http_client",
        new_callable=PropertyMock,
        return_value=mock_http,
    ):
        result = await client.forward_email_result(
            "mail-1",
            ["to@example.com"],
            "body-secret",
        )

    assert result == ExchangeSendResult.unknown()
    mock_http.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_draft_success(mock_settings):
    """Test creating a draft successfully."""
    client = ExchangeClient(settings=mock_settings)
    
    mock_http = AsyncMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_http.post.return_value = mock_resp

    with patch.object(type(client), "http_client", new_callable=PropertyMock, return_value=mock_http):
        result = await client.create_draft("test@example.com", "Draft Subj", "Draft Body")
        assert result is True
        
        endpoint = mock_http.post.call_args[0][0]
        assert "/drafts" in endpoint

def test_sync_api_is_available_as_a_dormant_explicit_client_method(mock_settings):
    client = ExchangeClient(settings=mock_settings)

    assert callable(getattr(client, "sync_emails", None))
    assert not hasattr(client, "validate_sync_permission")


@pytest.mark.asyncio
async def test_close_closes_shared_async_client(mock_settings):
    """Closing the ExchangeClient should close its shared httpx.AsyncClient."""
    client = ExchangeClient(settings=mock_settings)
    http_client = client.http_client

    try:
        await client.close()
    finally:
        if not http_client.is_closed:
            await http_client.aclose()

    assert http_client.is_closed
    assert client._http_client is None


def test_exchange_tls_verification_defaults_to_true():
    settings = Settings(_env_file=None)

    assert settings.EXCHANGE_SSL_VERIFY is True


def test_exchange_client_uses_configured_ca_bundle(mock_settings, tmp_path):
    ca_bundle = tmp_path / "exchange-ca.pem"
    ca_bundle.write_text("test-ca", encoding="utf-8")
    mock_settings.EXCHANGE_SSL_VERIFY = True
    mock_settings.EXCHANGE_CA_FILE = str(ca_bundle)
    mock_settings.EXCHANGE_API_KEY = SecretStr("api-key-sentinel")
    client = ExchangeClient(settings=mock_settings)

    with patch("src.utils.exchange_api.httpx.AsyncClient") as async_client:
        _ = client.http_client

    assert async_client.call_args.kwargs["verify"] == str(ca_bundle)
    assert async_client.call_args.kwargs["headers"] == {
        "X-API-KEY": "api-key-sentinel"
    }


def test_exchange_client_uses_system_ca_when_no_bundle(mock_settings):
    mock_settings.EXCHANGE_SSL_VERIFY = True
    mock_settings.EXCHANGE_CA_FILE = ""
    client = ExchangeClient(settings=mock_settings)

    with patch("src.utils.exchange_api.httpx.AsyncClient") as async_client:
        _ = client.http_client

    assert async_client.call_args.kwargs["verify"] is True
