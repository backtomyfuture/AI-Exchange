import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from src.utils.exchange_api import ExchangeClient


class StreamingResponse:
    def __init__(self, status_code: int, chunks: list[bytes]) -> None:
        self.status_code = status_code
        self._chunks = chunks
        self.chunks_consumed = 0

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

def test_sync_api_removed(mock_settings):
    """sync_emails has been removed after webhook migration."""
    client = ExchangeClient(settings=mock_settings)
    assert not hasattr(client, "sync_emails")


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
