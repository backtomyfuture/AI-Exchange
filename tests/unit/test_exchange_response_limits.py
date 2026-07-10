import pytest

from src.safety.http_response import read_json_limited
from src.safety.input_limits import InputLimitExceeded


class StreamingResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.chunks_consumed = 0

    async def aiter_bytes(self):
        for chunk in self._chunks:
            self.chunks_consumed += 1
            yield chunk

    @property
    def text(self) -> str:
        raise AssertionError("bounded parsing must never inspect response.text")


@pytest.mark.asyncio
async def test_read_json_limited_accepts_exact_byte_limit_across_async_chunks():
    response = StreamingResponse([b'{"ok":', b"true}"])

    result = await read_json_limited(response, max_bytes=11)

    assert result == {"ok": True}
    assert response.chunks_consumed == 2


@pytest.mark.asyncio
async def test_read_json_limited_aborts_as_soon_as_limit_is_exceeded():
    response = StreamingResponse([b"1234", b"56", b'{"never":"read"}'])

    with pytest.raises(InputLimitExceeded) as caught:
        await read_json_limited(response, max_bytes=5)

    assert caught.value.category == "exchange_response_bytes"
    assert response.chunks_consumed == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "secret_fragment"),
    [
        (b"\xffsecret", "secret"),
        (b'{"secret":', "secret"),
        (b'["secret"]', "secret"),
    ],
)
async def test_read_json_limited_rejects_invalid_payload_without_content_in_error(
    raw: bytes,
    secret_fragment: str,
):
    response = StreamingResponse([raw])

    with pytest.raises(ValueError) as caught:
        await read_json_limited(response, max_bytes=100)

    assert type(caught.value) is ValueError
    assert secret_fragment not in str(caught.value)
    assert caught.value.__cause__ is None
