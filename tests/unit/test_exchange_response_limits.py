import httpx
import pytest

import src.safety.http_response as http_response
from src.safety.http_response import read_json_limited
from src.safety.input_limits import InputLimitExceeded


class StreamingResponse:
    def __init__(
        self,
        chunks: list[bytes],
        *,
        headers: list[tuple[str, str]] | None = None,
    ) -> None:
        self._chunks = chunks
        self.chunks_consumed = 0
        self.headers = httpx.Headers(headers or [])

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
async def test_read_json_limited_rejects_excessive_width_before_json_decode(
    monkeypatch,
):
    raw = b'{"a":1,"b":2}'
    decode_calls = 0

    def fail_if_decoded(*_args, **_kwargs):
        nonlocal decode_calls
        decode_calls += 1
        raise AssertionError("over-wide JSON must fail before object construction")

    monkeypatch.setattr(http_response.json, "loads", fail_if_decoded)

    with pytest.raises(ValueError, match="invalid JSON response"):
        await read_json_limited(
            StreamingResponse([raw]),
            max_bytes=len(raw),
            max_structure_tokens=4,
        )

    assert decode_calls == 0


@pytest.mark.asyncio
async def test_read_json_limited_rejects_declared_oversize_before_first_chunk():
    response = StreamingResponse(
        [b'{"never":"read"}'],
        headers=[("Content-Length", "101")],
    )

    with pytest.raises(InputLimitExceeded) as caught:
        await read_json_limited(response, max_bytes=100)

    assert caught.value.category == "exchange_response_bytes"
    assert response.chunks_consumed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        [("Content-Length", "-1")],
        [("Content-Length", "1.5")],
        [("Content-Length", "10, 10")],
        [("Content-Length", "10"), ("Content-Length", "10")],
    ],
)
async def test_read_json_limited_rejects_malformed_or_repeated_content_length(
    headers: list[tuple[str, str]],
):
    response = StreamingResponse([b'{"ok":true}'], headers=headers)

    with pytest.raises(ValueError) as caught:
        await read_json_limited(response, max_bytes=100)

    assert str(caught.value) == "invalid JSON response"
    assert response.chunks_consumed == 0


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [
        b'{"outer":{"duplicate":1,"duplicate":2}}',
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b'{"value":-Infinity}',
        b'{"value":1e9999}',
        b'{"value":"\\ud800"}',
        b'{"\\ud800":"value"}',
        b"[1,2,3]",
        (b'{"value":' + (b"9" * 10_000) + b"}"),
    ],
)
async def test_read_json_limited_rejects_ambiguous_or_unrepresentable_json(
    raw: bytes,
):
    with pytest.raises(ValueError) as caught:
        await read_json_limited(StreamingResponse([raw]), max_bytes=len(raw) + 1)

    assert str(caught.value) == "invalid JSON response"
    assert caught.value.__cause__ is None


@pytest.mark.asyncio
async def test_read_json_limited_accepts_exact_container_depth_64() -> None:
    raw = (b'{"value":' * 64) + b"0" + (b"}" * 64)

    result = await read_json_limited(
        StreamingResponse([raw]),
        max_bytes=len(raw),
    )

    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_read_json_limited_rejects_container_depth_65_deterministically() -> None:
    raw = (b'{"value":' * 65) + b"0" + (b"}" * 65)

    with pytest.raises(ValueError) as caught:
        await read_json_limited(
            StreamingResponse([raw]),
            max_bytes=len(raw),
        )

    assert str(caught.value) == "invalid JSON response"
    assert caught.value.__cause__ is None


@pytest.mark.asyncio
async def test_read_json_limited_accepts_exact_128_digit_integer_token() -> None:
    raw = b'{"value":' + (b"9" * 128) + b"}"

    result = await read_json_limited(
        StreamingResponse([raw]),
        max_bytes=len(raw),
    )

    assert result["value"] == int("9" * 128)


@pytest.mark.asyncio
async def test_read_json_limited_rejects_129_digit_integer_token_deterministically() -> None:
    raw = b'{"value":' + (b"9" * 129) + b"}"

    with pytest.raises(ValueError) as caught:
        await read_json_limited(
            StreamingResponse([raw]),
            max_bytes=len(raw),
        )

    assert str(caught.value) == "invalid JSON response"
    assert caught.value.__cause__ is None
