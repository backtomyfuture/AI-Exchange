import json
from typing import Any

import httpx

from src.safety.input_limits import InputLimitExceeded


async def read_json_limited(
    response: httpx.Response,
    *,
    max_bytes: int,
) -> dict[str, Any]:
    chunks: list[bytes] = []
    total = 0

    async for chunk in response.aiter_bytes():
        total += len(chunk)
        if total > max_bytes:
            raise InputLimitExceeded("exchange_response_bytes")
        chunks.append(chunk)

    try:
        value = json.loads(b"".join(chunks).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("invalid JSON response") from None

    if not isinstance(value, dict):
        raise ValueError("invalid JSON response")

    return value
