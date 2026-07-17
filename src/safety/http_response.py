import json
import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

import httpx

from src.safety.input_limits import InputLimitExceeded


_ASCII_DECIMAL = re.compile(r"[0-9]+\Z")
_MAX_JSON_CONTAINER_DEPTH = 64
_MAX_JSON_INTEGER_DIGITS = 128
_DEFAULT_MAX_JSON_STRUCTURE_TOKENS = 100_000
_JSON_STRUCTURE_BYTES = frozenset(b"{}[],:" )


class _InvalidJsonResponse(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _InvalidJsonResponse
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise _InvalidJsonResponse


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise _InvalidJsonResponse
    return parsed


def _bounded_int(value: str) -> int:
    digits = value[1:] if value.startswith("-") else value
    if len(digits) > _MAX_JSON_INTEGER_DIGITS:
        raise _InvalidJsonResponse
    return int(value)


def _validate_unicode_scalars(value: object, *, container_depth: int = 0) -> None:
    if isinstance(value, str):
        value.encode("utf-8")
        return
    if isinstance(value, Mapping):
        next_depth = container_depth + 1
        if next_depth > _MAX_JSON_CONTAINER_DEPTH:
            raise _InvalidJsonResponse
        for key, item in value.items():
            key.encode("utf-8")
            _validate_unicode_scalars(item, container_depth=next_depth)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        next_depth = container_depth + 1
        if next_depth > _MAX_JSON_CONTAINER_DEPTH:
            raise _InvalidJsonResponse
        for item in value:
            _validate_unicode_scalars(item, container_depth=next_depth)


def _content_length(response: httpx.Response, *, max_bytes: int) -> None:
    values = response.headers.get_list("content-length", split_commas=False)
    if not values:
        return
    if len(values) != 1 or _ASCII_DECIMAL.fullmatch(values[0]) is None:
        raise ValueError("invalid JSON response")
    significant = values[0].lstrip("0") or "0"
    maximum = str(max_bytes)
    if len(significant) > len(maximum) or (
        len(significant) == len(maximum) and significant > maximum
    ):
        raise InputLimitExceeded("exchange_response_bytes")


async def read_json_limited(
    response: httpx.Response,
    *,
    max_bytes: int,
    max_structure_tokens: int = _DEFAULT_MAX_JSON_STRUCTURE_TOKENS,
) -> dict[str, Any]:
    if type(max_structure_tokens) is not int or max_structure_tokens < 1:
        raise ValueError("invalid JSON response")
    _content_length(response, max_bytes=max_bytes)
    body = bytearray()
    structure_tokens = 0
    in_string = False
    escaped = False

    async for chunk in response.aiter_bytes():
        if len(body) + len(chunk) > max_bytes:
            raise InputLimitExceeded("exchange_response_bytes")
        for byte in chunk:
            if in_string:
                if escaped:
                    escaped = False
                elif byte == 0x5C:
                    escaped = True
                elif byte == 0x22:
                    in_string = False
            elif byte == 0x22:
                in_string = True
            elif byte in _JSON_STRUCTURE_BYTES:
                structure_tokens += 1
                if structure_tokens > max_structure_tokens:
                    raise ValueError("invalid JSON response")
        body.extend(chunk)

    invalid = False
    value: object = None
    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
            parse_int=_bounded_int,
        )
        _validate_unicode_scalars(value)
    except (
        UnicodeDecodeError,
        UnicodeEncodeError,
        json.JSONDecodeError,
        _InvalidJsonResponse,
        OverflowError,
        RecursionError,
        ValueError,
    ):
        invalid = True
    if invalid:
        raise ValueError("invalid JSON response") from None

    if not isinstance(value, dict):
        raise ValueError("invalid JSON response")

    return value
