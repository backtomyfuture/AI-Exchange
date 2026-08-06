"""Shared parsing for Exchange's serialized ``Mailbox`` repr values.

Several surfaces (daily digest, Lark commands, HTML renderer) receive the
sender as Exchange's serialized ``Mailbox(...)`` string.  This module is the
single parser for that representation; callers choose their own display
format and escaping.
"""

from __future__ import annotations

import re
from typing import NamedTuple

# ``repr()`` switches quote style when the value contains a single quote and
# escapes quotes when it contains both, so each field pattern honours the
# opening quote via a backreference and consumes backslash-escaped chars.
_NAME_PATTERN = re.compile(r"name=(['\"])((?:\\.|(?!\1).)*)\1")
_ADDRESS_PATTERN = re.compile(r"email_address=(['\"])((?:\\.|(?!\1).)*)\1")
_ESCAPED_CHAR_PATTERN = re.compile(r"\\(.)")


class SerializedMailbox(NamedTuple):
    name: str
    address: str


def parse_serialized_mailbox(value: object) -> SerializedMailbox | None:
    """Extract the display name and address from a serialized Mailbox value.

    Returns ``None`` when the value does not look like a serialized
    ``Mailbox(...)`` repr, so callers can fall back to the raw text.  Field
    order does not matter, and either field may be empty.
    """

    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    name_match = _NAME_PATTERN.search(raw)
    address_match = _ADDRESS_PATTERN.search(raw)
    if name_match is None and address_match is None:
        return None
    name = _unescape(name_match.group(2)).strip() if name_match else ""
    address = _unescape(address_match.group(2)).strip() if address_match else ""
    return SerializedMailbox(name=name, address=address)


def _unescape(content: str) -> str:
    """Undo repr()-style backslash escapes for display text."""

    return _ESCAPED_CHAR_PATTERN.sub(r"\1", content)
