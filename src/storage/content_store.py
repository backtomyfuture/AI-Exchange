from __future__ import annotations

import base64
import binascii
import json
import re
import struct
from copy import deepcopy
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from typing import Any, Mapping, Protocol
from uuid import UUID


_KEY_VERSION_RE = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_PLAINTEXT_MAGIC = b"AIXC1"
_HEADER_LENGTH_BYTES = 8


class ContentStoreError(RuntimeError):
    """Base exception carrying only a safe content-store category."""


class ContentStoreConfigurationError(ContentStoreError):
    """Raised when the store cannot be configured safely."""


class ContentStoreReferenceError(ContentStoreError):
    """Raised before an unsafe reference can be used to derive a path."""


class ContentStoreNotFoundError(ContentStoreError):
    """Raised when an encrypted content object does not exist."""


class ContentStoreFormatError(ContentStoreError):
    """Raised for malformed or unsupported persisted content."""


class ContentStoreIntegrityError(ContentStoreError):
    """Raised when authentication or plaintext hash validation fails."""


class ContentStoreWriteError(ContentStoreError):
    """Raised when an atomic durable write cannot complete."""


def _invalid_envelope() -> ContentStoreFormatError:
    return ContentStoreFormatError("invalid_email_envelope")


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise _invalid_envelope() from None


def serialize_email_envelope(
    email_id: str,
    email: Mapping[str, Any],
) -> bytes:
    """Build a deterministic AIXC1 envelope without mutating caller data."""
    if not isinstance(email_id, str) or not email_id:
        raise _invalid_envelope()
    if not isinstance(email, MappingABC):
        raise _invalid_envelope()

    try:
        email_copy = deepcopy(dict(email))
    except Exception:
        raise _invalid_envelope() from None

    raw_attachments = email_copy.get("attachments", [])
    if not isinstance(raw_attachments, list):
        raise _invalid_envelope()

    metadata_attachments: list[dict[str, Any]] = []
    descriptors: list[dict[str, Any]] = []
    segments: list[bytes] = []
    for attachment in raw_attachments:
        if not isinstance(attachment, MappingABC):
            raise _invalid_envelope()
        try:
            metadata = deepcopy(dict(attachment))
        except Exception:
            raise _invalid_envelope() from None
        content_key_present = "content" in metadata
        encoded_content = metadata.pop("content", None)
        has_content = content_key_present and encoded_content is not None
        if has_content:
            if not isinstance(encoded_content, str):
                raise _invalid_envelope()
            try:
                decoded = base64.b64decode(
                    encoded_content.encode("ascii"),
                    validate=True,
                )
            except (UnicodeEncodeError, ValueError, binascii.Error):
                raise _invalid_envelope() from None
        else:
            decoded = b""
        metadata_attachments.append(metadata)
        descriptors.append(
            {"has_content": has_content, "byte_length": len(decoded)}
        )
        if has_content:
            segments.append(decoded)

    if "attachments" in email_copy:
        email_copy["attachments"] = metadata_attachments

    header = {
        "attachment_segments": descriptors,
        "email": email_copy,
        "email_id": email_id,
    }
    header_bytes = _canonical_json(header)
    return (
        _PLAINTEXT_MAGIC
        + struct.pack(">Q", len(header_bytes))
        + header_bytes
        + b"".join(segments)
    )


def _reject_json_constant(_value: str) -> None:
    raise ValueError


def deserialize_email_envelope(
    envelope: bytes,
    *,
    include_attachments: bool = False,
) -> dict[str, Any]:
    """Validate and parse an AIXC1 envelope, rejecting all trailing bytes."""
    if not isinstance(envelope, bytes):
        raise _invalid_envelope()
    prefix_length = len(_PLAINTEXT_MAGIC) + _HEADER_LENGTH_BYTES
    if len(envelope) < prefix_length or not envelope.startswith(_PLAINTEXT_MAGIC):
        raise _invalid_envelope()

    header_length = struct.unpack(">Q", envelope[5:prefix_length])[0]
    header_end = prefix_length + header_length
    if header_end > len(envelope):
        raise _invalid_envelope()
    header_bytes = envelope[prefix_length:header_end]
    try:
        header = json.loads(
            header_bytes.decode("utf-8"),
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise _invalid_envelope() from None
    if not isinstance(header, dict) or set(header) != {
        "attachment_segments",
        "email",
        "email_id",
    }:
        raise _invalid_envelope()
    if _canonical_json(header) != header_bytes:
        raise _invalid_envelope()

    email_id = header["email_id"]
    email = header["email"]
    descriptors = header["attachment_segments"]
    if not isinstance(email_id, str) or not email_id:
        raise _invalid_envelope()
    if not isinstance(email, dict) or not isinstance(descriptors, list):
        raise _invalid_envelope()

    attachments = email.get("attachments", [])
    if not isinstance(attachments, list) or len(attachments) != len(descriptors):
        raise _invalid_envelope()
    for attachment in attachments:
        if not isinstance(attachment, dict) or "content" in attachment:
            raise _invalid_envelope()

    offset = header_end
    restored_segments: list[bytes | None] = []
    for descriptor in descriptors:
        if not isinstance(descriptor, dict) or set(descriptor) != {
            "byte_length",
            "has_content",
        }:
            raise _invalid_envelope()
        has_content = descriptor["has_content"]
        byte_length = descriptor["byte_length"]
        if type(has_content) is not bool:
            raise _invalid_envelope()
        if type(byte_length) is not int or byte_length < 0:
            raise _invalid_envelope()
        if not has_content and byte_length != 0:
            raise _invalid_envelope()
        next_offset = offset + byte_length
        if next_offset > len(envelope):
            raise _invalid_envelope()
        restored_segments.append(
            envelope[offset:next_offset] if has_content else None
        )
        offset = next_offset
    if offset != len(envelope):
        raise _invalid_envelope()

    result = deepcopy(email)
    if include_attachments and "attachments" in result:
        for attachment, segment in zip(result["attachments"], restored_segments):
            if segment is not None:
                attachment["content"] = base64.b64encode(segment).decode("ascii")
    return result


def validate_key_version(value: object) -> str:
    if not isinstance(value, str) or _KEY_VERSION_RE.fullmatch(value) is None:
        raise ContentStoreReferenceError("invalid_content_ref")
    return value


@dataclass(frozen=True)
class ContentRef:
    account_id: int
    object_id: str
    key_version: str
    sha256: str

    def __post_init__(self) -> None:
        if type(self.account_id) is not int or self.account_id <= 0:
            raise ContentStoreReferenceError("invalid_content_ref")
        if not isinstance(self.object_id, str):
            raise ContentStoreReferenceError("invalid_content_ref")
        try:
            parsed = UUID(self.object_id)
        except (ValueError, AttributeError, TypeError) as exc:
            raise ContentStoreReferenceError("invalid_content_ref") from exc
        if str(parsed) != self.object_id:
            raise ContentStoreReferenceError("invalid_content_ref")
        validate_key_version(self.key_version)
        if not isinstance(self.sha256, str) or _SHA256_RE.fullmatch(self.sha256) is None:
            raise ContentStoreReferenceError("invalid_content_ref")


class ContentStore(Protocol):
    async def put_email(
        self,
        account_id: int,
        email_id: str,
        email: Mapping[str, Any],
    ) -> ContentRef:
        raise NotImplementedError

    async def load_email(
        self,
        ref: ContentRef,
        *,
        include_attachments: bool = False,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def delete(self, ref: ContentRef) -> None:
        raise NotImplementedError
