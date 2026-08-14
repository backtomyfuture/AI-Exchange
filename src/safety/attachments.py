"""Attachment admission at boundaries that can expose bytes externally.

Exchange detail is untrusted input.  ``AttachmentPolicy`` is deliberately a
small, pure module so both Feishu Drive uploads and visual-model input make the
same decision without relying on the sender-provided MIME type alone.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from dataclasses import dataclass


DEFAULT_MAX_ATTACHMENT_BYTES = 26_214_400
_MAX_FILENAME_BYTES = 255

_BLOCKED_SUFFIXES = frozenset(
    {
        ".ade",
        ".adp",
        ".apk",
        ".app",
        ".bat",
        ".cmd",
        ".com",
        ".cpl",
        ".dll",
        ".dmg",
        ".exe",
        ".hta",
        ".jar",
        ".js",
        ".jse",
        ".lnk",
        ".msi",
        ".msp",
        ".ps1",
        ".reg",
        ".scr",
        ".sh",
        ".vbe",
        ".vbs",
        ".wsf",
    }
)
_TEXT_SUFFIXES = frozenset({".csv", ".eml", ".ics", ".log", ".md", ".txt"})
_OFFICE_SUFFIXES = frozenset({".docx", ".pptx", ".xlsx"})
_IMAGE_SUFFIXES = frozenset({".gif", ".jpeg", ".jpg", ".png", ".webp"})


@dataclass(frozen=True, slots=True)
class AttachmentDecision:
    """One bounded decision suitable for a delivery or model boundary."""

    allowed: bool
    name: str
    content: bytes | None
    is_image: bool
    reason: str | None


class AttachmentPolicy:
    """Admit only supported file types whose bytes match their file suffix.

    The policy does not unpack archives or inspect document contents.  It is a
    boundary policy, not an antivirus engine: unsupported or ambiguous input is
    withheld from Feishu and model providers while the original remains in
    Exchange for manual review.
    """

    __slots__ = ("_max_bytes",)

    def __init__(self, *, max_bytes: int = DEFAULT_MAX_ATTACHMENT_BYTES) -> None:
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or max_bytes < 1
        ):
            raise ValueError("attachment_max_bytes_invalid")
        self._max_bytes = max_bytes

    def assess(self, attachment: object) -> AttachmentDecision:
        """Return a deterministic allow/deny decision without raising on mail data."""

        if not isinstance(attachment, Mapping):
            return self._rejected("", "attachment_format_invalid")

        raw_name = attachment.get("name")
        if not isinstance(raw_name, str) or not self._valid_name(raw_name):
            return self._rejected("", "attachment_name_invalid")
        name = raw_name

        encoded = attachment.get("content")
        if not isinstance(encoded, str) or not encoded:
            return self._rejected(name, "attachment_content_missing")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            return self._rejected(name, "attachment_format_invalid")
        if not content:
            return self._rejected(name, "attachment_content_missing")
        if len(content) > self._max_bytes:
            return self._rejected(name, "attachment_size_exceeded")

        suffix = self._suffix(name)
        if suffix in _BLOCKED_SUFFIXES:
            return self._rejected(name, "attachment_type_blocked")
        if suffix == ".pdf":
            return self._decision_for_signature(
                name,
                content,
                content.startswith(b"%PDF-"),
            )
        if suffix in _OFFICE_SUFFIXES:
            return self._decision_for_signature(
                name,
                content,
                self._is_zip_container(content),
            )
        if suffix in _TEXT_SUFFIXES:
            if b"\x00" in content:
                return self._rejected(name, "attachment_signature_mismatch")
            return AttachmentDecision(True, name, content, False, None)
        if suffix in _IMAGE_SUFFIXES:
            return self._decision_for_signature(
                name,
                content,
                self._image_matches_suffix(suffix, content),
                is_image=True,
            )
        return self._rejected(name, "attachment_type_unrecognized")

    @staticmethod
    def _rejected(name: str, reason: str) -> AttachmentDecision:
        return AttachmentDecision(False, name, None, False, reason)

    @staticmethod
    def _valid_name(value: str) -> bool:
        if (
            not value
            or value != value.strip()
            or len(value.encode("utf-8")) > _MAX_FILENAME_BYTES
            or any(character in value for character in ("\x00", "\r", "\n", "/", "\\"))
        ):
            return False
        return value not in {".", ".."} and ".." not in value

    @staticmethod
    def _suffix(name: str) -> str:
        dot = name.rfind(".")
        if dot <= 0 or dot == len(name) - 1:
            return ""
        return name[dot:].casefold()

    @staticmethod
    def _is_zip_container(content: bytes) -> bool:
        return content.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"))

    @staticmethod
    def _image_matches_suffix(suffix: str, content: bytes) -> bool:
        if suffix == ".png":
            return content.startswith(b"\x89PNG\r\n\x1a\n")
        if suffix in {".jpg", ".jpeg"}:
            return content.startswith(b"\xff\xd8\xff")
        if suffix == ".gif":
            return content.startswith((b"GIF87a", b"GIF89a"))
        if suffix == ".webp":
            return len(content) >= 12 and content.startswith(b"RIFF") and content[8:12] == b"WEBP"
        return False

    @classmethod
    def _decision_for_signature(
        cls,
        name: str,
        content: bytes,
        matches: bool,
        *,
        is_image: bool = False,
    ) -> AttachmentDecision:
        if not matches:
            return cls._rejected(name, "attachment_signature_mismatch")
        return AttachmentDecision(True, name, content, is_image, None)


_DEFAULT_POLICY: AttachmentPolicy | None = None


def get_attachment_policy(*, max_bytes: int | None = None) -> AttachmentPolicy:
    """Return the shared AttachmentPolicy used by retrieval and delivery."""

    if max_bytes is None:
        from src.config import get_settings
        from src.safety.input_limits import input_limits_from_settings

        max_bytes = input_limits_from_settings(get_settings()).attachment_single_bytes
    global _DEFAULT_POLICY
    if _DEFAULT_POLICY is None or _DEFAULT_POLICY._max_bytes != max_bytes:
        _DEFAULT_POLICY = AttachmentPolicy(max_bytes=max_bytes)
    return _DEFAULT_POLICY


__all__ = [
    "AttachmentDecision",
    "AttachmentPolicy",
    "DEFAULT_MAX_ATTACHMENT_BYTES",
    "get_attachment_policy",
]
