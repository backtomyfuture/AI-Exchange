"""Single pure source of truth for standard folder canonical identities."""

from __future__ import annotations

from typing import Final


_MAX_FOLDER_IDENTITY_LENGTH: Final = 512
_STANDARD_FOLDER_CANONICAL: Final = {
    "archive": "ARCHIVE",
    "deleted": "TRASH",
    "deleted items": "TRASH",
    "deleteditems": "TRASH",
    "draft": "DRAFTS",
    "drafts": "DRAFTS",
    "inbox": "INBOX",
    "junk": "JUNK",
    "junk email": "JUNK",
    "junkemail": "JUNK",
    "outbox": "OUTBOX",
    "sent": "SENT",
    "sent items": "SENT",
    "sentitems": "SENT",
    "spam": "JUNK",
    "trash": "TRASH",
    "已发送": "SENT",
    "已发送邮件": "SENT",
    "草稿": "DRAFTS",
}


def _require_exact_identity(value: object) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > _MAX_FOLDER_IDENTITY_LENGTH
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F
            for character in value
        )
    ):
        raise ValueError("folder identity must be exact bounded text")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("folder identity must contain valid UTF-8 text") from None
    return value


def canonicalize_folder_identity(value: object) -> str:
    """Map one already-trimmed standard alias to its durable identity."""

    identity = _require_exact_identity(value)
    lookup = identity.lower() if identity.isascii() else identity
    return _STANDARD_FOLDER_CANONICAL.get(lookup, identity)


def require_canonical_folder_identity(value: object) -> str:
    """Reject identities that normalization would rewrite."""

    identity = _require_exact_identity(value)
    if canonicalize_folder_identity(identity) != identity:
        raise ValueError("folder identity must already be normalization-canonical")
    return identity


__all__ = ["canonicalize_folder_identity", "require_canonical_folder_identity"]
