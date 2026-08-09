"""Conservative intake decisions made before content, Qdrant, or model effects."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Final


POLICY_VERSION: Final = "intake-guard-v1"
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_AUTO_REPLY = re.compile(
    r"(?:automatic reply|auto(?:matic)?[- ]?reply|out of office|vacation reply|"
    r"自动回复|外出|不在办公室)",
    re.IGNORECASE,
)
_NDR = re.compile(
    r"(?:delivery status notification|delivery has failed|undeliverable|"
    r"mail delivery failed|returned mail|无法送达|投递失败)",
    re.IGNORECASE,
)


class IntakeDisposition(StrEnum):
    PASS = "pass"
    SUPPRESS = "suppress"
    QUARANTINE = "quarantine"


def _snapshot_digest(snapshot: object) -> str:
    """Hash a canonical JSON snapshot without exposing it outside this module."""

    try:
        canonical = _canonical_json(snapshot)
        encoded = json.dumps(
            canonical,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        encoded = json.dumps(
            {"invalid_snapshot_type": type(snapshot).__name__},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_json(value: object) -> object:
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise TypeError("non-string snapshot key")
        return {key: _canonical_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_json(item) for item in value]
    raise TypeError("non-JSON snapshot value")


@dataclass(frozen=True, slots=True)
class IntakeDecision:
    policy_version: str
    reason_code: str
    message_snapshot_digest: str
    disposition: IntakeDisposition
    mark_read: bool
    index_in_qdrant: bool

    def __post_init__(self) -> None:
        if self.policy_version != POLICY_VERSION:
            raise ValueError("unsupported intake policy version")
        if type(self.reason_code) is not str or not _REASON.fullmatch(self.reason_code):
            raise ValueError("invalid intake reason code")
        if type(self.message_snapshot_digest) is not str or not _DIGEST.fullmatch(
            self.message_snapshot_digest
        ):
            raise ValueError("invalid message snapshot digest")
        if type(self.disposition) is not IntakeDisposition:
            raise ValueError("disposition must be an exact IntakeDisposition")
        if type(self.mark_read) is not bool or type(self.index_in_qdrant) is not bool:
            raise ValueError("intake effect flags must be booleans")
        if self.disposition is not IntakeDisposition.PASS and self.index_in_qdrant:
            raise ValueError("non-pass decisions cannot authorize Qdrant indexing")

    def audit_metadata(self) -> dict[str, object]:
        return {
            "policy_version": self.policy_version,
            "reason_code": self.reason_code,
            "message_snapshot_digest": self.message_snapshot_digest,
            "disposition": self.disposition.value,
            "mark_read": self.mark_read,
            "index_in_qdrant": self.index_in_qdrant,
        }


class IntakeGuard:
    """Pure deterministic checks; ambiguous or malformed input fails closed."""

    def evaluate(self, email: object) -> IntakeDecision:
        digest = _snapshot_digest(email)
        if not isinstance(email, Mapping):
            return self._decision(
                "detail_parse_error", digest, IntakeDisposition.QUARANTINE
            )
        try:
            subject = self._text(email.get("subject", ""))
            sender = self._text(email.get("sender", ""))
            headers = self._headers(email.get("headers", {}))
        except ValueError:
            return self._decision(
                "detail_parse_error", digest, IntakeDisposition.QUARANTINE
            )

        sensitivity = headers.get("sensitivity", "").strip().lower()
        if sensitivity and sensitivity not in {"normal", "none"}:
            return self._decision(
                "sensitive_message", digest, IntakeDisposition.QUARANTINE
            )
        if headers.get("x-ms-exchange-organization-confidentiality", "").strip():
            return self._decision(
                "confidential_message", digest, IntakeDisposition.QUARANTINE
            )
        if _NDR.search(subject) or sender.lower() in {"mailer-daemon", "postmaster"}:
            return self._decision("ndr_detected", digest, IntakeDisposition.QUARANTINE)
        auto_submitted = headers.get("auto-submitted", "").strip().lower()
        precedence = headers.get("precedence", "").strip().lower()
        if (
            auto_submitted not in {"", "no"}
            and ("auto-replied" in auto_submitted or _AUTO_REPLY.search(subject))
        ) or _AUTO_REPLY.search(subject):
            return self._decision("automatic_reply", digest, IntakeDisposition.SUPPRESS)
        if (
            headers.get("x-loop", "").strip()
            or headers.get("x-autoreply", "").strip().lower() in {"yes", "true"}
            or precedence in {"auto_reply", "autoreply"}
        ):
            return self._decision("mail_loop", digest, IntakeDisposition.SUPPRESS)
        return self._decision("accepted", digest, IntakeDisposition.PASS)

    def parsing_failure(self, snapshot: object) -> IntakeDecision:
        return self._decision(
            "detail_parse_error",
            _snapshot_digest(snapshot),
            IntakeDisposition.QUARANTINE,
        )

    @staticmethod
    def _text(value: object) -> str:
        if type(value) is not str or "\x00" in value:
            raise ValueError("invalid text")
        return value

    @classmethod
    def _headers(cls, value: object) -> dict[str, str]:
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise ValueError("invalid headers")
        result: dict[str, str] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("invalid header name")
            result[key.lower()] = cls._text(item)
        return result

    @staticmethod
    def _decision(
        reason: str, digest: str, disposition: IntakeDisposition
    ) -> IntakeDecision:
        return IntakeDecision(
            policy_version=POLICY_VERSION,
            reason_code=reason,
            message_snapshot_digest=digest,
            disposition=disposition,
            mark_read=False,
            index_in_qdrant=disposition is IntakeDisposition.PASS,
        )


@dataclass(frozen=True, slots=True)
class ReprocessInstruction:
    source_decision_digest: str
    require_new_attempt: bool = True

    def __post_init__(self) -> None:
        if not _DIGEST.fullmatch(self.source_decision_digest):
            raise ValueError("invalid source decision digest")
        if self.require_new_attempt is not True:
            raise ValueError("release must require a new attempt")


def release_quarantine(decision: IntakeDecision) -> ReprocessInstruction:
    """Request a new attempt; the immutable original decision is never rewritten."""

    if (
        type(decision) is not IntakeDecision
        or decision.disposition is not IntakeDisposition.QUARANTINE
    ):
        raise ValueError("only a quarantine decision can be released")
    return ReprocessInstruction(decision.message_snapshot_digest)
