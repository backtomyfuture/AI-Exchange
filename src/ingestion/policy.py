"""Pure, fail-closed processing-policy manifest and resolver."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, TypeAlias

from src.ingestion.folder_identity import require_canonical_folder_identity
from src.ingestion.models import ChangeKind, IngressSource, ProcessingPolicy


_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_MAX_FOLDER_IDENTITY_LENGTH: Final = 512
_MAX_EVENT_TYPE_LENGTH: Final = 128
_FOLDER_SCOPE_CONFIG_SCHEMA_VERSION: Final = 1
_RESOLVABLE_POLICIES: Final = frozenset(
    {
        ProcessingPolicy.FULL,
        ProcessingPolicy.ARCHIVE,
        ProcessingPolicy.METADATA_ONLY,
        ProcessingPolicy.IGNORED,
    }
)

PolicyMatrixKey: TypeAlias = tuple[IngressSource, str, ChangeKind]

_WEBHOOK_NEW_MAIL_KEY: Final[PolicyMatrixKey] = (
    IngressSource.WEBHOOK,
    "NewMailEvent",
    ChangeKind.CREATE,
)
_WEBHOOK_CREATED_KEY: Final[PolicyMatrixKey] = (
    IngressSource.WEBHOOK,
    "CreatedEvent",
    ChangeKind.CREATE,
)
_WEBHOOK_MODIFIED_KEY: Final[PolicyMatrixKey] = (
    IngressSource.WEBHOOK,
    "ModifiedEvent",
    ChangeKind.UPDATE,
)
_WEBHOOK_DELETED_KEY: Final[PolicyMatrixKey] = (
    IngressSource.WEBHOOK,
    "DeletedEvent",
    ChangeKind.DELETE,
)
_SYNC_CREATE_KEY: Final[PolicyMatrixKey] = (
    IngressSource.SYNC,
    "create",
    ChangeKind.CREATE,
)
_SYNC_UPDATE_KEY: Final[PolicyMatrixKey] = (
    IngressSource.SYNC,
    "update",
    ChangeKind.UPDATE,
)
_SYNC_DELETE_KEY: Final[PolicyMatrixKey] = (
    IngressSource.SYNC,
    "delete",
    ChangeKind.DELETE,
)
_REQUIRED_POLICY_MATRIX_KEYS: Final = frozenset(
    {
        _WEBHOOK_NEW_MAIL_KEY,
        _WEBHOOK_CREATED_KEY,
        _WEBHOOK_MODIFIED_KEY,
        _WEBHOOK_DELETED_KEY,
        _SYNC_CREATE_KEY,
        _SYNC_UPDATE_KEY,
        _SYNC_DELETE_KEY,
    }
)
_METADATA_POLICY_KEYS: Final = frozenset(
    {
        _WEBHOOK_MODIFIED_KEY,
        _WEBHOOK_DELETED_KEY,
        _SYNC_UPDATE_KEY,
        _SYNC_DELETE_KEY,
    }
)
_CREATE_POLICIES: Final = frozenset(
    {
        ProcessingPolicy.FULL,
        ProcessingPolicy.ARCHIVE,
        ProcessingPolicy.IGNORED,
    }
)


class PolicySnapshotUnavailableError(RuntimeError):
    """The account policy snapshot cannot safely authorize processing."""

    def __init__(self) -> None:
        super().__init__("policy snapshot unavailable")


def _require_exact_text(name: str, value: object, *, max_length: int) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > max_length
        or any(
            ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F
            for character in value
        )
    ):
        raise ValueError(f"{name} must be exact non-empty text")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must contain valid UTF-8 text") from None
    return value


def require_canonical_folder_key(value: object) -> str:
    """Validate that a cursor identity is already normalization-canonical."""

    try:
        return require_canonical_folder_identity(value)
    except ValueError:
        raise ValueError(
            "canonical_key must already be normalization-canonical"
        ) from None


def _require_source(value: object) -> IngressSource:
    if type(value) is IngressSource:
        return value
    if type(value) is not str:
        raise ValueError("source must be a valid IngressSource")
    try:
        return IngressSource(value)
    except ValueError:
        raise ValueError("source must be a valid IngressSource") from None


def _require_kind(value: object) -> ChangeKind:
    if type(value) is ChangeKind:
        return value
    if type(value) is not str:
        raise ValueError("change_kind must be a valid ChangeKind")
    try:
        return ChangeKind(value)
    except ValueError:
        raise ValueError("change_kind must be a valid ChangeKind") from None


def _require_policy(value: object) -> ProcessingPolicy:
    if type(value) is ProcessingPolicy:
        policy = value
    elif type(value) is str:
        try:
            policy = ProcessingPolicy(value)
        except ValueError:
            raise ValueError("event policy must be a valid ProcessingPolicy") from None
    else:
        raise ValueError("event policy must be a valid ProcessingPolicy")
    if policy not in _RESOLVABLE_POLICIES:
        raise ValueError("event policy is not resolvable for an ingress event")
    return policy


def _freeze_webhook_ids(value: object) -> frozenset[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise ValueError("webhook_ids must be a non-empty iterable")
    identities = tuple(
        _require_exact_text(
            "webhook_id",
            identity,
            max_length=_MAX_FOLDER_IDENTITY_LENGTH,
        )
        for identity in value
    )
    if not identities:
        raise ValueError("webhook_ids must be a non-empty iterable")
    if len(frozenset(identities)) != len(identities):
        raise ValueError("webhook_ids must be unique")
    return frozenset(identities)


def _freeze_policy_matrix(value: object) -> Mapping[PolicyMatrixKey, ProcessingPolicy]:
    if not isinstance(value, Mapping):
        raise ValueError("event_policy_matrix must be a mapping")
    frozen: dict[PolicyMatrixKey, ProcessingPolicy] = {}
    for raw_key, raw_policy in value.items():
        if not isinstance(raw_key, tuple) or len(raw_key) != 3:
            raise ValueError("event policy matrix keys must have three fields")
        source, raw_event_type, change_kind = raw_key
        exact_raw_event_type = _require_exact_text(
            "raw_event_type",
            raw_event_type,
            max_length=_MAX_EVENT_TYPE_LENGTH,
        )
        if exact_raw_event_type == "read_flag_change":
            raise ValueError("raw read_flag_change is contract-invalid")
        key = (
            _require_source(source),
            exact_raw_event_type,
            _require_kind(change_kind),
        )
        if key in frozen:
            raise ValueError("event policy matrix keys must be unique")
        frozen[key] = _require_policy(raw_policy)
    if frozenset(frozen) != _REQUIRED_POLICY_MATRIX_KEYS:
        raise ValueError(
            "event policy matrix must contain the exact seven supported keys"
        )
    if any(
        frozen[key] is not ProcessingPolicy.METADATA_ONLY
        for key in _METADATA_POLICY_KEYS
    ):
        raise ValueError("update and delete policies must be METADATA_ONLY")
    return MappingProxyType(frozen)


def _validate_scope_policy_matrix(
    canonical_key: str,
    event_policy_matrix: Mapping[PolicyMatrixKey, ProcessingPolicy],
) -> None:
    new_mail_policy = event_policy_matrix[_WEBHOOK_NEW_MAIL_KEY]
    sync_create_policy = event_policy_matrix[_SYNC_CREATE_KEY]
    created_policy = event_policy_matrix[_WEBHOOK_CREATED_KEY]
    if new_mail_policy is not sync_create_policy:
        raise ValueError("NewMailEvent and Sync create policies must be equivalent")
    if new_mail_policy not in _CREATE_POLICIES:
        raise ValueError("create policy must be FULL, ARCHIVE, or IGNORED")
    if canonical_key == "SENT":
        if (
            new_mail_policy is not ProcessingPolicy.ARCHIVE
            or created_policy is not ProcessingPolicy.ARCHIVE
        ):
            raise ValueError("SENT create policies must be ARCHIVE")
    elif canonical_key == "DRAFTS":
        if (
            new_mail_policy is not ProcessingPolicy.IGNORED
            or created_policy is not ProcessingPolicy.IGNORED
        ):
            raise ValueError("DRAFTS create policies must be IGNORED")
    elif canonical_key == "ARCHIVE":
        if new_mail_policy is not ProcessingPolicy.ARCHIVE:
            raise ValueError("ARCHIVE create policies must be ARCHIVE")
        if created_policy is not ProcessingPolicy.IGNORED:
            raise ValueError("CreatedEvent must be IGNORED outside SENT")
    elif created_policy is not ProcessingPolicy.IGNORED:
        raise ValueError("CreatedEvent must be IGNORED outside SENT")


def _folder_scope_config_hash(
    canonical_key: str,
    webhook_ids: Iterable[str],
    sync_folder: str,
    event_policy_matrix: Mapping[PolicyMatrixKey, ProcessingPolicy],
) -> str:
    canonical = {
        "schema_version": _FOLDER_SCOPE_CONFIG_SCHEMA_VERSION,
        "canonical_key": canonical_key,
        "webhook_ids": sorted(webhook_ids),
        "sync_folder": sync_folder,
        "event_policy_matrix": [
            {
                "source": source.value,
                "raw_event_type": raw_event_type,
                "change_kind": change_kind.value,
                "processing_policy": policy.value,
            }
            for (source, raw_event_type, change_kind), policy in sorted(
                event_policy_matrix.items(),
                key=lambda item: (
                    item[0][0].value,
                    item[0][1],
                    item[0][2].value,
                ),
            )
        ],
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class FolderScope:
    """One immutable configured folder scope shared by all ingress transports."""

    canonical_key: str
    webhook_ids: Iterable[str]
    sync_folder: str
    event_policy_matrix: Mapping[PolicyMatrixKey, ProcessingPolicy]
    config_hash: str

    @classmethod
    def configured(
        cls,
        *,
        canonical_key: str,
        webhook_ids: Iterable[str],
        sync_folder: str,
        event_policy_matrix: Mapping[PolicyMatrixKey, ProcessingPolicy],
    ) -> FolderScope:
        if cls is not FolderScope:
            raise ValueError("configured must be called on exact FolderScope")
        frozen_canonical_key = require_canonical_folder_key(canonical_key)
        frozen_webhook_ids = _freeze_webhook_ids(webhook_ids)
        frozen_sync_folder = _require_exact_text(
            "sync_folder",
            sync_folder,
            max_length=_MAX_FOLDER_IDENTITY_LENGTH,
        )
        frozen_matrix = _freeze_policy_matrix(event_policy_matrix)
        _validate_scope_policy_matrix(frozen_canonical_key, frozen_matrix)
        return cls(
            canonical_key=frozen_canonical_key,
            webhook_ids=frozen_webhook_ids,
            sync_folder=frozen_sync_folder,
            event_policy_matrix=frozen_matrix,
            config_hash=_folder_scope_config_hash(
                frozen_canonical_key,
                frozen_webhook_ids,
                frozen_sync_folder,
                frozen_matrix,
            ),
        )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "canonical_key",
            require_canonical_folder_key(self.canonical_key),
        )
        object.__setattr__(self, "webhook_ids", _freeze_webhook_ids(self.webhook_ids))
        object.__setattr__(
            self,
            "sync_folder",
            _require_exact_text(
                "sync_folder",
                self.sync_folder,
                max_length=_MAX_FOLDER_IDENTITY_LENGTH,
            ),
        )
        object.__setattr__(
            self,
            "event_policy_matrix",
            _freeze_policy_matrix(self.event_policy_matrix),
        )
        _validate_scope_policy_matrix(
            self.canonical_key,
            self.event_policy_matrix,
        )
        if (
            type(self.config_hash) is not str
            or _SHA256_PATTERN.fullmatch(self.config_hash) is None
        ):
            raise ValueError("config_hash must be a lowercase SHA-256 digest")
        expected_hash = _folder_scope_config_hash(
            self.canonical_key,
            self.webhook_ids,
            self.sync_folder,
            self.event_policy_matrix,
        )
        if self.config_hash != expected_hash:
            raise ValueError("config_hash does not match FolderScope semantics")


@dataclass(frozen=True, slots=True)
class PolicySnapshot:
    """Immutable account snapshot; ambiguity is represented as unready state."""

    scopes: Iterable[FolderScope]
    refreshed: bool = True
    refresh_failed: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.refreshed, bool) or not isinstance(
            self.refresh_failed, bool
        ):
            raise ValueError("snapshot state must be boolean")
        if isinstance(self.scopes, (str, bytes)) or not isinstance(
            self.scopes, Iterable
        ):
            raise ValueError("scopes must be an iterable of FolderScope")
        scopes = tuple(self.scopes)
        if any(type(scope) is not FolderScope for scope in scopes):
            raise ValueError("scopes must contain only exact FolderScope values")
        object.__setattr__(self, "scopes", scopes)

    @classmethod
    def failed(cls) -> PolicySnapshot:
        return cls(scopes=(), refreshed=False, refresh_failed=True)

    @property
    def ready(self) -> bool:
        if not self.refreshed or self.refresh_failed:
            return False
        canonical_keys: set[str] = set()
        sync_folders: set[str] = set()
        webhook_ids: set[str] = set()
        for scope in self.scopes:
            if scope.canonical_key in canonical_keys:
                return False
            if scope.sync_folder in sync_folders:
                return False
            if webhook_ids.intersection(scope.webhook_ids):
                return False
            canonical_keys.add(scope.canonical_key)
            sync_folders.add(scope.sync_folder)
            webhook_ids.update(scope.webhook_ids)
        return True


class ProcessingPolicyResolver:
    """Resolve exact transport identities without an implicit FULL fallback."""

    @staticmethod
    def _require_snapshot(snapshot: object) -> PolicySnapshot:
        if type(snapshot) is not PolicySnapshot or not snapshot.ready:
            raise PolicySnapshotUnavailableError()
        return snapshot

    def configured_scopes(self, snapshot: object) -> tuple[FolderScope, ...]:
        available = self._require_snapshot(snapshot)
        return tuple(sorted(available.scopes, key=lambda scope: scope.canonical_key))

    def resolve(
        self,
        source: IngressSource | str,
        raw_event_type: str,
        change_kind: ChangeKind | str,
        exact_folder_identity: str,
        snapshot: PolicySnapshot | None,
    ) -> ProcessingPolicy:
        available = self._require_snapshot(snapshot)
        resolved_source = _require_source(source)
        resolved_raw_type = _require_exact_text(
            "raw_event_type",
            raw_event_type,
            max_length=_MAX_EVENT_TYPE_LENGTH,
        )
        resolved_kind = _require_kind(change_kind)
        folder_identity = _require_exact_text(
            "exact_folder_identity",
            exact_folder_identity,
            max_length=_MAX_FOLDER_IDENTITY_LENGTH,
        )
        if resolved_raw_type == "read_flag_change":
            raise ValueError("raw read_flag_change is contract-invalid")

        scope = next(
            (
                candidate
                for candidate in available.scopes
                if (
                    resolved_source is IngressSource.WEBHOOK
                    and folder_identity in candidate.webhook_ids
                )
                or (
                    resolved_source is IngressSource.SYNC
                    and folder_identity == candidate.sync_folder
                )
            ),
            None,
        )
        if scope is None:
            return ProcessingPolicy.IGNORED
        return scope.event_policy_matrix.get(
            (resolved_source, resolved_raw_type, resolved_kind),
            ProcessingPolicy.IGNORED,
        )
