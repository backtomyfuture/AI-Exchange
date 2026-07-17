"""Pure greenfield runtime authority and initialization value contracts.

Database commands are added behind narrow repositories in later slices.  This
module first fixes the immutable identities they exchange so configuration can
never mint or widen runtime authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Final
from uuid import UUID

from src.ingestion.models import POSTGRES_BIGINT_MAX
from src.ingestion.policy import FolderScope, PolicySnapshot
from src.ingestion.runtime_capability import (
    PHASE2_SCHEMA_REVISION,
    RuntimeCapabilityManifest,
    install_phase2_capability,
)


GREENFIELD_PIPELINE_NAME: Final = "durable_v1"
_POLICY_SCHEMA_VERSION: Final = 1
_POLICY_HASH_DOMAIN: Final = b"ai-exchange-folder-policy-manifest-v1\x00"
_INITIALIZATION_SCHEMA_VERSION: Final = 1
_INITIALIZATION_HASH_DOMAIN: Final = b"ai-exchange-greenfield-initialize-v1\x00"
_TRANSITION_SCHEMA_VERSION: Final = 1
_TRANSITION_HASH_DOMAIN: Final = b"ai-exchange-runtime-authority-transition-v1\x00"
_PAUSE_COMMAND_NAME: Final = "runtime.pause"
_RESUME_COMMAND_NAME: Final = "runtime.resume_ingress"
_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_TRANSACTION_ID_PATTERN: Final = re.compile(r"[1-9][0-9]{0,19}\Z", flags=re.ASCII)
_BUILD_ID_PATTERN: Final = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}\Z",
    flags=re.ASCII,
)
_INSTANCE_ID_PATTERN: Final = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z",
    flags=re.ASCII,
)
_MAX_LEASE_SECONDS: Final = 3600
_MAX_WEBHOOK_IDS_PER_SCOPE: Final = 64
_MAX_WEBHOOK_IDS_JSON_BYTES: Final = 32768

_AUTHORITY_COLUMNS: Final = (
    "account_id",
    "state",
    "generation",
    "fencing_token",
    "pipeline_name",
    "authority_epoch",
    "version",
    "schema_revision",
    "protocol_version",
    "build_id",
    "config_hash",
    "capability_hash",
    "policy_manifest_hash",
    "initialization_id",
    "updated_at",
)
_INITIALIZATION_COLUMNS: Final = (
    "initialization_id",
    "command_receipt_id",
    "account_id",
    "generation",
    "fencing_token",
    "pipeline_name",
    "authority_epoch",
    "authority_version",
    "capability_hash",
    "policy_manifest_hash",
    "transaction_id",
    "replayed",
    "created_at",
)
_TRANSITION_COLUMNS: Final = (
    "command_receipt_id",
    "command_name",
    "previous_state",
    "previous_authority_epoch",
    "previous_version",
    "transaction_id",
    "replayed",
    "receipt_created_at",
    *_AUTHORITY_COLUMNS,
)
_INSTANCE_COLUMNS: Final = (
    "account_id",
    "workload",
    "instance_id",
    "session_id",
    "generation",
    "fencing_token",
    "authority_epoch",
    "capability_hash",
    "schema_revision",
    "protocol_version",
    "build_id",
    "config_hash",
    "lifecycle",
    "lease_version",
    "accepted_count",
    "rejected_count",
    "heartbeat_at",
    "lease_until",
)

_INITIALIZE_SQL: Final = (
    "SELECT initialization_id, command_receipt_id, account_id, generation, "
    "fencing_token, pipeline_name, authority_epoch, authority_version, "
    "capability_hash, policy_manifest_hash, transaction_id, replayed, created_at "
    "FROM public.greenfield_initialize_runtime("
    "%s, %s, %s, %s, %s, %s, %s, %s, %s, "
    "%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
_GET_AUTHORITY_SQL: Final = (
    "SELECT account_id, state, generation, fencing_token, pipeline_name, "
    "authority_epoch, version, schema_revision, protocol_version, build_id, "
    "config_hash, capability_hash, policy_manifest_hash, initialization_id, "
    "updated_at FROM public.greenfield_get_runtime_authority(%s)"
)
_PAUSE_SQL: Final = (
    "SELECT command_receipt_id, command_name, previous_state, "
    "previous_authority_epoch, previous_version, transaction_id, replayed, "
    "receipt_created_at, account_id, state, generation, fencing_token, "
    "pipeline_name, authority_epoch, version, schema_revision, protocol_version, "
    "build_id, config_hash, capability_hash, policy_manifest_hash, "
    "initialization_id, updated_at FROM public.greenfield_pause_runtime("
    "%s, %s, %s, %s, %s, %s, %s, %s)"
)
_RESUME_SQL: Final = (
    "SELECT command_receipt_id, command_name, previous_state, "
    "previous_authority_epoch, previous_version, transaction_id, replayed, "
    "receipt_created_at, account_id, state, generation, fencing_token, "
    "pipeline_name, authority_epoch, version, schema_revision, protocol_version, "
    "build_id, config_hash, capability_hash, policy_manifest_hash, "
    "initialization_id, updated_at FROM public.greenfield_resume_ingress("
    "%s, %s, %s, %s, %s, %s, %s, %s)"
)
_REGISTER_SQL: Final = (
    "SELECT account_id, workload, instance_id, session_id, generation, "
    "fencing_token, authority_epoch, capability_hash, schema_revision, "
    "protocol_version, build_id, config_hash, lifecycle, lease_version, "
    "accepted_count, rejected_count, heartbeat_at, lease_until "
    "FROM public.greenfield_register_web_instance("
    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
_HEARTBEAT_SQL: Final = (
    "SELECT account_id, workload, instance_id, session_id, generation, "
    "fencing_token, authority_epoch, capability_hash, schema_revision, "
    "protocol_version, build_id, config_hash, lifecycle, lease_version, "
    "accepted_count, rejected_count, heartbeat_at, lease_until "
    "FROM public.greenfield_heartbeat_web_instance("
    "%s, %s, %s, %s, %s, %s, %s, %s)"
)
_DRAIN_SQL: Final = (
    "SELECT account_id, workload, instance_id, session_id, generation, "
    "fencing_token, authority_epoch, capability_hash, schema_revision, "
    "protocol_version, build_id, config_hash, lifecycle, lease_version, "
    "accepted_count, rejected_count, heartbeat_at, lease_until "
    "FROM public.greenfield_drain_web_instance(%s, %s, %s, %s, %s)"
)


class RuntimeAuthorityState(StrEnum):
    INGEST_ONLY = "ingest_only"
    PAUSED = "paused"
    ACTIVE = "active"


class RuntimeWorkload(StrEnum):
    WEB = "web"
    WORKER = "worker"
    SCHEDULER = "scheduler"
    REAPER = "reaper"


class RuntimeInstanceLifecycle(StrEnum):
    STANDBY = "standby"
    ACTIVE = "active"
    DRAINING = "draining"


def _require_exact_enum(name: str, value: object, enum_type: type[StrEnum]) -> StrEnum:
    if type(value) is enum_type:
        return value
    if type(value) is not str:
        raise ValueError(f"{name} must be an exact {enum_type.__name__}")
    try:
        return enum_type(value)
    except ValueError:
        raise ValueError(f"{name} must be an exact {enum_type.__name__}") from None


def _require_bigint(name: str, value: object, *, minimum: int) -> int:
    if type(value) is not int or value < minimum or value > POSTGRES_BIGINT_MAX:
        raise ValueError(f"{name} must be a bounded PostgreSQL BIGINT")
    return value


def _require_stored_bigint(name: str, value: object, *, minimum: int) -> int:
    exact = _require_bigint(name, value, minimum=minimum)
    if exact >= POSTGRES_BIGINT_MAX:
        raise ValueError(f"{name} must remain below the reserved BIGINT ceiling")
    return exact


def _require_sha256(name: str, value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be an exact lowercase SHA-256 digest")
    return value


def _require_pattern_text(
    name: str,
    value: object,
    pattern: re.Pattern[str],
) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise ValueError(f"{name} must be bounded canonical ASCII text")
    return value


def _require_exact_text(name: str, value: object, *, max_bytes: int) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{name} must be exact bounded text")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must be valid UTF-8") from None
    if len(encoded) > max_bytes or any(
        ord(character) <= 0x1F or 0x7F <= ord(character) <= 0x9F for character in value
    ):
        raise ValueError(f"{name} must be exact bounded text")
    return value


def _canonical_json_string(value: str) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def _require_uuid4(name: str, value: object) -> str:
    if type(value) is not str:
        raise ValueError(f"{name} must be a canonical UUIDv4")
    try:
        parsed = UUID(value)
    except ValueError:
        raise ValueError(f"{name} must be a canonical UUIDv4") from None
    if str(parsed) != value or parsed.version != 4:
        raise ValueError(f"{name} must be a canonical UUIDv4")
    return value


def _require_utc(name: str, value: object) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    try:
        normalized = value.astimezone(UTC)
    except (OverflowError, ValueError):
        raise ValueError(f"{name} must be timezone-aware") from None
    if normalized.tzinfo is not UTC:
        raise ValueError(f"{name} must normalize to UTC")
    return normalized


def _require_transaction_id(value: object) -> str:
    if type(value) is not str or _TRANSACTION_ID_PATTERN.fullmatch(value) is None:
        raise ValueError("transaction_id must be a canonical xid8 decimal")
    return value


def _require_lease_seconds(value: object) -> int:
    if type(value) is not int or not 1 <= value <= _MAX_LEASE_SECONDS:
        raise ValueError("lease_seconds must be an exact bounded integer")
    return value


def _database_uuid4(name: str, value: object) -> str:
    if type(value) is UUID:
        if value.version != 4:
            raise ValueError(f"{name} must be a canonical UUIDv4")
        return str(value)
    return _require_uuid4(name, value)


def _exact_row_values(
    row: object,
    columns: tuple[str, ...],
    *,
    error: str,
) -> dict[str, object]:
    try:
        if type(row) is tuple:
            if len(row) != len(columns):
                raise ValueError
            values = row
        elif isinstance(row, Mapping):
            if set(row.keys()) != set(columns) or len(row) != len(columns):
                raise ValueError
            values = tuple(row[column] for column in columns)
        else:
            raise ValueError
        return dict(zip(columns, values, strict=True))
    except (KeyError, TypeError, ValueError):
        raise RuntimeError(error) from None


def _revalidate_runtime_contract(value: object) -> RuntimeContract:
    if type(value) is not RuntimeContract:
        raise ValueError("runtime_contract must be exact")
    try:
        return RuntimeContract(
            **{field: getattr(value, field) for field in value.__slots__}
        )
    except (AttributeError, TypeError, ValueError):
        raise ValueError("runtime_contract must be exact") from None


def _revalidate_authority(value: object) -> RuntimeAuthority:
    if type(value) is not RuntimeAuthority:
        raise ValueError("authority must be exact")
    try:
        return RuntimeAuthority(
            **{field: getattr(value, field) for field in value.__slots__}
        )
    except (AttributeError, TypeError, ValueError):
        raise ValueError("authority must be exact") from None


def _revalidate_instance_lease(value: object) -> RuntimeInstanceLease:
    if type(value) is not RuntimeInstanceLease:
        raise ValueError("lease must be exact")
    try:
        return RuntimeInstanceLease(
            **{field: getattr(value, field) for field in value.__slots__}
        )
    except (AttributeError, TypeError, ValueError):
        raise ValueError("lease must be exact") from None


@dataclass(frozen=True, slots=True)
class PolicyManifest:
    canonical_json: str
    hash: str
    scope_count: int

    def __post_init__(self) -> None:
        if type(self.canonical_json) is not str:
            raise ValueError("policy canonical_json must be exact text")
        try:
            parsed = json.loads(self.canonical_json)
        except (json.JSONDecodeError, TypeError):
            raise ValueError("policy canonical_json must be valid JSON") from None
        encoded = json.dumps(
            parsed,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if encoded != self.canonical_json:
            raise ValueError("policy canonical_json must be canonical")
        _require_sha256("policy hash", self.hash)
        _require_bigint("scope_count", self.scope_count, minimum=1)
        expected = hashlib.sha256(
            _POLICY_HASH_DOMAIN + self.canonical_json.encode("ascii")
        ).hexdigest()
        if self.hash != expected:
            raise ValueError("policy hash does not match canonical_json")


def _canonical_scope(scope: FolderScope) -> dict[str, object]:
    return {
        "canonical_key": scope.canonical_key,
        "event_policy_matrix": [
            {
                "change_kind": kind.value,
                "processing_policy": policy.value,
                "raw_event_type": raw_event_type,
                "source": source.value,
            }
            for (source, raw_event_type, kind), policy in sorted(
                scope.event_policy_matrix.items(),
                key=lambda item: (
                    item[0][0].value,
                    item[0][1],
                    item[0][2].value,
                ),
            )
        ],
        "scope_hash": scope.config_hash,
        "sync_folder": scope.sync_folder,
        "webhook_ids": sorted(scope.webhook_ids),
    }


def _database_webhook_ids_canonical_json(webhook_ids: frozenset[str]) -> str:
    """Mirror PostgreSQL jsonb array text used by the schema byte constraint."""

    return json.dumps(
        sorted(webhook_ids),
        ensure_ascii=False,
        separators=(", ", ": "),
        allow_nan=False,
    )


def _greenfield_policy_scopes(snapshot: object) -> tuple[FolderScope, ...]:
    if type(snapshot) is not PolicySnapshot:
        raise ValueError("policy snapshot must be exact, ready, and nonempty")
    try:
        raw_scopes = tuple(snapshot.scopes)
        refreshed = snapshot.refreshed
        refresh_failed = snapshot.refresh_failed
    except (AttributeError, TypeError):
        raise ValueError("policy snapshot must be exact, ready, and nonempty") from None

    exact_scopes: list[FolderScope] = []
    for scope in raw_scopes:
        if type(scope) is not FolderScope:
            raise ValueError("policy snapshot must contain exact folder scopes")
        try:
            exact_scope = FolderScope(
                canonical_key=scope.canonical_key,
                webhook_ids=scope.webhook_ids,
                sync_folder=scope.sync_folder,
                event_policy_matrix=scope.event_policy_matrix,
                config_hash=scope.config_hash,
            )
        except (AttributeError, TypeError, ValueError):
            raise ValueError(
                "policy snapshot contains an invalid folder scope"
            ) from None
        webhook_count = len(exact_scope.webhook_ids)
        webhook_json = _database_webhook_ids_canonical_json(exact_scope.webhook_ids)
        if (
            not 1 <= webhook_count <= _MAX_WEBHOOK_IDS_PER_SCOPE
            or len(webhook_json.encode("utf-8")) > _MAX_WEBHOOK_IDS_JSON_BYTES
        ):
            raise ValueError("policy scope webhook_ids exceed database bounds")
        exact_scopes.append(exact_scope)

    try:
        exact_snapshot = PolicySnapshot(
            scopes=tuple(exact_scopes),
            refreshed=refreshed,
            refresh_failed=refresh_failed,
        )
    except (TypeError, ValueError):
        raise ValueError("policy snapshot must be exact, ready, and nonempty") from None
    if not exact_snapshot.ready or not exact_snapshot.scopes:
        raise ValueError("policy snapshot must be exact, ready, and nonempty")
    return tuple(sorted(exact_snapshot.scopes, key=lambda scope: scope.canonical_key))


def canonical_policy_manifest(snapshot: PolicySnapshot) -> PolicyManifest:
    """Freeze one ready nonempty policy snapshot into its database identity."""

    scopes = _greenfield_policy_scopes(snapshot)
    canonical = {
        "schema_version": _POLICY_SCHEMA_VERSION,
        "scopes": [_canonical_scope(scope) for scope in scopes],
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    digest = hashlib.sha256(_POLICY_HASH_DOMAIN + encoded.encode("ascii")).hexdigest()
    return PolicyManifest(
        canonical_json=encoded,
        hash=digest,
        scope_count=len(scopes),
    )


@dataclass(frozen=True, slots=True)
class RuntimeContract:
    schema_revision: str
    schema_digest: str
    protocol_version: int
    build_id: str
    config_hash: str
    capability_manifest: RuntimeCapabilityManifest

    def __post_init__(self) -> None:
        if self.schema_revision != PHASE2_SCHEMA_REVISION:
            raise ValueError("runtime contract requires the exact greenfield schema")
        _require_sha256("schema_digest", self.schema_digest)
        _require_bigint("protocol_version", self.protocol_version, minimum=1)
        _require_pattern_text("build_id", self.build_id, _BUILD_ID_PATTERN)
        _require_sha256("config_hash", self.config_hash)
        if type(self.capability_manifest) is not RuntimeCapabilityManifest:
            raise ValueError("capability_manifest must be exact")
        capability = install_phase2_capability(self.capability_manifest)
        if (
            capability.schema_revision != self.schema_revision
            or capability.schema_digest != self.schema_digest
            or capability.protocol_version != self.protocol_version
            or capability.minimum_build_id != self.build_id
            or capability.config_hash != self.config_hash
        ):
            raise ValueError("runtime contract and capability manifest drift")


@dataclass(frozen=True, slots=True)
class RuntimeAuthority:
    account_id: int
    state: RuntimeAuthorityState
    generation: int
    fencing_token: int
    pipeline_name: str
    authority_epoch: int
    version: int
    schema_revision: str
    protocol_version: int
    build_id: str
    config_hash: str
    capability_hash: str
    policy_manifest_hash: str
    initialization_id: str
    updated_at: datetime

    def __post_init__(self) -> None:
        _require_bigint("account_id", self.account_id, minimum=1)
        object.__setattr__(
            self,
            "state",
            _require_exact_enum("state", self.state, RuntimeAuthorityState),
        )
        if self.generation != 1 or self.fencing_token != 1:
            raise ValueError("greenfield authority requires generation and fence 1")
        if self.pipeline_name != GREENFIELD_PIPELINE_NAME:
            raise ValueError("greenfield authority requires durable_v1")
        _require_stored_bigint("authority_epoch", self.authority_epoch, minimum=1)
        _require_stored_bigint("version", self.version, minimum=1)
        if self.schema_revision != PHASE2_SCHEMA_REVISION:
            raise ValueError("authority schema revision is invalid")
        _require_bigint("protocol_version", self.protocol_version, minimum=1)
        _require_pattern_text("build_id", self.build_id, _BUILD_ID_PATTERN)
        _require_sha256("config_hash", self.config_hash)
        _require_sha256("capability_hash", self.capability_hash)
        _require_sha256("policy_manifest_hash", self.policy_manifest_hash)
        object.__setattr__(
            self,
            "initialization_id",
            _require_uuid4("initialization_id", self.initialization_id),
        )
        object.__setattr__(
            self, "updated_at", _require_utc("updated_at", self.updated_at)
        )


def require_phase2_ingress_authority(authority: RuntimeAuthority) -> RuntimeAuthority:
    if type(authority) is not RuntimeAuthority:
        raise RuntimeError("runtime_authority_unavailable")
    try:
        RuntimeAuthority(
            **{field: getattr(authority, field) for field in authority.__slots__}
        )
    except (AttributeError, TypeError, ValueError):
        raise RuntimeError("runtime_authority_unavailable") from None
    if authority.state is not RuntimeAuthorityState.INGEST_ONLY:
        raise RuntimeError("runtime_authority_unavailable")
    return authority


@dataclass(frozen=True, slots=True)
class RuntimeInstanceLease:
    account_id: int
    workload: RuntimeWorkload
    instance_id: str
    session_id: str
    generation: int
    fencing_token: int
    authority_epoch: int
    capability_hash: str
    schema_revision: str
    protocol_version: int
    build_id: str
    config_hash: str
    lifecycle: RuntimeInstanceLifecycle
    lease_version: int
    accepted_count: int
    rejected_count: int
    heartbeat_at: datetime
    lease_until: datetime

    def __post_init__(self) -> None:
        _require_bigint("account_id", self.account_id, minimum=1)
        workload = _require_exact_enum("workload", self.workload, RuntimeWorkload)
        if workload is not RuntimeWorkload.WEB:
            raise ValueError("Phase 2 may register only a web workload")
        object.__setattr__(self, "workload", workload)
        _require_pattern_text("instance_id", self.instance_id, _INSTANCE_ID_PATTERN)
        object.__setattr__(
            self,
            "session_id",
            _require_uuid4("session_id", self.session_id),
        )
        if self.generation != 1 or self.fencing_token != 1:
            raise ValueError("greenfield instance requires generation and fence 1")
        _require_stored_bigint("authority_epoch", self.authority_epoch, minimum=1)
        _require_sha256("capability_hash", self.capability_hash)
        if self.schema_revision != PHASE2_SCHEMA_REVISION:
            raise ValueError("instance schema revision is invalid")
        _require_bigint("protocol_version", self.protocol_version, minimum=1)
        _require_pattern_text("build_id", self.build_id, _BUILD_ID_PATTERN)
        _require_sha256("config_hash", self.config_hash)
        lifecycle = _require_exact_enum(
            "lifecycle",
            self.lifecycle,
            RuntimeInstanceLifecycle,
        )
        if lifecycle not in {
            RuntimeInstanceLifecycle.ACTIVE,
            RuntimeInstanceLifecycle.DRAINING,
        }:
            raise ValueError("Phase 2 web lifecycle is invalid")
        object.__setattr__(self, "lifecycle", lifecycle)
        _require_stored_bigint("lease_version", self.lease_version, minimum=1)
        _require_stored_bigint("accepted_count", self.accepted_count, minimum=0)
        _require_stored_bigint("rejected_count", self.rejected_count, minimum=0)
        heartbeat = _require_utc("heartbeat_at", self.heartbeat_at)
        deadline = _require_utc("lease_until", self.lease_until)
        if deadline <= heartbeat:
            raise ValueError("lease_until must be after heartbeat_at")
        object.__setattr__(self, "heartbeat_at", heartbeat)
        object.__setattr__(self, "lease_until", deadline)


@dataclass(frozen=True, slots=True)
class InitializationReceipt:
    initialization_id: str
    command_receipt_id: str
    account_id: int
    generation: int
    fencing_token: int
    pipeline_name: str
    authority_epoch: int
    authority_version: int
    capability_hash: str
    policy_manifest_hash: str
    transaction_id: str
    replayed: bool
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "initialization_id",
            _require_uuid4("initialization_id", self.initialization_id),
        )
        object.__setattr__(
            self,
            "command_receipt_id",
            _require_uuid4("command_receipt_id", self.command_receipt_id),
        )
        _require_bigint("account_id", self.account_id, minimum=1)
        if (
            self.generation != 1
            or self.fencing_token != 1
            or self.pipeline_name != GREENFIELD_PIPELINE_NAME
            or self.authority_epoch != 1
        ):
            raise ValueError("initialization receipt is not the greenfield identity")
        _require_bigint("authority_version", self.authority_version, minimum=1)
        _require_sha256("capability_hash", self.capability_hash)
        _require_sha256("policy_manifest_hash", self.policy_manifest_hash)
        _require_transaction_id(self.transaction_id)
        if type(self.replayed) is not bool:
            raise ValueError("replayed must be an exact boolean")
        object.__setattr__(
            self, "created_at", _require_utc("created_at", self.created_at)
        )


@dataclass(frozen=True, slots=True)
class AuthorityTransitionReceipt:
    command_receipt_id: str
    command_name: str
    previous_state: RuntimeAuthorityState
    previous_authority_epoch: int
    previous_version: int
    transaction_id: str
    replayed: bool
    created_at: datetime
    authority: RuntimeAuthority

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "command_receipt_id",
            _require_uuid4("command_receipt_id", self.command_receipt_id),
        )
        previous_state = _require_exact_enum(
            "previous_state",
            self.previous_state,
            RuntimeAuthorityState,
        )
        object.__setattr__(self, "previous_state", previous_state)
        _require_stored_bigint(
            "previous_authority_epoch",
            self.previous_authority_epoch,
            minimum=1,
        )
        _require_stored_bigint("previous_version", self.previous_version, minimum=1)
        _require_transaction_id(self.transaction_id)
        if type(self.replayed) is not bool:
            raise ValueError("replayed must be an exact boolean")
        object.__setattr__(
            self,
            "created_at",
            _require_utc("created_at", self.created_at),
        )
        authority = _revalidate_authority(self.authority)
        transition = (
            self.command_name,
            previous_state,
            authority.state,
        )
        if transition not in {
            (
                _PAUSE_COMMAND_NAME,
                RuntimeAuthorityState.INGEST_ONLY,
                RuntimeAuthorityState.PAUSED,
            ),
            (
                _RESUME_COMMAND_NAME,
                RuntimeAuthorityState.PAUSED,
                RuntimeAuthorityState.INGEST_ONLY,
            ),
        }:
            raise ValueError("authority transition receipt is invalid")
        if (
            authority.authority_epoch != self.previous_authority_epoch + 1
            or authority.version != self.previous_version + 1
        ):
            raise ValueError("authority transition receipt CAS is invalid")


def canonical_initialization_payload(
    *,
    account_id: int,
    runtime_contract: RuntimeContract,
    policy_manifest: PolicyManifest,
    actor: str,
    reason: str,
) -> tuple[str, str]:
    """Return the complete command semantics and its domain-separated digest."""

    _require_bigint("account_id", account_id, minimum=1)
    if type(runtime_contract) is not RuntimeContract:
        raise ValueError("runtime_contract must be exact")
    RuntimeContract(
        **{
            field: getattr(runtime_contract, field)
            for field in runtime_contract.__slots__
        }
    )
    if type(policy_manifest) is not PolicyManifest:
        raise ValueError("policy_manifest must be exact")
    PolicyManifest(
        canonical_json=policy_manifest.canonical_json,
        hash=policy_manifest.hash,
        scope_count=policy_manifest.scope_count,
    )
    if (
        runtime_contract.capability_manifest.policy_manifest_hash
        != policy_manifest.hash
    ):
        raise ValueError("capability policy manifest does not match")
    exact_actor = _require_exact_text("actor", actor, max_bytes=128)
    exact_reason = _require_exact_text("reason", reason, max_bytes=512)
    capability = runtime_contract.capability_manifest
    encoded = (
        '{"account_id":'
        + str(account_id)
        + ',"actor":'
        + _canonical_json_string(exact_actor)
        + ',"capability_hash":'
        + _canonical_json_string(capability.capability_hash)
        + ',"pipeline_name":'
        + _canonical_json_string(GREENFIELD_PIPELINE_NAME)
        + ',"policy_manifest":'
        + policy_manifest.canonical_json
        + ',"policy_manifest_hash":'
        + _canonical_json_string(policy_manifest.hash)
        + ',"reason":'
        + _canonical_json_string(exact_reason)
        + ',"runtime_contract":{"build_id":'
        + _canonical_json_string(runtime_contract.build_id)
        + ',"config_hash":'
        + _canonical_json_string(runtime_contract.config_hash)
        + ',"protocol_version":'
        + str(runtime_contract.protocol_version)
        + ',"schema_digest":'
        + _canonical_json_string(runtime_contract.schema_digest)
        + ',"schema_revision":'
        + _canonical_json_string(runtime_contract.schema_revision)
        + '},"schema_version":'
        + str(_INITIALIZATION_SCHEMA_VERSION)
        + "}"
    )
    digest = hashlib.sha256(
        _INITIALIZATION_HASH_DOMAIN + encoded.encode("utf-8")
    ).hexdigest()
    return encoded, digest


def canonical_authority_transition_payload(
    *,
    authority: RuntimeAuthority,
    target_state: RuntimeAuthorityState,
    actor: str,
    reason: str,
) -> tuple[str, str]:
    """Freeze one of the only two Phase 2 authority transitions."""

    exact_authority = _revalidate_authority(authority)
    exact_target = _require_exact_enum(
        "target_state",
        target_state,
        RuntimeAuthorityState,
    )
    transition = (exact_authority.state, exact_target)
    if transition == (
        RuntimeAuthorityState.INGEST_ONLY,
        RuntimeAuthorityState.PAUSED,
    ):
        command_name = _PAUSE_COMMAND_NAME
    elif transition == (
        RuntimeAuthorityState.PAUSED,
        RuntimeAuthorityState.INGEST_ONLY,
    ):
        command_name = _RESUME_COMMAND_NAME
    else:
        raise ValueError("Phase 2 authority transition is invalid")
    if (
        exact_authority.authority_epoch >= POSTGRES_BIGINT_MAX - 1
        or exact_authority.version >= POSTGRES_BIGINT_MAX - 1
    ):
        raise ValueError("authority transition has no CAS increment capacity")
    exact_actor = _require_exact_text("actor", actor, max_bytes=128)
    exact_reason = _require_exact_text("reason", reason, max_bytes=512)
    canonical = {
        "account_id": exact_authority.account_id,
        "actor": exact_actor,
        "build_id": exact_authority.build_id,
        "capability_hash": exact_authority.capability_hash,
        "command_name": command_name,
        "config_hash": exact_authority.config_hash,
        "expected_authority_epoch": exact_authority.authority_epoch,
        "expected_version": exact_authority.version,
        "fencing_token": exact_authority.fencing_token,
        "generation": exact_authority.generation,
        "initialization_id": exact_authority.initialization_id,
        "pipeline_name": exact_authority.pipeline_name,
        "policy_manifest_hash": exact_authority.policy_manifest_hash,
        "previous_state": exact_authority.state.value,
        "protocol_version": exact_authority.protocol_version,
        "reason": exact_reason,
        "schema_revision": exact_authority.schema_revision,
        "schema_version": _TRANSITION_SCHEMA_VERSION,
        "target_state": exact_target.value,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    digest = hashlib.sha256(
        _TRANSITION_HASH_DOMAIN + encoded.encode("utf-8")
    ).hexdigest()
    return encoded, digest


def _authority_from_row(row: object) -> RuntimeAuthority:
    material = _exact_row_values(
        row,
        _AUTHORITY_COLUMNS,
        error="runtime authority row is invalid",
    )
    try:
        return RuntimeAuthority(
            account_id=material["account_id"],
            state=material["state"],
            generation=material["generation"],
            fencing_token=material["fencing_token"],
            pipeline_name=material["pipeline_name"],
            authority_epoch=material["authority_epoch"],
            version=material["version"],
            schema_revision=material["schema_revision"],
            protocol_version=material["protocol_version"],
            build_id=material["build_id"],
            config_hash=material["config_hash"],
            capability_hash=material["capability_hash"],
            policy_manifest_hash=material["policy_manifest_hash"],
            initialization_id=_database_uuid4(
                "initialization_id",
                material["initialization_id"],
            ),
            updated_at=material["updated_at"],
        )
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("runtime authority row is invalid") from None


def _initialization_receipt_from_row(row: object) -> InitializationReceipt:
    material = _exact_row_values(
        row,
        _INITIALIZATION_COLUMNS,
        error="initialization receipt row is invalid",
    )
    try:
        return InitializationReceipt(
            initialization_id=_database_uuid4(
                "initialization_id",
                material["initialization_id"],
            ),
            command_receipt_id=_database_uuid4(
                "command_receipt_id",
                material["command_receipt_id"],
            ),
            account_id=material["account_id"],
            generation=material["generation"],
            fencing_token=material["fencing_token"],
            pipeline_name=material["pipeline_name"],
            authority_epoch=material["authority_epoch"],
            authority_version=material["authority_version"],
            capability_hash=material["capability_hash"],
            policy_manifest_hash=material["policy_manifest_hash"],
            transaction_id=material["transaction_id"],
            replayed=material["replayed"],
            created_at=material["created_at"],
        )
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("initialization receipt row is invalid") from None


def _transition_receipt_from_row(row: object) -> AuthorityTransitionReceipt:
    material = _exact_row_values(
        row,
        _TRANSITION_COLUMNS,
        error="authority transition receipt row is invalid",
    )
    try:
        authority = _authority_from_row(
            {column: material[column] for column in _AUTHORITY_COLUMNS}
        )
        return AuthorityTransitionReceipt(
            command_receipt_id=_database_uuid4(
                "command_receipt_id",
                material["command_receipt_id"],
            ),
            command_name=material["command_name"],
            previous_state=material["previous_state"],
            previous_authority_epoch=material["previous_authority_epoch"],
            previous_version=material["previous_version"],
            transaction_id=material["transaction_id"],
            replayed=material["replayed"],
            created_at=material["receipt_created_at"],
            authority=authority,
        )
    except (KeyError, RuntimeError, TypeError, ValueError):
        raise RuntimeError("authority transition receipt row is invalid") from None


def _instance_lease_from_row(row: object) -> RuntimeInstanceLease:
    material = _exact_row_values(
        row,
        _INSTANCE_COLUMNS,
        error="runtime instance lease row is invalid",
    )
    try:
        return RuntimeInstanceLease(
            account_id=material["account_id"],
            workload=material["workload"],
            instance_id=material["instance_id"],
            session_id=_database_uuid4("session_id", material["session_id"]),
            generation=material["generation"],
            fencing_token=material["fencing_token"],
            authority_epoch=material["authority_epoch"],
            capability_hash=material["capability_hash"],
            schema_revision=material["schema_revision"],
            protocol_version=material["protocol_version"],
            build_id=material["build_id"],
            config_hash=material["config_hash"],
            lifecycle=material["lifecycle"],
            lease_version=material["lease_version"],
            accepted_count=material["accepted_count"],
            rejected_count=material["rejected_count"],
            heartbeat_at=material["heartbeat_at"],
            lease_until=material["lease_until"],
        )
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("runtime instance lease row is invalid") from None


def _require_initialization_matches(
    receipt: InitializationReceipt,
    *,
    account_id: int,
    runtime_contract: RuntimeContract,
    policy_manifest: PolicyManifest,
) -> None:
    if (
        receipt.account_id != account_id
        or receipt.authority_version != 1
        or receipt.capability_hash
        != runtime_contract.capability_manifest.capability_hash
        or receipt.policy_manifest_hash != policy_manifest.hash
    ):
        raise RuntimeError("initialization receipt does not match command")


def _require_transition_matches(
    receipt: AuthorityTransitionReceipt,
    previous: RuntimeAuthority,
    *,
    command_name: str,
    target_state: RuntimeAuthorityState,
) -> None:
    current = receipt.authority
    stable_fields = (
        "account_id",
        "generation",
        "fencing_token",
        "pipeline_name",
        "schema_revision",
        "protocol_version",
        "build_id",
        "config_hash",
        "capability_hash",
        "policy_manifest_hash",
        "initialization_id",
    )
    if (
        receipt.command_name != command_name
        or receipt.previous_state is not previous.state
        or receipt.previous_authority_epoch != previous.authority_epoch
        or receipt.previous_version != previous.version
        or current.state is not target_state
        or current.authority_epoch != previous.authority_epoch + 1
        or current.version != previous.version + 1
        or current.updated_at < previous.updated_at
        or receipt.created_at < previous.updated_at
        or receipt.created_at != current.updated_at
        or any(
            getattr(current, field) != getattr(previous, field)
            for field in stable_fields
        )
    ):
        raise RuntimeError("authority transition receipt does not match command")


def _require_authority_matches_contract(
    authority: RuntimeAuthority,
    runtime_contract: RuntimeContract,
) -> None:
    capability = runtime_contract.capability_manifest
    if (
        authority.schema_revision != runtime_contract.schema_revision
        or authority.protocol_version != runtime_contract.protocol_version
        or authority.build_id != runtime_contract.build_id
        or authority.config_hash != runtime_contract.config_hash
        or authority.capability_hash != capability.capability_hash
        or authority.policy_manifest_hash != capability.policy_manifest_hash
    ):
        raise RuntimeError("runtime authority and contract do not match")


def _require_registered_lease_matches(
    lease: RuntimeInstanceLease,
    authority: RuntimeAuthority,
    runtime_contract: RuntimeContract,
    *,
    instance_id: str,
    session_id: str,
    lease_seconds: int,
) -> None:
    if (
        lease.account_id != authority.account_id
        or lease.workload is not RuntimeWorkload.WEB
        or lease.instance_id != instance_id
        or lease.session_id != session_id
        or lease.generation != authority.generation
        or lease.fencing_token != authority.fencing_token
        or lease.authority_epoch != authority.authority_epoch
        or lease.capability_hash != authority.capability_hash
        or lease.schema_revision != runtime_contract.schema_revision
        or lease.protocol_version != runtime_contract.protocol_version
        or lease.build_id != runtime_contract.build_id
        or lease.config_hash != runtime_contract.config_hash
        or lease.lifecycle is not RuntimeInstanceLifecycle.ACTIVE
        or lease.lease_version != 1
        or lease.accepted_count != 0
        or lease.rejected_count != 0
        or lease.lease_until != lease.heartbeat_at + timedelta(seconds=lease_seconds)
    ):
        raise RuntimeError("runtime instance lease does not match registration")


def _require_updated_lease_matches(
    current: RuntimeInstanceLease,
    previous: RuntimeInstanceLease,
    *,
    lifecycle: RuntimeInstanceLifecycle,
    accepted_count: int,
    rejected_count: int,
    lease_seconds: int | None,
) -> None:
    stable_fields = (
        "account_id",
        "workload",
        "instance_id",
        "session_id",
        "generation",
        "fencing_token",
        "authority_epoch",
        "capability_hash",
        "schema_revision",
        "protocol_version",
        "build_id",
        "config_hash",
    )
    if (
        current.lifecycle is not lifecycle
        or current.lease_version != previous.lease_version + 1
        or current.accepted_count != accepted_count
        or current.rejected_count != rejected_count
        or current.heartbeat_at < previous.heartbeat_at
        or (
            lease_seconds is not None
            and current.lease_until
            != current.heartbeat_at + timedelta(seconds=lease_seconds)
        )
        or any(
            getattr(current, field) != getattr(previous, field)
            for field in stable_fields
        )
    ):
        raise RuntimeError("runtime instance lease does not match command")


class GreenfieldInitializer:
    """Initialize the one greenfield authority through a fixed DB function."""

    __slots__ = ("_pool",)

    def __init__(self, pool: object) -> None:
        self._pool = pool

    async def initialize(
        self,
        account_id: int,
        runtime_contract: RuntimeContract,
        policy_snapshot: PolicySnapshot,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> InitializationReceipt:
        exact_account_id = _require_bigint("account_id", account_id, minimum=1)
        exact_contract = _revalidate_runtime_contract(runtime_contract)
        policy_manifest = canonical_policy_manifest(policy_snapshot)
        exact_actor = _require_exact_text("actor", actor, max_bytes=128)
        exact_reason = _require_exact_text("reason", reason, max_bytes=512)
        exact_key = _require_exact_text(
            "idempotency_key",
            idempotency_key,
            max_bytes=4096,
        )
        _, payload_hash = canonical_initialization_payload(
            account_id=exact_account_id,
            runtime_contract=exact_contract,
            policy_manifest=policy_manifest,
            actor=exact_actor,
            reason=exact_reason,
        )
        capability = exact_contract.capability_manifest
        params = (
            exact_account_id,
            capability.capability_hash,
            capability.predecessor_hash,
            capability.stage.value,
            exact_contract.schema_revision,
            exact_contract.schema_digest,
            exact_contract.protocol_version,
            exact_contract.build_id,
            exact_contract.config_hash,
            capability.adapter_hash,
            policy_manifest.hash,
            capability.evidence_manifest_hash,
            policy_manifest.canonical_json,
            policy_manifest.scope_count,
            exact_actor,
            exact_reason,
            exact_key,
            payload_hash,
        )
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(_INITIALIZE_SQL, params)
                receipt = _initialization_receipt_from_row(await cursor.fetchone())
                _require_initialization_matches(
                    receipt,
                    account_id=exact_account_id,
                    runtime_contract=exact_contract,
                    policy_manifest=policy_manifest,
                )
        return receipt


class RuntimeAuthorityRepository:
    """Read, pause, or resume the Phase 2 ingress authority."""

    __slots__ = ("_pool",)

    def __init__(self, pool: object) -> None:
        self._pool = pool

    async def get(self, account_id: int) -> RuntimeAuthority | None:
        exact_account_id = _require_bigint("account_id", account_id, minimum=1)
        async with self._pool.connection() as connection:
            cursor = await connection.execute(_GET_AUTHORITY_SQL, (exact_account_id,))
            row = await cursor.fetchone()
            if row is None:
                authority = None
            else:
                authority = _authority_from_row(row)
                if authority.account_id != exact_account_id or authority.state not in {
                    RuntimeAuthorityState.INGEST_ONLY,
                    RuntimeAuthorityState.PAUSED,
                }:
                    raise RuntimeError("runtime authority row is invalid")
        return authority

    async def pause(
        self,
        authority: RuntimeAuthority,
        *,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> AuthorityTransitionReceipt:
        return await self._transition(
            authority,
            target_state=RuntimeAuthorityState.PAUSED,
            actor=actor,
            reason=reason,
            idempotency_key=idempotency_key,
        )

    async def resume_ingress(
        self,
        authority: RuntimeAuthority,
        *,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> AuthorityTransitionReceipt:
        return await self._transition(
            authority,
            target_state=RuntimeAuthorityState.INGEST_ONLY,
            actor=actor,
            reason=reason,
            idempotency_key=idempotency_key,
        )

    async def _transition(
        self,
        authority: RuntimeAuthority,
        *,
        target_state: RuntimeAuthorityState,
        actor: str,
        reason: str,
        idempotency_key: str,
    ) -> AuthorityTransitionReceipt:
        exact_authority = _revalidate_authority(authority)
        exact_actor = _require_exact_text("actor", actor, max_bytes=128)
        exact_reason = _require_exact_text("reason", reason, max_bytes=512)
        exact_key = _require_exact_text(
            "idempotency_key",
            idempotency_key,
            max_bytes=4096,
        )
        _, payload_hash = canonical_authority_transition_payload(
            authority=exact_authority,
            target_state=target_state,
            actor=exact_actor,
            reason=exact_reason,
        )
        if target_state is RuntimeAuthorityState.PAUSED:
            command_name = _PAUSE_COMMAND_NAME
        else:
            command_name = _RESUME_COMMAND_NAME
        params = (
            exact_authority.account_id,
            exact_authority.authority_epoch,
            exact_authority.version,
            exact_authority.capability_hash,
            exact_actor,
            exact_reason,
            exact_key,
            payload_hash,
        )
        async with self._pool.connection() as connection:
            async with connection.transaction():
                if target_state is RuntimeAuthorityState.PAUSED:
                    cursor = await connection.execute(_PAUSE_SQL, params)
                else:
                    cursor = await connection.execute(_RESUME_SQL, params)
                receipt = _transition_receipt_from_row(await cursor.fetchone())
                _require_transition_matches(
                    receipt,
                    exact_authority,
                    command_name=command_name,
                    target_state=target_state,
                )
        return receipt


class RuntimeInstanceRepository:
    """Register and maintain only Phase 2 web runtime sessions."""

    __slots__ = ("_pool",)

    def __init__(self, pool: object) -> None:
        self._pool = pool

    async def register(
        self,
        authority: RuntimeAuthority,
        runtime_contract: RuntimeContract,
        instance_id: str,
        session_id: str,
        lease_seconds: int,
    ) -> RuntimeInstanceLease:
        exact_authority = _revalidate_authority(authority)
        if exact_authority.state is not RuntimeAuthorityState.INGEST_ONLY:
            raise RuntimeError("runtime authority is not accepting ingress")
        exact_contract = _revalidate_runtime_contract(runtime_contract)
        _require_authority_matches_contract(exact_authority, exact_contract)
        exact_instance_id = _require_pattern_text(
            "instance_id",
            instance_id,
            _INSTANCE_ID_PATTERN,
        )
        exact_session_id = _require_uuid4("session_id", session_id)
        exact_lease_seconds = _require_lease_seconds(lease_seconds)
        params = (
            exact_authority.account_id,
            exact_instance_id,
            exact_session_id,
            exact_authority.authority_epoch,
            exact_authority.version,
            exact_contract.schema_revision,
            exact_contract.protocol_version,
            exact_contract.build_id,
            exact_contract.config_hash,
            exact_contract.capability_manifest.capability_hash,
            exact_lease_seconds,
        )
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(_REGISTER_SQL, params)
                lease = _instance_lease_from_row(await cursor.fetchone())
                _require_registered_lease_matches(
                    lease,
                    exact_authority,
                    exact_contract,
                    instance_id=exact_instance_id,
                    session_id=exact_session_id,
                    lease_seconds=exact_lease_seconds,
                )
        return lease

    async def heartbeat(
        self,
        lease: RuntimeInstanceLease,
        accepted_count: int,
        rejected_count: int,
        lease_seconds: int,
    ) -> RuntimeInstanceLease:
        previous = _revalidate_instance_lease(lease)
        if previous.lifecycle is not RuntimeInstanceLifecycle.ACTIVE:
            raise RuntimeError("only an active web lease may heartbeat")
        if previous.lease_version >= POSTGRES_BIGINT_MAX - 1:
            raise ValueError("lease_version has no CAS increment capacity")
        exact_accepted = _require_stored_bigint(
            "accepted_count",
            accepted_count,
            minimum=0,
        )
        exact_rejected = _require_stored_bigint(
            "rejected_count",
            rejected_count,
            minimum=0,
        )
        if (
            exact_accepted < previous.accepted_count
            or exact_rejected < previous.rejected_count
        ):
            raise ValueError("runtime counters must be monotonic")
        exact_lease_seconds = _require_lease_seconds(lease_seconds)
        params = (
            previous.account_id,
            previous.session_id,
            previous.lease_version,
            previous.authority_epoch,
            previous.capability_hash,
            exact_accepted,
            exact_rejected,
            exact_lease_seconds,
        )
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(_HEARTBEAT_SQL, params)
                current = _instance_lease_from_row(await cursor.fetchone())
                _require_updated_lease_matches(
                    current,
                    previous,
                    lifecycle=RuntimeInstanceLifecycle.ACTIVE,
                    accepted_count=exact_accepted,
                    rejected_count=exact_rejected,
                    lease_seconds=exact_lease_seconds,
                )
        return current

    async def drain(self, lease: RuntimeInstanceLease) -> RuntimeInstanceLease:
        previous = _revalidate_instance_lease(lease)
        if previous.lifecycle is not RuntimeInstanceLifecycle.ACTIVE:
            raise RuntimeError("only an active web lease may drain")
        if previous.lease_version >= POSTGRES_BIGINT_MAX - 1:
            raise ValueError("lease_version has no CAS increment capacity")
        params = (
            previous.account_id,
            previous.session_id,
            previous.lease_version,
            previous.authority_epoch,
            previous.capability_hash,
        )
        async with self._pool.connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(_DRAIN_SQL, params)
                current = _instance_lease_from_row(await cursor.fetchone())
                _require_updated_lease_matches(
                    current,
                    previous,
                    lifecycle=RuntimeInstanceLifecycle.DRAINING,
                    accepted_count=previous.accepted_count,
                    rejected_count=previous.rejected_count,
                    lease_seconds=None,
                )
        return current


__all__ = [
    "AuthorityTransitionReceipt",
    "GREENFIELD_PIPELINE_NAME",
    "GreenfieldInitializer",
    "InitializationReceipt",
    "PolicyManifest",
    "RuntimeAuthority",
    "RuntimeAuthorityRepository",
    "RuntimeAuthorityState",
    "RuntimeContract",
    "RuntimeInstanceLease",
    "RuntimeInstanceLifecycle",
    "RuntimeInstanceRepository",
    "RuntimeWorkload",
    "canonical_authority_transition_payload",
    "canonical_initialization_payload",
    "canonical_policy_manifest",
    "require_phase2_ingress_authority",
]
