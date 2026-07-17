from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from uuid import UUID

import pytest

from src.ingestion.models import (
    POSTGRES_BIGINT_MAX,
    ChangeKind,
    IngressSource,
    ProcessingPolicy,
)
from src.ingestion.policy import FolderScope, PolicySnapshot
from src.ingestion.runtime_authority import (
    AuthorityTransitionReceipt,
    GREENFIELD_PIPELINE_NAME,
    GreenfieldInitializer,
    InitializationReceipt,
    PolicyManifest,
    RuntimeAuthority,
    RuntimeAuthorityRepository,
    RuntimeAuthorityState,
    RuntimeContract,
    RuntimeInstanceRepository,
    RuntimeInstanceLease,
    RuntimeInstanceLifecycle,
    RuntimeWorkload,
    canonical_authority_transition_payload,
    canonical_initialization_payload,
    canonical_policy_manifest,
    require_phase2_ingress_authority,
)
from src.ingestion.runtime_capability import (
    CAPABILITY_CHAIN_ROOT_HASH,
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
)


_HASH = "a" * 64
_OTHER_HASH = "b" * 64
_SESSION = "00000000-0000-4000-8000-000000000001"
_INITIALIZATION = "00000000-0000-4000-8000-000000000002"
_RECEIPT = "00000000-0000-4000-8000-000000000003"
_NOW = datetime(2026, 7, 16, 8, 0, tzinfo=UTC)

_AUTHORITY_COLUMNS = (
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
_INITIALIZATION_COLUMNS = (
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
_TRANSITION_COLUMNS = (
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
_INSTANCE_COLUMNS = (
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

_INITIALIZE_SQL = (
    "SELECT initialization_id, command_receipt_id, account_id, generation, "
    "fencing_token, pipeline_name, authority_epoch, authority_version, "
    "capability_hash, policy_manifest_hash, transaction_id, replayed, created_at "
    "FROM public.greenfield_initialize_runtime(" + ", ".join(["%s"] * 18) + ")"
)
_GET_AUTHORITY_SQL = (
    "SELECT account_id, state, generation, fencing_token, pipeline_name, "
    "authority_epoch, version, schema_revision, protocol_version, build_id, "
    "config_hash, capability_hash, policy_manifest_hash, initialization_id, "
    "updated_at FROM public.greenfield_get_runtime_authority(%s)"
)
_TRANSITION_SELECT = (
    "SELECT command_receipt_id, command_name, previous_state, "
    "previous_authority_epoch, previous_version, transaction_id, replayed, "
    "receipt_created_at, account_id, state, generation, fencing_token, "
    "pipeline_name, authority_epoch, version, schema_revision, protocol_version, "
    "build_id, config_hash, capability_hash, policy_manifest_hash, "
    "initialization_id, updated_at FROM public."
)
_PAUSE_SQL = (
    _TRANSITION_SELECT + "greenfield_pause_runtime(" + ", ".join(["%s"] * 8) + ")"
)
_RESUME_SQL = (
    _TRANSITION_SELECT + "greenfield_resume_ingress(" + ", ".join(["%s"] * 8) + ")"
)
_INSTANCE_SELECT = (
    "SELECT account_id, workload, instance_id, session_id, generation, "
    "fencing_token, authority_epoch, capability_hash, schema_revision, "
    "protocol_version, build_id, config_hash, lifecycle, lease_version, "
    "accepted_count, rejected_count, heartbeat_at, lease_until FROM public."
)
_REGISTER_SQL = (
    _INSTANCE_SELECT
    + "greenfield_register_web_instance("
    + ", ".join(["%s"] * 11)
    + ")"
)
_HEARTBEAT_SQL = (
    _INSTANCE_SELECT
    + "greenfield_heartbeat_web_instance("
    + ", ".join(["%s"] * 8)
    + ")"
)
_DRAIN_SQL = (
    _INSTANCE_SELECT + "greenfield_drain_web_instance(" + ", ".join(["%s"] * 5) + ")"
)


def _matrix(
    create_policy: ProcessingPolicy = ProcessingPolicy.FULL,
) -> dict[tuple[IngressSource, str, ChangeKind], ProcessingPolicy]:
    return {
        (IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE): create_policy,
        (IngressSource.WEBHOOK, "CreatedEvent", ChangeKind.CREATE): (
            ProcessingPolicy.IGNORED
        ),
        (IngressSource.WEBHOOK, "ModifiedEvent", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.WEBHOOK, "DeletedEvent", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "create", ChangeKind.CREATE): create_policy,
        (IngressSource.SYNC, "update", ChangeKind.UPDATE): (
            ProcessingPolicy.METADATA_ONLY
        ),
        (IngressSource.SYNC, "delete", ChangeKind.DELETE): (
            ProcessingPolicy.METADATA_ONLY
        ),
    }


def _snapshot() -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("inbox-id", "inbox-alias"),
                sync_folder="Inbox",
                event_policy_matrix=_matrix(),
            ),
            FolderScope.configured(
                canonical_key="ARCHIVE",
                webhook_ids=("archive-id",),
                sync_folder="Archive",
                event_policy_matrix=_matrix(ProcessingPolicy.ARCHIVE),
            ),
        )
    )


def _unicode_snapshot() -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=("收件箱-id",),
                sync_folder="收件箱",
                event_policy_matrix=_matrix(),
            ),
        )
    )


def _snapshot_with_webhook_ids(webhook_ids: tuple[str, ...]) -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            FolderScope.configured(
                canonical_key="INBOX",
                webhook_ids=webhook_ids,
                sync_folder="Inbox",
                event_policy_matrix=_matrix(),
            ),
        )
    )


def _database_webhook_ids_json_size(webhook_ids: tuple[str, ...]) -> int:
    encoded = json.dumps(
        sorted(webhook_ids),
        ensure_ascii=False,
        separators=(", ", ": "),
        allow_nan=False,
    ).encode("utf-8")
    return len(encoded)


def _webhook_ids_at_database_json_size(size: int) -> tuple[str, ...]:
    if size not in {32768, 32769}:
        raise AssertionError(size)
    webhook_ids = tuple(
        f"{index:02d}" + "x" * (506 + (index == 63 and size == 32769))
        for index in range(64)
    )
    assert _database_webhook_ids_json_size(webhook_ids) == size
    return webhook_ids


def _capability(policy_manifest_hash: str) -> RuntimeCapabilityManifest:
    return RuntimeCapabilityManifest(
        stage=RuntimeCapabilityStage.PHASE2_INGESTION,
        schema_revision="20260716_0006",
        schema_digest=_HASH,
        protocol_version=1,
        minimum_build_id="build-1",
        config_hash=_OTHER_HASH,
        adapter_hash="c" * 64,
        policy_manifest_hash=policy_manifest_hash,
        evidence_manifest_hash="d" * 64,
        predecessor_hash=CAPABILITY_CHAIN_ROOT_HASH,
    )


def _contract(policy_manifest_hash: str | None = None) -> RuntimeContract:
    manifest_hash = policy_manifest_hash or canonical_policy_manifest(_snapshot()).hash
    return RuntimeContract(
        schema_revision="20260716_0006",
        schema_digest=_HASH,
        protocol_version=1,
        build_id="build-1",
        config_hash=_OTHER_HASH,
        capability_manifest=_capability(manifest_hash),
    )


def _authority(
    state: RuntimeAuthorityState = RuntimeAuthorityState.INGEST_ONLY,
) -> RuntimeAuthority:
    contract = _contract()
    return RuntimeAuthority(
        account_id=8,
        state=state,
        generation=1,
        fencing_token=1,
        pipeline_name=GREENFIELD_PIPELINE_NAME,
        authority_epoch=1,
        version=1,
        schema_revision=contract.schema_revision,
        protocol_version=contract.protocol_version,
        build_id=contract.build_id,
        config_hash=contract.config_hash,
        capability_hash=contract.capability_manifest.capability_hash,
        policy_manifest_hash=contract.capability_manifest.policy_manifest_hash,
        initialization_id=_INITIALIZATION,
        updated_at=_NOW,
    )


def _row(
    columns: tuple[str, ...],
    values: dict[str, object],
    *,
    mapping: bool = False,
) -> object:
    if mapping:
        return MappingProxyType(dict(values))
    return tuple(values[column] for column in columns)


def _authority_values(
    authority: RuntimeAuthority | None = None,
    **overrides: object,
) -> dict[str, object]:
    value = authority or _authority()
    result = {column: getattr(value, column) for column in _AUTHORITY_COLUMNS}
    result["state"] = value.state.value
    result.update(overrides)
    return result


def _authority_row(
    authority: RuntimeAuthority | None = None,
    *,
    mapping: bool = False,
    **overrides: object,
) -> object:
    return _row(
        _AUTHORITY_COLUMNS,
        _authority_values(authority, **overrides),
        mapping=mapping,
    )


def _initialization_values(**overrides: object) -> dict[str, object]:
    contract = _contract()
    result: dict[str, object] = {
        "initialization_id": _INITIALIZATION,
        "command_receipt_id": _RECEIPT,
        "account_id": 8,
        "generation": 1,
        "fencing_token": 1,
        "pipeline_name": GREENFIELD_PIPELINE_NAME,
        "authority_epoch": 1,
        "authority_version": 1,
        "capability_hash": contract.capability_manifest.capability_hash,
        "policy_manifest_hash": contract.capability_manifest.policy_manifest_hash,
        "transaction_id": "12345",
        "replayed": False,
        "created_at": _NOW,
    }
    result.update(overrides)
    return result


def _initialization_row(*, mapping: bool = False, **overrides: object) -> object:
    return _row(
        _INITIALIZATION_COLUMNS,
        _initialization_values(**overrides),
        mapping=mapping,
    )


def _transition_row(
    previous: RuntimeAuthority,
    current: RuntimeAuthority,
    *,
    command_name: str,
    mapping: bool = False,
    **overrides: object,
) -> object:
    result: dict[str, object] = {
        "command_receipt_id": _RECEIPT,
        "command_name": command_name,
        "previous_state": previous.state.value,
        "previous_authority_epoch": previous.authority_epoch,
        "previous_version": previous.version,
        "transaction_id": "12345",
        "replayed": False,
        "receipt_created_at": current.updated_at,
        **_authority_values(current),
    }
    result.update(overrides)
    return _row(_TRANSITION_COLUMNS, result, mapping=mapping)


def _transitioned_authority(
    previous: RuntimeAuthority,
    state: RuntimeAuthorityState,
) -> RuntimeAuthority:
    return replace(
        previous,
        state=state,
        authority_epoch=previous.authority_epoch + 1,
        version=previous.version + 1,
        updated_at=previous.updated_at + timedelta(seconds=1),
    )


def _instance_lease(
    *,
    authority: RuntimeAuthority | None = None,
    lifecycle: RuntimeInstanceLifecycle = RuntimeInstanceLifecycle.ACTIVE,
    lease_version: int = 1,
    accepted_count: int = 0,
    rejected_count: int = 0,
    heartbeat_at: datetime = _NOW,
) -> RuntimeInstanceLease:
    current = authority or _authority()
    return RuntimeInstanceLease(
        account_id=current.account_id,
        workload=RuntimeWorkload.WEB,
        instance_id="web-1",
        session_id=_SESSION,
        generation=current.generation,
        fencing_token=current.fencing_token,
        authority_epoch=current.authority_epoch,
        capability_hash=current.capability_hash,
        schema_revision=current.schema_revision,
        protocol_version=current.protocol_version,
        build_id=current.build_id,
        config_hash=current.config_hash,
        lifecycle=lifecycle,
        lease_version=lease_version,
        accepted_count=accepted_count,
        rejected_count=rejected_count,
        heartbeat_at=heartbeat_at,
        lease_until=heartbeat_at + timedelta(seconds=30),
    )


def _tampered_instance_lease(field: str, value: object) -> RuntimeInstanceLease:
    lease = _instance_lease()
    object.__setattr__(lease, field, value)
    return lease


def _instance_values(
    lease: RuntimeInstanceLease | None = None,
    **overrides: object,
) -> dict[str, object]:
    value = lease or _instance_lease()
    result = {column: getattr(value, column) for column in _INSTANCE_COLUMNS}
    result["workload"] = value.workload.value
    result["lifecycle"] = value.lifecycle.value
    result.update(overrides)
    return result


def _instance_row(
    lease: RuntimeInstanceLease | None = None,
    *,
    mapping: bool = False,
    **overrides: object,
) -> object:
    return _row(
        _INSTANCE_COLUMNS,
        _instance_values(lease, **overrides),
        mapping=mapping,
    )


class _Cursor:
    def __init__(
        self,
        events: list[str],
        row: object,
        *,
        fetch_failure: BaseException | None = None,
    ) -> None:
        self._events = events
        self._row = row
        self._fetch_failure = fetch_failure

    async def fetchone(self) -> object:
        self._events.append("cursor.fetchone")
        if self._fetch_failure is not None:
            raise self._fetch_failure
        return self._row


class _TransactionContext:
    def __init__(
        self,
        events: list[str],
        *,
        exit_failure: BaseException | None = None,
    ) -> None:
        self._events = events
        self._exit_failure = exit_failure

    async def __aenter__(self) -> object:
        self._events.append("transaction.enter")
        return self

    async def __aexit__(self, *_args: object) -> bool:
        self._events.append("transaction.exit")
        if self._exit_failure is not None:
            raise self._exit_failure
        return False


class _Connection:
    def __init__(
        self,
        events: list[str],
        row: object,
        *,
        execute_failure: BaseException | None = None,
        fetch_failure: BaseException | None = None,
        commit_failure: BaseException | None = None,
    ) -> None:
        self._events = events
        self._row = row
        self._execute_failure = execute_failure
        self._fetch_failure = fetch_failure
        self._commit_failure = commit_failure
        self.calls: list[tuple[object, object]] = []

    def transaction(self) -> _TransactionContext:
        self._events.append("transaction.create")
        return _TransactionContext(
            self._events,
            exit_failure=self._commit_failure,
        )

    async def execute(self, statement: object, params: object) -> _Cursor:
        self._events.append("connection.execute")
        self.calls.append((statement, params))
        if self._execute_failure is not None:
            raise self._execute_failure
        return _Cursor(
            self._events,
            self._row,
            fetch_failure=self._fetch_failure,
        )


class _ConnectionContext:
    def __init__(self, events: list[str], connection: _Connection) -> None:
        self._events = events
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        self._events.append("connection.enter")
        return self._connection

    async def __aexit__(self, *_args: object) -> bool:
        self._events.append("connection.exit")
        return False


class _Pool:
    def __init__(
        self,
        row: object,
        *,
        execute_failure: BaseException | None = None,
        fetch_failure: BaseException | None = None,
        commit_failure: BaseException | None = None,
    ) -> None:
        self.events: list[str] = []
        self.connection_value = _Connection(
            self.events,
            row,
            execute_failure=execute_failure,
            fetch_failure=fetch_failure,
            commit_failure=commit_failure,
        )
        self.connection_calls = 0

    def connection(self) -> _ConnectionContext:
        self.connection_calls += 1
        self.events.append("connection.create")
        return _ConnectionContext(self.events, self.connection_value)


class _NeverPool:
    def __init__(self) -> None:
        self.connection_calls = 0

    def connection(self) -> object:
        self.connection_calls += 1
        raise AssertionError("invalid input must fail before pool access")


async def _invoke_mutation(name: str, pool: object) -> object:
    if name == "initialize":
        return await GreenfieldInitializer(pool).initialize(
            8,
            _contract(),
            _snapshot(),
            "operator-1",
            "new system initialization",
            "initialize-1",
        )
    if name == "pause":
        return await RuntimeAuthorityRepository(pool).pause(
            _authority(),
            actor="operator-1",
            reason="maintenance window",
            idempotency_key="transition-1",
        )
    if name == "resume_ingress":
        return await RuntimeAuthorityRepository(pool).resume_ingress(
            _authority(RuntimeAuthorityState.PAUSED),
            actor="operator-1",
            reason="maintenance window",
            idempotency_key="transition-1",
        )
    if name == "register":
        return await RuntimeInstanceRepository(pool).register(
            _authority(),
            _contract(),
            "web-1",
            _SESSION,
            30,
        )
    if name == "heartbeat":
        return await RuntimeInstanceRepository(pool).heartbeat(
            _instance_lease(),
            2,
            1,
            30,
        )
    if name == "drain":
        return await RuntimeInstanceRepository(pool).drain(_instance_lease())
    raise AssertionError(name)


def _successful_mutation_row(name: str) -> object:
    if name == "initialize":
        return _initialization_row()
    if name == "pause":
        previous = _authority()
        return _transition_row(
            previous,
            _transitioned_authority(previous, RuntimeAuthorityState.PAUSED),
            command_name="runtime.pause",
        )
    if name == "resume_ingress":
        previous = _authority(RuntimeAuthorityState.PAUSED)
        return _transition_row(
            previous,
            _transitioned_authority(previous, RuntimeAuthorityState.INGEST_ONLY),
            command_name="runtime.resume_ingress",
        )
    if name == "register":
        return _instance_row()
    if name == "heartbeat":
        return _instance_row(
            replace(
                _instance_lease(),
                lease_version=2,
                accepted_count=2,
                rejected_count=1,
                heartbeat_at=_NOW + timedelta(seconds=1),
                lease_until=_NOW + timedelta(seconds=31),
            )
        )
    if name == "drain":
        return _instance_row(
            replace(
                _instance_lease(),
                lifecycle=RuntimeInstanceLifecycle.DRAINING,
                lease_version=2,
                heartbeat_at=_NOW + timedelta(seconds=1),
                lease_until=_NOW + timedelta(seconds=31),
            )
        )
    raise AssertionError(name)


def test_runtime_contract_is_frozen_slotted_and_exactly_binds_phase2_manifest() -> None:
    contract = _contract()

    assert [field.name for field in fields(contract)] == [
        "schema_revision",
        "schema_digest",
        "protocol_version",
        "build_id",
        "config_hash",
        "capability_manifest",
    ]
    assert not hasattr(contract, "__dict__")
    with pytest.raises(FrozenInstanceError):
        contract.build_id = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_revision", "20260713_0005"),
        ("schema_digest", "A" * 64),
        ("protocol_version", True),
        ("protocol_version", 0),
        ("protocol_version", 2**63),
        ("build_id", " build-1"),
        ("build_id", "x" * 129),
        ("config_hash", "a" * 63),
        ("capability_manifest", object()),
    ],
)
def test_runtime_contract_rejects_invalid_or_mismatched_fields(
    field: str,
    value: object,
) -> None:
    policy_hash = canonical_policy_manifest(_snapshot()).hash
    values: dict[str, object] = {
        "schema_revision": "20260716_0006",
        "schema_digest": _HASH,
        "protocol_version": 1,
        "build_id": "build-1",
        "config_hash": _OTHER_HASH,
        "capability_manifest": _capability(policy_hash),
    }
    values[field] = value

    with pytest.raises(ValueError):
        RuntimeContract(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "manifest_change",
    [
        {"schema_digest": "e" * 64},
        {"protocol_version": 2},
        {"minimum_build_id": "build-2"},
        {"config_hash": "e" * 64},
        {"stage": RuntimeCapabilityStage.PHASE3_APPROVAL_SEND},
    ],
)
def test_runtime_contract_rejects_capability_drift(
    manifest_change: dict[str, object],
) -> None:
    policy_hash = canonical_policy_manifest(_snapshot()).hash
    manifest_values = {
        field.name: getattr(_capability(policy_hash), field.name)
        for field in fields(RuntimeCapabilityManifest)
    }
    manifest_values.update(manifest_change)
    if manifest_change.get("stage") is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND:
        manifest_values["predecessor_hash"] = "e" * 64

    with pytest.raises(ValueError):
        RuntimeContract(
            schema_revision="20260716_0006",
            schema_digest=_HASH,
            protocol_version=1,
            build_id="build-1",
            config_hash=_OTHER_HASH,
            capability_manifest=RuntimeCapabilityManifest(**manifest_values),  # type: ignore[arg-type]
        )


def test_policy_manifest_is_canonical_immutable_and_order_independent() -> None:
    snapshot = _snapshot()
    reversed_snapshot = PolicySnapshot(scopes=tuple(reversed(snapshot.scopes)))

    first = canonical_policy_manifest(snapshot)
    second = canonical_policy_manifest(reversed_snapshot)

    assert type(first) is PolicyManifest
    assert first == second
    assert first.hash == second.hash
    assert first.canonical_json == second.canonical_json
    assert "inbox-id" in first.canonical_json
    assert "NewMailEvent" in first.canonical_json
    assert not hasattr(first, "__dict__")


@pytest.mark.parametrize(("webhook_count", "accepted"), [(64, True), (65, False)])
def test_policy_manifest_enforces_database_webhook_id_count_boundary(
    webhook_count: int,
    accepted: bool,
) -> None:
    snapshot = _snapshot_with_webhook_ids(
        tuple(f"folder-{index:02d}" for index in range(webhook_count))
    )

    if accepted:
        assert canonical_policy_manifest(snapshot).scope_count == 1
    else:
        with pytest.raises(ValueError, match="webhook_ids"):
            canonical_policy_manifest(snapshot)


@pytest.mark.parametrize(("encoded_size", "accepted"), [(32768, True), (32769, False)])
def test_policy_manifest_enforces_exact_database_webhook_json_byte_boundary(
    encoded_size: int,
    accepted: bool,
) -> None:
    webhook_ids = _webhook_ids_at_database_json_size(encoded_size)
    snapshot = _snapshot_with_webhook_ids(webhook_ids)

    assert _database_webhook_ids_json_size(webhook_ids) == encoded_size
    if accepted:
        assert canonical_policy_manifest(snapshot).scope_count == 1
    else:
        with pytest.raises(ValueError, match="webhook_ids"):
            canonical_policy_manifest(snapshot)


@pytest.mark.parametrize(("webhook_count", "accepted"), [(21, True), (22, False)])
def test_policy_manifest_measures_webhook_json_as_utf8_bytes(
    webhook_count: int,
    accepted: bool,
) -> None:
    webhook_ids = tuple(f"{index:02d}" + "界" * 510 for index in range(webhook_count))
    canonical_text = json.dumps(
        sorted(webhook_ids),
        ensure_ascii=False,
        separators=(", ", ": "),
        allow_nan=False,
    )
    snapshot = _snapshot_with_webhook_ids(webhook_ids)

    assert len(canonical_text) < 32768
    assert (len(canonical_text.encode("utf-8")) <= 32768) is accepted
    if accepted:
        assert canonical_policy_manifest(snapshot).scope_count == 1
    else:
        with pytest.raises(ValueError, match="webhook_ids"):
            canonical_policy_manifest(snapshot)


@pytest.mark.parametrize(
    "snapshot",
    [
        PolicySnapshot(scopes=()),
        PolicySnapshot.failed(),
        object(),
    ],
)
def test_policy_manifest_rejects_empty_unready_or_wrong_snapshot(
    snapshot: object,
) -> None:
    with pytest.raises(ValueError, match="policy"):
        canonical_policy_manifest(snapshot)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "tamper",
    [
        "non_iterable_scopes",
        "wrong_scope_type",
        "invalid_scope_state",
        "invalid_snapshot_state",
    ],
)
def test_policy_manifest_revalidates_all_post_construction_snapshot_state(
    tamper: str,
) -> None:
    snapshot = _snapshot()
    if tamper == "non_iterable_scopes":
        object.__setattr__(snapshot, "scopes", None)
    elif tamper == "wrong_scope_type":
        object.__setattr__(snapshot, "scopes", (object(),))
    elif tamper == "invalid_scope_state":
        object.__setattr__(snapshot.scopes[0], "webhook_ids", ())
    else:
        object.__setattr__(snapshot, "refreshed", 1)

    with pytest.raises(ValueError, match="policy"):
        canonical_policy_manifest(snapshot)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "webhook_ids",
    [
        tuple(f"folder-{index:02d}" for index in range(65)),
        _webhook_ids_at_database_json_size(32769),
        tuple(f"{index:02d}" + "界" * 510 for index in range(22)),
    ],
)
async def test_initializer_rejects_database_invalid_webhook_ids_before_pool_access(
    webhook_ids: tuple[str, ...],
) -> None:
    pool = _NeverPool()

    with pytest.raises(ValueError, match="webhook_ids"):
        await GreenfieldInitializer(pool).initialize(
            8,
            _contract(),
            _snapshot_with_webhook_ids(webhook_ids),
            "operator-1",
            "new system initialization",
            "initialize-1",
        )

    assert pool.connection_calls == 0


@pytest.mark.asyncio
async def test_initializer_revalidates_post_construction_policy_tampering_before_io() -> (
    None
):
    snapshot = _snapshot()
    oversized_scope = _snapshot_with_webhook_ids(
        tuple(f"folder-{index:02d}" for index in range(65))
    ).scopes[0]
    object.__setattr__(snapshot, "scopes", (oversized_scope,))
    pool = _NeverPool()

    with pytest.raises(ValueError, match="webhook_ids"):
        await GreenfieldInitializer(pool).initialize(
            8,
            _contract(),
            snapshot,
            "operator-1",
            "new system initialization",
            "initialize-1",
        )

    assert pool.connection_calls == 0


def test_runtime_authority_represents_active_but_phase2_never_authorizes_it() -> None:
    ingest = _authority()
    paused = _authority(RuntimeAuthorityState.PAUSED)
    active = _authority(RuntimeAuthorityState.ACTIVE)

    assert require_phase2_ingress_authority(ingest) is ingest
    with pytest.raises(RuntimeError, match="runtime_authority_unavailable"):
        require_phase2_ingress_authority(paused)
    with pytest.raises(RuntimeError, match="runtime_authority_unavailable"):
        require_phase2_ingress_authority(active)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("account_id", True),
        ("generation", 2),
        ("fencing_token", 2),
        ("pipeline_name", "legacy_compat"),
        ("authority_epoch", 0),
        ("version", 0),
        ("version", POSTGRES_BIGINT_MAX),
        ("capability_hash", "A" * 64),
        ("initialization_id", "not-a-uuid"),
        ("updated_at", datetime(2026, 7, 16)),
    ],
)
def test_runtime_authority_rejects_non_greenfield_or_invalid_stamp(
    field: str,
    value: object,
) -> None:
    values = {
        item.name: getattr(_authority(), item.name) for item in fields(RuntimeAuthority)
    }
    values[field] = value
    with pytest.raises(ValueError):
        RuntimeAuthority(**values)  # type: ignore[arg-type]


def test_runtime_instance_lease_binds_exact_authority_and_session() -> None:
    authority = _authority()
    lease = RuntimeInstanceLease(
        account_id=authority.account_id,
        workload=RuntimeWorkload.WEB,
        instance_id="web-1",
        session_id=_SESSION,
        generation=authority.generation,
        fencing_token=authority.fencing_token,
        authority_epoch=authority.authority_epoch,
        capability_hash=authority.capability_hash,
        schema_revision=authority.schema_revision,
        protocol_version=authority.protocol_version,
        build_id=authority.build_id,
        config_hash=authority.config_hash,
        lifecycle=RuntimeInstanceLifecycle.ACTIVE,
        lease_version=1,
        accepted_count=0,
        rejected_count=0,
        heartbeat_at=_NOW,
        lease_until=_NOW + timedelta(seconds=30),
    )

    assert lease.session_id == _SESSION
    assert lease.workload is RuntimeWorkload.WEB
    assert not hasattr(lease, "__dict__")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("workload", "worker"),
        ("instance_id", " web-1"),
        ("session_id", "bad"),
        ("lease_version", 0),
        ("lease_version", POSTGRES_BIGINT_MAX),
        ("accepted_count", -1),
        ("accepted_count", POSTGRES_BIGINT_MAX),
        ("rejected_count", POSTGRES_BIGINT_MAX),
        ("rejected_count", 2**63),
        ("heartbeat_at", datetime(2026, 7, 16)),
        ("lease_until", _NOW),
    ],
)
def test_phase2_instance_lease_rejects_worker_or_invalid_session_stamp(
    field: str,
    value: object,
) -> None:
    authority = _authority()
    values: dict[str, object] = {
        "account_id": authority.account_id,
        "workload": RuntimeWorkload.WEB,
        "instance_id": "web-1",
        "session_id": _SESSION,
        "generation": authority.generation,
        "fencing_token": authority.fencing_token,
        "authority_epoch": authority.authority_epoch,
        "capability_hash": authority.capability_hash,
        "schema_revision": authority.schema_revision,
        "protocol_version": authority.protocol_version,
        "build_id": authority.build_id,
        "config_hash": authority.config_hash,
        "lifecycle": RuntimeInstanceLifecycle.ACTIVE,
        "lease_version": 1,
        "accepted_count": 0,
        "rejected_count": 0,
        "heartbeat_at": _NOW,
        "lease_until": _NOW + timedelta(seconds=30),
    }
    values[field] = value

    with pytest.raises(ValueError):
        RuntimeInstanceLease(**values)  # type: ignore[arg-type]


def test_initialization_receipt_is_exact_frozen_greenfield_identity() -> None:
    contract = _contract()
    receipt = InitializationReceipt(
        initialization_id=_INITIALIZATION,
        command_receipt_id=_RECEIPT,
        account_id=8,
        generation=1,
        fencing_token=1,
        pipeline_name=GREENFIELD_PIPELINE_NAME,
        authority_epoch=1,
        authority_version=1,
        capability_hash=contract.capability_manifest.capability_hash,
        policy_manifest_hash=contract.capability_manifest.policy_manifest_hash,
        transaction_id="12345",
        replayed=False,
        created_at=_NOW,
    )

    assert UUID(receipt.initialization_id).version == 4
    assert receipt.transaction_id == "12345"
    with pytest.raises(FrozenInstanceError):
        receipt.replayed = True  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generation", 2),
        ("fencing_token", 2),
        ("pipeline_name", "legacy_compat"),
        ("authority_epoch", 2),
        ("authority_version", 0),
        ("transaction_id", "not-digits"),
        ("transaction_id", "0"),
        ("transaction_id", "0123"),
        ("transaction_id", "1" * 21),
        ("replayed", 0),
        ("created_at", datetime(2026, 7, 16)),
    ],
)
def test_initialization_receipt_rejects_non_greenfield_fact(
    field: str,
    value: object,
) -> None:
    contract = _contract()
    values: dict[str, object] = {
        "initialization_id": _INITIALIZATION,
        "command_receipt_id": _RECEIPT,
        "account_id": 8,
        "generation": 1,
        "fencing_token": 1,
        "pipeline_name": GREENFIELD_PIPELINE_NAME,
        "authority_epoch": 1,
        "authority_version": 1,
        "capability_hash": contract.capability_manifest.capability_hash,
        "policy_manifest_hash": contract.capability_manifest.policy_manifest_hash,
        "transaction_id": "12345",
        "replayed": False,
        "created_at": _NOW,
    }
    values[field] = value
    with pytest.raises(ValueError):
        InitializationReceipt(**values)  # type: ignore[arg-type]


def test_initialization_payload_is_stable_and_binds_all_semantic_inputs() -> None:
    policy = canonical_policy_manifest(_snapshot())
    contract = _contract(policy.hash)

    canonical, digest = canonical_initialization_payload(
        account_id=8,
        runtime_contract=contract,
        policy_manifest=policy,
        actor="operator-1",
        reason="new system initialization",
    )
    repeated = canonical_initialization_payload(
        account_id=8,
        runtime_contract=contract,
        policy_manifest=policy,
        actor="operator-1",
        reason="new system initialization",
    )
    expected = json.dumps(
        {
            "account_id": 8,
            "actor": "operator-1",
            "capability_hash": contract.capability_manifest.capability_hash,
            "pipeline_name": GREENFIELD_PIPELINE_NAME,
            "policy_manifest": json.loads(policy.canonical_json),
            "policy_manifest_hash": policy.hash,
            "reason": "new system initialization",
            "runtime_contract": {
                "build_id": contract.build_id,
                "config_hash": contract.config_hash,
                "protocol_version": contract.protocol_version,
                "schema_digest": contract.schema_digest,
                "schema_revision": contract.schema_revision,
            },
            "schema_version": 1,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    assert (canonical, digest) == repeated
    assert canonical == expected
    assert contract.capability_manifest.capability_hash in canonical
    assert policy.hash in canonical
    assert len(digest) == 64

    _, changed = canonical_initialization_payload(
        account_id=8,
        runtime_contract=contract,
        policy_manifest=policy,
        actor="operator-1",
        reason="changed reason",
    )
    assert changed != digest


def test_authority_transition_receipt_is_frozen_nested_and_exact() -> None:
    previous = _authority()
    current = replace(
        previous,
        state=RuntimeAuthorityState.PAUSED,
        authority_epoch=2,
        version=2,
        updated_at=_NOW + timedelta(seconds=1),
    )
    receipt = AuthorityTransitionReceipt(
        command_receipt_id=_RECEIPT,
        command_name="runtime.pause",
        previous_state=RuntimeAuthorityState.INGEST_ONLY,
        previous_authority_epoch=1,
        previous_version=1,
        transaction_id="12345",
        replayed=False,
        created_at=current.updated_at,
        authority=current,
    )

    assert receipt.authority is current
    assert [field.name for field in fields(receipt)][-1] == "authority"
    assert not hasattr(receipt, "__dict__")
    with pytest.raises(FrozenInstanceError):
        receipt.replayed = True  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("command_receipt_id", "bad"),
        ("command_name", "activate_runtime"),
        ("previous_state", RuntimeAuthorityState.ACTIVE),
        ("previous_authority_epoch", 0),
        ("previous_version", True),
        ("transaction_id", "0"),
        ("transaction_id", "0123"),
        ("transaction_id", "1" * 21),
        ("replayed", 0),
        ("created_at", datetime(2026, 7, 16)),
        ("authority", _authority(RuntimeAuthorityState.ACTIVE)),
    ],
)
def test_authority_transition_receipt_rejects_invalid_or_active_contract(
    field: str,
    value: object,
) -> None:
    previous = _authority()
    current = replace(
        previous,
        state=RuntimeAuthorityState.PAUSED,
        authority_epoch=2,
        version=2,
        updated_at=_NOW + timedelta(seconds=1),
    )
    values: dict[str, object] = {
        "command_receipt_id": _RECEIPT,
        "command_name": "runtime.pause",
        "previous_state": RuntimeAuthorityState.INGEST_ONLY,
        "previous_authority_epoch": 1,
        "previous_version": 1,
        "transaction_id": "12345",
        "replayed": False,
        "created_at": current.updated_at,
        "authority": current,
    }
    values[field] = value

    with pytest.raises(ValueError):
        AuthorityTransitionReceipt(**values)  # type: ignore[arg-type]


def test_authority_transition_payload_is_exact_domain_separated_command_semantics() -> (
    None
):
    authority = _authority()

    canonical, digest = canonical_authority_transition_payload(
        authority=authority,
        target_state=RuntimeAuthorityState.PAUSED,
        actor="operator-1",
        reason="maintenance window",
    )
    expected = json.dumps(
        {
            "account_id": authority.account_id,
            "actor": "operator-1",
            "build_id": authority.build_id,
            "capability_hash": authority.capability_hash,
            "command_name": "runtime.pause",
            "config_hash": authority.config_hash,
            "expected_authority_epoch": authority.authority_epoch,
            "expected_version": authority.version,
            "fencing_token": authority.fencing_token,
            "generation": authority.generation,
            "initialization_id": authority.initialization_id,
            "pipeline_name": authority.pipeline_name,
            "policy_manifest_hash": authority.policy_manifest_hash,
            "previous_state": authority.state.value,
            "protocol_version": authority.protocol_version,
            "reason": "maintenance window",
            "schema_revision": authority.schema_revision,
            "schema_version": 1,
            "target_state": RuntimeAuthorityState.PAUSED.value,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    assert canonical == expected
    assert (
        digest
        == hashlib.sha256(
            b"ai-exchange-runtime-authority-transition-v1\x00"
            + expected.encode("ascii")
        ).hexdigest()
    )
    with pytest.raises(ValueError, match="transition"):
        canonical_authority_transition_payload(
            authority=authority,
            target_state=RuntimeAuthorityState.ACTIVE,
            actor="operator-1",
            reason="not available in Phase 2",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mapping", [False, True])
async def test_initializer_calls_only_fixed_function_and_returns_after_commit(
    mapping: bool,
) -> None:
    policy = canonical_policy_manifest(_snapshot())
    contract = _contract(policy.hash)
    _, payload_hash = canonical_initialization_payload(
        account_id=8,
        runtime_contract=contract,
        policy_manifest=policy,
        actor="operator-1",
        reason="new system initialization",
    )
    pool = _Pool(_initialization_row(mapping=mapping))

    receipt = await GreenfieldInitializer(pool).initialize(
        8,
        contract,
        _snapshot(),
        "operator-1",
        "new system initialization",
        "initialize-1",
    )

    capability = contract.capability_manifest
    assert receipt.initialization_id == _INITIALIZATION
    assert pool.connection_value.calls == [
        (
            _INITIALIZE_SQL,
            (
                8,
                capability.capability_hash,
                capability.predecessor_hash,
                capability.stage.value,
                contract.schema_revision,
                contract.schema_digest,
                contract.protocol_version,
                contract.build_id,
                contract.config_hash,
                capability.adapter_hash,
                policy.hash,
                capability.evidence_manifest_hash,
                policy.canonical_json,
                policy.scope_count,
                "operator-1",
                "new system initialization",
                "initialize-1",
                payload_hash,
            ),
        )
    ]
    assert pool.events[-2:] == ["transaction.exit", "connection.exit"]


@pytest.mark.asyncio
async def test_initializer_accepts_database_uuid_objects_but_not_extra_columns() -> (
    None
):
    row = dict(_initialization_values())
    row["initialization_id"] = UUID(_INITIALIZATION)
    row["command_receipt_id"] = UUID(_RECEIPT)

    receipt = await GreenfieldInitializer(_Pool(MappingProxyType(row))).initialize(
        8,
        _contract(),
        _snapshot(),
        "operator-1",
        "new system initialization",
        "initialize-1",
    )

    assert receipt.command_receipt_id == _RECEIPT
    row["unexpected"] = "widened"
    with pytest.raises(RuntimeError, match="initialization receipt row is invalid"):
        await GreenfieldInitializer(_Pool(row)).initialize(
            8,
            _contract(),
            _snapshot(),
            "operator-1",
            "new system initialization",
            "initialize-1",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        None,
        (),
        _initialization_row()[:-1],  # type: ignore[index]
        {"account_id": 8},
        [*_initialization_row()],  # type: ignore[arg-type]
        object(),
    ],
)
async def test_initializer_rejects_missing_or_nonexact_database_rows(
    row: object,
) -> None:
    with pytest.raises(RuntimeError, match="initialization receipt row is invalid"):
        await GreenfieldInitializer(_Pool(row)).initialize(
            8,
            _contract(),
            _snapshot(),
            "operator-1",
            "new system initialization",
            "initialize-1",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"account_id": 9},
        {"authority_version": 2},
        {"capability_hash": "f" * 64},
        {"policy_manifest_hash": "f" * 64},
    ],
)
async def test_initializer_rejects_receipt_that_does_not_match_command(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(RuntimeError, match="initialization receipt does not match"):
        await GreenfieldInitializer(_Pool(_initialization_row(**overrides))).initialize(
            8,
            _contract(),
            _snapshot(),
            "operator-1",
            "new system initialization",
            "initialize-1",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args",
    [
        (True, _contract(), _snapshot(), "actor", "reason", "key"),
        (8, object(), _snapshot(), "actor", "reason", "key"),
        (8, _contract(), PolicySnapshot.failed(), "actor", "reason", "key"),
        (8, _contract(), _snapshot(), " actor", "reason", "key"),
        (8, _contract(), _snapshot(), "act\tor", "reason", "key"),
        (8, _contract(), _snapshot(), "actor", "bad\nreason", "key"),
        (8, _contract(), _snapshot(), "actor", "reason", ""),
    ],
)
async def test_initializer_rejects_invalid_inputs_before_pool_access(
    args: tuple[object, ...],
) -> None:
    pool = _NeverPool()

    with pytest.raises(ValueError):
        await GreenfieldInitializer(pool).initialize(*args)  # type: ignore[arg-type]

    assert pool.connection_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mapping", [False, True])
@pytest.mark.parametrize(
    "authority",
    [_authority(), _authority(RuntimeAuthorityState.PAUSED)],
)
async def test_authority_repository_get_parses_only_phase2_states(
    authority: RuntimeAuthority,
    mapping: bool,
) -> None:
    pool = _Pool(_authority_row(authority, mapping=mapping))

    result = await RuntimeAuthorityRepository(pool).get(authority.account_id)

    assert result == authority
    assert pool.connection_value.calls == [
        (_GET_AUTHORITY_SQL, (authority.account_id,))
    ]
    assert "transaction.create" not in pool.events


@pytest.mark.asyncio
async def test_authority_repository_get_returns_none_only_for_no_row() -> None:
    assert await RuntimeAuthorityRepository(_Pool(None)).get(8) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        (),
        _authority_row()[:-1],  # type: ignore[index]
        {"account_id": 8},
        {**_authority_values(), "unexpected": "widened"},
        _authority_row(_authority(RuntimeAuthorityState.ACTIVE)),
        _authority_row(account_id=9),
        object(),
    ],
)
async def test_authority_repository_get_rejects_invalid_active_or_cross_account_row(
    row: object,
) -> None:
    with pytest.raises(RuntimeError, match="runtime authority row is invalid"):
        await RuntimeAuthorityRepository(_Pool(row)).get(8)


@pytest.mark.asyncio
@pytest.mark.parametrize("account_id", [True, 0, 2**63])
async def test_authority_repository_get_validates_account_before_io(
    account_id: object,
) -> None:
    pool = _NeverPool()

    with pytest.raises(ValueError, match="account_id"):
        await RuntimeAuthorityRepository(pool).get(account_id)  # type: ignore[arg-type]

    assert pool.connection_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "previous", "target", "command_name", "expected_sql"),
    [
        (
            "pause",
            _authority(),
            RuntimeAuthorityState.PAUSED,
            "runtime.pause",
            _PAUSE_SQL,
        ),
        (
            "resume_ingress",
            _authority(RuntimeAuthorityState.PAUSED),
            RuntimeAuthorityState.INGEST_ONLY,
            "runtime.resume_ingress",
            _RESUME_SQL,
        ),
    ],
)
@pytest.mark.parametrize("mapping", [False, True])
async def test_authority_transitions_use_fixed_cas_function_and_commit_boundary(
    method_name: str,
    previous: RuntimeAuthority,
    target: RuntimeAuthorityState,
    command_name: str,
    expected_sql: str,
    mapping: bool,
) -> None:
    current = _transitioned_authority(previous, target)
    pool = _Pool(
        _transition_row(
            previous,
            current,
            command_name=command_name,
            mapping=mapping,
        )
    )
    repository = RuntimeAuthorityRepository(pool)
    _, payload_hash = canonical_authority_transition_payload(
        authority=previous,
        target_state=target,
        actor="operator-1",
        reason="maintenance window",
    )

    receipt = await getattr(repository, method_name)(
        previous,
        actor="operator-1",
        reason="maintenance window",
        idempotency_key="transition-1",
    )

    assert receipt.authority == current
    assert pool.connection_value.calls == [
        (
            expected_sql,
            (
                previous.account_id,
                previous.authority_epoch,
                previous.version,
                previous.capability_hash,
                "operator-1",
                "maintenance window",
                "transition-1",
                payload_hash,
            ),
        )
    ]
    assert pool.events[-2:] == ["transaction.exit", "connection.exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "previous", "target", "command_name", "overrides"),
    [
        (
            "pause",
            _authority(),
            RuntimeAuthorityState.PAUSED,
            "runtime.pause",
            {"command_name": "runtime.resume_ingress"},
        ),
        (
            "pause",
            _authority(),
            RuntimeAuthorityState.PAUSED,
            "runtime.pause",
            {"previous_authority_epoch": 2},
        ),
        (
            "pause",
            _authority(),
            RuntimeAuthorityState.PAUSED,
            "runtime.pause",
            {"authority_epoch": 3},
        ),
        (
            "pause",
            _authority(),
            RuntimeAuthorityState.PAUSED,
            "runtime.pause",
            {"receipt_created_at": _NOW + timedelta(seconds=2)},
        ),
        (
            "resume_ingress",
            _authority(RuntimeAuthorityState.PAUSED),
            RuntimeAuthorityState.INGEST_ONLY,
            "runtime.resume_ingress",
            {"state": RuntimeAuthorityState.ACTIVE.value},
        ),
        (
            "resume_ingress",
            _authority(RuntimeAuthorityState.PAUSED),
            RuntimeAuthorityState.INGEST_ONLY,
            "runtime.resume_ingress",
            {"account_id": 9},
        ),
    ],
)
async def test_authority_transition_rejects_mismatched_receipt(
    method_name: str,
    previous: RuntimeAuthority,
    target: RuntimeAuthorityState,
    command_name: str,
    overrides: dict[str, object],
) -> None:
    current = _transitioned_authority(previous, target)
    row_overrides = dict(overrides)
    row_command_name = row_overrides.pop("command_name", command_name)
    row = _transition_row(
        previous,
        current,
        command_name=row_command_name,
        **row_overrides,
    )

    with pytest.raises(RuntimeError, match="transition receipt"):
        await getattr(RuntimeAuthorityRepository(_Pool(row)), method_name)(
            previous,
            actor="operator-1",
            reason="maintenance window",
            idempotency_key="transition-1",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "authority"),
    [
        ("pause", _authority(RuntimeAuthorityState.PAUSED)),
        ("pause", _authority(RuntimeAuthorityState.ACTIVE)),
        ("resume_ingress", _authority()),
        ("resume_ingress", _authority(RuntimeAuthorityState.ACTIVE)),
        (
            "pause",
            replace(_authority(), authority_epoch=POSTGRES_BIGINT_MAX - 1),
        ),
    ],
)
async def test_authority_transition_rejects_wrong_state_or_exhausted_cas_before_io(
    method_name: str,
    authority: RuntimeAuthority,
) -> None:
    pool = _NeverPool()

    with pytest.raises((ValueError, RuntimeError)):
        await getattr(RuntimeAuthorityRepository(pool), method_name)(
            authority,
            actor="operator-1",
            reason="maintenance window",
            idempotency_key="transition-1",
        )

    assert pool.connection_calls == 0


@pytest.mark.asyncio
async def test_authority_transition_no_row_is_safe_failure() -> None:
    with pytest.raises(RuntimeError, match="transition receipt row is invalid"):
        await RuntimeAuthorityRepository(_Pool(None)).pause(
            _authority(),
            actor="operator-1",
            reason="maintenance window",
            idempotency_key="transition-1",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("mapping", [False, True])
async def test_instance_register_binds_web_session_to_authority_and_contract(
    mapping: bool,
) -> None:
    authority = _authority()
    contract = _contract()
    lease = _instance_lease(authority=authority)
    pool = _Pool(_instance_row(lease, mapping=mapping))

    result = await RuntimeInstanceRepository(pool).register(
        authority,
        contract,
        "web-1",
        _SESSION,
        30,
    )

    assert result == lease
    assert pool.connection_value.calls == [
        (
            _REGISTER_SQL,
            (
                authority.account_id,
                "web-1",
                _SESSION,
                authority.authority_epoch,
                authority.version,
                contract.schema_revision,
                contract.protocol_version,
                contract.build_id,
                contract.config_hash,
                contract.capability_manifest.capability_hash,
                30,
            ),
        )
    ]
    assert pool.events[-2:] == ["transaction.exit", "connection.exit"]


@pytest.mark.asyncio
async def test_instance_heartbeat_uses_only_session_cas_and_exact_counters() -> None:
    previous = _instance_lease()
    current = replace(
        previous,
        lease_version=2,
        accepted_count=2,
        rejected_count=1,
        heartbeat_at=_NOW + timedelta(seconds=1),
        lease_until=_NOW + timedelta(seconds=31),
    )
    pool = _Pool(_instance_row(current))

    result = await RuntimeInstanceRepository(pool).heartbeat(previous, 2, 1, 30)

    assert result == current
    assert pool.connection_value.calls == [
        (
            _HEARTBEAT_SQL,
            (
                previous.account_id,
                previous.session_id,
                previous.lease_version,
                previous.authority_epoch,
                previous.capability_hash,
                2,
                1,
                30,
            ),
        )
    ]
    assert pool.events[-2:] == ["transaction.exit", "connection.exit"]


@pytest.mark.asyncio
async def test_instance_heartbeat_allows_last_storable_counter_values() -> None:
    previous = _instance_lease()
    current = replace(
        previous,
        lease_version=2,
        accepted_count=POSTGRES_BIGINT_MAX - 1,
        rejected_count=POSTGRES_BIGINT_MAX - 1,
        heartbeat_at=_NOW + timedelta(seconds=1),
        lease_until=_NOW + timedelta(seconds=31),
    )
    pool = _Pool(_instance_row(current))

    result = await RuntimeInstanceRepository(pool).heartbeat(
        previous,
        POSTGRES_BIGINT_MAX - 1,
        POSTGRES_BIGINT_MAX - 1,
        30,
    )

    assert result == current
    assert pool.connection_value.calls == [
        (
            _HEARTBEAT_SQL,
            (
                previous.account_id,
                previous.session_id,
                previous.lease_version,
                previous.authority_epoch,
                previous.capability_hash,
                POSTGRES_BIGINT_MAX - 1,
                POSTGRES_BIGINT_MAX - 1,
                30,
            ),
        )
    ]


@pytest.mark.asyncio
async def test_instance_drain_uses_only_session_cas_and_preserves_counters() -> None:
    previous = _instance_lease(accepted_count=2, rejected_count=1)
    current = replace(
        previous,
        lifecycle=RuntimeInstanceLifecycle.DRAINING,
        lease_version=2,
        heartbeat_at=_NOW + timedelta(seconds=1),
        lease_until=_NOW + timedelta(seconds=31),
    )
    pool = _Pool(_instance_row(current, mapping=True))

    result = await RuntimeInstanceRepository(pool).drain(previous)

    assert result == current
    assert pool.connection_value.calls == [
        (
            _DRAIN_SQL,
            (
                previous.account_id,
                previous.session_id,
                previous.lease_version,
                previous.authority_epoch,
                previous.capability_hash,
            ),
        )
    ]
    assert pool.events[-2:] == ["transaction.exit", "connection.exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "row"),
    [
        ("register", None),
        ("register", ()),
        ("register", _instance_row()[:-1]),  # type: ignore[index]
        ("register", {"account_id": 8}),
        ("register", {**_instance_values(), "unexpected": "widened"}),
        ("register", _instance_row(workload=RuntimeWorkload.WORKER.value)),
        ("register", _instance_row(session_id="bad")),
        (
            "register",
            _instance_row(lease_until=_NOW + timedelta(seconds=31)),
        ),
        (
            "heartbeat",
            _instance_row(
                replace(
                    _instance_lease(),
                    lease_version=2,
                    accepted_count=2,
                    rejected_count=1,
                    heartbeat_at=_NOW + timedelta(seconds=1),
                    lease_until=_NOW + timedelta(seconds=31),
                ),
                instance_id="other-web",
            ),
        ),
        (
            "heartbeat",
            _instance_row(
                replace(
                    _instance_lease(),
                    lease_version=3,
                    accepted_count=2,
                    rejected_count=1,
                    heartbeat_at=_NOW + timedelta(seconds=1),
                    lease_until=_NOW + timedelta(seconds=31),
                )
            ),
        ),
        (
            "heartbeat",
            _instance_row(
                replace(
                    _instance_lease(),
                    lease_version=2,
                    accepted_count=2,
                    rejected_count=1,
                    heartbeat_at=_NOW + timedelta(seconds=1),
                    lease_until=_NOW + timedelta(seconds=32),
                )
            ),
        ),
        (
            "drain",
            _instance_row(
                replace(
                    _instance_lease(),
                    lifecycle=RuntimeInstanceLifecycle.DRAINING,
                    lease_version=2,
                    heartbeat_at=_NOW + timedelta(seconds=1),
                    lease_until=_NOW + timedelta(seconds=31),
                ),
                accepted_count=1,
            ),
        ),
    ],
)
async def test_instance_mutations_reject_missing_malformed_or_mismatched_rows(
    operation: str,
    row: object,
) -> None:
    with pytest.raises(RuntimeError, match="instance lease"):
        await _invoke_mutation(operation, _Pool(row))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "args"),
    [
        (
            "register",
            (
                _authority(RuntimeAuthorityState.PAUSED),
                _contract(),
                "web-1",
                _SESSION,
                30,
            ),
        ),
        (
            "register",
            (
                replace(_authority(), capability_hash="f" * 64),
                _contract(),
                "web-1",
                _SESSION,
                30,
            ),
        ),
        ("register", (_authority(), _contract(), " worker", _SESSION, 30)),
        ("register", (_authority(), _contract(), "web-1", "bad", 30)),
        ("register", (_authority(), _contract(), "web-1", _SESSION, True)),
        ("register", (_authority(), _contract(), "web-1", _SESSION, 0)),
        (
            "heartbeat",
            (_instance_lease(lifecycle=RuntimeInstanceLifecycle.DRAINING), 2, 1, 30),
        ),
        ("heartbeat", (_instance_lease(accepted_count=2), 1, 1, 30)),
        ("heartbeat", (_instance_lease(), True, 1, 30)),
        ("heartbeat", (_instance_lease(), 1, True, 30)),
        ("heartbeat", (_instance_lease(), POSTGRES_BIGINT_MAX, 1, 30)),
        ("heartbeat", (_instance_lease(), 1, POSTGRES_BIGINT_MAX, 30)),
        (
            "heartbeat",
            (
                _tampered_instance_lease("accepted_count", POSTGRES_BIGINT_MAX),
                POSTGRES_BIGINT_MAX - 1,
                1,
                30,
            ),
        ),
        (
            "heartbeat",
            (
                _tampered_instance_lease("rejected_count", POSTGRES_BIGINT_MAX),
                1,
                POSTGRES_BIGINT_MAX - 1,
                30,
            ),
        ),
        (
            "heartbeat",
            (_instance_lease(lease_version=POSTGRES_BIGINT_MAX - 1), 2, 1, 30),
        ),
        (
            "drain",
            (_instance_lease(lifecycle=RuntimeInstanceLifecycle.DRAINING),),
        ),
        ("drain", (_instance_lease(lease_version=POSTGRES_BIGINT_MAX - 1),)),
    ],
)
async def test_instance_mutations_reject_non_phase2_or_invalid_cas_before_io(
    operation: str,
    args: tuple[object, ...],
) -> None:
    pool = _NeverPool()

    with pytest.raises((ValueError, RuntimeError)):
        await getattr(RuntimeInstanceRepository(pool), operation)(*args)

    assert pool.connection_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    ["initialize", "pause", "resume_ingress", "register", "heartbeat", "drain"],
)
async def test_every_mutation_propagates_commit_acknowledgement_loss(
    operation: str,
) -> None:
    lost_ack = ConnectionError(f"{operation} commit acknowledgement lost")
    pool = _Pool(
        _successful_mutation_row(operation),
        commit_failure=lost_ack,
    )

    with pytest.raises(ConnectionError, match="commit acknowledgement lost") as caught:
        await _invoke_mutation(operation, pool)

    assert caught.value is lost_ack
    assert pool.events[-2:] == ["transaction.exit", "connection.exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["execute", "fetch", "commit"])
@pytest.mark.parametrize(
    "failure_factory",
    [
        lambda: asyncio.CancelledError("cancelled"),
        lambda: KeyboardInterrupt("interrupt"),
        lambda: SystemExit("exit"),
    ],
    ids=["cancelled", "keyboard-interrupt", "system-exit"],
)
async def test_initializer_process_control_failures_propagate_unchanged(
    stage: str,
    failure_factory,
) -> None:
    failure = failure_factory()
    failures = {
        "execute_failure": failure if stage == "execute" else None,
        "fetch_failure": failure if stage == "fetch" else None,
        "commit_failure": failure if stage == "commit" else None,
    }
    pool = _Pool(_initialization_row(), **failures)

    with pytest.raises(type(failure)) as caught:
        await _invoke_mutation("initialize", pool)

    assert caught.value is failure


def test_phase2_repository_api_does_not_expose_active_or_worker_controls() -> None:
    authority_members = set(dir(RuntimeAuthorityRepository))
    instance_members = set(dir(RuntimeInstanceRepository))

    assert {"activate", "activate_runtime", "resume_active"}.isdisjoint(
        authority_members
    )
    assert {"register_worker", "register_scheduler", "register_reaper"}.isdisjoint(
        instance_members
    )
    assert list(inspect.signature(RuntimeInstanceRepository.register).parameters) == [
        "self",
        "authority",
        "runtime_contract",
        "instance_id",
        "session_id",
        "lease_seconds",
    ]
    assert list(inspect.signature(RuntimeInstanceRepository.heartbeat).parameters) == [
        "self",
        "lease",
        "accepted_count",
        "rejected_count",
        "lease_seconds",
    ]
    assert list(inspect.signature(RuntimeInstanceRepository.drain).parameters) == [
        "self",
        "lease",
    ]


def test_database_boundary_uses_only_fixed_schema_qualified_select_functions() -> None:
    statements = [
        _INITIALIZE_SQL,
        _GET_AUTHORITY_SQL,
        _PAUSE_SQL,
        _RESUME_SQL,
        _REGISTER_SQL,
        _HEARTBEAT_SQL,
        _DRAIN_SQL,
    ]

    for statement in statements:
        assert statement.startswith("SELECT ")
        assert " FROM public.greenfield_" in statement
        assert statement.count("public.greenfield_") == 1
        assert statement.count("%s") in {1, 5, 8, 11, 18}
        assert all(
            token not in statement.upper()
            for token in ("INSERT ", "UPDATE ", "DELETE ", "CALL ", ";")
        )


def test_command_payloads_preserve_unicode_and_hash_exact_utf8_bytes() -> None:
    policy = canonical_policy_manifest(_snapshot())
    initialization, initialization_hash = canonical_initialization_payload(
        account_id=8,
        runtime_contract=_contract(policy.hash),
        policy_manifest=policy,
        actor="操作员",
        reason="从零初始化系统",
    )
    transition, transition_hash = canonical_authority_transition_payload(
        authority=_authority(),
        target_state=RuntimeAuthorityState.PAUSED,
        actor="操作员",
        reason="维护窗口",
    )

    assert "操作员" in initialization
    assert "从零初始化系统" in initialization
    assert (
        initialization_hash
        == hashlib.sha256(
            b"ai-exchange-greenfield-initialize-v1\x00" + initialization.encode("utf-8")
        ).hexdigest()
    )
    assert "操作员" in transition
    assert "维护窗口" in transition
    assert (
        transition_hash
        == hashlib.sha256(
            b"ai-exchange-runtime-authority-transition-v1\x00"
            + transition.encode("utf-8")
        ).hexdigest()
    )


def test_initialization_payload_embeds_unicode_policy_canonical_json_verbatim() -> None:
    policy = canonical_policy_manifest(_unicode_snapshot())

    canonical, digest = canonical_initialization_payload(
        account_id=8,
        runtime_contract=_contract(policy.hash),
        policy_manifest=policy,
        actor="操作员",
        reason="初始化中文文件夹",
    )

    assert "\\u6536\\u4ef6\\u7bb1" in policy.canonical_json
    assert f'"policy_manifest":{policy.canonical_json},' in canonical
    assert (
        digest
        == hashlib.sha256(
            b"ai-exchange-greenfield-initialize-v1\x00" + canonical.encode("utf-8")
        ).hexdigest()
    )


@pytest.mark.asyncio
async def test_database_commands_pass_unicode_text_and_its_utf8_hash_unchanged() -> (
    None
):
    initialization_pool = _Pool(_initialization_row())
    await GreenfieldInitializer(initialization_pool).initialize(
        8,
        _contract(),
        _snapshot(),
        "操作员",
        "从零初始化系统",
        "初始化-1",
    )
    initialization_params = initialization_pool.connection_value.calls[0][1]
    policy = canonical_policy_manifest(_snapshot())
    _, expected_initialization_hash = canonical_initialization_payload(
        account_id=8,
        runtime_contract=_contract(),
        policy_manifest=policy,
        actor="操作员",
        reason="从零初始化系统",
    )

    previous = _authority()
    current = _transitioned_authority(previous, RuntimeAuthorityState.PAUSED)
    transition_pool = _Pool(
        _transition_row(
            previous,
            current,
            command_name="runtime.pause",
        )
    )
    await RuntimeAuthorityRepository(transition_pool).pause(
        previous,
        actor="操作员",
        reason="维护窗口",
        idempotency_key="暂停-1",
    )
    transition_params = transition_pool.connection_value.calls[0][1]
    _, expected_transition_hash = canonical_authority_transition_payload(
        authority=previous,
        target_state=RuntimeAuthorityState.PAUSED,
        actor="操作员",
        reason="维护窗口",
    )

    assert initialization_params[12] == policy.canonical_json
    assert initialization_params[14:] == (
        "操作员",
        "从零初始化系统",
        "初始化-1",
        expected_initialization_hash,
    )
    assert transition_params[4:] == (
        "操作员",
        "维护窗口",
        "暂停-1",
        expected_transition_hash,
    )
