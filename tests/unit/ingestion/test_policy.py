from __future__ import annotations

from collections.abc import Iterator, Mapping
import hashlib
import json
from dataclasses import FrozenInstanceError

import pytest

from src.ingestion.models import ChangeKind, IngressSource, ProcessingPolicy
from src.ingestion.folder_identity import (
    canonicalize_folder_identity,
    require_canonical_folder_identity,
)
from src.ingestion.normalization import (
    normalize_sync_change,
    validate_sync_change_contract,
)
from src.ingestion.policy import (
    FolderScope,
    PolicySnapshot,
    PolicySnapshotUnavailableError,
    ProcessingPolicyResolver,
    require_canonical_folder_key,
)


class _HostileStr(str):
    __hash__ = str.__hash__

    def strip(self, *_args, **_kwargs):
        raise AssertionError("str subclass behavior must not execute")

    def __eq__(self, _other):
        raise AssertionError("str subclass behavior must not execute")


class _FolderScopeSubclass(FolderScope):
    pass


class _PolicySnapshotSubclass(PolicySnapshot):
    @property
    def ready(self) -> bool:
        return True


class _DuplicateItemsMapping(Mapping[object, object]):
    def __init__(self, items: tuple[tuple[object, object], ...]) -> None:
        self._items = items

    def __getitem__(self, key: object) -> object:
        raise KeyError(key)

    def __iter__(self) -> Iterator[object]:
        return iter(())

    def __len__(self) -> int:
        return len(self._items)

    def items(self):
        return self._items


class _HostileScopes:
    def __init__(self) -> None:
        self.iteration_calls = 0

    def __iter__(self):
        self.iteration_calls += 1
        raise AssertionError("scopes were read before snapshot state validation")


def _matrix(
    create_policy: ProcessingPolicy,
    *,
    created_policy: ProcessingPolicy = ProcessingPolicy.IGNORED,
):
    return {
        (IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE): create_policy,
        (
            IngressSource.WEBHOOK,
            "CreatedEvent",
            ChangeKind.CREATE,
        ): created_policy,
        (
            IngressSource.WEBHOOK,
            "ModifiedEvent",
            ChangeKind.UPDATE,
        ): ProcessingPolicy.METADATA_ONLY,
        (
            IngressSource.WEBHOOK,
            "DeletedEvent",
            ChangeKind.DELETE,
        ): ProcessingPolicy.METADATA_ONLY,
        (IngressSource.SYNC, "create", ChangeKind.CREATE): create_policy,
        (
            IngressSource.SYNC,
            "update",
            ChangeKind.UPDATE,
        ): ProcessingPolicy.METADATA_ONLY,
        (
            IngressSource.SYNC,
            "delete",
            ChangeKind.DELETE,
        ): ProcessingPolicy.METADATA_ONLY,
    }


def _scope(
    canonical_key: str,
    webhook_id: str,
    sync_folder: str,
    create_policy: ProcessingPolicy,
) -> FolderScope:
    return FolderScope.configured(
        canonical_key=canonical_key,
        webhook_ids=(webhook_id,),
        sync_folder=sync_folder,
        event_policy_matrix=_matrix(
            create_policy,
            created_policy=(
                ProcessingPolicy.ARCHIVE
                if canonical_key == "SENT"
                else ProcessingPolicy.IGNORED
            ),
        ),
    )


def _expected_config_hash(
    *,
    canonical_key: str,
    webhook_ids: tuple[str, ...],
    sync_folder: str,
    matrix,
) -> str:
    canonical = {
        "schema_version": 1,
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
                matrix.items(),
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


@pytest.mark.parametrize(
    "alias",
    ["inbox", "Inbox", "SentItems", "sent items", "草稿", "已发送邮件"],
)
def test_canonical_folder_key_rejects_standard_aliases_that_normalization_rewrites(
    alias: str,
) -> None:
    with pytest.raises(ValueError, match="canonical"):
        require_canonical_folder_key(alias)


@pytest.mark.parametrize("canonical", ["INBOX", "SENT", "DRAFTS", "Team Box"])
def test_canonical_folder_key_accepts_only_already_normalized_identity(
    canonical: str,
) -> None:
    assert require_canonical_folder_key(canonical) == canonical


@pytest.mark.parametrize(
    ("identity", "expected"),
    [
        ("inbox", "INBOX"),
        ("Inbox", "INBOX"),
        ("SentItems", "SENT"),
        ("sent items", "SENT"),
        ("草稿", "DRAFTS"),
        ("已发送邮件", "SENT"),
        ("Team Box", "Team Box"),
        ("team box", "team box"),
    ],
)
def test_folder_identity_manifest_is_the_normalization_parity_source(
    identity: str,
    expected: str,
) -> None:
    assert canonicalize_folder_identity(identity) == expected
    if identity == expected:
        assert require_canonical_folder_identity(identity) == expected
    else:
        with pytest.raises(ValueError, match="canonical"):
            require_canonical_folder_identity(identity)


def test_folder_scope_builder_rejects_noncanonical_key_before_hashing() -> None:
    with pytest.raises(ValueError, match="canonical_key"):
        FolderScope.configured(
            canonical_key="inbox",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        )


def test_folder_scope_rejects_invalid_utf8_identity_before_hashing() -> None:
    with pytest.raises(ValueError, match="sync_folder must contain valid UTF-8 text"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="\ud800",
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        )


@pytest.mark.parametrize(
    "webhook_ids",
    ("inbox-id", b"inbox-id", None, (), ("duplicate", "duplicate")),
)
def test_folder_scope_freezes_only_nonempty_unique_webhook_id_collections(
    webhook_ids: object,
) -> None:
    with pytest.raises(ValueError, match="webhook_ids"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=webhook_ids,  # type: ignore[arg-type]
            sync_folder="Inbox",
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        )


@pytest.mark.parametrize(
    "event_policy_matrix",
    (
        [],
        {(IngressSource.SYNC, "create"): ProcessingPolicy.FULL},
    ),
)
def test_folder_scope_rejects_nonmapping_and_nontriple_policy_matrix_shapes(
    event_policy_matrix: object,
) -> None:
    with pytest.raises(ValueError, match="event_policy_matrix|three fields"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=event_policy_matrix,  # type: ignore[arg-type]
        )


def test_policy_matrix_rejects_distinct_raw_keys_that_normalize_to_one_key() -> None:
    duplicate_matrix = _DuplicateItemsMapping(
        (
            (("sync", "create", "create"), "full"),
            (
                (IngressSource.SYNC, "create", ChangeKind.CREATE),
                ProcessingPolicy.FULL,
            ),
        )
    )

    with pytest.raises(ValueError, match="keys must be unique"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=duplicate_matrix,  # type: ignore[arg-type]
        )


def test_policy_matrix_converts_exact_raw_policy_strings_before_freezing() -> None:
    raw_matrix = {
        key: value.value for key, value in _matrix(ProcessingPolicy.FULL).items()
    }

    scope = FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=("inbox-id",),
        sync_folder="Inbox",
        event_policy_matrix=raw_matrix,
    )

    assert all(
        type(value) is ProcessingPolicy for value in scope.event_policy_matrix.values()
    )
    assert scope.event_policy_matrix == _matrix(ProcessingPolicy.FULL)


def test_policy_matrix_rejects_unknown_raw_policy_text_before_snapshot_creation() -> (
    None
):
    matrix = _matrix(ProcessingPolicy.FULL)
    matrix[(IngressSource.WEBHOOK, "NewMailEvent", ChangeKind.CREATE)] = "opaque"  # type: ignore[assignment]

    with pytest.raises(ValueError, match="valid ProcessingPolicy"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


def test_folder_scope_direct_constructor_rejects_noncanonical_key() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    with pytest.raises(ValueError, match="canonical_key"):
        FolderScope(
            canonical_key="inbox",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
            config_hash=_expected_config_hash(
                canonical_key="inbox",
                webhook_ids=("inbox-id",),
                sync_folder="Inbox",
                matrix=matrix,
            ),
        )


def test_config_hash_is_versioned_canonical_and_input_order_independent() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    reversed_matrix = dict(reversed(tuple(matrix.items())))

    first = FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=("webhook-b", "webhook-a"),
        sync_folder="Inbox",
        event_policy_matrix=reversed_matrix,
    )
    second = FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=("webhook-a", "webhook-b"),
        sync_folder="Inbox",
        event_policy_matrix=matrix,
    )

    assert first.config_hash == second.config_hash
    assert first.config_hash == _expected_config_hash(
        canonical_key="INBOX",
        webhook_ids=("webhook-a", "webhook-b"),
        sync_folder="Inbox",
        matrix=matrix,
    )


def test_every_folder_scope_semantic_field_changes_config_hash() -> None:
    base = FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=("inbox-id",),
        sync_folder="Inbox",
        event_policy_matrix=_matrix(ProcessingPolicy.FULL),
    )
    changed_matrix = _matrix(ProcessingPolicy.IGNORED)
    variants = (
        FolderScope.configured(
            canonical_key="SECONDARY",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        ),
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("other-id",),
            sync_folder="Inbox",
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        ),
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox-Other",
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        ),
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=changed_matrix,
        ),
    )

    assert all(variant.config_hash != base.config_hash for variant in variants)


def test_direct_constructor_rejects_old_hash_with_changed_matrix() -> None:
    original = FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=("inbox-id",),
        sync_folder="Inbox",
        event_policy_matrix=_matrix(ProcessingPolicy.FULL),
    )

    with pytest.raises(ValueError, match="config_hash does not match"):
        FolderScope(
            canonical_key=original.canonical_key,
            webhook_ids=original.webhook_ids,
            sync_folder=original.sync_folder,
            event_policy_matrix=_matrix(ProcessingPolicy.IGNORED),
            config_hash=original.config_hash,
        )


@pytest.mark.parametrize("shape", ["missing", "extra", "unreachable"])
def test_policy_matrix_requires_the_exact_seven_reachable_keys(shape: str) -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    sync_update = (IngressSource.SYNC, "update", ChangeKind.UPDATE)
    if shape == "missing":
        del matrix[sync_update]
    elif shape == "extra":
        matrix[(IngressSource.WEBHOOK, "UnknownEvent", ChangeKind.CREATE)] = (
            ProcessingPolicy.IGNORED
        )
    else:
        del matrix[sync_update]
        matrix[(IngressSource.SYNC, "create", ChangeKind.UPDATE)] = (
            ProcessingPolicy.METADATA_ONLY
        )

    with pytest.raises(ValueError, match="exact seven"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


@pytest.mark.parametrize(
    "key",
    [
        (IngressSource.WEBHOOK, "ModifiedEvent", ChangeKind.UPDATE),
        (IngressSource.WEBHOOK, "DeletedEvent", ChangeKind.DELETE),
        (IngressSource.SYNC, "update", ChangeKind.UPDATE),
        (IngressSource.SYNC, "delete", ChangeKind.DELETE),
    ],
)
def test_update_and_delete_matrix_entries_are_always_metadata_only(key) -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    matrix[key] = ProcessingPolicy.FULL

    with pytest.raises(ValueError, match="METADATA_ONLY"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


def test_new_mail_and_sync_create_must_have_the_same_policy() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    matrix[(IngressSource.SYNC, "create", ChangeKind.CREATE)] = ProcessingPolicy.ARCHIVE

    with pytest.raises(ValueError, match="equivalent"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


@pytest.mark.parametrize(
    ("canonical_key", "matrix"),
    [
        (
            "SENT",
            _matrix(
                ProcessingPolicy.ARCHIVE,
                created_policy=ProcessingPolicy.IGNORED,
            ),
        ),
        ("DRAFTS", _matrix(ProcessingPolicy.FULL)),
    ],
)
def test_sent_and_drafts_have_fixed_create_policies(
    canonical_key: str,
    matrix,
) -> None:
    with pytest.raises(ValueError, match=canonical_key):
        FolderScope.configured(
            canonical_key=canonical_key,
            webhook_ids=(f"{canonical_key.lower()}-id",),
            sync_folder=canonical_key.title(),
            event_policy_matrix=matrix,
        )


@pytest.mark.parametrize(
    "create_policy",
    [ProcessingPolicy.FULL, ProcessingPolicy.IGNORED],
)
def test_archive_has_fixed_equivalent_create_policy(
    create_policy: ProcessingPolicy,
) -> None:
    with pytest.raises(ValueError, match="ARCHIVE"):
        FolderScope.configured(
            canonical_key="ARCHIVE",
            webhook_ids=("archive-id",),
            sync_folder="Archive",
            event_policy_matrix=_matrix(create_policy),
        )


def test_archive_created_event_is_ignored_not_archived() -> None:
    matrix = _matrix(
        ProcessingPolicy.ARCHIVE,
        created_policy=ProcessingPolicy.ARCHIVE,
    )

    with pytest.raises(ValueError, match="CreatedEvent"):
        FolderScope.configured(
            canonical_key="ARCHIVE",
            webhook_ids=("archive-id",),
            sync_folder="Archive",
            event_policy_matrix=matrix,
        )


def test_created_event_is_ignored_for_every_nonspecial_folder() -> None:
    matrix = _matrix(
        ProcessingPolicy.FULL,
        created_policy=ProcessingPolicy.FULL,
    )

    with pytest.raises(ValueError, match="CreatedEvent must be IGNORED"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


def test_create_policy_cannot_be_metadata_only() -> None:
    with pytest.raises(ValueError, match="create policy"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=_matrix(ProcessingPolicy.METADATA_ONLY),
        )


def test_manifest_rejects_raw_read_flag_change_before_hashing() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    del matrix[(IngressSource.SYNC, "update", ChangeKind.UPDATE)]
    matrix[(IngressSource.SYNC, "read_flag_change", ChangeKind.UPDATE)] = (
        ProcessingPolicy.METADATA_ONLY
    )

    with pytest.raises(ValueError, match="read_flag_change"):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


@pytest.mark.parametrize(
    "field",
    ["canonical_key", "webhook_id", "sync_folder"],
)
def test_folder_scope_identity_rejects_str_subclasses_before_behavior(
    field: str,
) -> None:
    values = {
        "canonical_key": "INBOX",
        "webhook_ids": ("inbox-id",),
        "sync_folder": "Inbox",
    }
    if field == "webhook_id":
        values["webhook_ids"] = (_HostileStr("inbox-id"),)
    else:
        values[field] = _HostileStr(values[field])

    with pytest.raises(ValueError):
        FolderScope.configured(
            **values,
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        )


def test_matrix_raw_event_type_rejects_str_subclass_before_behavior() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    exact_key = (IngressSource.SYNC, "update", ChangeKind.UPDATE)
    policy = matrix.pop(exact_key)
    matrix[(IngressSource.SYNC, _HostileStr("update"), ChangeKind.UPDATE)] = policy

    with pytest.raises(ValueError):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


def test_direct_constructor_rejects_config_hash_str_subclass() -> None:
    scope = _scope("INBOX", "inbox-id", "Inbox", ProcessingPolicy.FULL)

    with pytest.raises(ValueError, match="config_hash"):
        FolderScope(
            canonical_key=scope.canonical_key,
            webhook_ids=scope.webhook_ids,
            sync_folder=scope.sync_folder,
            event_policy_matrix=scope.event_policy_matrix,
            config_hash=_HostileStr(scope.config_hash),
        )


@pytest.mark.parametrize("field", ["source", "kind", "folder"])
def test_resolver_rejects_identity_str_subclasses(
    snapshot: PolicySnapshot,
    field: str,
) -> None:
    values = {
        "source": IngressSource.SYNC,
        "raw_event_type": "create",
        "change_kind": ChangeKind.CREATE,
        "exact_folder_identity": "Inbox",
    }
    if field == "source":
        values["source"] = _HostileStr("sync")
    elif field == "kind":
        values["change_kind"] = _HostileStr("create")
    else:
        values["exact_folder_identity"] = _HostileStr("Inbox")

    with pytest.raises(ValueError):
        ProcessingPolicyResolver().resolve(**values, snapshot=snapshot)


def test_policy_value_rejects_str_subclass_before_enum_conversion() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    matrix[(IngressSource.SYNC, "create", ChangeKind.CREATE)] = _HostileStr("full")

    with pytest.raises(ValueError):
        FolderScope.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
        )


def test_policy_snapshot_accepts_only_exact_folder_scope() -> None:
    scope = _scope("INBOX", "inbox-id", "Inbox", ProcessingPolicy.FULL)
    subclass = _FolderScopeSubclass(
        canonical_key=scope.canonical_key,
        webhook_ids=scope.webhook_ids,
        sync_folder=scope.sync_folder,
        event_policy_matrix=scope.event_policy_matrix,
        config_hash=scope.config_hash,
    )

    with pytest.raises(ValueError, match="only exact FolderScope"):
        PolicySnapshot(scopes=(subclass,))


@pytest.mark.parametrize(
    ("refreshed", "refresh_failed"),
    ((1, False), (True, 0)),
)
def test_policy_snapshot_rejects_nonboolean_state_before_reading_scopes(
    refreshed: object,
    refresh_failed: object,
) -> None:
    scopes = _HostileScopes()

    with pytest.raises(ValueError, match="snapshot state must be boolean"):
        PolicySnapshot(
            scopes=scopes,
            refreshed=refreshed,  # type: ignore[arg-type]
            refresh_failed=refresh_failed,  # type: ignore[arg-type]
        )

    assert scopes.iteration_calls == 0


@pytest.mark.parametrize("scopes", ("INBOX", b"INBOX", None, object()))
def test_policy_snapshot_rejects_scalar_and_noniterable_scope_containers(
    scopes: object,
) -> None:
    with pytest.raises(ValueError, match="scopes must be an iterable"):
        PolicySnapshot(scopes=scopes)  # type: ignore[arg-type]


def test_folder_scope_builder_rejects_subclass_receiver() -> None:
    with pytest.raises(ValueError, match="exact FolderScope"):
        _FolderScopeSubclass.configured(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=_matrix(ProcessingPolicy.FULL),
        )


def test_resolver_accepts_only_exact_policy_snapshot() -> None:
    scope = _scope("INBOX", "inbox-id", "Inbox", ProcessingPolicy.FULL)
    snapshot = _PolicySnapshotSubclass(scopes=(scope,))

    with pytest.raises(PolicySnapshotUnavailableError):
        ProcessingPolicyResolver().configured_scopes(snapshot)


@pytest.fixture
def snapshot() -> PolicySnapshot:
    return PolicySnapshot(
        scopes=(
            _scope("ARCHIVE", "archive-id", "Archive", ProcessingPolicy.ARCHIVE),
            _scope("DRAFTS", "drafts-id", "Drafts", ProcessingPolicy.IGNORED),
            _scope("INBOX", "inbox-id", "Inbox", ProcessingPolicy.FULL),
            _scope("SENT", "sent-id", "Sent Items", ProcessingPolicy.ARCHIVE),
        )
    )


@pytest.mark.parametrize(
    ("source", "raw_type", "kind", "folder", "expected"),
    [
        ("webhook", "NewMailEvent", "create", "inbox-id", "full"),
        ("sync", "create", "create", "Inbox", "full"),
        ("webhook", "CreatedEvent", "create", "sent-id", "archive"),
        ("sync", "create", "create", "Sent Items", "archive"),
        ("webhook", "CreatedEvent", "create", "drafts-id", "ignored"),
        ("sync", "create", "create", "Drafts", "ignored"),
        ("webhook", "NewMailEvent", "create", "archive-id", "archive"),
        ("sync", "create", "create", "Archive", "archive"),
        ("webhook", "ModifiedEvent", "update", "inbox-id", "metadata_only"),
        ("webhook", "DeletedEvent", "delete", "inbox-id", "metadata_only"),
        ("sync", "update", "update", "Inbox", "metadata_only"),
        ("sync", "delete", "delete", "Inbox", "metadata_only"),
    ],
)
def test_policy_matrix_is_equivalent_across_webhook_and_sync(
    snapshot: PolicySnapshot,
    source: str,
    raw_type: str,
    kind: str,
    folder: str,
    expected: str,
) -> None:
    resolver = ProcessingPolicyResolver()

    assert resolver.resolve(source, raw_type, kind, folder, snapshot).value == expected


def test_folder_identity_never_grants_a_policy_missing_from_the_matrix() -> None:
    scope = _scope("INBOX", "inbox-id", "Inbox", ProcessingPolicy.FULL)
    snapshot = PolicySnapshot(scopes=(scope,))

    assert (
        ProcessingPolicyResolver().resolve(
            IngressSource.WEBHOOK,
            "CreatedEvent",
            ChangeKind.CREATE,
            "inbox-id",
            snapshot,
        )
        is ProcessingPolicy.IGNORED
    )


def test_unknown_folder_in_ready_snapshot_is_terminally_ignored(
    snapshot: PolicySnapshot,
) -> None:
    assert (
        ProcessingPolicyResolver().resolve(
            IngressSource.SYNC,
            "create",
            ChangeKind.CREATE,
            "Unknown",
            snapshot,
        )
        is ProcessingPolicy.IGNORED
    )


def test_sync_authenticated_update_with_is_read_uses_metadata_policy(
    snapshot: PolicySnapshot,
) -> None:
    assert (
        ProcessingPolicyResolver().resolve(
            IngressSource.SYNC,
            "update",
            ChangeKind.UPDATE,
            "Inbox",
            snapshot,
        )
        is ProcessingPolicy.METADATA_ONLY
    )


def test_authenticated_v2_is_read_update_normalizes_with_resolved_metadata_policy(
    snapshot: PolicySnapshot,
) -> None:
    change = validate_sync_change_contract(
        {
            "change_type": "update",
            "id": "message-1",
            "item": {
                "id": "message-1",
                "subject": "Authenticated update",
                "sender": "sender@example.com",
                "received_time": "2026-07-15T08:09:10",
                "is_read": True,
                "has_attachments": False,
            },
        }
    )
    resolver = ProcessingPolicyResolver()
    policy = resolver.resolve(
        IngressSource.SYNC,
        change.kind.value,
        change.kind,
        "Inbox",
        snapshot,
    )

    event = normalize_sync_change(
        8,
        "Inbox",
        "opaque+cursor/%3D",
        change,
        processing_policy=policy,
    )

    assert event.raw_event_type == "update"
    assert event.kind is ChangeKind.UPDATE
    assert event.processing_policy is ProcessingPolicy.METADATA_ONLY
    assert event.payload_for_storage()["item"]["is_read"] is True
    assert (
        resolver.resolve(
            event.source,
            event.raw_event_type,
            event.kind,
            "Inbox",
            snapshot,
        )
        is ProcessingPolicy.METADATA_ONLY
    )


@pytest.mark.parametrize(
    ("raw_type", "kind"),
    [("read_flag_change", ChangeKind.UPDATE), ("read_flag_change", ChangeKind.READ)],
)
def test_raw_read_flag_change_is_never_a_policy_event(
    snapshot: PolicySnapshot,
    raw_type: str,
    kind: ChangeKind,
) -> None:
    with pytest.raises(ValueError, match="read_flag_change"):
        ProcessingPolicyResolver().resolve(
            IngressSource.SYNC,
            raw_type,
            kind,
            "Inbox",
            snapshot,
        )


def test_scope_and_matrix_are_immutable() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    webhook_ids = ["inbox-id"]
    scope = FolderScope.configured(
        canonical_key="INBOX",
        webhook_ids=webhook_ids,
        sync_folder="Inbox",
        event_policy_matrix=matrix,
    )
    scopes = [scope]
    snapshot = PolicySnapshot(scopes=scopes)
    webhook_ids.append("forged-id")
    matrix[(IngressSource.SYNC, "create", ChangeKind.CREATE)] = ProcessingPolicy.IGNORED
    scopes.clear()

    assert scope.webhook_ids == frozenset({"inbox-id"})
    assert snapshot.scopes == (scope,)
    assert (
        scope.event_policy_matrix[(IngressSource.SYNC, "create", ChangeKind.CREATE)]
        is ProcessingPolicy.FULL
    )
    with pytest.raises(TypeError):
        scope.event_policy_matrix[(IngressSource.SYNC, "delete", ChangeKind.DELETE)] = (
            ProcessingPolicy.FULL
        )
    with pytest.raises(FrozenInstanceError):
        scope.canonical_key = "OTHER"  # type: ignore[misc]


def test_exact_identities_are_not_trimmed_casefolded_or_unicode_normalized() -> None:
    composed = "Caf\N{LATIN SMALL LETTER E WITH ACUTE}"
    decomposed = "Cafe\N{COMBINING ACUTE ACCENT}"
    scope = FolderScope.configured(
        canonical_key="UNICODE",
        webhook_ids=("Opaque-ID",),
        sync_folder=composed,
        event_policy_matrix=_matrix(ProcessingPolicy.FULL),
    )
    snapshot = PolicySnapshot(scopes=(scope,))
    resolver = ProcessingPolicyResolver()

    assert (
        resolver.resolve("webhook", "NewMailEvent", "create", "opaque-id", snapshot)
        is ProcessingPolicy.IGNORED
    )
    assert (
        resolver.resolve("sync", "create", "create", decomposed, snapshot)
        is ProcessingPolicy.IGNORED
    )
    assert (
        resolver.resolve("sync", "Create", "create", composed, snapshot)
        is ProcessingPolicy.IGNORED
    )
    with pytest.raises(ValueError, match="exact_folder_identity"):
        resolver.resolve("sync", "create", "create", f"{composed} ", snapshot)


@pytest.mark.parametrize(
    ("source", "raw_type", "kind"),
    [
        ("unknown", "create", "create"),
        (IngressSource.SYNC, " create", ChangeKind.CREATE),
        (IngressSource.SYNC, "create", "unknown"),
    ],
)
def test_invalid_resolution_shape_fails_closed_without_echoing_input(
    snapshot: PolicySnapshot,
    source: object,
    raw_type: object,
    kind: object,
) -> None:
    with pytest.raises(ValueError) as caught:
        ProcessingPolicyResolver().resolve(
            source,  # type: ignore[arg-type]
            raw_type,  # type: ignore[arg-type]
            kind,  # type: ignore[arg-type]
            "Inbox",
            snapshot,
        )

    assert "unknown" not in str(caught.value)


def test_historical_suppression_cannot_be_configured_as_event_policy() -> None:
    matrix = _matrix(ProcessingPolicy.FULL)
    matrix[(IngressSource.SYNC, "create", ChangeKind.CREATE)] = (
        ProcessingPolicy.HISTORICAL_SUPPRESSED
    )

    with pytest.raises(ValueError, match="event policy"):
        FolderScope(
            canonical_key="INBOX",
            webhook_ids=("inbox-id",),
            sync_folder="Inbox",
            event_policy_matrix=matrix,
            config_hash="1" * 64,
        )


@pytest.mark.parametrize(
    "snapshot",
    [None, PolicySnapshot(scopes=(), refreshed=False), PolicySnapshot.failed()],
)
def test_missing_or_failed_snapshot_fails_closed(
    snapshot: PolicySnapshot | None,
) -> None:
    with pytest.raises(PolicySnapshotUnavailableError):
        ProcessingPolicyResolver().resolve(
            IngressSource.SYNC,
            "create",
            ChangeKind.CREATE,
            "Inbox",
            snapshot,
        )


@pytest.mark.parametrize("duplicate_field", ["canonical", "sync", "webhook"])
def test_ambiguous_snapshot_fails_closed(duplicate_field: str) -> None:
    first = _scope("INBOX", "inbox-id", "Inbox", ProcessingPolicy.FULL)
    second = _scope(
        "INBOX" if duplicate_field == "canonical" else "SENT",
        "inbox-id" if duplicate_field == "webhook" else "sent-id",
        "Inbox" if duplicate_field == "sync" else "Sent Items",
        ProcessingPolicy.ARCHIVE,
    )
    snapshot = PolicySnapshot(scopes=(first, second))

    assert snapshot.ready is False
    with pytest.raises(PolicySnapshotUnavailableError):
        ProcessingPolicyResolver().configured_scopes(snapshot)


def test_snapshot_error_is_fixed_shape_and_does_not_disclose_identity() -> None:
    secret_identity = "private-folder-identity"
    first = _scope("INBOX", secret_identity, "Inbox", ProcessingPolicy.FULL)
    second = _scope("SENT", secret_identity, "Sent Items", ProcessingPolicy.ARCHIVE)

    with pytest.raises(PolicySnapshotUnavailableError) as caught:
        ProcessingPolicyResolver().configured_scopes(
            PolicySnapshot(scopes=(first, second))
        )

    assert caught.value.args == ("policy snapshot unavailable",)
    assert secret_identity not in str(caught.value)


def test_configured_scopes_are_deterministic(snapshot: PolicySnapshot) -> None:
    assert tuple(
        scope.canonical_key
        for scope in ProcessingPolicyResolver().configured_scopes(snapshot)
    ) == (
        "ARCHIVE",
        "DRAFTS",
        "INBOX",
        "SENT",
    )
