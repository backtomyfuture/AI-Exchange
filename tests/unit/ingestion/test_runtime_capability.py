from __future__ import annotations

import dataclasses
import hashlib
import json

import pytest

from src.ingestion.runtime_capability import (
    CAPABILITY_CHAIN_ROOT_HASH,
    CAPABILITY_STAGE_ORDER,
    PHASE2_AUTHORIZED_AUTHORITY_STATES,
    PHASE2_SCHEMA_REVISION,
    POSTGRES_BIGINT_MAX,
    RuntimeCapabilityManifest,
    RuntimeCapabilityStage,
    canonical_capability_hash,
    install_phase2_capability,
    require_exact_predecessor,
    validate_capability_chain,
)


_HASHES = {
    "schema_digest": "1" * 64,
    "config_hash": "2" * 64,
    "adapter_hash": "3" * 64,
    "policy_manifest_hash": "4" * 64,
    "evidence_manifest_hash": "5" * 64,
}


class _HostileString(str):
    __hash__ = str.__hash__

    def strip(self, *args: object, **kwargs: object) -> str:
        raise AssertionError("hostile string normalization must not run")

    def __eq__(self, other: object) -> bool:
        raise AssertionError("hostile string comparison must not run")


class _HostileInt(int):
    def __int__(self) -> int:
        raise AssertionError("hostile integer conversion must not run")


def _manifest(
    *,
    stage: RuntimeCapabilityStage = RuntimeCapabilityStage.PHASE2_INGESTION,
    predecessor_hash: str = CAPABILITY_CHAIN_ROOT_HASH,
    schema_revision: str = PHASE2_SCHEMA_REVISION,
    schema_digest: str = _HASHES["schema_digest"],
    protocol_version: int = 1,
    minimum_build_id: str = "build-20260716.1+abc123",
    config_hash: str = _HASHES["config_hash"],
    adapter_hash: str = _HASHES["adapter_hash"],
    policy_manifest_hash: str = _HASHES["policy_manifest_hash"],
    evidence_manifest_hash: str = _HASHES["evidence_manifest_hash"],
) -> RuntimeCapabilityManifest:
    return RuntimeCapabilityManifest(
        stage=stage,
        schema_revision=schema_revision,
        schema_digest=schema_digest,
        protocol_version=protocol_version,
        minimum_build_id=minimum_build_id,
        config_hash=config_hash,
        adapter_hash=adapter_hash,
        policy_manifest_hash=policy_manifest_hash,
        evidence_manifest_hash=evidence_manifest_hash,
        predecessor_hash=predecessor_hash,
    )


def _successor(
    predecessor: RuntimeCapabilityManifest,
    stage: RuntimeCapabilityStage,
) -> RuntimeCapabilityManifest:
    return _manifest(
        stage=stage,
        predecessor_hash=predecessor.capability_hash,
        schema_revision=(
            "20260716_0007"
            if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND
            else "20260716_0008"
        ),
        schema_digest=(
            "6" * 64
            if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND
            else "7" * 64
        ),
        protocol_version=(
            2 if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND else 3
        ),
        minimum_build_id=(
            "build-20260716.2"
            if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND
            else "build-20260716.3"
        ),
        config_hash=(
            "8" * 64
            if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND
            else "9" * 64
        ),
        adapter_hash=(
            "a" * 64
            if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND
            else "b" * 64
        ),
        policy_manifest_hash=(
            "c" * 64
            if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND
            else "d" * 64
        ),
        evidence_manifest_hash=(
            "e" * 64
            if stage is RuntimeCapabilityStage.PHASE3_APPROVAL_SEND
            else "f" * 64
        ),
    )


def test_stage_order_and_phase2_authority_boundary_are_exact() -> None:
    assert tuple(RuntimeCapabilityStage) == (
        RuntimeCapabilityStage.PHASE2_INGESTION,
        RuntimeCapabilityStage.PHASE3_APPROVAL_SEND,
        RuntimeCapabilityStage.PHASE4_GRAPH_PROJECTION,
    )
    assert tuple(stage.value for stage in CAPABILITY_STAGE_ORDER) == (
        "phase2_ingestion",
        "phase3_approval_send",
        "phase4_graph_projection",
    )
    assert PHASE2_AUTHORIZED_AUTHORITY_STATES == frozenset({"ingest_only", "paused"})
    assert "active" not in PHASE2_AUTHORIZED_AUTHORITY_STATES


def test_manifest_is_frozen_slotted_and_has_only_the_frozen_contract_fields() -> None:
    manifest = _manifest()

    assert [field.name for field in dataclasses.fields(manifest)] == [
        "stage",
        "schema_revision",
        "schema_digest",
        "protocol_version",
        "minimum_build_id",
        "config_hash",
        "adapter_hash",
        "policy_manifest_hash",
        "evidence_manifest_hash",
        "predecessor_hash",
    ]
    assert not hasattr(manifest, "__dict__")
    with pytest.raises(dataclasses.FrozenInstanceError):
        manifest.protocol_version = 2  # type: ignore[misc]


def test_manifest_normalizes_exact_plain_stage_text_to_enum() -> None:
    values = dataclasses.asdict(_manifest())
    values["stage"] = "phase2_ingestion"

    manifest = RuntimeCapabilityManifest(**values)

    assert manifest.stage is RuntimeCapabilityStage.PHASE2_INGESTION


@pytest.mark.parametrize(
    "stage",
    [
        "",
        " phase2_ingestion",
        "phase2_ingestion ",
        "PHASE2_INGESTION",
        "phase5_unknown",
        2,
        True,
        _HostileString("phase2_ingestion"),
    ],
)
def test_manifest_rejects_nonexact_stage(stage: object) -> None:
    values = dataclasses.asdict(_manifest())
    values["stage"] = stage

    with pytest.raises(ValueError, match="stage"):
        RuntimeCapabilityManifest(**values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_revision", ""),
        ("schema_revision", " 20260716_0006"),
        ("schema_revision", "20260716/0006"),
        ("schema_revision", "r" * 129),
        ("schema_revision", "版本_0006"),
        ("schema_revision", "20260716_0006\n"),
        ("schema_revision", "20260716_0006\x00"),
        ("minimum_build_id", ""),
        ("minimum_build_id", " build-1"),
        ("minimum_build_id", "build 1"),
        ("minimum_build_id", "build/1"),
        ("minimum_build_id", "build-版本"),
        ("minimum_build_id", "b" * 129),
        ("minimum_build_id", "build-1\ud800"),
        ("minimum_build_id", _HostileString("build-1")),
        ("schema_revision", _HostileString(PHASE2_SCHEMA_REVISION)),
    ],
)
def test_manifest_rejects_noncanonical_or_unbounded_ascii_identifiers(
    field: str,
    value: object,
) -> None:
    values = dataclasses.asdict(_manifest())
    values[field] = value

    with pytest.raises(ValueError, match=field):
        RuntimeCapabilityManifest(**values)


@pytest.mark.parametrize(
    "protocol_version",
    [True, False, 0, -1, POSTGRES_BIGINT_MAX + 1, 1.0, "1", _HostileInt(1)],
)
def test_manifest_requires_positive_exact_postgres_bigint_protocol(
    protocol_version: object,
) -> None:
    values = dataclasses.asdict(_manifest())
    values["protocol_version"] = protocol_version

    with pytest.raises(ValueError, match="protocol_version"):
        RuntimeCapabilityManifest(**values)


def test_manifest_accepts_postgres_bigint_protocol_upper_bound() -> None:
    assert _manifest(protocol_version=POSTGRES_BIGINT_MAX).protocol_version == (
        POSTGRES_BIGINT_MAX
    )


@pytest.mark.parametrize(
    "field",
    [
        "schema_digest",
        "config_hash",
        "adapter_hash",
        "policy_manifest_hash",
        "evidence_manifest_hash",
        "predecessor_hash",
    ],
)
@pytest.mark.parametrize(
    "invalid_hash",
    [
        "",
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        "a" * 63 + "\n",
        _HostileString("a" * 64),
    ],
)
def test_manifest_requires_exact_lowercase_sha256_fields(
    field: str,
    invalid_hash: object,
) -> None:
    values = dataclasses.asdict(_manifest())
    values[field] = invalid_hash

    with pytest.raises(ValueError, match=field):
        RuntimeCapabilityManifest(**values)


def test_capability_hash_uses_versioned_domain_separated_canonical_json() -> None:
    manifest = _manifest()
    canonical = {
        "adapter_hash": manifest.adapter_hash,
        "config_hash": manifest.config_hash,
        "evidence_manifest_hash": manifest.evidence_manifest_hash,
        "minimum_build_id": manifest.minimum_build_id,
        "policy_manifest_hash": manifest.policy_manifest_hash,
        "predecessor_hash": manifest.predecessor_hash,
        "protocol_version": manifest.protocol_version,
        "schema_digest": manifest.schema_digest,
        "schema_revision": manifest.schema_revision,
        "schema_version": 1,
        "stage": manifest.stage.value,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")
    expected = hashlib.sha256(
        b"ai-exchange-runtime-capability-manifest-v1\x00" + encoded
    ).hexdigest()

    assert canonical_capability_hash(manifest) == expected
    assert manifest.capability_hash == expected
    assert (
        CAPABILITY_CHAIN_ROOT_HASH
        == hashlib.sha256(
            b"ai-exchange-runtime-capability-chain-root-v1\x00"
        ).hexdigest()
    )
    assert CAPABILITY_CHAIN_ROOT_HASH != expected


def test_capability_hash_is_stable_across_equivalent_field_construction_order() -> None:
    values = dataclasses.asdict(_manifest())
    reverse_values = dict(reversed(tuple(values.items())))

    left = RuntimeCapabilityManifest(**values)
    right = RuntimeCapabilityManifest(**reverse_values)

    assert left == right
    assert left.capability_hash == right.capability_hash


@pytest.mark.parametrize(
    "changed",
    [
        {"schema_revision": "20260716_0007"},
        {"schema_digest": "6" * 64},
        {"protocol_version": 2},
        {"minimum_build_id": "build-20260716.2"},
        {"config_hash": "7" * 64},
        {"adapter_hash": "8" * 64},
        {"policy_manifest_hash": "9" * 64},
        {"evidence_manifest_hash": "a" * 64},
        {"predecessor_hash": "b" * 64},
    ],
)
def test_every_bound_manifest_field_changes_capability_hash(
    changed: dict[str, object],
) -> None:
    original = _manifest()
    changed_manifest = dataclasses.replace(original, **changed)

    assert changed_manifest.capability_hash != original.capability_hash


def test_exact_predecessor_accepts_the_complete_ordered_chain() -> None:
    phase2 = _manifest()
    phase3 = _successor(phase2, RuntimeCapabilityStage.PHASE3_APPROVAL_SEND)
    phase4 = _successor(phase3, RuntimeCapabilityStage.PHASE4_GRAPH_PROJECTION)

    assert require_exact_predecessor(phase2, None) is phase2
    assert require_exact_predecessor(phase3, phase2) is phase3
    assert require_exact_predecessor(phase4, phase3) is phase4
    assert validate_capability_chain((phase2, phase3, phase4)) == (
        phase2,
        phase3,
        phase4,
    )


def test_phase2_requires_the_exact_domain_separated_chain_root() -> None:
    with pytest.raises(ValueError, match="predecessor"):
        require_exact_predecessor(
            _manifest(predecessor_hash="0" * 64),
            None,
        )

    phase2 = _manifest()
    with pytest.raises(ValueError, match="predecessor"):
        require_exact_predecessor(phase2, phase2)


@pytest.mark.parametrize(
    "candidate_stage",
    [
        RuntimeCapabilityStage.PHASE3_APPROVAL_SEND,
        RuntimeCapabilityStage.PHASE4_GRAPH_PROJECTION,
    ],
)
def test_successor_requires_a_predecessor(
    candidate_stage: RuntimeCapabilityStage,
) -> None:
    candidate = _manifest(stage=candidate_stage, predecessor_hash="0" * 64)

    with pytest.raises(ValueError, match="predecessor"):
        require_exact_predecessor(candidate, None)


def test_successor_rejects_wrong_stage_or_mismatched_predecessor_hash() -> None:
    phase2 = _manifest()
    phase3 = _successor(phase2, RuntimeCapabilityStage.PHASE3_APPROVAL_SEND)
    wrong_phase2 = dataclasses.replace(phase2, config_hash="f" * 64)

    with pytest.raises(ValueError, match="predecessor"):
        require_exact_predecessor(phase3, wrong_phase2)

    phase4 = _successor(phase3, RuntimeCapabilityStage.PHASE4_GRAPH_PROJECTION)
    with pytest.raises(ValueError, match="predecessor"):
        require_exact_predecessor(phase4, phase2)


@pytest.mark.parametrize(
    "chain_builder",
    [
        lambda phase2, phase3, phase4: (),
        lambda phase2, phase3, phase4: (phase3,),
        lambda phase2, phase3, phase4: (phase2, phase2),
        lambda phase2, phase3, phase4: (phase2, phase4),
        lambda phase2, phase3, phase4: (phase2, phase3, phase3),
        lambda phase2, phase3, phase4: (phase2, phase3, phase4, phase4),
    ],
)
def test_chain_validator_rejects_empty_duplicate_gapped_or_extra_chains(
    chain_builder,
) -> None:
    phase2 = _manifest()
    phase3 = _successor(phase2, RuntimeCapabilityStage.PHASE3_APPROVAL_SEND)
    phase4 = _successor(phase3, RuntimeCapabilityStage.PHASE4_GRAPH_PROJECTION)

    with pytest.raises(ValueError, match="capability chain"):
        validate_capability_chain(chain_builder(phase2, phase3, phase4))


@pytest.mark.parametrize(
    "chain",
    [
        "phase2_ingestion",
        [_manifest(), object()],
        (_HostileString("phase2_ingestion"),),
    ],
)
def test_chain_validator_rejects_nonmanifest_inputs(chain: object) -> None:
    with pytest.raises(ValueError, match="capability chain"):
        validate_capability_chain(chain)  # type: ignore[arg-type]


def test_phase2_install_accepts_only_exact_first_stage_and_head() -> None:
    phase2 = _manifest()

    installed = install_phase2_capability(phase2)

    assert installed is phase2
    assert installed.stage is RuntimeCapabilityStage.PHASE2_INGESTION
    assert installed.predecessor_hash == CAPABILITY_CHAIN_ROOT_HASH
    assert "active" not in PHASE2_AUTHORIZED_AUTHORITY_STATES


def test_phase2_install_rejects_later_stage_wrong_root_or_wrong_schema_head() -> None:
    phase2 = _manifest()
    phase3 = _successor(phase2, RuntimeCapabilityStage.PHASE3_APPROVAL_SEND)

    for manifest in (
        phase3,
        _manifest(predecessor_hash="0" * 64),
        _manifest(schema_revision="20260716_0005"),
    ):
        with pytest.raises(ValueError, match="phase2 capability"):
            install_phase2_capability(manifest)


def test_public_functions_reject_manifest_subclasses_and_plain_objects() -> None:
    class _ManifestSubclass(RuntimeCapabilityManifest):
        pass

    values = dataclasses.asdict(_manifest())
    subclass = _ManifestSubclass(**values)

    for callable_ in (
        canonical_capability_hash,
        install_phase2_capability,
    ):
        with pytest.raises(ValueError, match="exact RuntimeCapabilityManifest"):
            callable_(subclass)
        with pytest.raises(ValueError, match="exact RuntimeCapabilityManifest"):
            callable_(object())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="exact RuntimeCapabilityManifest"):
        require_exact_predecessor(_manifest(), subclass)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("stage", _HostileString("phase2_ingestion")),
        ("schema_revision", "20260716/0006"),
        ("schema_digest", "A" * 64),
        ("protocol_version", True),
        ("minimum_build_id", "build/1"),
        ("config_hash", "A" * 64),
        ("adapter_hash", "A" * 64),
        ("policy_manifest_hash", "A" * 64),
        ("evidence_manifest_hash", "A" * 64),
        ("predecessor_hash", "A" * 64),
    ],
    ids=(
        "stage",
        "schema-revision",
        "schema-digest",
        "protocol",
        "build",
        "config-hash",
        "adapter-hash",
        "policy-hash",
        "evidence-hash",
        "predecessor-hash",
    ),
)
def test_public_hash_boundary_revalidates_hostile_post_construction_mutation(
    field: str,
    value: object,
) -> None:
    manifest = _manifest()
    object.__setattr__(manifest, field, value)

    with pytest.raises(ValueError, match=field):
        canonical_capability_hash(manifest)
