"""Immutable, fail-closed runtime capability manifests.

The capability chain is a pure value contract.  It performs no database,
configuration, network, or authority mutation.  Phase 2 may prepare only the
first manifest; later phases append exact hash-linked successors.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final


POSTGRES_BIGINT_MAX: Final = 2**63 - 1
PHASE2_SCHEMA_REVISION: Final = "20260716_0006"

_CANONICAL_SCHEMA_VERSION: Final = 1
_CAPABILITY_HASH_DOMAIN: Final = b"ai-exchange-runtime-capability-manifest-v1\x00"
_CAPABILITY_CHAIN_ROOT_DOMAIN: Final = (
    b"ai-exchange-runtime-capability-chain-root-v1\x00"
)
CAPABILITY_CHAIN_ROOT_HASH: Final = hashlib.sha256(
    _CAPABILITY_CHAIN_ROOT_DOMAIN
).hexdigest()

_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}\Z")
_SCHEMA_REVISION_PATTERN: Final = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z",
    flags=re.ASCII,
)
_BUILD_ID_PATTERN: Final = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}\Z",
    flags=re.ASCII,
)


class RuntimeCapabilityStage(StrEnum):
    """The only legal append order for runtime capabilities."""

    PHASE2_INGESTION = "phase2_ingestion"
    PHASE3_APPROVAL_SEND = "phase3_approval_send"
    PHASE4_GRAPH_PROJECTION = "phase4_graph_projection"


CAPABILITY_STAGE_ORDER: Final = (
    RuntimeCapabilityStage.PHASE2_INGESTION,
    RuntimeCapabilityStage.PHASE3_APPROVAL_SEND,
    RuntimeCapabilityStage.PHASE4_GRAPH_PROJECTION,
)
PHASE2_AUTHORIZED_AUTHORITY_STATES: Final = frozenset(
    {
        "ingest_only",
        "paused",
    }
)


def _require_stage(value: object) -> RuntimeCapabilityStage:
    if type(value) is RuntimeCapabilityStage:
        return value
    if type(value) is not str:
        raise ValueError("stage must be an exact runtime capability stage")
    try:
        return RuntimeCapabilityStage(value)
    except ValueError:
        raise ValueError("stage must be an exact runtime capability stage") from None


def _require_identifier(
    name: str,
    value: object,
    *,
    pattern: re.Pattern[str],
) -> str:
    if type(value) is not str:
        raise ValueError(f"{name} must be bounded canonical ASCII text")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{name} must be valid UTF-8") from None
    if len(encoded) > 128 or not encoded.isascii() or pattern.fullmatch(value) is None:
        raise ValueError(f"{name} must be bounded canonical ASCII text")
    return value


def _require_positive_bigint(name: str, value: object) -> int:
    if type(value) is not int or value < 1 or value > POSTGRES_BIGINT_MAX:
        raise ValueError(f"{name} must be a positive PostgreSQL BIGINT")
    return value


def _require_sha256(name: str, value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be an exact lowercase SHA-256 digest")
    return value


def _validate_manifest_fields(manifest: RuntimeCapabilityManifest) -> None:
    if type(manifest.stage) is not RuntimeCapabilityStage:
        raise ValueError("stage must be an exact runtime capability stage")
    _require_identifier(
        "schema_revision",
        manifest.schema_revision,
        pattern=_SCHEMA_REVISION_PATTERN,
    )
    _require_sha256("schema_digest", manifest.schema_digest)
    _require_positive_bigint("protocol_version", manifest.protocol_version)
    _require_identifier(
        "minimum_build_id",
        manifest.minimum_build_id,
        pattern=_BUILD_ID_PATTERN,
    )
    _require_sha256("config_hash", manifest.config_hash)
    _require_sha256("adapter_hash", manifest.adapter_hash)
    _require_sha256("policy_manifest_hash", manifest.policy_manifest_hash)
    _require_sha256("evidence_manifest_hash", manifest.evidence_manifest_hash)
    _require_sha256("predecessor_hash", manifest.predecessor_hash)


@dataclass(frozen=True, slots=True)
class RuntimeCapabilityManifest:
    """One immutable, secret-free capability-chain node."""

    stage: RuntimeCapabilityStage
    schema_revision: str
    schema_digest: str
    protocol_version: int
    minimum_build_id: str
    config_hash: str
    adapter_hash: str
    policy_manifest_hash: str
    evidence_manifest_hash: str
    predecessor_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", _require_stage(self.stage))
        _validate_manifest_fields(self)

    @property
    def capability_hash(self) -> str:
        """Return the domain-separated identity of this exact manifest."""

        return canonical_capability_hash(self)


def _require_exact_manifest(value: object) -> RuntimeCapabilityManifest:
    if type(value) is not RuntimeCapabilityManifest:
        raise ValueError("value must be an exact RuntimeCapabilityManifest")
    _validate_manifest_fields(value)
    return value


def canonical_capability_hash(manifest: RuntimeCapabilityManifest) -> str:
    """Hash every bound manifest fact using canonical, versioned JSON."""

    exact = _require_exact_manifest(manifest)
    canonical = {
        "adapter_hash": exact.adapter_hash,
        "config_hash": exact.config_hash,
        "evidence_manifest_hash": exact.evidence_manifest_hash,
        "minimum_build_id": exact.minimum_build_id,
        "policy_manifest_hash": exact.policy_manifest_hash,
        "predecessor_hash": exact.predecessor_hash,
        "protocol_version": exact.protocol_version,
        "schema_digest": exact.schema_digest,
        "schema_revision": exact.schema_revision,
        "schema_version": _CANONICAL_SCHEMA_VERSION,
        "stage": exact.stage.value,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(_CAPABILITY_HASH_DOMAIN + encoded).hexdigest()


def require_exact_predecessor(
    candidate: RuntimeCapabilityManifest,
    predecessor: RuntimeCapabilityManifest | None,
) -> RuntimeCapabilityManifest:
    """Require the candidate's one exact stage and hash predecessor."""

    exact_candidate = _require_exact_manifest(candidate)
    exact_predecessor = (
        None if predecessor is None else _require_exact_manifest(predecessor)
    )
    candidate_position = CAPABILITY_STAGE_ORDER.index(exact_candidate.stage)

    if candidate_position == 0:
        if (
            exact_predecessor is not None
            or exact_candidate.predecessor_hash != CAPABILITY_CHAIN_ROOT_HASH
        ):
            raise ValueError("capability predecessor does not match the chain root")
        return exact_candidate

    if exact_predecessor is None:
        raise ValueError("capability predecessor is required")
    expected_stage = CAPABILITY_STAGE_ORDER[candidate_position - 1]
    if (
        exact_predecessor.stage is not expected_stage
        or exact_candidate.predecessor_hash
        != canonical_capability_hash(exact_predecessor)
    ):
        raise ValueError("capability predecessor stage or hash does not match")
    return exact_candidate


def validate_capability_chain(
    manifests: object,
) -> tuple[RuntimeCapabilityManifest, ...]:
    """Return an exact contiguous chain or fail closed."""

    if type(manifests) not in (list, tuple):
        raise ValueError("capability chain must be an exact list or tuple")
    chain = tuple(manifests)
    if not chain or len(chain) > len(CAPABILITY_STAGE_ORDER):
        raise ValueError("capability chain has an invalid length")
    if any(type(item) is not RuntimeCapabilityManifest for item in chain):
        raise ValueError("capability chain contains a non-manifest value")

    predecessor: RuntimeCapabilityManifest | None = None
    try:
        for position, candidate in enumerate(chain):
            if candidate.stage is not CAPABILITY_STAGE_ORDER[position]:
                raise ValueError("capability stage is out of order")
            require_exact_predecessor(candidate, predecessor)
            predecessor = candidate
    except ValueError as exc:
        raise ValueError("capability chain predecessor is invalid") from exc
    return chain


def install_phase2_capability(
    manifest: RuntimeCapabilityManifest,
) -> RuntimeCapabilityManifest:
    """Validate the only capability Task 10G is allowed to install.

    Persistence remains the responsibility of the later DB-authority layer.
    This API deliberately has no activation operation.
    """

    exact = _require_exact_manifest(manifest)
    if (
        exact.stage is not RuntimeCapabilityStage.PHASE2_INGESTION
        or exact.schema_revision != PHASE2_SCHEMA_REVISION
        or exact.predecessor_hash != CAPABILITY_CHAIN_ROOT_HASH
    ):
        raise ValueError("phase2 capability manifest is not the exact first stage")
    require_exact_predecessor(exact, None)
    return exact
