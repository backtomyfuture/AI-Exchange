from __future__ import annotations

import json
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.exc import DBAPIError

from src.db.bootstrap import bootstrap_database
from src.db.roles import (
    DatabaseRoleError,
    require_maintenance_database_role,
    require_migration_database_role,
    require_runtime_database_role,
)
from src.db.schema_contract import (
    DatabaseSchemaContractError,
    require_database_schema_contract,
)
from src.ingestion.models import MAX_INBOX_PAYLOAD_BYTES, NormalizedIngressEvent


DURABLE_TABLES = {
    "audit_events",
    "emails",
    "event_inbox",
    "pipeline_ownership",
    "pipeline_shadow_comparisons",
    "sync_cursors",
}

EXPECTED_RELATIONS = DURABLE_TABLES | {
    "alembic_version",
    "app_kv_store",
    "emails_log",
    "processed_emails",
}

# Test-owned schema baseline.  Keep this independent from the production
# revision/access manifests so coordinated production drift cannot bless itself.
EXPECTED_RELATION_KINDS = {
    "alembic_version": "r",
    "app_kv_store": "r",
    "audit_events": "r",
    "emails": "r",
    "emails_log": "r",
    "event_inbox": "r",
    "pipeline_ownership": "r",
    "pipeline_shadow_comparisons": "r",
    "processed_emails": "v",
    "sync_cursors": "r",
}
EXPECTED_RELATION_CONTRACTS = {
    name: (
        relation_kind,
        "p",
        False,
        False,
        True,
        "heap" if relation_kind in {"r", "p"} else None,
    )
    for name, relation_kind in EXPECTED_RELATION_KINDS.items()
}

EXPECTED_COLUMN_TYPES = {
    "pipeline_ownership.account_id": "int8",
    "pipeline_ownership.generation": "int8",
    "pipeline_ownership.pipeline_name": "text",
    "pipeline_ownership.state": "text",
    "pipeline_ownership.fencing_token": "int8",
    "pipeline_ownership.created_by": "text",
    "pipeline_ownership.reason": "text",
    "pipeline_ownership.created_at": "timestamptz",
    "pipeline_ownership.updated_at": "timestamptz",
    "event_inbox.id": "uuid",
    "event_inbox.account_id": "int8",
    "event_inbox.external_email_id": "text",
    "event_inbox.folder_key": "text",
    "event_inbox.source": "text",
    "event_inbox.raw_event_type": "text",
    "event_inbox.change_kind": "text",
    "event_inbox.dedupe_key": "bpchar",
    "event_inbox.source_version": "text",
    "event_inbox.source_event_at": "timestamptz",
    "event_inbox.payload": "jsonb",
    "event_inbox.processing_policy": "text",
    "event_inbox.pipeline_name": "text",
    "event_inbox.generation": "int8",
    "event_inbox.fencing_token": "int8",
    "event_inbox.status": "text",
    "event_inbox.lease_owner": "text",
    "event_inbox.lease_until": "timestamptz",
    "event_inbox.attempts": "int8",
    "event_inbox.available_at": "timestamptz",
    "event_inbox.processing_started_at": "timestamptz",
    "event_inbox.effect_started_at": "timestamptz",
    "event_inbox.safe_error_code": "text",
    "event_inbox.safe_error_summary": "text",
    "event_inbox.received_at": "timestamptz",
    "event_inbox.updated_at": "timestamptz",
    "sync_cursors.account_id": "int8",
    "sync_cursors.folder_key": "text",
    "sync_cursors.cursor": "text",
    "sync_cursors.status": "text",
    "sync_cursors.blocked_reason_code": "text",
    "sync_cursors.contract_fingerprint": "bpchar",
    "sync_cursors.blocked_at": "timestamptz",
    "sync_cursors.version": "int8",
    "sync_cursors.last_success_at": "timestamptz",
    "sync_cursors.last_attempt_at": "timestamptz",
    "sync_cursors.created_at": "timestamptz",
    "sync_cursors.updated_at": "timestamptz",
    "emails.id": "uuid",
    "emails.account_id": "int8",
    "emails.external_email_id": "text",
    "emails.source_folder_key": "text",
    "emails.status": "text",
    "emails.version": "int8",
    "emails.owner_generation": "int8",
    "emails.owner_fencing_token": "int8",
    "emails.processing_inbox_id": "uuid",
    "emails.create_seen_at": "timestamptz",
    "emails.processing_started_at": "timestamptz",
    "emails.source_deleted_at": "timestamptz",
    "emails.external_effects_started_at": "timestamptz",
    "emails.safe_error_code": "text",
    "emails.safe_error_summary": "text",
    "emails.content_ref": "jsonb",
    "emails.is_read": "bool",
    "emails.is_read_refresh_required": "bool",
    "emails.created_at": "timestamptz",
    "emails.updated_at": "timestamptz",
    "audit_events.id": "uuid",
    "audit_events.event_key": "bpchar",
    "audit_events.account_id": "int8",
    "audit_events.email_id": "uuid",
    "audit_events.object_type": "text",
    "audit_events.object_fingerprint": "bpchar",
    "audit_events.action": "text",
    "audit_events.result": "text",
    "audit_events.actor": "text",
    "audit_events.reason": "text",
    "audit_events.safe_metadata": "jsonb",
    "audit_events.created_at": "timestamptz",
    "pipeline_shadow_comparisons.id": "uuid",
    "pipeline_shadow_comparisons.account_id": "int8",
    "pipeline_shadow_comparisons.generation": "int8",
    "pipeline_shadow_comparisons.fencing_token": "int8",
    "pipeline_shadow_comparisons.pipeline_name": "text",
    "pipeline_shadow_comparisons.candidate_pipeline_name": "text",
    "pipeline_shadow_comparisons.candidate_build_id": "text",
    "pipeline_shadow_comparisons.candidate_config_hash": "bpchar",
    "pipeline_shadow_comparisons.event_key": "bpchar",
    "pipeline_shadow_comparisons.input_hash": "bpchar",
    "pipeline_shadow_comparisons.legacy_status": "text",
    "pipeline_shadow_comparisons.shadow_status": "text",
    "pipeline_shadow_comparisons.comparison_status": "text",
    "pipeline_shadow_comparisons.legacy_decision_hash": "bpchar",
    "pipeline_shadow_comparisons.legacy_failure_code": "text",
    "pipeline_shadow_comparisons.shadow_decision_hash": "bpchar",
    "pipeline_shadow_comparisons.shadow_failure_code": "text",
    "pipeline_shadow_comparisons.safe_metadata": "jsonb",
    "pipeline_shadow_comparisons.created_at": "timestamptz",
    "pipeline_shadow_comparisons.updated_at": "timestamptz",
}

NULLABLE_COLUMNS = {
    "pipeline_ownership.reason",
    "event_inbox.source_version",
    "event_inbox.source_event_at",
    "event_inbox.lease_owner",
    "event_inbox.lease_until",
    "event_inbox.processing_started_at",
    "event_inbox.effect_started_at",
    "event_inbox.safe_error_code",
    "event_inbox.safe_error_summary",
    "sync_cursors.cursor",
    "sync_cursors.blocked_reason_code",
    "sync_cursors.contract_fingerprint",
    "sync_cursors.blocked_at",
    "sync_cursors.last_success_at",
    "sync_cursors.last_attempt_at",
    "emails.processing_inbox_id",
    "emails.create_seen_at",
    "emails.processing_started_at",
    "emails.source_deleted_at",
    "emails.external_effects_started_at",
    "emails.safe_error_code",
    "emails.safe_error_summary",
    "emails.content_ref",
    "emails.is_read",
    "audit_events.email_id",
    "audit_events.reason",
    "pipeline_shadow_comparisons.legacy_decision_hash",
    "pipeline_shadow_comparisons.legacy_failure_code",
    "pipeline_shadow_comparisons.shadow_decision_hash",
    "pipeline_shadow_comparisons.shadow_failure_code",
}

DEFAULTED_COLUMNS = {
    "pipeline_ownership.created_at",
    "pipeline_ownership.updated_at",
    "event_inbox.payload",
    "event_inbox.attempts",
    "event_inbox.available_at",
    "event_inbox.received_at",
    "event_inbox.updated_at",
    "sync_cursors.version",
    "sync_cursors.created_at",
    "sync_cursors.updated_at",
    "emails.version",
    "emails.is_read_refresh_required",
    "emails.created_at",
    "emails.updated_at",
    "audit_events.safe_metadata",
    "audit_events.created_at",
    "pipeline_shadow_comparisons.safe_metadata",
    "pipeline_shadow_comparisons.created_at",
    "pipeline_shadow_comparisons.updated_at",
}

EXPECTED_PHASE2_DEFAULTS = {
    ("audit_events", "created_at"): "CURRENT_TIMESTAMP",
    ("audit_events", "safe_metadata"): "'{}'::jsonb",
    ("emails", "created_at"): "CURRENT_TIMESTAMP",
    ("emails", "is_read_refresh_required"): "false",
    ("emails", "updated_at"): "CURRENT_TIMESTAMP",
    ("emails", "version"): "0",
    ("event_inbox", "attempts"): "0",
    ("event_inbox", "available_at"): "CURRENT_TIMESTAMP",
    ("event_inbox", "payload"): "'{}'::jsonb",
    ("event_inbox", "received_at"): "CURRENT_TIMESTAMP",
    ("event_inbox", "updated_at"): "CURRENT_TIMESTAMP",
    ("pipeline_ownership", "created_at"): "CURRENT_TIMESTAMP",
    ("pipeline_ownership", "updated_at"): "CURRENT_TIMESTAMP",
    ("pipeline_shadow_comparisons", "created_at"): "CURRENT_TIMESTAMP",
    ("pipeline_shadow_comparisons", "safe_metadata"): "'{}'::jsonb",
    ("pipeline_shadow_comparisons", "updated_at"): "CURRENT_TIMESTAMP",
    ("sync_cursors", "created_at"): "CURRENT_TIMESTAMP",
    ("sync_cursors", "updated_at"): "CURRENT_TIMESTAMP",
    ("sync_cursors", "version"): "0",
}

EXPECTED_PHASE2_CHECKS = {
    ("audit_events", "ck_audit_events_account"): (
        "25341c3ce11ea0a3cda1fb848bf14cc227abc891a221df7e6c0d094f11f14124"
    ),
    ("audit_events", "ck_audit_events_action"): (
        "9a38ff8a9537836c6e522445823653010087efeb12805222a64a7c443fa39b8b"
    ),
    ("audit_events", "ck_audit_events_actor"): (
        "b95f8e61105ded4bf2f1847fd7a56a861ef9d0d174dd8f1edf70599dedb2474b"
    ),
    ("audit_events", "ck_audit_events_event_key"): (
        "3139e131264bc6378794d05778fec7457430371de4a75b4ace6091706afd3c60"
    ),
    ("audit_events", "ck_audit_events_object_fingerprint"): (
        "ceab2d8a8a6bccb684c03c924138c6281b93559356d00de9abfbb41b1ef785a9"
    ),
    ("audit_events", "ck_audit_events_object_type"): (
        "5c98d1723733b30645e66b9f76fb4328e97311782060ccb2e46415557a4685da"
    ),
    ("audit_events", "ck_audit_events_reason"): (
        "1a0a92be50dd169bba961253f4ff7308467600c26dd5d79e495e4805ad7c0c4a"
    ),
    ("audit_events", "ck_audit_events_result"): (
        "2bc8bf5ed443609be6fb121816d28db1c54864d4d0b7aaded793d8981160adb5"
    ),
    ("audit_events", "ck_audit_events_safe_metadata"): (
        "56cb6aa7377f563da2330cc104acfb5c4d60a845eae3fa7e6cd8141b515818e1"
    ),
    ("emails", "ck_emails_content_ref"): (
        "113c64ca6306493d0501edea98dc113cadff6e9f0e315a886f48d208e206e0ea"
    ),
    ("emails", "ck_emails_error"): (
        "d87755b4f26a4fcb9bee9dde2ac28265d4697f4223fc87bf8405b05b0b1998c5"
    ),
    ("emails", "ck_emails_external_email_id"): (
        "7f847c55903c32bc957ab4cabc8a00b1bb7d7fb8bdf7bd840047be80c179c19d"
    ),
    ("emails", "ck_emails_positive_identity"): (
        "ae2a612767ac0522f48dad483274bd03b714011583eb92706184017f56990589"
    ),
    ("emails", "ck_emails_processing_state"): (
        "ad2a39eb641488973d62f5223cb563821fba4655104ebee4e81e9f4742dc071a"
    ),
    ("emails", "ck_emails_read_projection"): (
        "55e51217b5904db5acb73a50d1d01a8f5616a82b926a907811b4e9234160e5ca"
    ),
    ("emails", "ck_emails_source_folder_key"): (
        "e2530071cf19c3724031ca9c4fbea7b03635256157bce6f675200f8b17efad7b"
    ),
    ("emails", "ck_emails_status"): (
        "beb95a50e0308f89b2eb7d059aea240925e1347847d3b56b5e2cd8eedcda8a60"
    ),
    ("emails", "ck_emails_version"): (
        "13b6703b3832f8093fcd588c3f98b34dab1891a6f3c8033c72f8f246941a6998"
    ),
    ("event_inbox", "ck_event_inbox_attempts"): (
        "34c54dbe05a2425b0e7833733f9e95202d6757ede512840257d78ac8c14682aa"
    ),
    ("event_inbox", "ck_event_inbox_change_kind"): (
        "5c93ddcd9500c893104ce111262e7836b25c900c2f4b7921ac55eb151b3c58a9"
    ),
    ("event_inbox", "ck_event_inbox_dedupe_key"): (
        "14e5d968744dec1affd7e3354d2e98cc79dd2ccb15a684dee629404cd9bcf55d"
    ),
    ("event_inbox", "ck_event_inbox_effect_order"): (
        "08ff42f63c01ce08ab0b68674b328ad0efa1512dbc6e56b5295263b4312c8700"
    ),
    ("event_inbox", "ck_event_inbox_error"): (
        "d87755b4f26a4fcb9bee9dde2ac28265d4697f4223fc87bf8405b05b0b1998c5"
    ),
    ("event_inbox", "ck_event_inbox_error_state"): (
        "1ac70182f61b5817019d1f1f83886867476f7a6d3e6fe61f02b9248358845e62"
    ),
    ("event_inbox", "ck_event_inbox_external_email_id"): (
        "7f847c55903c32bc957ab4cabc8a00b1bb7d7fb8bdf7bd840047be80c179c19d"
    ),
    ("event_inbox", "ck_event_inbox_folder_key"): (
        "2b226c8f6fea8ce4c93e8ebc0005964a00f4df08cdebcd11b3bc96d247d08629"
    ),
    ("event_inbox", "ck_event_inbox_lease"): (
        "26e3a18624ee3fcd2376649f035f26ad0aa0c9f59b7e7f5a0f5ff71af6533ec1"
    ),
    ("event_inbox", "ck_event_inbox_payload"): (
        "62e7ed765f66f9428b4e03af688bffc57d127deaf9fd53059e0791edd88d836f"
    ),
    ("event_inbox", "ck_event_inbox_pipeline_name"): (
        "6505916d0a680d292a459d1072069232644bb16413f7d4b249c2fde926109d1f"
    ),
    ("event_inbox", "ck_event_inbox_positive_identity"): (
        "e5e43f686cf6aca5560d22d5bfa18bd0e5cba8341a5f26e09eff5a37b6bba9c6"
    ),
    ("event_inbox", "ck_event_inbox_processing_policy"): (
        "f2c35a7d5a10689cc78f15a3d83cf656c89dc26578f2390519a7679012f1d9bb"
    ),
    ("event_inbox", "ck_event_inbox_raw_event_type"): (
        "d18bc1df47462aa76d502b521bfb7195468040b95103992c63b6470f073a9375"
    ),
    ("event_inbox", "ck_event_inbox_source"): (
        "250d0bdd97c26384d2dbb67d39e0db6d4fdf421ccb33ac5712077f5a35f8588e"
    ),
    ("event_inbox", "ck_event_inbox_source_version"): (
        "8e48335b54714361ed4128f5873618bc56b3acc350a9da16d827d98e955c04e2"
    ),
    ("event_inbox", "ck_event_inbox_status"): (
        "74820621741dd0bbe71ec8329d53f74450d9a26020475c08bf39be09c7f89029"
    ),
    ("pipeline_ownership", "ck_pipeline_ownership_created_by"): (
        "5e463ea76990e36f378b0235804f0c8a4b091276223ee2c3bb6dfe8358b89601"
    ),
    ("pipeline_ownership", "ck_pipeline_ownership_pipeline_name"): (
        "6505916d0a680d292a459d1072069232644bb16413f7d4b249c2fde926109d1f"
    ),
    ("pipeline_ownership", "ck_pipeline_ownership_positive_identity"): (
        "e5e43f686cf6aca5560d22d5bfa18bd0e5cba8341a5f26e09eff5a37b6bba9c6"
    ),
    ("pipeline_ownership", "ck_pipeline_ownership_reason"): (
        "1a0a92be50dd169bba961253f4ff7308467600c26dd5d79e495e4805ad7c0c4a"
    ),
    ("pipeline_ownership", "ck_pipeline_ownership_state"): (
        "bc871752dda202262a17e2713fec0838fdf8ab7e9b30f855b818db23e4d9368b"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_build_id"): (
        "333501848ed7a2f85e4174ee9c77e566a8eccc479d8ed1c8cc75cf9d692611a2"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_comparison_state"): (
        "8b2c3524d3def0618cb82fc2433ef5cdf6b3ce9f54520dbe58c7277e39748813"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_failure_codes"): (
        "bf53ee0cb29e0b20b669823868602e318eb4f16b04e1af1da94e8f075f0806ea"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_hashes"): (
        "a23a49c4e77c8071a0bc071073ffb6145a46dd68a18a9d389256c47cba26509c"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_legacy_state"): (
        "91fa582ca73156cbae9eaa54a8248fae662bf72e1e2ec04018a9f58b014455cf"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_pipeline_names"): (
        "7a0d4ec115da3dc5c2405f07acb2797426431f32f3883667fcaec6e610808088"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_positive_identity"): (
        "e5e43f686cf6aca5560d22d5bfa18bd0e5cba8341a5f26e09eff5a37b6bba9c6"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_safe_metadata"): (
        "56cb6aa7377f563da2330cc104acfb5c4d60a845eae3fa7e6cd8141b515818e1"
    ),
    ("pipeline_shadow_comparisons", "ck_pipeline_shadow_shadow_state"): (
        "6136a06fd25608dc230078767188c1032f230cb345c7d1e1a63e00f54613f34c"
    ),
    ("sync_cursors", "ck_sync_cursors_account"): (
        "25341c3ce11ea0a3cda1fb848bf14cc227abc891a221df7e6c0d094f11f14124"
    ),
    ("sync_cursors", "ck_sync_cursors_cursor"): (
        "9c648abac74d552dda2f149a1d37aadbb15daa5ffa0b37c5e5cb3dfd246b5698"
    ),
    ("sync_cursors", "ck_sync_cursors_fingerprint"): (
        "cb4e1ca94aa59cbc3477a3bb9ff0ea174db63b4cf44ca650c028c00b10d5ee64"
    ),
    ("sync_cursors", "ck_sync_cursors_folder_key"): (
        "2b226c8f6fea8ce4c93e8ebc0005964a00f4df08cdebcd11b3bc96d247d08629"
    ),
    ("sync_cursors", "ck_sync_cursors_reason"): (
        "e1809a680586dfe440e9df1a6b79dca29df570b85a472e7bc5804ddf0abc0839"
    ),
    ("sync_cursors", "ck_sync_cursors_state_matrix"): (
        "d03deda2282b8a0c6dd3813340cd04416b6721192097fcc2274b396ede99b387"
    ),
    ("sync_cursors", "ck_sync_cursors_status"): (
        "da64e450250826bd6c8f5703ac95682b45bc5d45dbb1da89745a2b48a24278b6"
    ),
    ("sync_cursors", "ck_sync_cursors_version"): (
        "13b6703b3832f8093fcd588c3f98b34dab1891a6f3c8033c72f8f246941a6998"
    ),
}


def _expected_unique_index(
    relation,
    index_name,
    constraint_kind,
    columns,
    *,
    predicate_sha256=None,
):
    is_constraint = constraint_kind is not None
    return (
        relation,
        index_name,
        index_name if is_constraint else None,
        constraint_kind,
        columns,
        (0,) * len(columns),
        predicate_sha256,
        False,
        False if is_constraint else None,
        False if is_constraint else None,
        True if is_constraint else None,
        True,
        True,
        "btree",
        True,
        True,
        True,
        True,
    )


EXPECTED_PHASE2_UNIQUE_INDEXES = {
    _expected_unique_index("audit_events", "pk_audit_events", "p", ("id",)),
    _expected_unique_index(
        "audit_events",
        "uq_audit_events_event_key",
        "u",
        ("event_key",),
    ),
    _expected_unique_index("emails", "pk_emails", "p", ("id",)),
    _expected_unique_index(
        "emails",
        "uq_email_external",
        "u",
        ("account_id", "external_email_id"),
    ),
    _expected_unique_index(
        "emails",
        "uq_emails_account_id",
        "u",
        ("account_id", "id"),
    ),
    _expected_unique_index(
        "emails",
        "uq_emails_outbox_identity",
        "u",
        ("id", "account_id", "owner_generation", "owner_fencing_token"),
    ),
    _expected_unique_index("event_inbox", "pk_event_inbox", "p", ("id",)),
    _expected_unique_index(
        "event_inbox",
        "uq_event_inbox_dedupe",
        "u",
        ("dedupe_key",),
    ),
    _expected_unique_index(
        "event_inbox",
        "uq_event_inbox_processing_identity",
        "u",
        ("id", "account_id", "external_email_id", "generation", "fencing_token"),
    ),
    _expected_unique_index(
        "pipeline_ownership",
        "pk_pipeline_ownership",
        "p",
        ("account_id", "generation"),
    ),
    _expected_unique_index(
        "pipeline_ownership",
        "uq_pipeline_current_ingress",
        None,
        ("account_id",),
        predicate_sha256=(
            "35cc984d6efa9a70d61a14ab89954b9170d47cbb4531bcfda034203fc85c6ebd"
        ),
    ),
    _expected_unique_index(
        "pipeline_ownership",
        "uq_pipeline_ownership_event_identity",
        "u",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
    ),
    _expected_unique_index(
        "pipeline_ownership",
        "uq_pipeline_ownership_fence",
        "u",
        ("account_id", "fencing_token"),
    ),
    _expected_unique_index(
        "pipeline_ownership",
        "uq_pipeline_ownership_generation_fence",
        "u",
        ("account_id", "generation", "fencing_token"),
    ),
    _expected_unique_index(
        "pipeline_shadow_comparisons",
        "pk_pipeline_shadow_comparisons",
        "p",
        ("id",),
    ),
    _expected_unique_index(
        "pipeline_shadow_comparisons",
        "uq_pipeline_shadow_candidate_event",
        "u",
        (
            "account_id",
            "generation",
            "pipeline_name",
            "candidate_pipeline_name",
            "candidate_build_id",
            "candidate_config_hash",
            "event_key",
        ),
    ),
    _expected_unique_index(
        "sync_cursors",
        "pk_sync_cursors",
        "p",
        ("account_id", "folder_key"),
    ),
}


def _expected_nonconstraint_index(
    relation,
    index_name,
    columns,
    *,
    options=None,
    predicate_sha256=None,
    unique=False,
):
    return (
        relation,
        index_name,
        unique,
        True,
        True,
        "btree",
        columns,
        options if options is not None else (0,) * len(columns),
        predicate_sha256,
        True,
        True,
        True,
        True,
    )


EXPECTED_PHASE2_NONCONSTRAINT_INDEXES = {
    _expected_nonconstraint_index(
        "audit_events",
        "ix_audit_events_account_time",
        ("account_id", "created_at", "id"),
        options=(0, 3, 0),
    ),
    _expected_nonconstraint_index(
        "audit_events",
        "ix_audit_events_email_time",
        ("email_id", "created_at", "id"),
        options=(0, 3, 0),
        predicate_sha256=(
            "124b13523b2508c02d37c10f1ef2999184c84589b3af8cc4a2b289a6454e3f29"
        ),
    ),
    _expected_nonconstraint_index(
        "emails",
        "ix_emails_account_status",
        ("account_id", "status", "updated_at", "id"),
    ),
    _expected_nonconstraint_index(
        "emails",
        "ix_emails_owner_status",
        ("account_id", "owner_generation", "status"),
    ),
    _expected_nonconstraint_index(
        "event_inbox",
        "ix_event_inbox_claim",
        ("pipeline_name", "status", "available_at", "received_at", "id"),
        predicate_sha256=(
            "9e53978fc33ba59a2cf9a36c367398b4831f503cdc9c9bb39a07f372b3bb8673"
        ),
    ),
    _expected_nonconstraint_index(
        "event_inbox",
        "ix_event_inbox_expired_lease",
        ("lease_until", "id"),
        predicate_sha256=(
            "0d8854756b3b05c4ae1bb96a0493add03c0099df50cfb267871e9b5368f7c54c"
        ),
    ),
    _expected_nonconstraint_index(
        "pipeline_ownership",
        "uq_pipeline_current_ingress",
        ("account_id",),
        predicate_sha256=(
            "35cc984d6efa9a70d61a14ab89954b9170d47cbb4531bcfda034203fc85c6ebd"
        ),
        unique=True,
    ),
    _expected_nonconstraint_index(
        "pipeline_shadow_comparisons",
        "ix_pipeline_shadow_pending",
        ("comparison_status", "created_at", "id"),
        predicate_sha256=(
            "7794b997301bccde547fa3dfc24f52827ea050d642d9f0a836e82dfb9e2e12e3"
        ),
    ),
    _expected_nonconstraint_index(
        "sync_cursors",
        "ix_sync_cursors_status_attempt",
        ("status", "last_attempt_at"),
    ),
}

EXPECTED_TRIGGER_FUNCTION_SHA256 = {
    "enforce_email_processing_owner": (
        "2541b4aa295b0208f98c9b93f400a6b1e1ba7ad1fda85be061129ff6345e2d5d"
    ),
    "guard_event_inbox_update": (
        "05035f2fd586fe1ed0ce59a329589f7d2322abab78f4afa8ee5ea0c5669d7002"
    ),
    "guard_pipeline_ownership": (
        "c898a988c2bfca60837cda5ce37ef8cdb00fd12312c312b3ceecd90b3356fc5c"
    ),
    "guard_pipeline_shadow_comparison": (
        "9026616ac400120ca6bb46c309580f5f7404b929d3b9766ea37f22955a21810c"
    ),
    "reject_audit_events_mutation": (
        "5ba2612faea4adf49b92395f87102f166df17b65aa64bf3f42ab5172bf375c5b"
    ),
}

EXPECTED_FOREIGN_KEYS = {
    (
        "fk_event_inbox_pipeline_ownership",
        "event_inbox",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
        "r",
        "r",
        False,
        False,
        True,
    ),
    (
        "fk_emails_pipeline_ownership",
        "emails",
        ("account_id", "owner_generation", "owner_fencing_token"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token"),
        "f",
        "r",
        "r",
        False,
        False,
        True,
    ),
    (
        "fk_emails_processing_inbox",
        "emails",
        (
            "processing_inbox_id",
            "account_id",
            "external_email_id",
            "owner_generation",
            "owner_fencing_token",
        ),
        "event_inbox",
        ("id", "account_id", "external_email_id", "generation", "fencing_token"),
        "s",
        "r",
        "r",
        False,
        False,
        True,
    ),
    (
        "fk_audit_events_email",
        "audit_events",
        ("account_id", "email_id"),
        "emails",
        ("account_id", "id"),
        "s",
        "r",
        "r",
        False,
        False,
        True,
    ),
    (
        "fk_pipeline_shadow_ownership",
        "pipeline_shadow_comparisons",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
        "r",
        "r",
        False,
        False,
        True,
    ),
}

EXPECTED_USER_TRIGGERS = {
    (
        "trg_pipeline_ownership_guard_row",
        "pipeline_ownership",
        "guard_pipeline_ownership",
        31,
        False,
        "O",
    ),
    (
        "trg_pipeline_ownership_guard_truncate",
        "pipeline_ownership",
        "guard_pipeline_ownership",
        34,
        False,
        "O",
    ),
    (
        "trg_event_inbox_guard_update",
        "event_inbox",
        "guard_event_inbox_update",
        19,
        False,
        "O",
    ),
    (
        "trg_emails_processing_owner",
        "emails",
        "enforce_email_processing_owner",
        21,
        True,
        "O",
    ),
    (
        "trg_audit_events_guard_row",
        "audit_events",
        "reject_audit_events_mutation",
        27,
        False,
        "O",
    ),
    (
        "trg_audit_events_guard_truncate",
        "audit_events",
        "reject_audit_events_mutation",
        34,
        False,
        "O",
    ),
    (
        "trg_pipeline_shadow_guard_row",
        "pipeline_shadow_comparisons",
        "guard_pipeline_shadow_comparison",
        27,
        False,
        "O",
    ),
    (
        "trg_pipeline_shadow_guard_truncate",
        "pipeline_shadow_comparisons",
        "guard_pipeline_shadow_comparison",
        34,
        False,
        "O",
    ),
}


def _insert_generation(
    db,
    *,
    account_id: int = 8,
    generation: int = 1,
    state: str = "current_ingress",
    fencing_token: int | None = None,
    pipeline_name: str = "durable_v1",
) -> None:
    db.execute(
        "INSERT INTO pipeline_ownership ("
        "account_id, generation, pipeline_name, state, fencing_token, "
        "created_by, reason) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (
            account_id,
            generation,
            pipeline_name,
            state,
            fencing_token or generation,
            "schema-test",
            "constraint-proof",
        ),
    )


def _insert_inbox(
    db,
    *,
    inbox_id: str | None = None,
    external_email_id: str = "message-1",
    dedupe_key: str = "a" * 64,
    attempts: int = 0,
    status: str = "pending",
    generation: int = 1,
    fencing_token: int = 1,
    pipeline_name: str = "durable_v1",
    lease_owner: str | None = None,
    lease_until: str | None = None,
    safe_error_code: str | None = None,
    safe_error_summary: str | None = None,
    effect_started_at: str | None = None,
    processing_started_at: str | None = None,
    change_kind: str = "create",
) -> str:
    resolved_id = inbox_id or str(uuid4())
    db.execute(
        "INSERT INTO event_inbox ("
        "id, account_id, external_email_id, folder_key, source, "
        "raw_event_type, change_kind, dedupe_key, payload, "
        "processing_policy, pipeline_name, generation, fencing_token, "
        "status, lease_owner, lease_until, attempts, safe_error_code, "
        "safe_error_summary, effect_started_at, processing_started_at) "
        "VALUES (%s, 8, %s, 'inbox', 'webhook', 'NewMailEvent', %s, %s, "
        "'{}'::pg_catalog.jsonb, 'full', %s, %s, %s, %s, %s, "
        "%s::pg_catalog.timestamptz, %s, %s, %s, "
        "%s::pg_catalog.timestamptz, %s::pg_catalog.timestamptz)",
        (
            resolved_id,
            external_email_id,
            change_kind,
            dedupe_key,
            pipeline_name,
            generation,
            fencing_token,
            status,
            lease_owner,
            lease_until,
            attempts,
            safe_error_code,
            safe_error_summary,
            effect_started_at,
            processing_started_at,
        ),
    )
    return resolved_id


@pytest.mark.integration
def test_durable_ingestion_head_creates_exact_relation_set(db):
    assert db.scalar("SELECT version_num FROM alembic_version") == "20260710_0003"
    relations = set(
        db.scalar(
            "SELECT COALESCE("
            "pg_catalog.array_agg(relation.relname::pg_catalog.text "
            "ORDER BY relation.relname), ARRAY[]::pg_catalog.text[]) "
            "FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS relation_schema "
            "ON relation_schema.oid = relation.relnamespace "
            "WHERE relation_schema.nspname = pg_catalog.current_schema() "
            "AND relation.relkind IN ('r', 'p', 'v', 'm')"
        )
    )
    assert relations == EXPECTED_RELATIONS


@pytest.mark.integration
def test_phase2_schema_matches_test_owned_exact_manifest(db):
    phase2_relations = sorted(DURABLE_TABLES)
    with psycopg.connect(db.dsn, autocommit=True) as conn:
        relation_contracts = {
            relation_name: (
                relation_kind,
                persistence,
                row_security_enabled,
                row_security_forced,
                has_no_policies,
                access_method,
            )
            for (
                relation_name,
                relation_kind,
                persistence,
                row_security_enabled,
                row_security_forced,
                has_no_policies,
                access_method,
            ) in conn.execute(
                "SELECT relation.relname::pg_catalog.text, "
                "relation.relkind::pg_catalog.text, "
                "relation.relpersistence::pg_catalog.text, "
                "relation.relrowsecurity, relation.relforcerowsecurity, "
                "NOT EXISTS (SELECT 1 FROM pg_catalog.pg_policy AS policy "
                "WHERE policy.polrelid = relation.oid), "
                "access_method.amname::pg_catalog.text "
                "FROM pg_catalog.pg_class AS relation "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "LEFT JOIN pg_catalog.pg_am AS access_method "
                "ON access_method.oid = relation.relam "
                "WHERE relation_schema.nspname = pg_catalog.current_schema() "
                "AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')"
            ).fetchall()
        }
        defaults = {
            (relation_name, column_name): expression
            for relation_name, column_name, expression in conn.execute(
                "SELECT relation.relname::pg_catalog.text, "
                "attribute.attname::pg_catalog.text, "
                "pg_catalog.pg_get_expr("
                "default_value.adbin, default_value.adrelid, true"
                ")::pg_catalog.text "
                "FROM pg_catalog.pg_attrdef AS default_value "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = default_value.adrelid "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "JOIN pg_catalog.pg_attribute AS attribute "
                "ON attribute.attrelid = relation.oid "
                "AND attribute.attnum = default_value.adnum "
                "WHERE relation_schema.nspname = pg_catalog.current_schema() "
                "AND relation.relname = ANY(%s::pg_catalog.text[])",
                (phase2_relations,),
            ).fetchall()
        }
        checks = {
            (relation_name, constraint_name): (
                expression_sha256,
                is_validated,
                is_no_inherit,
            )
            for (
                relation_name,
                constraint_name,
                expression_sha256,
                is_validated,
                is_no_inherit,
            ) in conn.execute(
                "SELECT relation.relname::pg_catalog.text, "
                "constraint_record.conname::pg_catalog.text, "
                "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to("
                "pg_catalog.pg_get_expr(constraint_record.conbin, "
                "constraint_record.conrelid, true), 'UTF8')), "
                "'hex')::pg_catalog.text, "
                "constraint_record.convalidated, "
                "constraint_record.connoinherit "
                "FROM pg_catalog.pg_constraint AS constraint_record "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = constraint_record.conrelid "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "WHERE relation_schema.nspname = pg_catalog.current_schema() "
                "AND relation.relname = ANY(%s::pg_catalog.text[]) "
                "AND constraint_record.contype = 'c'",
                (phase2_relations,),
            ).fetchall()
        }
        unique_indexes = {
            (
                relation_name,
                index_name,
                constraint_name,
                constraint_kind,
                tuple(columns),
                tuple(index_options),
                predicate_sha256,
                nulls_not_distinct,
                is_deferrable,
                is_deferred,
                is_constraint_validated,
                is_index_valid,
                is_index_ready,
                access_method,
                has_no_included_columns,
                has_only_plain_columns,
                uses_default_opclasses,
                uses_default_collations,
            )
            for (
                relation_name,
                index_name,
                constraint_name,
                constraint_kind,
                columns,
                index_options,
                predicate_sha256,
                nulls_not_distinct,
                is_deferrable,
                is_deferred,
                is_constraint_validated,
                is_index_valid,
                is_index_ready,
                access_method,
                has_no_included_columns,
                has_only_plain_columns,
                uses_default_opclasses,
                uses_default_collations,
            ) in conn.execute(
                "SELECT relation.relname::pg_catalog.text, "
                "index_relation.relname::pg_catalog.text, "
                "backing_constraint.conname::pg_catalog.text, "
                "backing_constraint.contype::pg_catalog.text, "
                "ARRAY(SELECT attribute.attname::pg_catalog.text "
                "FROM pg_catalog.unnest("
                "index_metadata.indkey::pg_catalog.int2[]"
                ") WITH ORDINALITY AS key_column(attnum, position) "
                "JOIN pg_catalog.pg_attribute AS attribute "
                "ON attribute.attrelid = relation.oid "
                "AND attribute.attnum = key_column.attnum "
                "WHERE key_column.position <= index_metadata.indnkeyatts "
                "ORDER BY key_column.position), "
                "index_metadata.indoption::pg_catalog.int2[], "
                "CASE WHEN index_metadata.indpred IS NULL THEN NULL ELSE "
                "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to("
                "pg_catalog.pg_get_expr(index_metadata.indpred, "
                "index_metadata.indrelid, true), 'UTF8')), "
                "'hex')::pg_catalog.text END, "
                "index_metadata.indnullsnotdistinct, "
                "backing_constraint.condeferrable, "
                "backing_constraint.condeferred, "
                "backing_constraint.convalidated, "
                "index_metadata.indisvalid, index_metadata.indisready, "
                "access_method.amname::pg_catalog.text, "
                "index_metadata.indnatts = index_metadata.indnkeyatts, "
                "index_metadata.indnkeyatts = (SELECT pg_catalog.count(*) "
                "FROM pg_catalog.unnest("
                "index_metadata.indkey::pg_catalog.int2[]"
                ") WITH ORDINALITY AS key_column(attnum, position) "
                "WHERE key_column.position <= index_metadata.indnkeyatts "
                "AND key_column.attnum > 0), "
                "(SELECT pg_catalog.bool_and(operator_class.opcdefault) "
                "FROM pg_catalog.unnest("
                "index_metadata.indclass::pg_catalog.oid[]"
                ") WITH ORDINALITY AS indexed_opclass(opclass_oid, position) "
                "JOIN pg_catalog.pg_opclass AS operator_class "
                "ON operator_class.oid = indexed_opclass.opclass_oid "
                "WHERE indexed_opclass.position <= index_metadata.indnkeyatts), "
                "(SELECT pg_catalog.bool_and("
                "indexed_collation.collation_oid = attribute.attcollation"
                ") FROM pg_catalog.unnest("
                "index_metadata.indcollation::pg_catalog.oid[]"
                ") WITH ORDINALITY AS indexed_collation(collation_oid, position) "
                "JOIN pg_catalog.unnest("
                "index_metadata.indkey::pg_catalog.int2[]"
                ") WITH ORDINALITY AS key_column(attnum, position) "
                "ON key_column.position = indexed_collation.position "
                "JOIN pg_catalog.pg_attribute AS attribute "
                "ON attribute.attrelid = relation.oid "
                "AND attribute.attnum = key_column.attnum "
                "WHERE indexed_collation.position "
                "<= index_metadata.indnkeyatts) "
                "FROM pg_catalog.pg_index AS index_metadata "
                "JOIN pg_catalog.pg_class AS index_relation "
                "ON index_relation.oid = index_metadata.indexrelid "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = index_metadata.indrelid "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "JOIN pg_catalog.pg_am AS access_method "
                "ON access_method.oid = index_relation.relam "
                "LEFT JOIN pg_catalog.pg_constraint AS backing_constraint "
                "ON backing_constraint.conindid = index_relation.oid "
                "AND backing_constraint.contype IN ('p', 'u') "
                "WHERE relation_schema.nspname = pg_catalog.current_schema() "
                "AND relation.relname = ANY(%s::pg_catalog.text[]) "
                "AND index_metadata.indisunique",
                (phase2_relations,),
            ).fetchall()
        }
        nonconstraint_indexes = {
            (
                relation_name,
                index_name,
                is_unique,
                is_valid,
                is_ready,
                access_method,
                tuple(columns),
                tuple(index_options),
                predicate_sha256,
                has_no_included_columns,
                has_only_plain_columns,
                uses_default_opclasses,
                uses_default_collations,
            )
            for (
                relation_name,
                index_name,
                is_unique,
                is_valid,
                is_ready,
                access_method,
                columns,
                index_options,
                predicate_sha256,
                has_no_included_columns,
                has_only_plain_columns,
                uses_default_opclasses,
                uses_default_collations,
            ) in conn.execute(
                "SELECT relation.relname::pg_catalog.text, "
                "index_relation.relname::pg_catalog.text, "
                "index_metadata.indisunique, index_metadata.indisvalid, "
                "index_metadata.indisready, "
                "access_method.amname::pg_catalog.text, "
                "ARRAY(SELECT attribute.attname::pg_catalog.text "
                "FROM pg_catalog.unnest("
                "index_metadata.indkey::pg_catalog.int2[]"
                ") WITH ORDINALITY AS key_column(attnum, position) "
                "JOIN pg_catalog.pg_attribute AS attribute "
                "ON attribute.attrelid = relation.oid "
                "AND attribute.attnum = key_column.attnum "
                "WHERE key_column.position <= index_metadata.indnkeyatts "
                "ORDER BY key_column.position), "
                "index_metadata.indoption::pg_catalog.int2[], "
                "CASE WHEN index_metadata.indpred IS NULL THEN NULL ELSE "
                "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to("
                "pg_catalog.pg_get_expr(index_metadata.indpred, "
                "index_metadata.indrelid, true), 'UTF8')), "
                "'hex')::pg_catalog.text END, "
                "index_metadata.indnatts = index_metadata.indnkeyatts, "
                "index_metadata.indnkeyatts = (SELECT pg_catalog.count(*) "
                "FROM pg_catalog.unnest("
                "index_metadata.indkey::pg_catalog.int2[]"
                ") WITH ORDINALITY AS key_column(attnum, position) "
                "WHERE key_column.position <= index_metadata.indnkeyatts "
                "AND key_column.attnum > 0), "
                "(SELECT pg_catalog.bool_and(operator_class.opcdefault) "
                "FROM pg_catalog.unnest("
                "index_metadata.indclass::pg_catalog.oid[]"
                ") WITH ORDINALITY AS indexed_opclass(opclass_oid, position) "
                "JOIN pg_catalog.pg_opclass AS operator_class "
                "ON operator_class.oid = indexed_opclass.opclass_oid "
                "WHERE indexed_opclass.position <= index_metadata.indnkeyatts), "
                "(SELECT pg_catalog.bool_and("
                "indexed_collation.collation_oid = attribute.attcollation"
                ") FROM pg_catalog.unnest("
                "index_metadata.indcollation::pg_catalog.oid[]"
                ") WITH ORDINALITY AS indexed_collation(collation_oid, position) "
                "JOIN pg_catalog.unnest("
                "index_metadata.indkey::pg_catalog.int2[]"
                ") WITH ORDINALITY AS key_column(attnum, position) "
                "ON key_column.position = indexed_collation.position "
                "JOIN pg_catalog.pg_attribute AS attribute "
                "ON attribute.attrelid = relation.oid "
                "AND attribute.attnum = key_column.attnum "
                "WHERE indexed_collation.position "
                "<= index_metadata.indnkeyatts) "
                "FROM pg_catalog.pg_index AS index_metadata "
                "JOIN pg_catalog.pg_class AS index_relation "
                "ON index_relation.oid = index_metadata.indexrelid "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = index_metadata.indrelid "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "JOIN pg_catalog.pg_am AS access_method "
                "ON access_method.oid = index_relation.relam "
                "LEFT JOIN pg_catalog.pg_constraint AS backing_constraint "
                "ON backing_constraint.conindid = index_relation.oid "
                "AND backing_constraint.contype IN ('p', 'u', 'x') "
                "WHERE relation_schema.nspname = pg_catalog.current_schema() "
                "AND relation.relname = ANY(%s::pg_catalog.text[]) "
                "AND backing_constraint.oid IS NULL",
                (phase2_relations,),
            ).fetchall()
        }

    assert relation_contracts == EXPECTED_RELATION_CONTRACTS
    assert defaults == EXPECTED_PHASE2_DEFAULTS
    assert checks == {
        key: (expression_sha256, True, False)
        for key, expression_sha256 in EXPECTED_PHASE2_CHECKS.items()
    }
    assert unique_indexes == EXPECTED_PHASE2_UNIQUE_INDEXES
    assert nonconstraint_indexes == EXPECTED_PHASE2_NONCONSTRAINT_INDEXES


@pytest.mark.integration
def test_0002_to_0003_is_expand_only_and_leaves_all_new_tables_empty(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    alembic_runner.upgrade(schema, "20260710_0002")
    schema.execute(
        "INSERT INTO emails_log (id, subject, status) "
        "VALUES ('expand-sentinel', 'preserve-me', 'sent')"
    )
    schema.execute(
        "INSERT INTO app_kv_store (key, value) "
        "VALUES ('expand-sentinel', 'preserve-me')"
    )
    legacy_columns_before = schema.scalar(
        "SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array("
        "attribute.attname, column_type.typname, attribute.attnotnull) "
        "ORDER BY attribute.attnum) "
        "FROM pg_catalog.pg_attribute AS attribute "
        "JOIN pg_catalog.pg_class AS relation "
        "ON relation.oid = attribute.attrelid "
        "JOIN pg_catalog.pg_type AS column_type "
        "ON column_type.oid = attribute.atttypid "
        "WHERE relation.oid = 'emails_log'::pg_catalog.regclass "
        "AND attribute.attnum > 0 AND NOT attribute.attisdropped"
    )

    alembic_runner.upgrade(schema, "20260710_0003")

    assert schema.scalar("SELECT version_num FROM alembic_version") == ("20260710_0003")
    assert (
        schema.scalar("SELECT subject FROM emails_log WHERE id = 'expand-sentinel'")
        == "preserve-me"
    )
    assert (
        schema.scalar("SELECT value FROM app_kv_store WHERE key = 'expand-sentinel'")
        == "preserve-me"
    )
    assert (
        schema.scalar("SELECT id FROM processed_emails WHERE id = 'expand-sentinel'")
        == "expand-sentinel"
    )
    assert (
        schema.scalar(
            "SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array("
            "attribute.attname, column_type.typname, attribute.attnotnull) "
            "ORDER BY attribute.attnum) "
            "FROM pg_catalog.pg_attribute AS attribute "
            "JOIN pg_catalog.pg_class AS relation "
            "ON relation.oid = attribute.attrelid "
            "JOIN pg_catalog.pg_type AS column_type "
            "ON column_type.oid = attribute.atttypid "
            "WHERE relation.oid = 'emails_log'::pg_catalog.regclass "
            "AND attribute.attnum > 0 AND NOT attribute.attisdropped"
        )
        == legacy_columns_before
    )
    for relation in DURABLE_TABLES:
        assert (
            schema.scalar(
                sql.SQL("SELECT pg_catalog.count(*) FROM {}").format(
                    sql.Identifier(relation)
                )
            )
            == 0
        )


@pytest.mark.integration
def test_0003_conflicting_relation_fails_and_rolls_back_entire_revision(
    postgres_database_factory,
    alembic_runner,
):
    schema = postgres_database_factory()
    alembic_runner.upgrade(schema, "20260710_0002")
    schema.execute(
        "CREATE TABLE pipeline_ownership (sentinel pg_catalog.text NOT NULL)"
    )
    schema.execute("INSERT INTO pipeline_ownership VALUES ('preserve-me')")

    with pytest.raises(DBAPIError):
        alembic_runner.upgrade(schema, "20260710_0003")

    assert schema.scalar("SELECT version_num FROM alembic_version") == ("20260710_0002")
    assert schema.scalar("SELECT sentinel FROM pipeline_ownership") == "preserve-me"
    for relation in DURABLE_TABLES - {"pipeline_ownership"}:
        assert not schema.table_exists(relation)


@pytest.mark.integration
def test_durable_column_type_nullability_and_default_manifest_is_exact(db):
    rows = db.scalar(
        "SELECT COALESCE(pg_catalog.jsonb_object_agg("
        "relation.relname || '.' || attribute.attname, "
        "pg_catalog.jsonb_build_object("
        "'type_schema', type_schema.nspname, 'type_name', column_type.typname, "
        "'nullable', NOT attribute.attnotnull, "
        "'has_default', default_value.adbin IS NOT NULL, "
        "'typmod', attribute.atttypmod, "
        "'identity', attribute.attidentity, "
        "'generated', attribute.attgenerated, "
        "'uses_type_collation', "
        "attribute.attcollation = column_type.typcollation, "
        "'uses_type_storage', attribute.attstorage = column_type.typstorage, "
        "'compression', attribute.attcompression)), "
        "'{}'::pg_catalog.jsonb) "
        "FROM pg_catalog.pg_class AS relation "
        "JOIN pg_catalog.pg_namespace AS relation_schema "
        "ON relation_schema.oid = relation.relnamespace "
        "JOIN pg_catalog.pg_attribute AS attribute "
        "ON attribute.attrelid = relation.oid "
        "JOIN pg_catalog.pg_type AS column_type "
        "ON column_type.oid = attribute.atttypid "
        "JOIN pg_catalog.pg_namespace AS type_schema "
        "ON type_schema.oid = column_type.typnamespace "
        "LEFT JOIN pg_catalog.pg_attrdef AS default_value "
        "ON default_value.adrelid = relation.oid "
        "AND default_value.adnum = attribute.attnum "
        "WHERE relation_schema.nspname = pg_catalog.current_schema() "
        "AND relation.relname = ANY(%s::pg_catalog.text[]) "
        "AND attribute.attnum > 0 AND NOT attribute.attisdropped",
        (sorted(DURABLE_TABLES),),
    )
    assert set(rows) == set(EXPECTED_COLUMN_TYPES)
    for column, expected_type in EXPECTED_COLUMN_TYPES.items():
        metadata = rows[column]
        assert metadata["type_schema"] == "pg_catalog"
        assert metadata["type_name"] == expected_type
        assert metadata["nullable"] is (column in NULLABLE_COLUMNS)
        assert metadata["has_default"] is (column in DEFAULTED_COLUMNS)
        assert metadata["identity"] == ""
        assert metadata["generated"] == ""
        assert metadata["uses_type_collation"] is True
        assert metadata["uses_type_storage"] is True
        assert metadata["compression"] == ""
        if expected_type == "bpchar":
            assert metadata["typmod"] == 68


@pytest.mark.integration
def test_trigger_function_sources_match_revision_manifest(db):
    actual = db.scalar(
        "SELECT pg_catalog.jsonb_object_agg("
        "routine.proname, pg_catalog.encode("
        "pg_catalog.sha256(pg_catalog.convert_to(routine.prosrc, 'UTF8')), "
        "'hex')) "
        "FROM pg_catalog.pg_proc AS routine "
        "JOIN pg_catalog.pg_namespace AS routine_schema "
        "ON routine_schema.oid = routine.pronamespace "
        "WHERE routine_schema.nspname = pg_catalog.current_schema() "
        "AND routine.prorettype = "
        "'pg_catalog.trigger'::pg_catalog.regtype"
    )

    assert actual == EXPECTED_TRIGGER_FUNCTION_SHA256


@pytest.mark.integration
def test_foreign_key_and_user_trigger_catalogs_match_independent_contract(db):
    with psycopg.connect(db.dsn, autocommit=True) as conn:
        foreign_keys = {
            (
                row[0],
                row[1],
                tuple(row[2]),
                row[3],
                tuple(row[4]),
                *row[5:],
            )
            for row in conn.execute(
                "SELECT foreign_key.conname::pg_catalog.text, "
                "child.relname::pg_catalog.text, "
                "ARRAY(SELECT attribute.attname::pg_catalog.text "
                "FROM pg_catalog.unnest(foreign_key.conkey) WITH ORDINALITY "
                "AS key_column(attnum, position) "
                "JOIN pg_catalog.pg_attribute AS attribute "
                "ON attribute.attrelid = child.oid "
                "AND attribute.attnum = key_column.attnum "
                "ORDER BY key_column.position), "
                "parent.relname::pg_catalog.text, "
                "ARRAY(SELECT attribute.attname::pg_catalog.text "
                "FROM pg_catalog.unnest(foreign_key.confkey) WITH ORDINALITY "
                "AS key_column(attnum, position) "
                "JOIN pg_catalog.pg_attribute AS attribute "
                "ON attribute.attrelid = parent.oid "
                "AND attribute.attnum = key_column.attnum "
                "ORDER BY key_column.position), "
                "foreign_key.confmatchtype::pg_catalog.text, "
                "foreign_key.confupdtype::pg_catalog.text, "
                "foreign_key.confdeltype::pg_catalog.text, "
                "foreign_key.condeferrable, foreign_key.condeferred, "
                "foreign_key.convalidated "
                "FROM pg_catalog.pg_constraint AS foreign_key "
                "JOIN pg_catalog.pg_class AS child "
                "ON child.oid = foreign_key.conrelid "
                "JOIN pg_catalog.pg_class AS parent "
                "ON parent.oid = foreign_key.confrelid "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = child.relnamespace "
                "WHERE relation_schema.nspname = pg_catalog.current_schema() "
                "AND foreign_key.contype = 'f'"
            ).fetchall()
        }
        user_triggers = {
            tuple(row)
            for row in conn.execute(
                "SELECT trigger.tgname::pg_catalog.text, "
                "relation.relname::pg_catalog.text, "
                "routine.proname::pg_catalog.text, trigger.tgtype, "
                "trigger.tgconstraint <> 0, "
                "trigger.tgenabled::pg_catalog.text "
                "FROM pg_catalog.pg_trigger AS trigger "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid = trigger.tgrelid "
                "JOIN pg_catalog.pg_proc AS routine "
                "ON routine.oid = trigger.tgfoid "
                "JOIN pg_catalog.pg_namespace AS relation_schema "
                "ON relation_schema.oid = relation.relnamespace "
                "WHERE relation_schema.nspname = pg_catalog.current_schema() "
                "AND NOT trigger.tgisinternal"
            ).fetchall()
        }

    assert foreign_keys == EXPECTED_FOREIGN_KEYS
    assert user_triggers == EXPECTED_USER_TRIGGERS


@pytest.mark.integration
def test_dto_payload_at_database_limit_round_trips(db):
    _insert_generation(db)
    payload = {"blob": "x" * (MAX_INBOX_PAYLOAD_BYTES - len('{"blob": ""}'))}
    event = NormalizedIngressEvent(
        account_id=8,
        source="webhook",
        raw_event_type="NewMailEvent",
        kind="create",
        external_email_id="payload-boundary-message",
        folder="inbox",
        source_version=None,
        dedupe_key="9" * 64,
        payload=payload,
    )
    encoded = json.dumps(
        dict(event.payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(", ", ": "),
    )
    assert len(encoded.encode("utf-8")) == MAX_INBOX_PAYLOAD_BYTES

    db.execute(
        "INSERT INTO event_inbox ("
        "id, account_id, external_email_id, folder_key, source, "
        "raw_event_type, change_kind, dedupe_key, payload, processing_policy, "
        "pipeline_name, generation, fencing_token, status) VALUES ("
        "%s, 8, %s, 'inbox', 'webhook', 'NewMailEvent', 'create', %s, "
        "%s::pg_catalog.jsonb, 'full', 'durable_v1', 1, 1, 'pending')",
        (
            str(uuid4()),
            event.external_email_id,
            event.dedupe_key,
            encoded,
        ),
    )


@pytest.mark.integration
def test_one_current_ingress_per_account_and_unique_fence(db):
    _insert_generation(db)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _insert_generation(db, generation=2)

    db.execute("UPDATE pipeline_ownership SET state = 'quiescing'")
    db.execute("UPDATE pipeline_ownership SET state = 'draining'")
    _insert_generation(db, generation=2, fencing_token=2)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _insert_generation(db, account_id=8, generation=3, fencing_token=2)


@pytest.mark.integration
def test_ownership_rejects_non_current_insert_and_state_skip(db):
    with pytest.raises(psycopg.errors.RaiseException):
        _insert_generation(db, state="quiescing")

    _insert_generation(db)
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE pipeline_ownership SET state = 'draining'")


@pytest.mark.integration
def test_ownership_identity_history_and_retired_state_are_immutable(db):
    _insert_generation(db)
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE pipeline_ownership SET fencing_token = 2")
    db.execute("UPDATE pipeline_ownership SET state = 'quiescing'")
    db.execute("UPDATE pipeline_ownership SET state = 'draining'")
    db.execute("UPDATE pipeline_ownership SET state = 'retired'")
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE pipeline_ownership SET state = 'retired'")
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM pipeline_ownership")
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("TRUNCATE pipeline_ownership CASCADE")


@pytest.mark.integration
def test_inbox_dedupe_and_exact_ownership_are_enforced(db):
    _insert_generation(db)
    _insert_inbox(db)
    with pytest.raises(psycopg.errors.UniqueViolation):
        _insert_inbox(db)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        _insert_inbox(db, dedupe_key="b" * 64, fencing_token=2)
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        _insert_inbox(db, dedupe_key="c" * 64, pipeline_name="other")


@pytest.mark.integration
@pytest.mark.parametrize(
    "mutation",
    [
        "invalid_inbox_status",
        "negative_attempts",
        "invalid_dedupe_hash",
        "leased_without_holder",
        "pending_with_owner_only",
        "completed_with_expiry_only",
        "retry_without_error",
        "retry_with_blank_summary",
        "summary_without_code",
        "effect_without_processing",
    ],
)
def test_inbox_rejects_invalid_status_lease_error_and_effect_matrix(db, mutation):
    _insert_generation(db)
    with pytest.raises(psycopg.errors.CheckViolation):
        if mutation == "invalid_inbox_status":
            _insert_inbox(db, status="unknown")
        elif mutation == "negative_attempts":
            _insert_inbox(db, attempts=-1)
        elif mutation == "invalid_dedupe_hash":
            _insert_inbox(db, dedupe_key="not-a-sha256")
        elif mutation == "leased_without_holder":
            _insert_inbox(db, status="leased")
        elif mutation == "pending_with_owner_only":
            _insert_inbox(db, lease_owner="stale-worker")
        elif mutation == "completed_with_expiry_only":
            _insert_inbox(
                db,
                status="completed",
                lease_until="2026-07-12T08:00:00Z",
            )
        elif mutation == "retry_without_error":
            _insert_inbox(db, status="retry_wait")
        elif mutation == "retry_with_blank_summary":
            _insert_inbox(
                db,
                status="retry_wait",
                safe_error_code="temporary_failure",
                safe_error_summary="   ",
            )
        elif mutation == "summary_without_code":
            _insert_inbox(db, safe_error_summary="unsafe")
        else:
            _insert_inbox(
                db,
                status="manual_review",
                safe_error_code="manual_review_required",
                effect_started_at="2026-07-12T08:00:00Z",
            )


@pytest.mark.integration
def test_email_owner_and_processing_inbox_are_sticky_and_tenant_bound(db):
    _insert_generation(db)
    inbox_id = _insert_inbox(db)
    email_id = str(uuid4())
    db.execute(
        "INSERT INTO emails ("
        "id, account_id, external_email_id, source_folder_key, status, "
        "owner_generation, owner_fencing_token, processing_inbox_id, "
        "is_read_refresh_required) "
        "VALUES (%s, 8, 'message-1', 'inbox', 'processing', 1, 1, %s, true)",
        (email_id, inbox_id),
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            "UPDATE emails SET status = 'retry_wait', "
            "safe_error_code = 'temporary_failure', safe_error_summary = '   ' "
            "WHERE id = %s",
            (email_id,),
        )
    with pytest.raises(
        (psycopg.errors.RaiseException, psycopg.errors.ForeignKeyViolation)
    ):
        db.execute(
            "UPDATE emails SET owner_generation = 2 WHERE id = %s",
            (email_id,),
        )
    noncreate_inbox_id = _insert_inbox(
        db,
        inbox_id=str(uuid4()),
        external_email_id="message-2",
        dedupe_key="c" * 64,
        change_kind="read",
    )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute(
            "INSERT INTO emails ("
            "id, account_id, external_email_id, source_folder_key, status, "
            "owner_generation, owner_fencing_token, processing_inbox_id, "
            "is_read_refresh_required) VALUES ("
            "%s, 8, 'message-2', 'inbox', 'processing', 1, 1, %s, true)",
            (str(uuid4()), noncreate_inbox_id),
        )

    replacement = _insert_inbox(
        db,
        dedupe_key="b" * 64,
        inbox_id=str(uuid4()),
    )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute(
            "UPDATE emails SET processing_inbox_id = %s WHERE id = %s",
            (replacement, email_id),
        )
    db.execute(
        "UPDATE emails SET create_seen_at = pg_catalog.now(), "
        "processing_started_at = pg_catalog.now(), "
        "source_deleted_at = pg_catalog.now(), "
        "external_effects_started_at = pg_catalog.now() WHERE id = %s",
        (email_id,),
    )
    for marker in (
        "create_seen_at",
        "processing_started_at",
        "source_deleted_at",
        "external_effects_started_at",
    ):
        with pytest.raises(psycopg.errors.RaiseException):
            db.execute(
                f"UPDATE emails SET {marker} = NULL WHERE id = %s",
                (email_id,),
            )
        with pytest.raises(psycopg.errors.RaiseException):
            db.execute(
                f"UPDATE emails SET {marker} = {marker} + interval '1 second' "
                "WHERE id = %s",
                (email_id,),
            )
    db.execute(
        "UPDATE emails SET status = 'sent', processing_inbox_id = NULL WHERE id = %s",
        (email_id,),
    )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute(
            "UPDATE emails SET status = 'ingested' WHERE id = %s",
            (email_id,),
        )
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db.execute(
            "INSERT INTO emails ("
            "id, account_id, external_email_id, source_folder_key, status, "
            "owner_generation, owner_fencing_token, is_read_refresh_required) "
            "VALUES (%s, 8, 'message-2', 'inbox', 'ingested', 1, 2, true)",
            (str(uuid4()),),
        )


@pytest.mark.integration
def test_sync_cursor_state_matrix_is_fail_closed(db):
    db.execute(
        "INSERT INTO sync_cursors ("
        "account_id, folder_key, cursor, status, last_success_at) "
        "VALUES (8, 'inbox', 'cursor-1', 'active', pg_catalog.now())"
    )
    db.execute(
        "INSERT INTO sync_cursors (account_id, folder_key, cursor, status, "
        "blocked_reason_code, last_attempt_at) VALUES ("
        "8, 'reset', 'cursor-old', 'reset_required', 'cursor_invalid', "
        "pg_catalog.now())"
    )
    db.execute(
        "INSERT INTO sync_cursors (account_id, folder_key, status, "
        "blocked_reason_code) VALUES ("
        "8, 'cold', 'cold_start_pending', 'stateless_contract_required')"
    )
    db.execute(
        "INSERT INTO sync_cursors (account_id, folder_key, cursor, status, "
        "blocked_reason_code, contract_fingerprint, blocked_at) VALUES ("
        "8, 'blocked', 'cursor-old', 'blocked_contract', "
        "'contract_mismatch', %s, pg_catalog.now())",
        ("f" * 64,),
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            "INSERT INTO sync_cursors (account_id, folder_key, status) "
            "VALUES (8, 'missing-cursor', 'active')"
        )
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            "INSERT INTO sync_cursors ("
            "account_id, folder_key, cursor, status, blocked_reason_code) "
            "VALUES (8, 'bad-reset', 'cursor-1', 'reset_required', NULL)"
        )
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            "INSERT INTO sync_cursors (account_id, folder_key, cursor, status, "
            "last_success_at, last_attempt_at) VALUES ("
            "8, 'time-regression', 'cursor-1', 'active', "
            "'2026-07-12T09:00:00Z', '2026-07-12T08:00:00Z')"
        )


@pytest.mark.integration
def test_audit_history_is_append_only(db):
    _insert_generation(db)
    email_id = str(uuid4())
    db.execute(
        "INSERT INTO emails ("
        "id, account_id, external_email_id, source_folder_key, status, "
        "owner_generation, owner_fencing_token, is_read_refresh_required) "
        "VALUES (%s, 8, 'message-1', 'inbox', 'ingested', 1, 1, true)",
        (email_id,),
    )
    db.execute(
        "INSERT INTO audit_events ("
        "id, event_key, account_id, email_id, object_type, "
        "object_fingerprint, action, result, actor) "
        "VALUES (%s, %s, 8, %s, 'email', %s, 'ingested', 'success', 'test')",
        (str(uuid4()), "a" * 64, email_id, "b" * 64),
    )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE audit_events SET result = 'changed'")
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM audit_events")
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("TRUNCATE audit_events")


@pytest.mark.integration
def test_shadow_identity_and_decisions_are_immutable(db):
    _insert_generation(db, pipeline_name="legacy_compat")
    shadow_id = str(uuid4())
    db.execute(
        "INSERT INTO pipeline_shadow_comparisons ("
        "id, account_id, generation, fencing_token, pipeline_name, "
        "candidate_pipeline_name, candidate_build_id, candidate_config_hash, "
        "event_key, input_hash, legacy_status, shadow_status, "
        "comparison_status) VALUES ("
        "%s, 8, 1, 1, 'legacy_compat', 'durable_v1', 'build-1', %s, %s, %s, "
        "'pending', 'pending', 'pending')",
        (shadow_id, "a" * 64, "b" * 64, "c" * 64),
    )
    db.execute(
        "INSERT INTO pipeline_shadow_comparisons ("
        "id, account_id, generation, fencing_token, pipeline_name, "
        "candidate_pipeline_name, candidate_build_id, candidate_config_hash, "
        "event_key, input_hash, legacy_status, shadow_status, "
        "comparison_status) VALUES ("
        "%s, 8, 1, 1, 'legacy_compat', 'legacy_compat', 'build-2', %s, %s, %s, "
        "'pending', 'pending', 'pending')",
        (str(uuid4()), "d" * 64, "e" * 64, "f" * 64),
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(
            "INSERT INTO pipeline_shadow_comparisons ("
            "id, account_id, generation, fencing_token, pipeline_name, "
            "candidate_pipeline_name, candidate_build_id, candidate_config_hash, "
            "event_key, input_hash, legacy_status, shadow_status, "
            "comparison_status) SELECT %s, account_id, generation, "
            "fencing_token, pipeline_name, candidate_pipeline_name, "
            "candidate_build_id, candidate_config_hash, event_key, input_hash, "
            "legacy_status, shadow_status, comparison_status "
            "FROM pipeline_shadow_comparisons WHERE id = %s",
            (str(uuid4()), shadow_id),
        )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute(
            "UPDATE pipeline_shadow_comparisons SET input_hash = %s WHERE id = %s",
            ("d" * 64, shadow_id),
        )
    db.execute(
        "UPDATE pipeline_shadow_comparisons SET "
        "legacy_status = 'completed', legacy_decision_hash = %s, "
        "shadow_status = 'completed', shadow_decision_hash = %s, "
        "comparison_status = 'matched' WHERE id = %s",
        ("1" * 64, "1" * 64, shadow_id),
    )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute(
            "UPDATE pipeline_shadow_comparisons "
            "SET legacy_decision_hash = %s WHERE id = %s",
            ("2" * 64, shadow_id),
        )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute(
            "DELETE FROM pipeline_shadow_comparisons WHERE id = %s",
            (shadow_id,),
        )
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("TRUNCATE pipeline_shadow_comparisons")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fresh_bootstrap_reaches_0003_and_passes_both_role_gates(
    postgres_database_factory,
):
    schema = postgres_database_factory()

    summary = await bootstrap_database(schema.dsn, **schema.bootstrap_identity)

    assert summary["alembic"] == "20260710_0003"
    await require_migration_database_role(
        schema.dsn,
        **schema.bootstrap_identity,
    )
    await require_runtime_database_role(
        schema.runtime_dsn,
        **schema.runtime_identity,
    )
    await require_maintenance_database_role(
        schema.maintenance_dsn,
        **schema.maintenance_identity,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bootstrap_recovers_after_0003_commits_before_acl_reconciliation(
    postgres_database_factory,
    monkeypatch,
):
    from src.db import bootstrap as bootstrap_module

    schema = postgres_database_factory()
    real_apply_checkpoint_migrations = bootstrap_module._apply_checkpoint_migrations

    async def interrupt_after_business_schema(*_args, **_kwargs):
        raise RuntimeError("injected_post_alembic_failure")

    monkeypatch.setattr(
        bootstrap_module,
        "_apply_checkpoint_migrations",
        interrupt_after_business_schema,
    )
    with pytest.raises(RuntimeError, match="injected_post_alembic_failure"):
        await bootstrap_module.bootstrap_database(
            schema.dsn,
            **schema.bootstrap_identity,
        )
    assert schema.scalar("SELECT version_num FROM alembic_version") == ("20260710_0003")
    assert not schema.table_exists("checkpoints")

    monkeypatch.setattr(
        bootstrap_module,
        "_apply_checkpoint_migrations",
        real_apply_checkpoint_migrations,
    )
    summary = await bootstrap_module.bootstrap_database(
        schema.dsn,
        **schema.bootstrap_identity,
    )

    assert summary["alembic"] == "20260710_0003"
    await require_runtime_database_role(
        schema.runtime_dsn,
        **schema.runtime_identity,
    )
    await require_maintenance_database_role(
        schema.maintenance_dsn,
        **schema.maintenance_identity,
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "acl_drift",
    [
        "missing_required_column_grant",
        "unexpected_relation_grant",
        "table_level_update",
        "grant_option",
    ],
)
async def test_all_role_gates_reject_runtime_acl_manifest_drift(
    postgres_database_factory,
    acl_drift,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    runtime = sql.Identifier(schema.runtime_role)

    if acl_drift == "missing_required_column_grant":
        schema.execute(
            sql.SQL("REVOKE INSERT (event_key) ON TABLE audit_events FROM {}").format(
                runtime
            )
        )
    elif acl_drift == "unexpected_relation_grant":
        schema.execute(
            sql.SQL("GRANT SELECT ON TABLE app_kv_store TO {}").format(runtime)
        )
    elif acl_drift == "table_level_update":
        schema.execute(
            sql.SQL("GRANT UPDATE ON TABLE event_inbox TO {}").format(runtime)
        )
    else:
        schema.execute(
            sql.SQL("GRANT SELECT ON TABLE event_inbox TO {} WITH GRANT OPTION").format(
                runtime
            )
        )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            schema.runtime_dsn,
            schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            schema.dsn,
            schema.bootstrap_identity,
        ),
        (
            require_maintenance_database_role,
            schema.maintenance_dsn,
            schema.maintenance_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_both_role_gates_reject_trigger_function_body_drift(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    schema.execute(
        "CREATE OR REPLACE FUNCTION guard_event_inbox_update() "
        "RETURNS pg_catalog.trigger LANGUAGE plpgsql "
        "SET search_path FROM CURRENT AS $$ BEGIN RETURN NEW; END $$"
    )
    schema.execute("REVOKE ALL ON FUNCTION guard_event_inbox_update() FROM PUBLIC")

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            schema.runtime_dsn,
            schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            schema.dsn,
            schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "schema_drift",
    [
        "check_constraint",
        "unvalidated_check",
        "unique_index",
        "included_column",
        "nondefault_opclass",
        "custom_collation",
        "default_expression",
        "unknown_relation",
    ],
)
async def test_schema_contract_rejects_phase2_constraint_index_or_default_drift(
    postgres_database_factory,
    schema_drift,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)

    if schema_drift == "check_constraint":
        schema.execute("ALTER TABLE event_inbox DROP CONSTRAINT ck_event_inbox_status")
    elif schema_drift == "unvalidated_check":
        definition = schema.scalar(
            "SELECT pg_catalog.pg_get_constraintdef(oid, true) "
            "FROM pg_catalog.pg_constraint "
            "WHERE conname = 'ck_event_inbox_status'"
        )
        schema.execute("ALTER TABLE event_inbox DROP CONSTRAINT ck_event_inbox_status")
        schema.execute(
            sql.SQL(
                "ALTER TABLE event_inbox ADD CONSTRAINT "
                "ck_event_inbox_status {} NOT VALID"
            ).format(sql.SQL(definition))
        )
    elif schema_drift == "unique_index":
        schema.execute("DROP INDEX uq_pipeline_current_ingress")
    elif schema_drift in {
        "included_column",
        "nondefault_opclass",
        "custom_collation",
    }:
        schema.execute("DROP INDEX ix_event_inbox_claim")
        first_column = {
            "included_column": "pipeline_name",
            "nondefault_opclass": "pipeline_name text_pattern_ops",
            "custom_collation": 'pipeline_name COLLATE "C"',
        }[schema_drift]
        include_clause = (
            " INCLUDE (payload)" if schema_drift == "included_column" else ""
        )
        schema.execute(
            "CREATE INDEX ix_event_inbox_claim ON event_inbox ("
            f"{first_column}, status, available_at, received_at, id)"
            f"{include_clause} WHERE status IN ('pending', 'retry_wait')"
        )
    elif schema_drift == "unknown_relation":
        schema.execute("CREATE TABLE unexpected_phase2_relation (id int)")
    else:
        schema.execute("ALTER TABLE event_inbox ALTER COLUMN attempts SET DEFAULT 1")

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            schema.runtime_dsn,
            target_schema="public",
            require_complete=True,
            expected_revision="20260710_0003",
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constraint_definition",
    [
        "UNIQUE (event_key) DEFERRABLE INITIALLY DEFERRED",
        "UNIQUE NULLS NOT DISTINCT (event_key)",
    ],
    ids=("deferrable-initially-deferred", "nulls-not-distinct"),
)
async def test_schema_contract_rejects_same_key_unique_physical_drift(
    postgres_database_factory,
    constraint_definition,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    schema.execute("ALTER TABLE audit_events DROP CONSTRAINT uq_audit_events_event_key")
    schema.execute(
        "ALTER TABLE audit_events "
        "ADD CONSTRAINT uq_audit_events_event_key "
        f"{constraint_definition}"
    )

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            schema.runtime_dsn,
            target_schema="public",
            require_complete=True,
            expected_revision="20260710_0003",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fresh_0003_passes_runtime_schema_contract(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)

    await require_database_schema_contract(
        schema.runtime_dsn,
        target_schema="public",
        require_complete=True,
        expected_revision="20260710_0003",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_schema_contract_rejects_unexpected_exclusion_constraint(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    schema.execute(
        "ALTER TABLE audit_events ADD CONSTRAINT "
        "unexpected_audit_event_exclusion "
        "EXCLUDE USING btree (event_key WITH =)"
    )

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            schema.runtime_dsn,
            target_schema="public",
            require_complete=True,
            expected_revision="20260710_0003",
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relation_drift_ddl",
    [
        "ALTER TABLE audit_events SET UNLOGGED",
        "ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY unexpected_audit_policy ON audit_events USING (false)",
    ],
    ids=("unlogged", "row-security", "policy"),
)
async def test_schema_contract_rejects_phase2_relation_security_or_durability_drift(
    postgres_database_factory,
    relation_drift_ddl,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    schema.execute(relation_drift_ddl)

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            schema.runtime_dsn,
            target_schema="public",
            require_complete=True,
            expected_revision="20260710_0003",
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "column_drift_ddl",
    [
        (
            "ALTER TABLE event_inbox ALTER COLUMN account_id "
            "ADD GENERATED BY DEFAULT AS IDENTITY",
        ),
        ('ALTER TABLE emails ALTER COLUMN external_email_id TYPE text COLLATE "C"',),
        ("ALTER TABLE event_inbox ALTER COLUMN payload SET STORAGE PLAIN",),
        ("ALTER TABLE event_inbox ALTER COLUMN payload SET COMPRESSION pglz",),
        (
            "ALTER TABLE audit_events DROP COLUMN reason",
            "ALTER TABLE audit_events ADD COLUMN reason text "
            "GENERATED ALWAYS AS (actor) STORED",
        ),
    ],
    ids=("identity", "collation", "storage", "compression", "generated"),
)
async def test_schema_contract_rejects_phase2_column_catalog_drift(
    postgres_database_factory,
    column_drift_ddl,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    for statement in column_drift_ddl:
        schema.execute(statement)

    with pytest.raises(
        DatabaseSchemaContractError,
        match="database_schema_contract_invalid",
    ):
        await require_database_schema_contract(
            schema.runtime_dsn,
            target_schema="public",
            require_complete=True,
            expected_revision="20260710_0003",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_role_gates_reject_disabled_internal_foreign_key_trigger(
    postgres_database_factory,
):
    schema = postgres_database_factory()
    await bootstrap_database(schema.dsn, **schema.bootstrap_identity)
    trigger_name = schema.scalar(
        "SELECT trigger.tgname::pg_catalog.text "
        "FROM pg_catalog.pg_trigger AS trigger "
        "JOIN pg_catalog.pg_constraint AS constraint_record "
        "ON constraint_record.oid = trigger.tgconstraint "
        "JOIN pg_catalog.pg_class AS relation "
        "ON relation.oid = trigger.tgrelid "
        "WHERE trigger.tgisinternal "
        "AND constraint_record.conname = 'fk_event_inbox_pipeline_ownership' "
        "AND relation.relname = 'event_inbox' "
        "ORDER BY trigger.tgname LIMIT 1"
    )
    schema.admin_execute(
        sql.SQL("ALTER TABLE event_inbox DISABLE TRIGGER {}").format(
            sql.Identifier(trigger_name)
        )
    )

    for gate, dsn, identity in (
        (
            require_runtime_database_role,
            schema.runtime_dsn,
            schema.runtime_identity,
        ),
        (
            require_migration_database_role,
            schema.dsn,
            schema.bootstrap_identity,
        ),
    ):
        with pytest.raises(DatabaseRoleError, match="database_role_preflight_failed"):
            await gate(dsn, **identity)
