"""Pure-data manifests for the revisioned PostgreSQL access boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


PHASE2_DATABASE_REVISION: Final = "20260710_0003"
PHASE2_RELATIONS: Final = (
    "audit_events",
    "emails",
    "event_inbox",
    "pipeline_ownership",
    "pipeline_shadow_comparisons",
    "sync_cursors",
)

PHASE2_DEFAULT_EXPRESSIONS: Final[dict[tuple[str, str], str]] = {
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

PHASE2_CHECK_CONSTRAINT_SHA256: Final[dict[tuple[str, str], str]] = {
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


@dataclass(frozen=True)
class UniqueConstraintSpec:
    relation: str
    name: str
    constraint_type: str
    columns: tuple[str, ...]
    index_options: tuple[int, ...]
    nulls_not_distinct: bool = False
    deferrable: bool = False
    initially_deferred: bool = False
    validated: bool = True
    index_valid: bool = True
    index_ready: bool = True
    access_method: str = "btree"
    has_no_included_columns: bool = True
    has_only_plain_columns: bool = True
    uses_default_operator_classes: bool = True
    uses_default_collations: bool = True


PHASE2_UNIQUE_CONSTRAINTS: Final = (
    UniqueConstraintSpec("audit_events", "pk_audit_events", "p", ("id",), (0,)),
    UniqueConstraintSpec(
        "audit_events",
        "uq_audit_events_event_key",
        "u",
        ("event_key",),
        (0,),
    ),
    UniqueConstraintSpec("emails", "pk_emails", "p", ("id",), (0,)),
    UniqueConstraintSpec(
        "emails",
        "uq_email_external",
        "u",
        ("account_id", "external_email_id"),
        (0, 0),
    ),
    UniqueConstraintSpec(
        "emails",
        "uq_emails_account_id",
        "u",
        ("account_id", "id"),
        (0, 0),
    ),
    UniqueConstraintSpec(
        "emails",
        "uq_emails_outbox_identity",
        "u",
        ("id", "account_id", "owner_generation", "owner_fencing_token"),
        (0, 0, 0, 0),
    ),
    UniqueConstraintSpec(
        "event_inbox",
        "pk_event_inbox",
        "p",
        ("id",),
        (0,),
    ),
    UniqueConstraintSpec(
        "event_inbox",
        "uq_event_inbox_dedupe",
        "u",
        ("dedupe_key",),
        (0,),
    ),
    UniqueConstraintSpec(
        "event_inbox",
        "uq_event_inbox_processing_identity",
        "u",
        ("id", "account_id", "external_email_id", "generation", "fencing_token"),
        (0, 0, 0, 0, 0),
    ),
    UniqueConstraintSpec(
        "pipeline_ownership",
        "pk_pipeline_ownership",
        "p",
        ("account_id", "generation"),
        (0, 0),
    ),
    UniqueConstraintSpec(
        "pipeline_ownership",
        "uq_pipeline_ownership_event_identity",
        "u",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        (0, 0, 0, 0),
    ),
    UniqueConstraintSpec(
        "pipeline_ownership",
        "uq_pipeline_ownership_fence",
        "u",
        ("account_id", "fencing_token"),
        (0, 0),
    ),
    UniqueConstraintSpec(
        "pipeline_ownership",
        "uq_pipeline_ownership_generation_fence",
        "u",
        ("account_id", "generation", "fencing_token"),
        (0, 0, 0),
    ),
    UniqueConstraintSpec(
        "pipeline_shadow_comparisons",
        "pk_pipeline_shadow_comparisons",
        "p",
        ("id",),
        (0,),
    ),
    UniqueConstraintSpec(
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
        (0, 0, 0, 0, 0, 0, 0),
    ),
    UniqueConstraintSpec(
        "sync_cursors",
        "pk_sync_cursors",
        "p",
        ("account_id", "folder_key"),
        (0, 0),
    ),
)


@dataclass(frozen=True)
class IndexSpec:
    relation: str
    name: str
    unique: bool
    columns: tuple[str, ...]
    options: tuple[int, ...]
    predicate_sha256: str | None = None


PHASE2_INDEX_SPECS: Final = (
    IndexSpec(
        "audit_events",
        "ix_audit_events_account_time",
        False,
        ("account_id", "created_at", "id"),
        (0, 3, 0),
    ),
    IndexSpec(
        "audit_events",
        "ix_audit_events_email_time",
        False,
        ("email_id", "created_at", "id"),
        (0, 3, 0),
        "124b13523b2508c02d37c10f1ef2999184c84589b3af8cc4a2b289a6454e3f29",
    ),
    IndexSpec(
        "emails",
        "ix_emails_account_status",
        False,
        ("account_id", "status", "updated_at", "id"),
        (0, 0, 0, 0),
    ),
    IndexSpec(
        "emails",
        "ix_emails_owner_status",
        False,
        ("account_id", "owner_generation", "status"),
        (0, 0, 0),
    ),
    IndexSpec(
        "event_inbox",
        "ix_event_inbox_claim",
        False,
        ("pipeline_name", "status", "available_at", "received_at", "id"),
        (0, 0, 0, 0, 0),
        "9e53978fc33ba59a2cf9a36c367398b4831f503cdc9c9bb39a07f372b3bb8673",
    ),
    IndexSpec(
        "event_inbox",
        "ix_event_inbox_expired_lease",
        False,
        ("lease_until", "id"),
        (0, 0),
        "0d8854756b3b05c4ae1bb96a0493add03c0099df50cfb267871e9b5368f7c54c",
    ),
    IndexSpec(
        "pipeline_ownership",
        "uq_pipeline_current_ingress",
        True,
        ("account_id",),
        (0,),
        "35cc984d6efa9a70d61a14ab89954b9170d47cbb4531bcfda034203fc85c6ebd",
    ),
    IndexSpec(
        "pipeline_shadow_comparisons",
        "ix_pipeline_shadow_pending",
        False,
        ("comparison_status", "created_at", "id"),
        (0, 0, 0),
        "7794b997301bccde547fa3dfc24f52827ea050d642d9f0a836e82dfb9e2e12e3",
    ),
    IndexSpec(
        "sync_cursors",
        "ix_sync_cursors_status_attempt",
        False,
        ("status", "last_attempt_at"),
        (0, 0),
    ),
)


@dataclass(frozen=True)
class RelationAccess:
    table_privileges: tuple[str, ...] = ()
    select_columns: tuple[str, ...] = ()
    insert_columns: tuple[str, ...] = ()
    update_columns: tuple[str, ...] = ()
    delete: bool = False


RUNTIME_RELATION_ACCESS: Final[dict[str, RelationAccess]] = {
    "alembic_version": RelationAccess(table_privileges=("SELECT",)),
    "emails_log": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=("id", "subject", "sender", "received_at", "status"),
        update_columns=(
            "status",
            "classification",
            "draft_content",
            "updated_at",
            "routing_log",
            "active_skills",
            "original_draft",
            "final_draft",
            "draft_diff",
            "approver_user_id",
            "rejection_reason",
            "error_message",
            "content_ref",
            "version",
        ),
    ),
    "checkpoints": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "parent_checkpoint_id",
            "checkpoint",
            "metadata",
        ),
        update_columns=("checkpoint", "metadata"),
    ),
    "checkpoint_blobs": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "thread_id",
            "checkpoint_ns",
            "channel",
            "version",
            "type",
            "blob",
        ),
    ),
    "checkpoint_writes": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "task_id",
            "task_path",
            "idx",
            "channel",
            "type",
            "blob",
        ),
        update_columns=("channel", "type", "blob"),
    ),
    "pipeline_ownership": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "account_id",
            "generation",
            "pipeline_name",
            "state",
            "fencing_token",
            "created_by",
            "reason",
        ),
        update_columns=("state", "reason", "updated_at"),
    ),
    "event_inbox": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "account_id",
            "external_email_id",
            "folder_key",
            "source",
            "raw_event_type",
            "change_kind",
            "dedupe_key",
            "source_version",
            "source_event_at",
            "payload",
            "processing_policy",
            "pipeline_name",
            "generation",
            "fencing_token",
            "status",
            "available_at",
            "received_at",
        ),
        update_columns=(
            "status",
            "lease_owner",
            "lease_until",
            "attempts",
            "available_at",
            "processing_started_at",
            "effect_started_at",
            "safe_error_code",
            "safe_error_summary",
            "updated_at",
        ),
    ),
    "sync_cursors": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "account_id",
            "folder_key",
            "cursor",
            "status",
            "blocked_reason_code",
            "contract_fingerprint",
            "blocked_at",
            "last_success_at",
            "last_attempt_at",
        ),
        update_columns=(
            "cursor",
            "status",
            "blocked_reason_code",
            "contract_fingerprint",
            "blocked_at",
            "version",
            "last_success_at",
            "last_attempt_at",
            "updated_at",
        ),
    ),
    "emails": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "account_id",
            "external_email_id",
            "source_folder_key",
            "status",
            "owner_generation",
            "owner_fencing_token",
            "processing_inbox_id",
            "create_seen_at",
            "processing_started_at",
            "source_deleted_at",
            "external_effects_started_at",
            "safe_error_code",
            "safe_error_summary",
            "content_ref",
            "is_read",
            "is_read_refresh_required",
        ),
        update_columns=(
            "source_folder_key",
            "status",
            "version",
            "processing_inbox_id",
            "create_seen_at",
            "processing_started_at",
            "source_deleted_at",
            "external_effects_started_at",
            "safe_error_code",
            "safe_error_summary",
            "content_ref",
            "is_read",
            "is_read_refresh_required",
            "updated_at",
        ),
    ),
    "audit_events": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "event_key",
            "account_id",
            "email_id",
            "object_type",
            "object_fingerprint",
            "action",
            "result",
            "actor",
            "reason",
            "safe_metadata",
        ),
    ),
    "pipeline_shadow_comparisons": RelationAccess(
        table_privileges=("SELECT",),
        insert_columns=(
            "id",
            "account_id",
            "generation",
            "fencing_token",
            "pipeline_name",
            "candidate_pipeline_name",
            "candidate_build_id",
            "candidate_config_hash",
            "event_key",
            "input_hash",
            "legacy_status",
            "shadow_status",
            "comparison_status",
            "legacy_decision_hash",
            "legacy_failure_code",
            "shadow_decision_hash",
            "shadow_failure_code",
            "safe_metadata",
        ),
        update_columns=(
            "legacy_status",
            "shadow_status",
            "comparison_status",
            "legacy_decision_hash",
            "legacy_failure_code",
            "shadow_decision_hash",
            "shadow_failure_code",
            "safe_metadata",
            "updated_at",
        ),
    ),
}


MAINTENANCE_RELATION_ACCESS: Final[dict[str, RelationAccess]] = {
    "alembic_version": RelationAccess(table_privileges=("SELECT",)),
    "checkpoint_migrations": RelationAccess(table_privileges=("SELECT",)),
    "emails_log": RelationAccess(table_privileges=("SELECT",)),
    "checkpoints": RelationAccess(table_privileges=("SELECT",), delete=True),
    "checkpoint_blobs": RelationAccess(table_privileges=("SELECT",), delete=True),
    "checkpoint_writes": RelationAccess(table_privileges=("SELECT",), delete=True),
}

AUDITOR_RELATION_ACCESS: Final[dict[str, RelationAccess]] = {
    "alembic_version": RelationAccess(select_columns=("version_num",)),
    "checkpoint_migrations": RelationAccess(select_columns=("v",)),
    "emails_log": RelationAccess(select_columns=("id", "status", "updated_at")),
    "checkpoints": RelationAccess(
        select_columns=(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "parent_checkpoint_id",
            "type",
            "checkpoint",
            "metadata",
        )
    ),
    "checkpoint_blobs": RelationAccess(
        select_columns=(
            "thread_id",
            "checkpoint_ns",
            "channel",
            "version",
            "type",
            "blob",
        )
    ),
    "checkpoint_writes": RelationAccess(
        select_columns=(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "task_id",
            "idx",
            "channel",
            "type",
            "blob",
            "task_path",
        )
    ),
}


@dataclass(frozen=True)
class ForeignKeySpec:
    name: str
    child_relation: str
    child_columns: tuple[str, ...]
    parent_relation: str
    parent_columns: tuple[str, ...]
    match_type: str


FOREIGN_KEY_SPECS: Final = (
    ForeignKeySpec(
        "fk_event_inbox_pipeline_ownership",
        "event_inbox",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
    ForeignKeySpec(
        "fk_emails_pipeline_ownership",
        "emails",
        ("account_id", "owner_generation", "owner_fencing_token"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token"),
        "f",
    ),
    ForeignKeySpec(
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
    ),
    ForeignKeySpec(
        "fk_audit_events_email",
        "audit_events",
        ("account_id", "email_id"),
        "emails",
        ("account_id", "id"),
        "s",
    ),
    ForeignKeySpec(
        "fk_pipeline_shadow_ownership",
        "pipeline_shadow_comparisons",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
)


@dataclass(frozen=True)
class TriggerSpec:
    name: str
    relation: str
    function: str
    trigger_type: int
    is_constraint: bool = False
    is_deferrable: bool = False
    is_initially_deferred: bool = False
    arguments: tuple[str, ...] = ()
    update_attribute_numbers: tuple[int, ...] = ()
    when_clause_sha256: str | None = None
    old_transition_table: str | None = None
    new_transition_table: str | None = None


TRIGGER_SPECS: Final = (
    TriggerSpec(
        "trg_pipeline_ownership_guard_row",
        "pipeline_ownership",
        "guard_pipeline_ownership",
        31,
    ),
    TriggerSpec(
        "trg_pipeline_ownership_guard_truncate",
        "pipeline_ownership",
        "guard_pipeline_ownership",
        34,
    ),
    TriggerSpec(
        "trg_event_inbox_guard_update",
        "event_inbox",
        "guard_event_inbox_update",
        19,
    ),
    TriggerSpec(
        "trg_emails_processing_owner",
        "emails",
        "enforce_email_processing_owner",
        21,
        is_constraint=True,
    ),
    TriggerSpec(
        "trg_audit_events_guard_row",
        "audit_events",
        "reject_audit_events_mutation",
        27,
    ),
    TriggerSpec(
        "trg_audit_events_guard_truncate",
        "audit_events",
        "reject_audit_events_mutation",
        34,
    ),
    TriggerSpec(
        "trg_pipeline_shadow_guard_row",
        "pipeline_shadow_comparisons",
        "guard_pipeline_shadow_comparison",
        27,
    ),
    TriggerSpec(
        "trg_pipeline_shadow_guard_truncate",
        "pipeline_shadow_comparisons",
        "guard_pipeline_shadow_comparison",
        34,
    ),
)

TRIGGER_FUNCTIONS: Final = tuple(sorted({spec.function for spec in TRIGGER_SPECS}))

# SHA-256 of the exact ``pg_proc.prosrc`` installed by revision 0003.  The
# database-role preflight checks these digests so a same-named trigger routine
# cannot silently replace the approved state-machine guards.
TRIGGER_FUNCTION_SOURCE_SHA256: Final[dict[str, str]] = {
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
