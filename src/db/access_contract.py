"""Pure-data manifests for the revisioned PostgreSQL access boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


PHASE2_DATABASE_REVISION: Final = "20260713_0004"
SYNC_RECONCILIATION_DATABASE_REVISION: Final = "20260713_0005"
GREENFIELD_DATABASE_REVISION: Final = "20260716_0006"
PHASE2_RELATIONS: Final = (
    "audit_events",
    "emails",
    "event_inbox",
    "pipeline_ownership",
    "pipeline_shadow_comparisons",
    "sync_cursors",
)
PHASE2_RELATIONS_BY_REVISION: Final[dict[str, tuple[str, ...]]] = {
    "20260710_0002": (),
    "20260710_0003": PHASE2_RELATIONS,
    PHASE2_DATABASE_REVISION: PHASE2_RELATIONS,
    SYNC_RECONCILIATION_DATABASE_REVISION: (
        *PHASE2_RELATIONS,
        "pipeline_command_receipts",
        "sync_cold_start_plans",
    ),
    GREENFIELD_DATABASE_REVISION: (
        "audit_events",
        "emails",
        "event_inbox",
        "pipeline_command_receipts",
        "pipeline_folder_scopes",
        "pipeline_initializations",
        "pipeline_ownership",
        "pipeline_runtime_authority",
        "pipeline_runtime_capabilities",
        "pipeline_runtime_instances",
        "sync_cold_start_plans",
        "sync_cursors",
    ),
}


@dataclass(frozen=True)
class ViewSpec:
    name: str
    relation_kind: str
    check_option: str
    definition_sha256: str


PHASE2_VIEW_SPECS_BY_REVISION: Final[dict[str, tuple[ViewSpec, ...]]] = {
    "20260710_0002": (),
    "20260710_0003": (),
    PHASE2_DATABASE_REVISION: (),
    SYNC_RECONCILIATION_DATABASE_REVISION: (
        ViewSpec(
            name="cold_start_command_receipts",
            relation_kind="v",
            check_option="CASCADED",
            definition_sha256=(
                "66461da92b70d58eaa079c2850896a42d920a4fbe7b78e0cb43474470f938b26"
            ),
        ),
    ),
    GREENFIELD_DATABASE_REVISION: (
        ViewSpec(
            name="cold_start_command_receipts",
            relation_kind="v",
            check_option="CASCADED",
            definition_sha256=(
                "66461da92b70d58eaa079c2850896a42d920a4fbe7b78e0cb43474470f938b26"
            ),
        ),
    ),
}

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
        "d8fa97e98d89b2275a29c6899ce83136be195423cfdb907070a06074a4d7ab7c"
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

# Revision 0004 changes exactly one existing CHECK constraint.  Keep the prior
# digest explicit so rolling bootstrap/readiness checks can still prove an
# unmodified 0003 schema before applying the forward-only revision.
PHASE2_CHECK_CONSTRAINT_SHA256_OVERRIDES_BY_REVISION: Final[
    dict[str, dict[tuple[str, str], str]]
] = {
    "20260710_0003": {
        ("event_inbox", "ck_event_inbox_processing_policy"): (
            "f2c35a7d5a10689cc78f15a3d83cf656c89dc26578f2390519a7679012f1d9bb"
        )
    }
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


PHASE2_DEFAULT_EXPRESSIONS_BY_REVISION: Final[dict[str, dict[tuple[str, str], str]]] = {
    "20260710_0003": PHASE2_DEFAULT_EXPRESSIONS,
    PHASE2_DATABASE_REVISION: PHASE2_DEFAULT_EXPRESSIONS,
    SYNC_RECONCILIATION_DATABASE_REVISION: {
        **PHASE2_DEFAULT_EXPRESSIONS,
        ("pipeline_command_receipts", "created_at"): "CURRENT_TIMESTAMP",
        ("sync_cold_start_plans", "created_at"): "CURRENT_TIMESTAMP",
        ("sync_cold_start_plans", "item_count"): "0",
        ("sync_cold_start_plans", "page_count"): "0",
        ("sync_cold_start_plans", "preview_cursor_version"): "0",
        ("sync_cold_start_plans", "redacted_samples"): "'[]'::jsonb",
        ("sync_cold_start_plans", "updated_at"): "CURRENT_TIMESTAMP",
        ("sync_cold_start_plans", "version"): "0",
        ("sync_cursors", "transient_failures"): "0",
    },
}

PHASE2_GENERATED_EXPRESSION_SHA256_BY_REVISION: Final[
    dict[str, dict[tuple[str, str], str]]
] = {
    "20260710_0002": {},
    "20260710_0003": {},
    PHASE2_DATABASE_REVISION: {},
    SYNC_RECONCILIATION_DATABASE_REVISION: {
        ("sync_cold_start_plans", "cursor_binding_plan_id"): (
            "f1b485141675d63e566b2bea4995c507c6c40e5250d4a48228bcdec03825aff4"
        ),
    },
}

PHASE2_CHECK_CONSTRAINT_SHA256_BY_REVISION: Final[
    dict[str, dict[tuple[str, str], str]]
] = {
    "20260710_0003": {
        **PHASE2_CHECK_CONSTRAINT_SHA256,
        **PHASE2_CHECK_CONSTRAINT_SHA256_OVERRIDES_BY_REVISION["20260710_0003"],
    },
    PHASE2_DATABASE_REVISION: PHASE2_CHECK_CONSTRAINT_SHA256,
    SYNC_RECONCILIATION_DATABASE_REVISION: {
        **PHASE2_CHECK_CONSTRAINT_SHA256,
        ("sync_cursors", "ck_sync_cursors_plan_binding"): (
            "0170ce730371779dcd870254702aa219eb3674d4a93e48dd0324a8a7a1ae6274"
        ),
        ("sync_cursors", "ck_sync_cursors_retry"): (
            "22253a06d7542d0243a0059b94220ce1e8c7ddd88e95d16227c5c5724d58735d"
        ),
        ("sync_cursors", "ck_sync_cursors_state_matrix"): (
            "014b54cd0be647f224e1cd88f077648471ae1fc27f753919eb6f3701b12d9e24"
        ),
        ("sync_cursors", "ck_sync_cursors_status"): (
            "8dc20405aefb36a48f6c66633c4288494de00d53fe0bca736196ed8776053742"
        ),
        ("sync_cursors", "ck_sync_cursors_transient_failures"): (
            "77ef5160b8bc845bf7a092e61bb68898c1fa71c1745f7724946dbcfad0e0db22"
        ),
        ("pipeline_command_receipts", "ck_pipeline_command_receipts_account"): (
            "25341c3ce11ea0a3cda1fb848bf14cc227abc891a221df7e6c0d094f11f14124"
        ),
        (
            "pipeline_command_receipts",
            "ck_pipeline_command_receipts_authority_epoch",
        ): "d42138ae3aaf50c15e446c5c4ad962e65a6969bf643871b818855908f5f27c0c",
        (
            "pipeline_command_receipts",
            "ck_pipeline_command_receipts_command_name",
        ): "932648b521410d05bce12997387e4d301ea23f8f71d758f6e26c1b805f39bbca",
        ("pipeline_command_receipts", "ck_pipeline_command_receipts_hashes"): (
            "58797557ba965ff3fcb7b305317f649f9e06101043145cfd57ec887bcc60db2e"
        ),
        ("pipeline_command_receipts", "ck_pipeline_command_receipts_outcome"): (
            "81e16f1ec1ab1573e19f67503718f9d03e4460c38ca80758acc437919c6d338d"
        ),
        ("pipeline_command_receipts", "ck_pipeline_command_receipts_result"): (
            "4a67dd7bc60851165175c7aa41454ae6ed17e0e198530af1ec639135578306b2"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_blocked_reason"): (
            "e1809a680586dfe440e9df1a6b79dca29df570b85a472e7bc5804ddf0abc0839"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_counts"): (
            "29c7e8324ea5c879d0763f40ee717e3354efd1c4b63aeb38fddc144753b58355"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_cursors"): (
            "48f9d0fe4594045b5d96e12ee51e70582a95054ab0032d9a33de75000fb4723f"
        ),
        (
            "sync_cold_start_plans",
            "ck_sync_cold_start_plans_expected_cursor",
        ): "68deb84441caaf17ca54758f32c96e1d1e2908e0c8bd3e150a2df6be4ded94b1",
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_folder_key"): (
            "2b226c8f6fea8ce4c93e8ebc0005964a00f4df08cdebcd11b3bc96d247d08629"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_hashes"): (
            "50c8c34d0a563d21b22802ee011b8c84b7a5631d5c89e7a1823492044712d4dc"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_operator"): (
            "b5b8600fa9d4622182db78be598537d41662ea6682bb0e29aa24aff92c8b0c4e"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_pipeline_name"): (
            "6505916d0a680d292a459d1072069232644bb16413f7d4b249c2fde926109d1f"
        ),
        (
            "sync_cold_start_plans",
            "ck_sync_cold_start_plans_positive_identity",
        ): "e5e43f686cf6aca5560d22d5bfa18bd0e5cba8341a5f26e09eff5a37b6bba9c6",
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_samples"): (
            "47a10b8a376047a101cd29e7f926c14ba517601cd784dc3ef9843583a0aa5ad2"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_state"): (
            "7f6bec2f50be67ffe098ce5ff49c29101d0d3b1be43826e5a305a2c9072179cd"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_state_matrix"): (
            "a84f7de95804cfc2c7f0d2cb06145951f654c7dcbee46aedfebdc6ac08665ecb"
        ),
        ("sync_cold_start_plans", "ck_sync_cold_start_plans_versions"): (
            "a846d31aba0c59e9c2984f58560fcc226b0f17c4d6140f5979ecf7d07e2448c3"
        ),
    },
}

PHASE2_UNIQUE_CONSTRAINTS_BY_REVISION: Final[
    dict[str, tuple[UniqueConstraintSpec, ...]]
] = {
    "20260710_0003": PHASE2_UNIQUE_CONSTRAINTS,
    PHASE2_DATABASE_REVISION: PHASE2_UNIQUE_CONSTRAINTS,
    SYNC_RECONCILIATION_DATABASE_REVISION: (
        *PHASE2_UNIQUE_CONSTRAINTS,
        UniqueConstraintSpec(
            "pipeline_command_receipts",
            "pk_pipeline_command_receipts",
            "p",
            ("id",),
            (0,),
        ),
        UniqueConstraintSpec(
            "pipeline_command_receipts",
            "uq_pipeline_command_receipts_identity",
            "u",
            ("account_id", "command_name", "idempotency_key_hash"),
            (0, 0, 0),
        ),
        UniqueConstraintSpec(
            "sync_cold_start_plans",
            "pk_sync_cold_start_plans",
            "p",
            ("plan_id",),
            (0,),
        ),
        UniqueConstraintSpec(
            "sync_cold_start_plans",
            "uq_sync_cold_start_plan_identity",
            "u",
            ("plan_id", "account_id", "folder_key"),
            (0, 0, 0),
        ),
        UniqueConstraintSpec(
            "sync_cold_start_plans",
            "uq_sync_cold_start_plan_apply_binding",
            "u",
            (
                "plan_id",
                "account_id",
                "folder_key",
                "apply_cursor",
                "apply_cursor_version",
                "state",
            ),
            (0, 0, 0, 0, 0, 0),
        ),
        UniqueConstraintSpec(
            "sync_cursors",
            "uq_sync_cursors_cold_start_binding",
            "u",
            (
                "cold_start_plan_id",
                "account_id",
                "folder_key",
                "cursor",
                "version",
                "cold_start_plan_state",
            ),
            (0, 0, 0, 0, 0, 0),
        ),
    ),
}

PHASE2_INDEX_SPECS_BY_REVISION: Final[dict[str, tuple[IndexSpec, ...]]] = {
    "20260710_0003": PHASE2_INDEX_SPECS,
    PHASE2_DATABASE_REVISION: PHASE2_INDEX_SPECS,
    SYNC_RECONCILIATION_DATABASE_REVISION: (
        *PHASE2_INDEX_SPECS,
        IndexSpec(
            "sync_cold_start_plans",
            "ix_sync_cold_start_plans_state_expiry",
            False,
            ("state", "expires_at", "plan_id"),
            (0, 0, 0),
        ),
        IndexSpec(
            "sync_cold_start_plans",
            "uq_sync_cold_start_open_plan",
            True,
            ("account_id", "folder_key"),
            (0, 0),
            "4499fc6ca5e6a33cd2dab332bd299c9a30d9f65ebf92c5634832dbb34076acad",
        ),
        IndexSpec(
            "sync_cursors",
            "uq_sync_cursors_cold_start_plan",
            True,
            ("cold_start_plan_id",),
            (0,),
            "e971963bd734284263889850d56c3e4a1e3f1748509f63b8ffc0fea5c7a67d9a",
        ),
    ),
}


@dataclass(frozen=True)
class RelationAccess:
    table_privileges: tuple[str, ...] = ()
    select_columns: tuple[str, ...] = ()
    insert_columns: tuple[str, ...] = ()
    update_columns: tuple[str, ...] = ()
    delete: bool = False


@dataclass(frozen=True)
class RoutineAccess:
    """One exact ``pg_get_function_identity_arguments`` execute grant."""

    name: str
    identity_arguments: str


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

_LEGACY_RELATION_NAMES: Final = frozenset(PHASE2_RELATIONS)

RUNTIME_RELATION_ACCESS_BY_REVISION: Final[dict[str, dict[str, RelationAccess]]] = {
    "20260710_0002": {
        name: access
        for name, access in RUNTIME_RELATION_ACCESS.items()
        if name not in _LEGACY_RELATION_NAMES
    },
    "20260710_0003": RUNTIME_RELATION_ACCESS,
    PHASE2_DATABASE_REVISION: RUNTIME_RELATION_ACCESS,
    SYNC_RECONCILIATION_DATABASE_REVISION: {
        **RUNTIME_RELATION_ACCESS,
        "sync_cursors": RelationAccess(
            table_privileges=("SELECT",),
            insert_columns=(
                *RUNTIME_RELATION_ACCESS["sync_cursors"].insert_columns,
                "transient_failures",
                "retry_after_at",
            ),
            update_columns=(
                *RUNTIME_RELATION_ACCESS["sync_cursors"].update_columns,
                "transient_failures",
                "retry_after_at",
            ),
        ),
    },
    GREENFIELD_DATABASE_REVISION: {
        "alembic_version": RelationAccess(table_privileges=("SELECT",)),
        "emails_log": RelationAccess(
            table_privileges=("SELECT",),
            insert_columns=(
                "id",
                "subject",
                "sender",
                "received_at",
                "status",
            ),
            update_columns=(
                "status",
                "classification",
                "draft_content",
                "updated_at",
                "routing_log",
                "active_skills",
                "original_draft",
                "final_draft",
                "approver_user_id",
                "rejection_reason",
                "error_message",
                "content_ref",
            ),
        ),
        "checkpoints": RUNTIME_RELATION_ACCESS["checkpoints"],
        "checkpoint_blobs": RUNTIME_RELATION_ACCESS["checkpoint_blobs"],
        "checkpoint_writes": RUNTIME_RELATION_ACCESS["checkpoint_writes"],
        "pipeline_ownership": RelationAccess(table_privileges=("SELECT",)),
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
            ),
            update_columns=(
                "status",
                "lease_owner",
                "lease_session_id",
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
        "sync_cursors": RelationAccess(table_privileges=("SELECT",)),
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
                "owner_authority_epoch",
                "owner_capability_hash",
                "processing_inbox_id",
                "processing_execution_epoch",
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
                "processing_execution_epoch",
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
            insert_columns=RUNTIME_RELATION_ACCESS["audit_events"].insert_columns,
        ),
        "pipeline_command_receipts": RelationAccess(table_privileges=("SELECT",)),
        "sync_cold_start_plans": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_capabilities": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_initializations": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_folder_scopes": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_authority": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_instances": RelationAccess(table_privileges=("SELECT",)),
    },
}

MAINTENANCE_RELATION_ACCESS_BY_REVISION: Final[dict[str, dict[str, RelationAccess]]] = {
    "20260710_0002": MAINTENANCE_RELATION_ACCESS,
    "20260710_0003": MAINTENANCE_RELATION_ACCESS,
    PHASE2_DATABASE_REVISION: MAINTENANCE_RELATION_ACCESS,
    SYNC_RECONCILIATION_DATABASE_REVISION: {
        **MAINTENANCE_RELATION_ACCESS,
        "pipeline_ownership": RelationAccess(table_privileges=("SELECT",)),
        "event_inbox": RelationAccess(
            table_privileges=("SELECT",),
            insert_columns=tuple(
                column
                for column in RUNTIME_RELATION_ACCESS["event_inbox"].insert_columns
                if column != "received_at"
            ),
        ),
        "sync_cursors": RelationAccess(
            table_privileges=("SELECT",),
            insert_columns=(
                *RUNTIME_RELATION_ACCESS_BY_REVISION[
                    SYNC_RECONCILIATION_DATABASE_REVISION
                ]["sync_cursors"].insert_columns,
                "cold_start_plan_id",
                "cold_start_plan_state",
            ),
            update_columns=(
                *RUNTIME_RELATION_ACCESS_BY_REVISION[
                    SYNC_RECONCILIATION_DATABASE_REVISION
                ]["sync_cursors"].update_columns,
                "cold_start_plan_id",
                "cold_start_plan_state",
            ),
        ),
        "audit_events": RelationAccess(
            table_privileges=("SELECT",),
            insert_columns=RUNTIME_RELATION_ACCESS["audit_events"].insert_columns,
        ),
        "sync_cold_start_plans": RelationAccess(
            table_privileges=("SELECT",),
            insert_columns=(
                "plan_id",
                "account_id",
                "folder_key",
                "expected_cursor_status",
                "expected_cursor",
                "expected_cursor_version",
                "pipeline_name",
                "generation",
                "fencing_token",
                "state",
                "version",
                "preview_cursor",
                "preview_cursor_version",
                "boundary_cursor",
                "boundary_cursor_version",
                "apply_cursor",
                "apply_cursor_version",
                "rolling_hash",
                "page_count",
                "item_count",
                "redacted_samples",
                "contract_fingerprint",
                "folder_scope_config_hash",
                "plan_hash",
                "actor",
                "reason",
                "blocked_reason_code",
                "blocked_fingerprint",
                "expires_at",
                "ready_at",
                "approved_at",
                "completed_at",
                "blocked_at",
                "created_at",
                "updated_at",
            ),
            update_columns=(
                "state",
                "version",
                "preview_cursor",
                "preview_cursor_version",
                "boundary_cursor",
                "boundary_cursor_version",
                "apply_cursor",
                "apply_cursor_version",
                "rolling_hash",
                "page_count",
                "item_count",
                "redacted_samples",
                "plan_hash",
                "blocked_reason_code",
                "blocked_fingerprint",
                "ready_at",
                "approved_at",
                "completed_at",
                "blocked_at",
                "updated_at",
            ),
        ),
        "cold_start_command_receipts": RelationAccess(
            table_privileges=("SELECT", "INSERT"),
        ),
    },
    GREENFIELD_DATABASE_REVISION: {
        **MAINTENANCE_RELATION_ACCESS,
        "pipeline_ownership": RelationAccess(table_privileges=("SELECT",)),
        "event_inbox": RelationAccess(table_privileges=("SELECT",)),
        "sync_cursors": RelationAccess(table_privileges=("SELECT",)),
        "emails": RelationAccess(table_privileges=("SELECT",)),
        "audit_events": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_command_receipts": RelationAccess(table_privileges=("SELECT",)),
        "sync_cold_start_plans": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_capabilities": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_initializations": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_folder_scopes": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_authority": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_instances": RelationAccess(table_privileges=("SELECT",)),
    },
}

AUDITOR_RELATION_ACCESS_BY_REVISION: Final[dict[str, dict[str, RelationAccess]]] = {
    "20260710_0002": AUDITOR_RELATION_ACCESS,
    "20260710_0003": AUDITOR_RELATION_ACCESS,
    PHASE2_DATABASE_REVISION: AUDITOR_RELATION_ACCESS,
    SYNC_RECONCILIATION_DATABASE_REVISION: {
        **AUDITOR_RELATION_ACCESS,
        "pipeline_command_receipts": RelationAccess(
            table_privileges=("SELECT",),
        ),
    },
    GREENFIELD_DATABASE_REVISION: {
        **AUDITOR_RELATION_ACCESS,
        "pipeline_runtime_capabilities": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_initializations": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_authority": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_runtime_instances": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_ownership": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_command_receipts": RelationAccess(table_privileges=("SELECT",)),
        "audit_events": RelationAccess(table_privileges=("SELECT",)),
        "pipeline_folder_scopes": RelationAccess(
            select_columns=(
                "initialization_id",
                "account_id",
                "canonical_key",
                "scope_hash",
                "policy_manifest_hash",
                "created_at",
            )
        ),
        "event_inbox": RelationAccess(
            select_columns=(
                "id",
                "account_id",
                "source",
                "raw_event_type",
                "change_kind",
                "dedupe_key",
                "source_event_at",
                "processing_policy",
                "pipeline_name",
                "generation",
                "fencing_token",
                "execution_epoch",
                "authority_epoch",
                "capability_hash",
                "status",
                "lease_owner",
                "lease_session_id",
                "lease_until",
                "attempts",
                "available_at",
                "processing_started_at",
                "effect_started_at",
                "safe_error_code",
                "received_at",
                "updated_at",
            )
        ),
        "emails": RelationAccess(
            select_columns=(
                "id",
                "account_id",
                "status",
                "version",
                "owner_generation",
                "owner_fencing_token",
                "owner_authority_epoch",
                "owner_capability_hash",
                "processing_inbox_id",
                "processing_execution_epoch",
                "create_seen_at",
                "processing_started_at",
                "source_deleted_at",
                "external_effects_started_at",
                "safe_error_code",
                "is_read",
                "is_read_refresh_required",
                "created_at",
                "updated_at",
            )
        ),
    },
}


RUNTIME_ROUTINE_EXECUTE_BY_REVISION: Final[dict[str, tuple[RoutineAccess, ...]]] = {
    "20260710_0002": (),
    "20260710_0003": (),
    PHASE2_DATABASE_REVISION: (),
    SYNC_RECONCILIATION_DATABASE_REVISION: (),
    GREENFIELD_DATABASE_REVISION: (
        RoutineAccess(
            "greenfield_get_runtime_authority",
            "p_account_id bigint",
        ),
        RoutineAccess(
            "greenfield_register_web_instance",
            "p_account_id bigint, p_instance_id text, p_session_id uuid, "
            "p_expected_authority_epoch bigint, p_expected_authority_version "
            "bigint, p_schema_revision text, p_protocol_version bigint, "
            "p_build_id text, p_config_hash text, p_capability_hash text, "
            "p_lease_seconds bigint",
        ),
        RoutineAccess(
            "greenfield_heartbeat_web_instance",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_expected_authority_epoch bigint, "
            "p_expected_capability_hash text, p_accepted_count bigint, "
            "p_rejected_count bigint, p_lease_seconds bigint",
        ),
        RoutineAccess(
            "greenfield_drain_web_instance",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_expected_authority_epoch bigint, "
            "p_expected_capability_hash text",
        ),
        RoutineAccess(
            "greenfield_insert_webhook_event",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_external_email_id text, "
            "p_folder_key text, p_raw_event_type text, p_change_kind text, "
            "p_dedupe_key text, p_source_version text, "
            "p_source_event_at timestamp with time zone, p_payload jsonb, "
            "p_processing_policy text",
        ),
        RoutineAccess(
            "greenfield_claim_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_lease_owner text, "
            "p_limit bigint, p_lease_seconds bigint",
        ),
        RoutineAccess(
            "greenfield_renew_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_lease_owner text, p_attempts bigint, "
            "p_lease_seconds bigint",
        ),
        RoutineAccess(
            "greenfield_apply_email_event",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_expected_email_version bigint",
        ),
        RoutineAccess(
            "greenfield_begin_inbox_effect",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_attempts bigint",
        ),
        RoutineAccess(
            "greenfield_finish_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_attempts bigint, p_completion jsonb",
        ),
        RoutineAccess(
            "greenfield_fail_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_inbox_id uuid, "
            "p_execution_epoch bigint, p_attempts bigint, "
            "p_safe_error_code text, p_safe_error_summary text",
        ),
        RoutineAccess(
            "greenfield_reap_inbox",
            "p_account_id bigint, p_session_id uuid, "
            "p_expected_lease_version bigint, p_limit bigint",
        ),
    ),
}

MAINTENANCE_ROUTINE_EXECUTE_BY_REVISION: Final[dict[str, tuple[RoutineAccess, ...]]] = {
    "20260710_0002": (),
    "20260710_0003": (),
    PHASE2_DATABASE_REVISION: (),
    SYNC_RECONCILIATION_DATABASE_REVISION: (),
    GREENFIELD_DATABASE_REVISION: (
        RoutineAccess(
            "greenfield_initialize_runtime",
            "p_account_id bigint, p_capability_hash text, p_predecessor_hash "
            "text, p_capability_stage text, p_schema_revision text, "
            "p_schema_digest text, p_protocol_version bigint, "
            "p_minimum_build_id text, p_config_hash text, p_adapter_hash text, "
            "p_policy_manifest_hash text, p_evidence_manifest_hash text, "
            "p_policy_manifest_json text, p_policy_scope_count bigint, "
            "p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
        ),
        RoutineAccess(
            "greenfield_get_runtime_authority",
            "p_account_id bigint",
        ),
        RoutineAccess(
            "greenfield_pause_runtime",
            "p_account_id bigint, p_expected_authority_epoch bigint, "
            "p_expected_version bigint, p_expected_capability_hash text, "
            "p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
        ),
        RoutineAccess(
            "greenfield_resume_ingress",
            "p_account_id bigint, p_expected_authority_epoch bigint, "
            "p_expected_version bigint, p_expected_capability_hash text, "
            "p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
        ),
        RoutineAccess(
            "greenfield_requeue_inbox",
            "p_account_id bigint, p_inbox_id uuid, "
            "p_expected_execution_epoch bigint, p_expected_email_version "
            "bigint, p_actor text, p_reason text, p_idempotency_key text, "
            "p_canonical_payload_hash text",
        ),
    ),
}

AUDITOR_ROUTINE_EXECUTE_BY_REVISION: Final[dict[str, tuple[RoutineAccess, ...]]] = {
    "20260710_0002": (),
    "20260710_0003": (),
    PHASE2_DATABASE_REVISION: (),
    SYNC_RECONCILIATION_DATABASE_REVISION: (),
    GREENFIELD_DATABASE_REVISION: (),
}


@dataclass(frozen=True)
class ForeignKeySpec:
    name: str
    child_relation: str
    child_columns: tuple[str, ...]
    parent_relation: str
    parent_columns: tuple[str, ...]
    match_type: str
    update_action: str = "r"
    delete_action: str = "r"
    deferrable: bool = False
    initially_deferred: bool = False
    validated: bool = True


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

_SYNC_RECONCILIATION_FOREIGN_KEY_SPECS: Final = (
    *FOREIGN_KEY_SPECS,
    ForeignKeySpec(
        "fk_sync_cold_start_plan_ownership",
        "sync_cold_start_plans",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
    ForeignKeySpec(
        "fk_sync_cold_start_plan_active_cursor",
        "sync_cold_start_plans",
        (
            "cursor_binding_plan_id",
            "account_id",
            "folder_key",
            "apply_cursor",
            "apply_cursor_version",
            "state",
        ),
        "sync_cursors",
        (
            "cold_start_plan_id",
            "account_id",
            "folder_key",
            "cursor",
            "version",
            "cold_start_plan_state",
        ),
        "s",
        update_action="a",
        deferrable=True,
        initially_deferred=True,
    ),
    ForeignKeySpec(
        "fk_sync_cursors_cold_start_plan",
        "sync_cursors",
        (
            "cold_start_plan_id",
            "account_id",
            "folder_key",
            "cursor",
            "version",
            "cold_start_plan_state",
        ),
        "sync_cold_start_plans",
        (
            "plan_id",
            "account_id",
            "folder_key",
            "apply_cursor",
            "apply_cursor_version",
            "state",
        ),
        "s",
        update_action="a",
        deferrable=True,
        initially_deferred=True,
    ),
)


_GREENFIELD_RETAINED_FOREIGN_KEYS: Final = frozenset(
    {
        "fk_audit_events_email",
        "fk_emails_pipeline_ownership",
        "fk_event_inbox_pipeline_ownership",
        "fk_sync_cold_start_plan_active_cursor",
        "fk_sync_cold_start_plan_ownership",
        "fk_sync_cursors_cold_start_plan",
    }
)
_GREENFIELD_FOREIGN_KEY_SPECS: Final = (
    *(
        spec
        for spec in _SYNC_RECONCILIATION_FOREIGN_KEY_SPECS
        if spec.name in _GREENFIELD_RETAINED_FOREIGN_KEYS
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
            "processing_execution_epoch",
            "owner_authority_epoch",
            "owner_capability_hash",
        ),
        "event_inbox",
        (
            "id",
            "account_id",
            "external_email_id",
            "generation",
            "fencing_token",
            "execution_epoch",
            "authority_epoch",
            "capability_hash",
        ),
        "s",
        update_action="a",
        deferrable=True,
        initially_deferred=True,
    ),
    ForeignKeySpec(
        "fk_emails_runtime_capability",
        "emails",
        ("owner_capability_hash",),
        "pipeline_runtime_capabilities",
        ("capability_hash",),
        "f",
    ),
    ForeignKeySpec(
        "fk_event_inbox_lease_session",
        "event_inbox",
        (
            "lease_session_id",
            "account_id",
            "generation",
            "fencing_token",
            "authority_epoch",
            "capability_hash",
        ),
        "pipeline_runtime_instances",
        (
            "session_id",
            "account_id",
            "generation",
            "fencing_token",
            "authority_epoch",
            "capability_hash",
        ),
        "s",
    ),
    ForeignKeySpec(
        "fk_event_inbox_runtime_capability",
        "event_inbox",
        ("capability_hash",),
        "pipeline_runtime_capabilities",
        ("capability_hash",),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_folder_scopes_initialization",
        "pipeline_folder_scopes",
        ("initialization_id", "account_id", "policy_manifest_hash"),
        "pipeline_initializations",
        ("initialization_id", "account_id", "policy_manifest_hash"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_initializations_capability",
        "pipeline_initializations",
        (
            "capability_hash",
            "capability_stage_ordinal",
            "policy_manifest_hash",
        ),
        "pipeline_runtime_capabilities",
        ("capability_hash", "stage_ordinal", "policy_manifest_hash"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_initializations_ownership",
        "pipeline_initializations",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_initializations_receipt",
        "pipeline_initializations",
        (
            "command_receipt_id",
            "account_id",
            "receipt_command_name",
            "authority_epoch",
        ),
        "pipeline_command_receipts",
        ("id", "account_id", "command_name", "authority_epoch"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_runtime_authority_capability",
        "pipeline_runtime_authority",
        (
            "capability_hash",
            "capability_stage_ordinal",
            "schema_revision",
            "protocol_version",
            "build_id",
            "config_hash",
            "policy_manifest_hash",
        ),
        "pipeline_runtime_capabilities",
        (
            "capability_hash",
            "stage_ordinal",
            "schema_revision",
            "protocol_version",
            "minimum_build_id",
            "config_hash",
            "policy_manifest_hash",
        ),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_runtime_authority_initialization",
        "pipeline_runtime_authority",
        (
            "initialization_id",
            "account_id",
            "generation",
            "fencing_token",
            "pipeline_name",
            "policy_manifest_hash",
        ),
        "pipeline_initializations",
        (
            "initialization_id",
            "account_id",
            "generation",
            "fencing_token",
            "pipeline_name",
            "policy_manifest_hash",
        ),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_runtime_authority_ownership",
        "pipeline_runtime_authority",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "pipeline_ownership",
        ("account_id", "generation", "fencing_token", "pipeline_name"),
        "f",
    ),
    ForeignKeySpec(
        "fk_pipeline_runtime_capabilities_predecessor",
        "pipeline_runtime_capabilities",
        ("predecessor_hash", "predecessor_stage_ordinal"),
        "pipeline_runtime_capabilities",
        ("capability_hash", "stage_ordinal"),
        "s",
    ),
    ForeignKeySpec(
        "fk_pipeline_runtime_instances_capability",
        "pipeline_runtime_instances",
        (
            "capability_hash",
            "capability_stage_ordinal",
            "schema_revision",
            "protocol_version",
            "build_id",
            "config_hash",
        ),
        "pipeline_runtime_capabilities",
        (
            "capability_hash",
            "stage_ordinal",
            "schema_revision",
            "protocol_version",
            "minimum_build_id",
            "config_hash",
        ),
        "f",
    ),
)

FOREIGN_KEY_SPECS_BY_REVISION: Final[dict[str, tuple[ForeignKeySpec, ...]]] = {
    "20260710_0003": FOREIGN_KEY_SPECS,
    PHASE2_DATABASE_REVISION: FOREIGN_KEY_SPECS,
    SYNC_RECONCILIATION_DATABASE_REVISION: (_SYNC_RECONCILIATION_FOREIGN_KEY_SPECS),
    GREENFIELD_DATABASE_REVISION: _GREENFIELD_FOREIGN_KEY_SPECS,
}


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

TRIGGER_SPECS_BY_REVISION: Final[dict[str, tuple[TriggerSpec, ...]]] = {
    "20260710_0003": TRIGGER_SPECS,
    PHASE2_DATABASE_REVISION: TRIGGER_SPECS,
    SYNC_RECONCILIATION_DATABASE_REVISION: (
        *TRIGGER_SPECS,
        TriggerSpec(
            "trg_pipeline_command_receipts_guard_row",
            "pipeline_command_receipts",
            "reject_pipeline_command_receipts_mutation",
            27,
        ),
        TriggerSpec(
            "trg_pipeline_command_receipts_guard_truncate",
            "pipeline_command_receipts",
            "reject_pipeline_command_receipts_mutation",
            34,
        ),
    ),
    GREENFIELD_DATABASE_REVISION: (
        *(
            spec
            for spec in TRIGGER_SPECS
            if spec.name
            in {
                "trg_audit_events_guard_row",
                "trg_audit_events_guard_truncate",
                "trg_pipeline_ownership_guard_row",
                "trg_pipeline_ownership_guard_truncate",
            }
        ),
        TriggerSpec(
            "trg_pipeline_command_receipts_guard_row",
            "pipeline_command_receipts",
            "reject_pipeline_command_receipts_mutation",
            27,
        ),
        TriggerSpec(
            "trg_pipeline_command_receipts_guard_truncate",
            "pipeline_command_receipts",
            "reject_pipeline_command_receipts_mutation",
            34,
        ),
        TriggerSpec(
            "trg_emails_runtime_identity",
            "emails",
            "guard_emails_runtime_identity",
            21,
            is_constraint=True,
            is_deferrable=True,
            is_initially_deferred=True,
        ),
        TriggerSpec(
            "trg_event_inbox_runtime_identity",
            "event_inbox",
            "guard_event_inbox_runtime_identity",
            23,
        ),
        TriggerSpec(
            "trg_pipeline_folder_scopes_guard_row",
            "pipeline_folder_scopes",
            "reject_pipeline_folder_scopes_mutation",
            31,
        ),
        TriggerSpec(
            "trg_pipeline_folder_scopes_guard_truncate",
            "pipeline_folder_scopes",
            "reject_pipeline_folder_scopes_mutation",
            34,
        ),
        TriggerSpec(
            "trg_pipeline_initializations_guard_row",
            "pipeline_initializations",
            "reject_pipeline_initializations_mutation",
            31,
        ),
        TriggerSpec(
            "trg_pipeline_initializations_guard_truncate",
            "pipeline_initializations",
            "reject_pipeline_initializations_mutation",
            34,
        ),
        TriggerSpec(
            "trg_pipeline_runtime_authority_guard_row",
            "pipeline_runtime_authority",
            "guard_pipeline_runtime_authority",
            31,
        ),
        TriggerSpec(
            "trg_pipeline_runtime_authority_guard_truncate",
            "pipeline_runtime_authority",
            "guard_pipeline_runtime_authority",
            34,
        ),
        TriggerSpec(
            "trg_pipeline_runtime_capabilities_guard_row",
            "pipeline_runtime_capabilities",
            "reject_pipeline_runtime_capabilities_mutation",
            27,
        ),
        TriggerSpec(
            "trg_pipeline_runtime_capabilities_guard_truncate",
            "pipeline_runtime_capabilities",
            "reject_pipeline_runtime_capabilities_mutation",
            34,
        ),
        TriggerSpec(
            "trg_pipeline_runtime_instances_guard_row",
            "pipeline_runtime_instances",
            "guard_pipeline_runtime_instances",
            31,
        ),
        TriggerSpec(
            "trg_pipeline_runtime_instances_guard_truncate",
            "pipeline_runtime_instances",
            "guard_pipeline_runtime_instances",
            34,
        ),
    ),
}

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

TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION: Final[dict[str, dict[str, str]]] = {
    "20260710_0003": TRIGGER_FUNCTION_SOURCE_SHA256,
    PHASE2_DATABASE_REVISION: TRIGGER_FUNCTION_SOURCE_SHA256,
    SYNC_RECONCILIATION_DATABASE_REVISION: {
        **TRIGGER_FUNCTION_SOURCE_SHA256,
        "reject_pipeline_command_receipts_mutation": (
            "2a5ebd74102b1adf35afc2bf49d0a2317b867c5f57c7d0080361826f28b97f16"
        ),
    },
    GREENFIELD_DATABASE_REVISION: {
        "guard_emails_runtime_identity": (
            "7bc574d299fa3bc6f2ad10d027776f53e24473d77a2edac35cc587d69d5452e1"
        ),
        "guard_event_inbox_runtime_identity": (
            "f314df8f1cdd5d1c67160c14243e7da906f84749f17a6a59eeba7ba76e42f576"
        ),
        "guard_pipeline_ownership": (
            "c898a988c2bfca60837cda5ce37ef8cdb00fd12312c312b3ceecd90b3356fc5c"
        ),
        "guard_pipeline_runtime_authority": (
            "9ce80deea362439e39fe9f02739145042b40d553f06ca689dd79e41ecb2fe059"
        ),
        "guard_pipeline_runtime_instances": (
            "20feb0f127036518702fd6d3ea4d13575a36fca70942109796643a1431b34f6d"
        ),
        "reject_audit_events_mutation": (
            "5ba2612faea4adf49b92395f87102f166df17b65aa64bf3f42ab5172bf375c5b"
        ),
        "reject_pipeline_command_receipts_mutation": (
            "2a5ebd74102b1adf35afc2bf49d0a2317b867c5f57c7d0080361826f28b97f16"
        ),
        "reject_pipeline_folder_scopes_mutation": (
            "4f0c3e20e0f837d713b3bc89b536b4bf4421b736ebe81961daa4245dd5e1e044"
        ),
        "reject_pipeline_initializations_mutation": (
            "d74a5146da3ed09d01bde3725343ee536f64d6ca151b79fa0be802fd30d5bbea"
        ),
        "reject_pipeline_runtime_capabilities_mutation": (
            "4f451f9f20e5538a7bd18117b7cd207474350055e2baa20f9595f38f11b20461"
        ),
    },
}

# Trigger routines created with ``SET search_path FROM CURRENT`` are bound to
# the deployment target schema.  Greenfield guards use ``pg_catalog`` instead;
# keep that distinction revisioned so the role preflight can prove it exactly.
TRIGGER_FUNCTION_SEARCH_PATH_BY_REVISION: Final[dict[str, dict[str, str]]] = {
    revision: {
        name: (
            "pg_catalog"
            if revision == GREENFIELD_DATABASE_REVISION
            and name
            in {
                "guard_emails_runtime_identity",
                "guard_event_inbox_runtime_identity",
                "guard_pipeline_runtime_authority",
                "guard_pipeline_runtime_instances",
                "reject_pipeline_folder_scopes_mutation",
                "reject_pipeline_initializations_mutation",
                "reject_pipeline_runtime_capabilities_mutation",
            }
            else "target_schema"
        )
        for name in functions
    }
    for revision, functions in TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION.items()
}

TRIGGER_FUNCTIONS_BY_REVISION: Final[dict[str, tuple[str, ...]]] = {
    revision: tuple(sorted(functions))
    for revision, functions in TRIGGER_FUNCTION_SOURCE_SHA256_BY_REVISION.items()
}
