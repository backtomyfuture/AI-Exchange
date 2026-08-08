from __future__ import annotations

import importlib
import re
from dataclasses import FrozenInstanceError
from itertools import product
from pathlib import Path

import pytest

from src.ingestion.models import ChangeKind


DATABASE_STATUSES = {
    "ingested",
    "processing",
    "retry_wait",
    "manual_review",
    "waiting_approval",
    "notified_readonly",
    "send_queued",
    "sending",
    "accepted",
    "sent",
    "send_failed",
    "delivery_failed",
    "send_unknown",
    "no_action",
    "archived",
    "rejected",
    "draft_saved",
    "expired",
    "cancelled",
    "dead_letter",
}

REASONS = {
    "first_create",
    "processing_resumed",
    "processing_attempt_already_elected",
    "metadata_event",
    "duplicate_create",
    "source_tombstone",
    "source_delete_cancelled",
    "source_delete_recorded",
    "source_deleted_preserved",
    "status_preserved",
}

DISPOSITIONS = {
    "creator_elected",
    "processing_resumed",
    "processing_already_elected",
    "metadata_shell_created",
    "tombstone_created",
    "aggregate_updated",
    "aggregate_noop",
}

CANCELLABLE_STATUSES = {
    "ingested",
    "processing",
    "retry_wait",
    "manual_review",
    "waiting_approval",
    "send_queued",
}


def _email_events():
    try:
        return importlib.import_module("src.ingestion.email_events")
    except ModuleNotFoundError as error:
        pytest.fail(f"email event state machine is missing: {error}")


def _decision(**overrides: object):
    module = _email_events()
    values: dict[str, object] = {
        "should_process": False,
        "should_cancel": False,
        "new_status": "ingested",
        "cancel_pending_side_effects": False,
        "create_seen": False,
        "reason": "metadata_event",
    }
    values.update(overrides)
    return module.EmailEventDecision(**values)


def _application(**overrides: object):
    module = _email_events()
    values: dict[str, object] = {
        "decision": _decision(),
        "email_id": "64cf6a62-b957-4aa0-8706-a80dfd1650dc",
        "persisted_status": "ingested",
        "version": 0,
        "disposition": "metadata_shell_created",
        "may_complete_without_processing": True,
    }
    values.update(overrides)
    return module.EmailEventApplication(**values)


def _baseline_email_statuses() -> set[str]:
    baseline = (
        Path(__file__).parents[3]
        / "alembic"
        / "versions"
        / "20260808_0001_polling_baseline.sql"
    ).read_text(encoding="utf-8")
    match = re.search(
        r"CONSTRAINT ck_emails_status CHECK \(\(status = ANY \(ARRAY\[(?P<values>.*?)\]\)\)\)",
        baseline,
        flags=re.DOTALL,
    )
    assert match is not None
    return set(re.findall(r"'([^']+)'", match.group("values")))


def test_email_event_enums_and_transition_manifest_match_database_vocabulary() -> None:
    module = _email_events()

    assert {status.value for status in module.EmailStatus} == DATABASE_STATUSES
    assert set(module.EMAIL_STATUS_TRANSITIONS) == set(module.EmailStatus)
    assert {
        status.value for status in module.EMAIL_STATUS_TRANSITIONS
    } == DATABASE_STATUSES
    assert _baseline_email_statuses() == DATABASE_STATUSES
    assert {reason.value for reason in module.EmailEventReason} == REASONS
    assert {value.value for value in module.EmailEventDisposition} == DISPOSITIONS


def test_email_event_dtos_are_frozen_slotted_and_normalize_enum_uuid_values() -> None:
    module = _email_events()
    decision = _decision()
    application = _application(decision=decision)

    assert decision.new_status is module.EmailStatus.INGESTED
    assert decision.reason is module.EmailEventReason.METADATA_EVENT
    assert application.persisted_status is module.EmailStatus.INGESTED
    assert (
        application.disposition is module.EmailEventDisposition.METADATA_SHELL_CREATED
    )
    assert application.email_id == "64cf6a62-b957-4aa0-8706-a80dfd1650dc"
    assert not hasattr(decision, "__dict__")
    assert not hasattr(application, "__dict__")
    with pytest.raises(FrozenInstanceError):
        decision.should_process = True
    with pytest.raises(FrozenInstanceError):
        application.version = 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("should_process", 1),
        ("should_cancel", 0),
        ("new_status", "unknown"),
        ("new_status", 1),
        ("cancel_pending_side_effects", "false"),
        ("create_seen", None),
        ("reason", "unbounded_reason"),
        ("reason", 1),
    ],
)
def test_email_event_decision_rejects_invalid_inputs(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        _decision(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("decision", object()),
        ("email_id", "not-a-uuid"),
        ("email_id", 1),
        ("persisted_status", "unknown"),
        ("persisted_status", 1),
        ("version", -1),
        ("version", True),
        ("version", 2**63),
        ("disposition", "unbounded_disposition"),
        ("disposition", 1),
        ("may_complete_without_processing", 1),
    ],
)
def test_email_event_application_rejects_invalid_inputs(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        _application(**{field: value})


def test_email_event_application_requires_persisted_decision_status_equality() -> None:
    with pytest.raises(ValueError):
        _application(
            decision=_decision(new_status="processing", create_seen=True),
            persisted_status="ingested",
        )


APPLICATION_FLAG_MANIFEST = {
    ("creator_elected", "first_create", "processing"): {
        (True, False, False, True, False)
    },
    ("processing_resumed", "processing_resumed", "processing"): {
        (True, False, False, True, False)
    },
    (
        "processing_already_elected",
        "processing_attempt_already_elected",
        "processing",
    ): {(False, False, False, True, False)},
    ("metadata_shell_created", "metadata_event", "ingested"): {
        (False, False, False, False, True)
    },
    ("tombstone_created", "source_tombstone", "cancelled"): {
        (False, True, False, False, True)
    },
    ("aggregate_updated", "metadata_event", "ingested"): {
        (False, False, False, False, True),
        (False, False, False, True, True),
    },
    ("aggregate_updated", "source_delete_cancelled", "cancelled"): {
        (False, True, False, False, True),
        (False, True, True, False, True),
        (False, True, True, True, True),
    },
    ("aggregate_updated", "source_delete_recorded", "sent"): {
        (False, False, False, False, True),
        (False, False, False, True, True),
    },
    ("aggregate_updated", "status_preserved", "sent"): {
        (False, False, False, False, True),
        (False, False, False, True, True),
    },
    ("aggregate_noop", "duplicate_create", "sent"): {
        (False, False, False, False, True),
        (False, False, False, True, True),
    },
    ("aggregate_noop", "source_deleted_preserved", "cancelled"): {
        (False, False, False, False, True),
        (False, False, False, True, True),
    },
    ("aggregate_noop", "status_preserved", "sent"): {
        (False, False, False, False, True),
        (False, False, False, True, True),
    },
    ("aggregate_noop", "metadata_event", "sent"): {
        (False, False, False, False, True),
        (False, False, False, True, True),
    },
}


APPLICATION_VALID_CASES = [
    (disposition, reason, status, *flags)
    for (
        disposition,
        reason,
        status,
    ), allowed_flags in APPLICATION_FLAG_MANIFEST.items()
    for flags in sorted(allowed_flags)
]

PURE_APPLICATION_CLOSURE_CASES = list(
    product(
        [None, *sorted(DATABASE_STATUSES)],
        (False, True),
        ("create", "update", "read", "delete"),
        (False, True),
        (False, True),
        (False, True),
    )
)


@pytest.mark.parametrize(
    (
        "disposition",
        "reason",
        "status",
        "should_process",
        "should_cancel",
        "cancel_pending",
        "create_seen",
        "may_complete",
    ),
    APPLICATION_VALID_CASES,
)
def test_email_event_application_accepts_complete_disposition_manifest(
    disposition: str,
    reason: str,
    status: str,
    should_process: bool,
    should_cancel: bool,
    cancel_pending: bool,
    create_seen: bool,
    may_complete: bool,
) -> None:
    decision = _decision(
        should_process=should_process,
        should_cancel=should_cancel,
        new_status=status,
        cancel_pending_side_effects=cancel_pending,
        create_seen=create_seen,
        reason=reason,
    )

    application = _application(
        decision=decision,
        persisted_status=status,
        disposition=disposition,
        may_complete_without_processing=may_complete,
    )

    assert application.should_process is should_process
    assert application.should_cancel is should_cancel
    assert application.cancel_pending_side_effects is cancel_pending
    assert application.may_complete_without_processing is may_complete


@pytest.mark.parametrize(
    (
        "current_status",
        "create_seen",
        "kind",
        "processing_owner_matches",
        "external_effects_started",
        "source_deleted",
    ),
    PURE_APPLICATION_CLOSURE_CASES,
)
def test_every_pure_decision_has_every_repository_application_shape(
    current_status: str | None,
    create_seen: bool,
    kind: str,
    processing_owner_matches: bool,
    external_effects_started: bool,
    source_deleted: bool,
) -> None:
    module = _email_events()
    decision = module.decide_email_event(
        current_status=current_status,
        create_seen=create_seen,
        kind=kind,
        source_is_read=None,
        processing_owner_matches=processing_owner_matches,
        external_effects_started=external_effects_started,
        source_deleted=source_deleted,
    )

    if current_status is None:
        dispositions = {
            "create": ("creator_elected",),
            "update": ("metadata_shell_created",),
            "read": ("metadata_shell_created",),
            "delete": ("tombstone_created",),
        }[kind]
    elif decision.should_process:
        dispositions = (
            "creator_elected"
            if decision.reason.value == "first_create"
            else "processing_resumed",
        )
    elif source_deleted:
        dispositions = ("aggregate_noop",)
    elif kind in {"update", "read"}:
        dispositions = ("aggregate_updated", "aggregate_noop")
    elif kind == "delete":
        dispositions = ("aggregate_updated",)
    else:
        dispositions = ("aggregate_noop",)

    for disposition in dispositions:
        _application(
            decision=decision,
            persisted_status=decision.new_status,
            disposition=disposition,
            may_complete_without_processing=not decision.should_process,
        )


def test_create_unseen_duplicate_cannot_replace_first_create_from_ingested() -> None:
    with pytest.raises(ValueError):
        _application(
            decision=_decision(
                new_status="ingested",
                create_seen=False,
                reason="duplicate_create",
            ),
            persisted_status="ingested",
            disposition="aggregate_noop",
        )


ALLOWED_REASONS_BY_DISPOSITION = {
    "creator_elected": {"first_create"},
    "processing_resumed": {"processing_resumed"},
    "processing_already_elected": {"processing_attempt_already_elected"},
    "metadata_shell_created": {"metadata_event"},
    "tombstone_created": {"source_tombstone"},
    "aggregate_updated": {
        "metadata_event",
        "source_delete_cancelled",
        "source_delete_recorded",
        "status_preserved",
    },
    "aggregate_noop": {
        "duplicate_create",
        "source_deleted_preserved",
        "status_preserved",
        "metadata_event",
    },
}


@pytest.mark.parametrize(
    ("disposition", "reason"),
    [
        (disposition, reason)
        for disposition, allowed_reasons in ALLOWED_REASONS_BY_DISPOSITION.items()
        for reason in sorted(REASONS - allowed_reasons)
    ],
)
def test_email_event_application_rejects_every_invalid_disposition_reason_pair(
    disposition: str,
    reason: str,
) -> None:
    requires_processing = disposition in {"creator_elected", "processing_resumed"}
    status = (
        "processing"
        if requires_processing or disposition == "processing_already_elected"
        else "ingested"
    )
    with pytest.raises(ValueError):
        _application(
            decision=_decision(
                should_process=requires_processing,
                new_status=status,
                create_seen=requires_processing,
                reason=reason,
            ),
            persisted_status=status,
            disposition=disposition,
            may_complete_without_processing=not requires_processing
            and disposition != "processing_already_elected",
        )


INVALID_APPLICATION_FLAG_CASES = [
    (disposition, reason, status, *flags)
    for (
        disposition,
        reason,
        status,
    ), allowed_flags in APPLICATION_FLAG_MANIFEST.items()
    for flags in product((False, True), repeat=5)
    if flags not in allowed_flags
]


@pytest.mark.parametrize(
    (
        "disposition",
        "reason",
        "status",
        "should_process",
        "should_cancel",
        "cancel_pending",
        "create_seen",
        "may_complete",
    ),
    INVALID_APPLICATION_FLAG_CASES,
)
def test_email_event_application_rejects_every_invalid_boolean_manifest_row(
    disposition: str,
    reason: str,
    status: str,
    should_process: bool,
    should_cancel: bool,
    cancel_pending: bool,
    create_seen: bool,
    may_complete: bool,
) -> None:
    with pytest.raises(ValueError):
        _application(
            decision=_decision(
                should_process=should_process,
                should_cancel=should_cancel,
                new_status=status,
                cancel_pending_side_effects=cancel_pending,
                create_seen=create_seen,
                reason=reason,
            ),
            persisted_status=status,
            disposition=disposition,
            may_complete_without_processing=may_complete,
        )


@pytest.mark.parametrize(
    "disposition",
    ["creator_elected", "processing_resumed", "processing_already_elected"],
)
def test_processing_dispositions_require_persisted_processing_status(
    disposition: str,
) -> None:
    reason = {
        "creator_elected": "first_create",
        "processing_resumed": "processing_resumed",
        "processing_already_elected": "processing_attempt_already_elected",
    }[disposition]
    should_process = disposition != "processing_already_elected"
    with pytest.raises(ValueError):
        _application(
            decision=_decision(
                should_process=should_process,
                new_status="ingested",
                create_seen=should_process,
                reason=reason,
            ),
            persisted_status="ingested",
            disposition=disposition,
            may_complete_without_processing=False,
        )


@pytest.mark.parametrize(
    ("kind", "status", "process", "cancel", "cancel_pending", "create_seen", "reason"),
    [
        ("create", "processing", True, False, False, True, "first_create"),
        ("update", "ingested", False, False, False, False, "metadata_event"),
        ("read", "ingested", False, False, False, False, "metadata_event"),
        ("delete", "cancelled", False, True, False, False, "source_tombstone"),
    ],
)
def test_every_event_from_missing_aggregate_has_explicit_decision(
    kind: str,
    status: str,
    process: bool,
    cancel: bool,
    cancel_pending: bool,
    create_seen: bool,
    reason: str,
) -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status=None,
        create_seen=False,
        kind=kind,
        source_is_read=None,
    )

    assert decision == module.EmailEventDecision(
        should_process=process,
        should_cancel=cancel,
        new_status=status,
        cancel_pending_side_effects=cancel_pending,
        create_seen=create_seen,
        reason=reason,
    )


def test_first_create_after_metadata_shell_enters_processing() -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status="ingested",
        create_seen=False,
        kind="create",
        source_is_read=False,
    )

    assert decision.new_status is module.EmailStatus.PROCESSING
    assert decision.should_process is True
    assert decision.create_seen is True
    assert decision.reason is module.EmailEventReason.FIRST_CREATE


@pytest.mark.parametrize("status", ["processing", "retry_wait"])
def test_same_processing_owner_may_resume_when_effects_have_not_started(
    status: str,
) -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status=status,
        create_seen=True,
        kind=ChangeKind.CREATE,
        source_is_read=False,
        processing_owner_matches=True,
    )

    assert decision.new_status is module.EmailStatus.PROCESSING
    assert decision.should_process is True
    assert decision.reason is module.EmailEventReason.PROCESSING_RESUMED


@pytest.mark.parametrize(
    ("status", "owner_matches", "effects_started"),
    [
        (status, owner_matches, effects_started)
        for status in DATABASE_STATUSES
        for owner_matches, effects_started in [(False, False), (True, True)]
        if not (
            status in {"processing", "retry_wait"}
            and owner_matches
            and not effects_started
        )
    ],
)
def test_every_other_existing_create_is_a_status_preserving_duplicate(
    status: str,
    owner_matches: bool,
    effects_started: bool,
) -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status=status,
        create_seen=True,
        kind="create",
        source_is_read=None,
        processing_owner_matches=owner_matches,
        external_effects_started=effects_started,
    )

    assert decision.new_status.value == status
    assert decision.should_process is False
    assert decision.should_cancel is False
    assert decision.create_seen is True
    assert decision.reason is module.EmailEventReason.DUPLICATE_CREATE


@pytest.mark.parametrize("status", sorted(DATABASE_STATUSES))
@pytest.mark.parametrize("kind", ["update", "read"])
def test_update_and_read_preserve_every_existing_status(
    status: str,
    kind: str,
) -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status=status,
        create_seen=status != "ingested",
        kind=kind,
        source_is_read=True,
    )

    assert decision.new_status.value == status
    assert decision.should_process is False
    assert decision.should_cancel is False
    assert decision.reason is module.EmailEventReason.METADATA_EVENT


@pytest.mark.parametrize("status", sorted(DATABASE_STATUSES))
@pytest.mark.parametrize("create_seen", [False, True])
def test_delete_matrix_without_external_effect_marker(
    status: str,
    create_seen: bool,
) -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status=status,
        create_seen=create_seen,
        kind="delete",
        source_is_read=None,
    )

    if status in CANCELLABLE_STATUSES:
        assert decision.new_status is module.EmailStatus.CANCELLED
        assert decision.should_cancel is True
        assert decision.cancel_pending_side_effects is not (
            status == "ingested" and not create_seen
        )
        assert decision.reason is module.EmailEventReason.SOURCE_DELETE_CANCELLED
    else:
        assert decision.new_status.value == status
        assert decision.should_cancel is False
        assert decision.cancel_pending_side_effects is False
        assert decision.reason is module.EmailEventReason.SOURCE_DELETE_RECORDED
    assert decision.should_process is False
    assert decision.create_seen is create_seen


@pytest.mark.parametrize("status", sorted(DATABASE_STATUSES))
def test_delete_after_external_effect_start_preserves_every_status(status: str) -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status=status,
        create_seen=True,
        kind="delete",
        source_is_read=False,
        external_effects_started=True,
    )

    assert decision.new_status.value == status
    assert decision.should_process is False
    assert decision.should_cancel is False
    assert decision.cancel_pending_side_effects is False
    assert decision.reason is module.EmailEventReason.SOURCE_DELETE_RECORDED


@pytest.mark.parametrize("status", sorted(DATABASE_STATUSES))
@pytest.mark.parametrize("kind", list(ChangeKind))
def test_source_tombstone_preserves_every_status_and_never_repeats_cancellation(
    status: str,
    kind: ChangeKind,
) -> None:
    module = _email_events()

    decision = module.decide_email_event(
        current_status=status,
        create_seen=status != "cancelled",
        kind=kind,
        source_is_read=None,
        processing_owner_matches=True,
        source_deleted=True,
    )

    assert decision.new_status.value == status
    assert decision.should_process is False
    assert decision.should_cancel is False
    assert decision.cancel_pending_side_effects is False
    assert decision.reason is module.EmailEventReason.SOURCE_DELETED_PRESERVED


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("current_status", "unknown"),
        ("current_status", 1),
        ("create_seen", 1),
        ("kind", "move"),
        ("kind", 1),
        ("source_is_read", 1),
        ("source_is_read", "true"),
        ("processing_owner_matches", 1),
        ("external_effects_started", 0),
        ("source_deleted", None),
    ],
)
def test_decide_email_event_strictly_rejects_invalid_inputs(
    field: str,
    value: object,
) -> None:
    module = _email_events()
    values: dict[str, object] = {
        "current_status": None,
        "create_seen": False,
        "kind": "create",
        "source_is_read": None,
    }
    values[field] = value

    with pytest.raises(ValueError):
        module.decide_email_event(**values)


def test_decide_email_event_is_keyword_only() -> None:
    module = _email_events()

    with pytest.raises(TypeError):
        module.decide_email_event(None, False, "create", None)
