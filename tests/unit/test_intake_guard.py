from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from src.intake_guard import (
    IntakeDisposition,
    IntakeGuard,
    release_quarantine,
)


def test_digest_is_stable_across_mapping_order_and_decision_is_immutable() -> None:
    guard = IntakeGuard()
    first = guard.evaluate({"sender": "a@example.test", "subject": "Hello"})
    second = guard.evaluate({"subject": "Hello", "sender": "a@example.test"})

    assert first.message_snapshot_digest == second.message_snapshot_digest
    with pytest.raises(FrozenInstanceError):
        first.reason_code = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("email", "disposition", "reason"),
    [
        (
            {"subject": "Automatic reply: away", "sender": "person@example.test"},
            IntakeDisposition.SUPPRESS,
            "automatic_reply",
        ),
        (
            {
                "subject": "hello",
                "sender": "a@example.test",
                "headers": {"X-Loop": "agent"},
            },
            IntakeDisposition.SUPPRESS,
            "mail_loop",
        ),
        (
            {"subject": "Undeliverable", "sender": "postmaster"},
            IntakeDisposition.QUARANTINE,
            "ndr_detected",
        ),
        (
            {
                "subject": "hello",
                "sender": "a@example.test",
                "headers": {"Sensitivity": "confidential"},
            },
            IntakeDisposition.QUARANTINE,
            "sensitive_message",
        ),
        (
            {"subject": 7, "sender": "a@example.test"},
            IntakeDisposition.QUARANTINE,
            "detail_parse_error",
        ),
    ],
)
def test_conservative_detections(email, disposition, reason) -> None:
    decision = IntakeGuard().evaluate(email)

    assert decision.disposition is disposition
    assert decision.reason_code == reason
    assert decision.index_in_qdrant is False


def test_normal_and_marketing_mail_pass() -> None:
    guard = IntakeGuard()

    normal = guard.evaluate({"subject": "Question", "sender": "a@example.test"})
    assert normal.disposition is IntakeDisposition.PASS
    marketing = guard.evaluate(
        {
            "subject": "August product newsletter and special offer",
            "sender": "marketing@example.test",
            "headers": {"List-Unsubscribe": "https://example.test/unsubscribe"},
        }
    )
    assert marketing.disposition is IntakeDisposition.PASS
    assert marketing.index_in_qdrant is True


def test_release_creates_new_attempt_instruction_without_mutating_decision() -> None:
    decision = IntakeGuard().evaluate(
        {"subject": "Undeliverable", "sender": "postmaster"}
    )

    instruction = release_quarantine(decision)

    assert instruction.require_new_attempt is True
    assert instruction.source_decision_digest == decision.message_snapshot_digest
    assert decision.disposition is IntakeDisposition.QUARANTINE
    with pytest.raises(ValueError, match="only a quarantine"):
        release_quarantine(
            IntakeGuard().evaluate({"subject": "hello", "sender": "a@example.test"})
        )
