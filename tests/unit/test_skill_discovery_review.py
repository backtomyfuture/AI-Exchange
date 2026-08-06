"""Tests for chronological candidate review and explicit declarative promotion."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from src.skills_discovery.analyzer import DiscoveredPattern, EmailRecord
from src.skills_discovery.generator import (
    SkillPromotionConflict,
    SkillPromotionValidationError,
    promote_selected_candidates,
)
from src.skills_discovery.review import (
    apply_conversational_selections,
    create_candidate_review,
    load_review,
    render_review,
    split_records_chronologically,
    write_review,
)


def _record(
    index: int,
    *,
    sender: str = "other@example.com",
    message_type: str = "received",
    subject: str | None = None,
    to: list[str] | None = None,
) -> EmailRecord:
    return EmailRecord(
        id=f"mail-{index}",
        subject=subject or f"邮件 {index}",
        sender=sender,
        to=to or (["me@example.com"] if message_type != "sent" else [sender]),
        cc=[],
        received_at=f"2025-01-{index:02d}T09:00:00+08:00",
        message_type=message_type,
        body_preview="请处理" if message_type != "sent" else "已处理",
    )


def _pattern(**overrides) -> DiscoveredPattern:
    base = {
        "id": "discovered_001",
        "name": "财务通知",
        "description": "来自财务的固定通知",
        "trigger_type": "sender_match",
        "conditions": [{"type": "sender_match", "operator": "contains", "value": "finance@example.com"}],
        "reply_rate": 0.75,
        "sample_count": 8,
        "suggested_priority": "P2",
        "suggested_need_reply": True,
        "suggested_tone": "简洁专业",
        "confidence": 0.8,
    }
    base.update(overrides)
    return DiscoveredPattern(**base)


def _review_with_held_out_reply():
    records = [
        _record(index)
        for index in range(1, 9)
    ] + [
        _record(9, sender="finance@example.com", subject="财务通知"),
        _record(
            10,
            sender="me@example.com",
            message_type="sent",
            subject="Re: 财务通知",
            to=["finance@example.com"],
        ),
    ]
    training, held_out = split_records_chronologically(records)
    return create_candidate_review(
        [_pattern()],
        training_records=training,
        validation_records=held_out,
        source="qdrant",
        my_email="me@example.com",
    )


def test_chronological_split_uses_earliest_eighty_percent_for_discovery():
    records = [_record(index) for index in range(10, 0, -1)]

    training, held_out = split_records_chronologically(records)

    assert [record.id for record in training] == [f"mail-{index}" for index in range(1, 9)]
    assert [record.id for record in held_out] == ["mail-9", "mail-10"]


def test_review_replays_latest_twenty_percent_and_renders_all_effective_fields():
    review = _review_with_held_out_reply()
    candidate = review.candidates[0]

    assert review.training_records == 8
    assert review.held_out_records == 2
    assert candidate.validation.matched_count == 1
    assert candidate.validation.observed_reply_rate == 1.0
    rendered = render_review(review)
    assert "触发（and）" in rendered
    assert "建议结果" in rendered
    assert "最新 20% 回放：1/1 封收件命中" in rendered
    assert "固定收件人=无" in rendered


def test_selected_edits_replay_against_persisted_held_out_snapshot():
    review = create_candidate_review(
        [_pattern(conditions=[{"type": "sender_match", "operator": "contains", "value": "wrong@example.com"}])],
        training_records=[],
        validation_records=[_record(9, sender="finance@example.com")],
        source="qdrant",
        my_email="me@example.com",
    )
    assert review.candidates[0].validation.matched_count == 0

    selected = apply_conversational_selections(
        review,
        {
            "selections": [{
                "candidate_id": "discovered_001",
                "overrides": {
                    "conditions": [{
                        "type": "sender_match",
                        "operator": "contains",
                        "value": "finance@example.com",
                    }],
                },
            }],
        },
    )

    assert selected[0].validation.matched_count == 1
    assert selected[0].promotion_issues == []


def test_review_round_trip_preserves_replay_snapshot_for_later_selection(tmp_path: Path):
    review = _review_with_held_out_reply()
    output = write_review(review, tmp_path / "review.json")

    loaded = load_review(output)
    selected = apply_conversational_selections(
        loaded,
        [{"candidate_id": "discovered_001"}],
    )

    assert len(loaded.validation_records) == 2
    assert selected[0].validation.observed_reply_rate == 1.0


def test_forward_candidate_writes_manifest_only_after_explicit_selection(tmp_path: Path):
    review = create_candidate_review(
        [_pattern(
            suggested_action="forward",
            suggested_forward_to=["open_id=leader"],
            suggested_need_reply=True,
        )],
        training_records=[],
        validation_records=[],
        source="qdrant",
        my_email="me@example.com",
    )
    selected = apply_conversational_selections(
        review,
        [{
            "candidate_id": "discovered_001",
            "overrides": {"skill_id": "skill_auto_finance_forward"},
        }],
    )

    paths = promote_selected_candidates(selected, registry_path=str(tmp_path))

    skill_dir = Path(paths[0])
    assert (skill_dir / "manifest.yaml").exists()
    assert not (skill_dir / "handler.py").exists()
    manifest = yaml.safe_load((skill_dir / "manifest.yaml").read_text())
    assert manifest["auto_outcome"]["action"] == "forward"
    assert manifest["auto_outcome"]["forward_to"] == ["open_id=leader"]


def test_existing_target_conflict_stops_entire_selected_batch(tmp_path: Path):
    existing = tmp_path / "skill_auto_existing"
    existing.mkdir()
    review = create_candidate_review(
        [_pattern(id="discovered_001"), _pattern(id="discovered_002", name="另一条规则")],
        training_records=[],
        validation_records=[],
        source="qdrant",
    )
    selected = apply_conversational_selections(
        review,
        [
            {
                "candidate_id": "discovered_001",
                "overrides": {"skill_id": "skill_auto_new_rule"},
            },
            {
                "candidate_id": "discovered_002",
                "overrides": {"skill_id": "skill_auto_existing"},
            },
        ],
    )

    with pytest.raises(SkillPromotionConflict, match="skill_auto_existing"):
        promote_selected_candidates(selected, registry_path=str(tmp_path))

    assert not (tmp_path / "skill_auto_new_rule").exists()


def test_unsupported_condition_can_be_reviewed_but_not_promoted(tmp_path: Path):
    review = create_candidate_review(
        [_pattern(conditions=[{"type": "thread_depth", "operator": "gte", "value": "3"}])],
        training_records=[],
        validation_records=[],
        source="qdrant",
    )
    selected = apply_conversational_selections(review, [{"candidate_id": "discovered_001"}])

    with pytest.raises(SkillPromotionValidationError, match="不受运行时支持"):
        promote_selected_candidates(selected, registry_path=str(tmp_path))
