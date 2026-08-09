import json
from copy import deepcopy
from collections.abc import Sequence

import pytest

from src.graph.state_factory import (
    MAX_CHECKPOINT_BYTES,
    MAX_IMAGE_ANALYSIS_BYTES,
    MAX_LOGICAL_STATE_BYTES,
    build_initial_graph_state,
    cap_string_list,
    content_ref_from_json,
    content_ref_to_json,
    ensure_state_size,
    sanitize_graph_delta,
    truncate_utf8,
)
from src.storage import ContentRef, ContentStoreReferenceError


def _content_ref(*, account_id: int = 8) -> ContentRef:
    return ContentRef(
        account_id=account_id,
        object_id="00000000-0000-4000-8000-000000000007",
        key_version="v1",
        sha256="0" * 64,
    )


def test_content_ref_json_round_trip_is_strict_and_account_owned():
    ref = _content_ref()

    encoded = content_ref_to_json(ref)

    assert encoded == {
        "account_id": 8,
        "object_id": "00000000-0000-4000-8000-000000000007",
        "key_version": "v1",
        "sha256": "0" * 64,
    }
    assert content_ref_from_json(encoded, expected_account_id=8) == ref
    with pytest.raises(ContentStoreReferenceError):
        content_ref_from_json(encoded, expected_account_id=9)
    with pytest.raises(ContentStoreReferenceError):
        content_ref_from_json({**encoded, "extra": "forbidden"}, expected_account_id=8)


def test_utf8_truncation_and_collection_caps_are_byte_aware():
    assert truncate_utf8("汉汉汉", max_bytes=7) == "汉汉"
    assert cap_string_list(
        ["a" * 20, "汉汉汉", "third", "ignored"],
        max_items=3,
        max_item_bytes=7,
    ) == ["a" * 7, "汉汉", "third"]


def test_collection_cap_does_not_materialize_an_unbounded_sequence():
    class GuardedSequence(Sequence):
        def __len__(self):
            return 1_000_000_000

        def __getitem__(self, index):
            if index >= 3:
                raise AssertionError("sequence consumed past max_items")
            return f"item-{index}"

    assert cap_string_list(
        GuardedSequence(),
        max_items=3,
        max_item_bytes=20,
    ) == ["item-0", "item-1", "item-2"]


def test_initial_state_allowlists_metadata_without_mutating_caller():
    metadata = {
        "id": "mail-1",
        "subject": "主题" * 1_000,
        "sender": "sender@example.com",
        "thread_id": "thread-1",
        "conversation_id": "conversation-1",
        "received_at": "2026-07-11T00:00:00+08:00",
        "draft_to": [f"to-{i}@example.com" for i in range(50)],
        "draft_cc": [f"cc-{i}@example.com" for i in range(50)],
        "body": "BODY-SENTINEL" * 1_000,
        "html": "<p>HTML-SENTINEL</p>",
        "attachments": [{"content": "BASE64-SENTINEL"}],
        "_image_attachments": [{"content": "IMAGE-SENTINEL"}],
        "unknown": "must-not-survive",
    }
    before = deepcopy(metadata)

    state = build_initial_graph_state(metadata, _content_ref())
    encoded = json.dumps(state, ensure_ascii=False, sort_keys=True).encode("utf-8")

    assert metadata == before
    assert state["email_id"] == "mail-1"
    assert set(state["email"]) == {
        "subject",
        "sender",
        "thread_id",
        "conversation_id",
        "received_at",
    }
    assert state["content_ref"] == content_ref_to_json(_content_ref())
    assert state["draft_id"] is None
    assert state["draft_to"] == metadata["draft_to"][:10]
    assert state["draft_cc"] == metadata["draft_cc"][:10]
    assert state["classification"] == {}
    assert state["context_summaries"] == []
    for forbidden in (
        b"BODY-SENTINEL",
        b"HTML-SENTINEL",
        b"BASE64-SENTINEL",
        b"IMAGE-SENTINEL",
        b"must-not-survive",
    ):
        assert forbidden not in encoded
    assert len(encoded) < MAX_CHECKPOINT_BYTES
    ensure_state_size(state)


def test_state_size_guard_rejects_oversized_serialized_state():
    with pytest.raises(ValueError, match="graph_state_too_large"):
        ensure_state_size({"safe_error_summary": "x" * MAX_LOGICAL_STATE_BYTES})


def test_draft_id_must_belong_to_the_current_email():
    state = build_initial_graph_state({"id": "mail-A"}, _content_ref())

    with pytest.raises(ValueError, match="draft_email_mismatch"):
        sanitize_graph_delta(state, {"draft_id": "mail-B"})


@pytest.mark.parametrize("stage", ("tier1", "tier2", "tier3", "pending", "none"))
def test_routing_stage_is_bounded_to_the_known_lifecycle(stage: str):
    state = build_initial_graph_state({"id": "mail-1"}, _content_ref())

    assert sanitize_graph_delta(state, {"routing_stage": stage}) == {
        "routing_stage": stage
    }


@pytest.mark.parametrize("stage", ("unknown", ["tier1"], 1, None))
def test_routing_stage_rejects_invalid_or_unhashable_values(stage: object):
    state = build_initial_graph_state({"id": "mail-1"}, _content_ref())

    with pytest.raises(ValueError, match="invalid_routing_stage"):
        sanitize_graph_delta(state, {"routing_stage": stage})


def test_persistent_email_identifier_fails_closed_instead_of_being_truncated():
    with pytest.raises(ValueError, match="invalid_email_id"):
        build_initial_graph_state(
            {"id": "x" * 513, "subject": "subject"},
            _content_ref(),
        )


def test_persistent_recipient_and_attachment_tokens_fail_closed():
    with pytest.raises(ValueError, match="invalid_draft_to"):
        build_initial_graph_state(
            {"id": "mail-1", "draft_to": ["x" * 321]},
            _content_ref(),
        )

    state = build_initial_graph_state({"id": "mail-1"}, _content_ref())
    with pytest.raises(ValueError, match="invalid_attachment_token"):
        sanitize_graph_delta(
            state,
            {"attachment_tokens": ["x" * 513]},
        )

    twenty_tokens = [f"file-token-{index}" for index in range(20)]
    assert sanitize_graph_delta(
        state,
        {"attachment_tokens": twenty_tokens},
    )["attachment_tokens"] == twenty_tokens
    with pytest.raises(ValueError, match="invalid_attachment_token"):
        sanitize_graph_delta(
            state,
            {"attachment_tokens": [f"token-{index}" for index in range(33)]},
        )


def test_graph_delta_caps_model_values_and_rejects_oversized_human_values():
    state = build_initial_graph_state({"id": "mail-1"}, _content_ref())
    delta = sanitize_graph_delta(
        state,
        {
            "classification": {
                "priority": "P1",
                "need_reply": True,
                "summary": "汉" * 1_000,
                "reasoning": "r" * 5_000,
                "unknown": "drop-me",
            },
            "routing_log": ["route" * 1_000] * 100,
            "draft_to": [f"recipient-{i}@example.com" for i in range(10)],
            "metadata": {
                "review_count": 1,
                "review_issues": "issue" * 1_000,
                "image_analysis": "图" * 1_000,
                "unknown": "drop-me",
            },
            "email": {"body": "BODY-MUST-NOT-ENTER"},
            "draft": "DRAFT-MUST-NOT-ENTER",
            "feedback": "FEEDBACK-MUST-NOT-ENTER",
            "context": [{"chunk_text": "HIT-MUST-NOT-ENTER"}],
        },
    )
    encoded = json.dumps(delta, ensure_ascii=False).encode("utf-8")

    assert set(delta) == {
        "classification",
        "routing_log",
        "draft_to",
        "metadata",
    }
    assert len(delta["classification"]["summary"].encode("utf-8")) <= 512
    assert len(delta["classification"]["reasoning"].encode("utf-8")) <= 512
    assert len(delta["routing_log"]) <= 8
    assert len(delta["draft_to"]) <= 10
    assert set(delta["metadata"]) == {
        "review_count",
        "review_issues",
        "image_analysis",
    }
    assert (
        len(delta["metadata"]["image_analysis"].encode("utf-8"))
        <= MAX_IMAGE_ANALYSIS_BYTES
    )
    with pytest.raises(ValueError, match="invalid_draft_to"):
        sanitize_graph_delta(
            state,
            {"draft_to": [f"recipient-{i}@example.com" for i in range(11)]},
        )
    for forbidden in (
        b"BODY-MUST-NOT-ENTER",
        b"DRAFT-MUST-NOT-ENTER",
        b"FEEDBACK-MUST-NOT-ENTER",
        b"HIT-MUST-NOT-ENTER",
        b"drop-me",
    ):
        assert forbidden not in encoded
    ensure_state_size({**state, **delta})
