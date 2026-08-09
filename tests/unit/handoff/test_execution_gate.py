from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.domain.send_result import ExchangeSendResult
from src.graph.dependencies import GraphDependencies
from src.graph.state_factory import build_initial_graph_state
from src.nodes.sender import send_final_email
from src.router.decision import DecisionOutcome, RouteDecision, RouteProvenance, RouteTier
from src.safety.execution_gate import ApprovedExecutionEnvelope, ExecutionGate
from src.storage import ContentRef


INBOX_ID = "00000000-0000-4000-8000-000000000101"
EMAIL_ID = "durable-mail-1"


def _decision() -> RouteDecision:
    return RouteDecision(
        outcome=DecisionOutcome.MATCHED,
        route="forward",
        params={
            "fixed_recipients": ["approved@example.com"],
            "cc": [],
            "allow_recipient_edit": True,
            "include_attachments": False,
        },
        provenance=RouteProvenance(
            tier=RouteTier.TIER3,
            source_version="router-model-v1",
            confidence=1.0,
        ),
        reason_code="forward_for_review",
        handoff_profile_id="generic_forward_v1",
    )


def _envelope() -> ApprovedExecutionEnvelope:
    draft = "approved frozen draft"
    decision = _decision()
    payload = {
        "decision_digest": decision.canonical_digest(),
        "plan_digest": "1" * 64,
        "evidence_digest": "2" * 64,
        "draft_digest": sha256(draft.encode()).hexdigest(),
        "draft_content": draft,
        "draft_ref": {"draft_id": EMAIL_ID},
        "to": ["approved@example.com"],
        "cc": [],
        "attachment_refs": [],
        "attachment_digests": [],
        "external_recipient_acknowledged": True,
    }
    return ApprovedExecutionEnvelope(
        inbox_id=INBOX_ID,
        account_id=8,
        email_id=EMAIL_ID,
        payload_revision=1,
        payload_digest=sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode()
        ).hexdigest(),
        route_decision=decision,
        decision_digest=decision.canonical_digest(),
        plan_digest="1" * 64,
        evidence_digest="2" * 64,
        draft_digest=sha256(draft.encode()).hexdigest(),
        draft_content=draft,
        draft_ref={"draft_id": EMAIL_ID},
        to=("approved@example.com",),
        cc=(),
        attachment_refs=(),
        attachment_digests=(),
        external_recipient_acknowledged=True,
        approver="ou_approver",
        approved_at=datetime.now(UTC),
    )


def test_execution_gate_rejects_any_change_to_the_approved_envelope():
    envelope = _envelope()
    gate = ExecutionGate()
    assert gate.validate(
        envelope.model_dump(mode="json"),
        expected_envelope_digest=envelope.canonical_digest(),
    ) == envelope

    changed = envelope.model_dump(mode="json")
    changed["draft_content"] = "mutable checkpoint draft"
    with pytest.raises(ValueError, match="draft_digest_mismatch"):
        gate.validate(changed)


def test_execution_gate_rejects_unbound_original_attachments():
    raw = _envelope().model_dump(mode="json")
    raw["route_decision"]["params"]["include_attachments"] = True
    raw["decision_digest"] = RouteDecision.model_validate(
        raw["route_decision"]
    ).canonical_digest()
    raw["payload_digest"] = sha256(
        json.dumps(
            {
                key: raw[key]
                for key in (
                    "decision_digest",
                    "plan_digest",
                    "evidence_digest",
                    "draft_digest",
                    "draft_content",
                    "draft_ref",
                    "to",
                    "cc",
                    "attachment_refs",
                    "attachment_digests",
                    "external_recipient_acknowledged",
                )
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
    ).hexdigest()
    with pytest.raises(ValueError, match="unbound_forward_attachments"):
        ExecutionGate().validate(raw)


class _ContentStore:
    async def load_email(self, _ref, *, include_attachments=False):
        del include_attachments
        return {
            "id": EMAIL_ID,
            "sender": "sender@example.com",
            "subject": "subject",
            "body": "body",
        }


class _DraftStore:
    async def load_draft(self, _draft_id):
        return "mutable checkpoint draft"


class _DurableDB:
    def __init__(self, envelope: ApprovedExecutionEnvelope):
        self.envelope = envelope
        self.run = {
            "state": "approved",
            "version": 3,
            "payload_revision": 1,
            "decision_digest": envelope.decision_digest,
            "plan_digest": envelope.plan_digest,
            "evidence_digest": envelope.evidence_digest,
        }
        self.email_status = "approved"

    async def get_handoff_run(self, inbox_id):
        assert inbox_id == INBOX_ID
        return deepcopy(self.run)

    async def get_approved_execution_envelope(self, *, inbox_id, revision):
        assert (inbox_id, revision) == (INBOX_ID, 1)
        return {
            "envelope": self.envelope.model_dump(mode="json"),
            "envelope_digest": self.envelope.canonical_digest(),
        }

    async def claim_execution(self, *, inbox_id, revision, expected_version, claim_id):
        assert (inbox_id, revision, expected_version) == (INBOX_ID, 1, 3)
        assert claim_id
        self.run.update(state="executing", version=4)
        self.email_status = "sending"
        return True

    async def complete_execution(self, *, inbox_id, expected_version, sent):
        assert (inbox_id, expected_version, sent) == (INBOX_ID, 4, True)
        self.run.update(state="completed", version=5)
        self.email_status = "sent"
        return True

    async def get_email_status(self, _email_id):
        return self.email_status


@pytest.mark.asyncio
async def test_sender_uses_frozen_envelope_not_mutable_route_draft_or_recipients(monkeypatch):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    ref = ContentRef(
        account_id=8,
        object_id="00000000-0000-4000-8000-000000000008",
        key_version="v1",
        sha256="8" * 64,
    )
    state = build_initial_graph_state(
        {
            "id": EMAIL_ID,
            "sender": "sender@example.com",
            "subject": "subject",
            "draft_to": ["mutable@example.com"],
            "draft_cc": [],
        },
        ref,
    )
    state.update(
        {
            "inbox_id": INBOX_ID,
            "classification": {"action": "reply"},
            "draft_id": EMAIL_ID,
            "draft_to": ["mutable@example.com"],
            "approval_status": "approved",
        }
    )
    envelope = _envelope()
    db = _DurableDB(envelope)
    forward = AsyncMock(return_value=ExchangeSendResult.sent())
    reply = AsyncMock(return_value=ExchangeSendResult.sent())
    ctx = SimpleNamespace(
        db_manager=db,
        exchange_client=SimpleNamespace(
            forward_email_result=forward,
            reply_email_result=reply,
        ),
        email_processor=SimpleNamespace(process_sent_email=MagicMock()),
    )
    dependencies = GraphDependencies(
        content_store=_ContentStore(),
        drafts=_DraftStore(),
    )

    with patch("src.init_app.get_app_context", return_value=ctx):
        result = await send_final_email(state, dependencies)

    assert result == {"next_step": "end"}
    reply.assert_not_awaited()
    forward.assert_awaited_once_with(
        email_id=EMAIL_ID,
        to=["approved@example.com"],
        body="approved frozen draft",
        include_attachments=False,
    )
