import pytest
import os
import sys
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("PYTHON_DOTENV_DISABLED", "1")

@pytest.fixture
def mock_settings():
    """Mock settings environment variables."""
    msg = MagicMock()
    msg.EXCHANGE_API_URL = "http://mock-api"
    msg.EXCHANGE_API_KEY = "test-key"
    msg.EXCHANGE_ACCOUNT_ID = "test-account-id"
    msg.EXCHANGE_SSL_VERIFY = False
    return msg

@pytest.fixture
def mock_env(monkeypatch):
    """Set environment variables for testing."""
    from src.config import get_settings
    get_settings.cache_clear()
    monkeypatch.setenv("EXCHANGE_API_URL", "http://mock-api")
    monkeypatch.setenv("EXCHANGE_API_KEY", "test-key")
    monkeypatch.setenv("EXCHANGE_ACCOUNT_ID", "8")
    monkeypatch.setenv("LARK_APP_ID", "test-lark-id")
    monkeypatch.setenv("LARK_APP_SECRET", "test-lark-secret")
    monkeypatch.setenv("LARK_CHAT_ID", "test-chat-id")
    yield
    get_settings.cache_clear()


class GraphNodeHarness:
    def __init__(self):
        from src.graph.dependencies import GraphDependencies

        self.email = {}
        self.loads = []
        self.drafts = {}
        self.draft_saves = []
        self.dependencies = GraphDependencies(content_store=self, drafts=self)

    async def load_email(self, ref, *, include_attachments=False):
        self.loads.append((ref, include_attachments))
        return deepcopy(self.email)

    async def save_draft(self, email_id, content):
        self.draft_saves.append((email_id, content))
        self.drafts[email_id] = content
        return email_id

    async def load_draft(self, draft_id):
        return self.drafts[draft_id]

    def state(self, email=None, *, draft=None, **updates):
        from src.graph.state_factory import build_initial_graph_state
        from src.storage import ContentRef

        email = deepcopy(email or {})
        email.setdefault("id", "test-mail-1")
        self.email = deepcopy(email)
        ref = ContentRef(
            account_id=8,
            object_id="00000000-0000-4000-8000-000000000117",
            key_version="v1",
            sha256="b" * 64,
        )
        state = build_initial_graph_state(email, ref)
        if "context" in updates:
            contexts = updates.pop("context") or []
            state["context_summaries"] = [
                {
                    "id": str(item.get("id", index)),
                    "sender": item.get("sender", ""),
                    "subject": item.get("subject", ""),
                    "snippet": item.get("body") or item.get("chunk_text") or "",
                }
                for index, item in enumerate(contexts[:5])
            ]
        updates.pop("feedback", None)
        updates.pop("email", None)
        updates.pop("draft", None)
        state.update(updates)
        if draft is not None:
            self.drafts[email["id"]] = draft
            state["draft_id"] = email["id"]
        return state


@pytest.fixture
def graph_node_harness(monkeypatch):
    monkeypatch.setattr(
        "src.graph.state_factory.get_settings",
        lambda: SimpleNamespace(EXCHANGE_ACCOUNT_ID=8),
    )
    return GraphNodeHarness()


@pytest.fixture
def route_decision_factory():
    """Build a finalized canonical route for graph/node contract tests."""

    from src.router.decision import (
        DecisionOutcome,
        RouteDecision,
        RouteProvenance,
        RouteTier,
    )

    def build(route="reply", *, params=None, profile_id=None, reason_code="test_route"):
        if params is None:
            if route == "forward":
                params = {
                    "fixed_recipients": ["approved@example.com"],
                    "cc": [],
                    "allow_recipient_edit": True,
                    "include_attachments": False,
                }
            elif route in {"no_action", "manual_review"}:
                params = {"reason_code": reason_code}
            else:
                params = {}
        if profile_id is None:
            profile_id = (
                "generic_reply_v1"
                if route == "reply"
                else "generic_forward_v1"
                if route == "forward"
                else None
            )
        return RouteDecision(
            outcome=DecisionOutcome.MATCHED,
            route=route,
            params=params,
            provenance=RouteProvenance(
                tier=RouteTier.SYSTEM,
                source_version="test-route-v1",
                confidence=1.0,
            ),
            reason_code=reason_code,
            handoff_profile_id=profile_id,
        ).model_dump(mode="json")

    return build
