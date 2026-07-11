#!/usr/bin/env python3
"""在不把正文或草稿写入 Graph State 的前提下手工调用核心节点。"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from copy import deepcopy
from typing import Any, Awaitable, Callable
from uuid import uuid4

from dotenv import load_dotenv

from src.config import get_settings
from src.graph.dependencies import GraphDependencies
from src.graph.state import AgentState
from src.graph.state_factory import build_initial_graph_state
from src.nodes.categorizer import categorize_email
from src.nodes.drafter import generate_draft
from src.storage import ContentRef

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NodeCallable = Callable[[AgentState, GraphDependencies], Awaitable[AgentState]]

DEFAULT_EMAIL = {
    "id": "manual-node-smoke",
    "subject": "Test Email",
    "body": "This is a test email to check categorization and drafting.",
    "sender": "test@example.com",
}


class _InMemoryContentStore:
    """显式的手工测试边界；完整邮件只保存在节点外。"""

    def __init__(self, ref: ContentRef, email: dict[str, Any]):
        self._ref = ref
        self._email = deepcopy(email)

    async def load_email(
        self,
        ref: ContentRef,
        *,
        include_attachments: bool = False,
    ) -> dict[str, Any]:
        if ref != self._ref:
            raise KeyError("manual_content_ref_not_found")
        return deepcopy(self._email)


class _InMemoryDraftStore:
    """完整草稿只保存在独立存储中，State 只接收 draft_id。"""

    def __init__(self) -> None:
        self._drafts: dict[str, str] = {}

    async def save_draft(self, email_id: str, content: str) -> str:
        self._drafts[email_id] = content
        return email_id

    async def load_draft(self, draft_id: str) -> str:
        return self._drafts[draft_id]


def build_manual_node_boundary(
    email_data: dict[str, Any],
) -> tuple[dict[str, Any], GraphDependencies]:
    """为手工节点测试创建严格引用和瘦 State。"""
    email = deepcopy(email_data)
    email_id = email.get("id")
    if not isinstance(email_id, str) or not email_id:
        raise ValueError("manual_email_id_required")

    ref = ContentRef(
        account_id=get_settings().EXCHANGE_ACCOUNT_ID,
        object_id=str(uuid4()),
        key_version="v1",
        sha256=hashlib.sha256(email_id.encode("utf-8")).hexdigest(),
    )
    dependencies = GraphDependencies(
        content_store=_InMemoryContentStore(ref, email),
        drafts=_InMemoryDraftStore(),
    )
    return build_initial_graph_state(email, ref), dependencies


async def run_node_smoke(
    *,
    email_data: dict[str, Any] | None = None,
    categorize_fn: NodeCallable = categorize_email,
    draft_fn: NodeCallable = generate_draft,
) -> tuple[dict[str, Any], str]:
    """依次调用分类和拟稿节点，并返回最终瘦 State 与外置草稿。"""
    state, dependencies = build_manual_node_boundary(email_data or DEFAULT_EMAIL)

    print("Testing Categorizer...")
    try:
        state.update(await categorize_fn(state, dependencies))
        print("Categorization Result:", state.get("classification"))
    except Exception as exc:
        logger.exception(
            "Categorizer failed: error_type=%s",
            type(exc).__name__,
        )

    print("\nTesting Drafter...")
    try:
        state.update(await draft_fn(state, dependencies))
        draft_id = state.get("draft_id")
        if not isinstance(draft_id, str) or not draft_id:
            raise RuntimeError("manual_draft_id_missing")
        draft = await dependencies.drafts.load_draft(draft_id)
        print("Draft Result:", draft[:200] + ("..." if len(draft) > 200 else ""))
        return state, draft
    except Exception as exc:
        logger.exception(
            "Drafter failed: error_type=%s",
            type(exc).__name__,
        )
        return state, ""


if __name__ == "__main__":
    asyncio.run(run_node_smoke())
