"""Regression coverage for ``resolve_bookkeeping_as_node``.

Production incident recap: a Tier 1/2/3 route that bypasses
``_run_ai_pipeline`` (e.g. ``manual_review`` from a Tier 1 rule conflict)
still seeds the LangGraph checkpoint via
``graph.aupdate_state(config, state, as_node="__start__")`` so cleanup
handles survive a crash. Every later out-of-band write in
``email_feishu_delivery.py`` / ``lark_pdf_flow.py`` that persisted
``pdf_token``/``attachment_tokens`` bookkeeping omitted ``as_node``, and
LangGraph deterministically raised ``InvalidUpdateError: Ambiguous update,
specify as_node`` because the seed leaves ``versions_seen`` with only an
empty ``"__start__"`` entry (zero writer candidates to infer from).

These tests exercise the real ``langgraph`` package (via ``MemorySaver``,
not a mock) for both the pristine and the already-advanced checkpoint, since
this exact failure mode is invisible to tests that mock ``aupdate_state``.
"""

from __future__ import annotations

import pytest
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from src.graph.builder import build_graph
from src.graph.state_factory import GRAPH_ENTRY_NODE, resolve_bookkeeping_as_node


def test_resolve_bookkeeping_as_node_accepts_a_snapshot_or_a_bare_tuple():
    """Both call shapes used in production must resolve identically."""

    class _Snapshot:
        def __init__(self, next_nodes):
            self.next = next_nodes

    assert resolve_bookkeeping_as_node(_Snapshot((GRAPH_ENTRY_NODE,))) == "__start__"
    assert resolve_bookkeeping_as_node((GRAPH_ENTRY_NODE,)) == "__start__"
    assert resolve_bookkeeping_as_node(_Snapshot(())) is None
    assert resolve_bookkeeping_as_node(()) is None
    assert resolve_bookkeeping_as_node(_Snapshot(("reviewer",))) is None
    assert resolve_bookkeeping_as_node(None) is None


@pytest.mark.asyncio
async def test_pristine_seed_checkpoint_resolves_to_start():
    """The exact production shape: seeded via as_node="__start__", never run."""
    checkpointer = MemorySaver()
    graph = build_graph(checkpointer, dependencies=None)
    config = {"configurable": {"thread_id": "pristine"}}

    await graph.aupdate_state(
        config,
        {
            "email_id": "pristine",
            "content_ref": "ref-1",
            "attachment_tokens": [],
            "pdf_token": None,
        },
        as_node="__start__",
    )
    state = await graph.aget_state(config)
    assert state.next == (GRAPH_ENTRY_NODE,)
    assert resolve_bookkeeping_as_node(state) == "__start__"

    # This is the exact write shape used for PDF/attachment bookkeeping.
    # Without as_node, this raises InvalidUpdateError.
    await graph.aupdate_state(
        config,
        {"pdf_token": "pdf-1", "attachment_tokens": ["pdf-1"]},
        as_node=resolve_bookkeeping_as_node(state),
    )
    confirmed = await graph.aget_state(config)
    assert confirmed.values["pdf_token"] == "pdf-1"
    # The bookkeeping write must not resurrect the pipeline.
    assert confirmed.next == (GRAPH_ENTRY_NODE,)


@pytest.mark.asyncio
async def test_bookkeeping_write_does_not_rewind_a_checkpoint_that_already_ran():
    """Once a real node has run, forcing as_node="__start__" would corrupt state.

    Uses a minimal graph with the same entry point / interrupt topology as
    ``src.graph.builder.build_graph`` but trivial node bodies, so the test
    exercises genuine LangGraph execution (real ``versions_seen`` bookkeeping)
    without pulling in the categorizer/retriever/drafter LLM dependencies.
    """

    class _State(TypedDict, total=False):
        email_id: str
        pdf_token: str | None
        attachment_tokens: list
        approval_status: str

    async def _entry(_state):
        return {}

    async def _reviewer(_state):
        return {"approval_status": "pending_review"}

    workflow = StateGraph(_State)
    workflow.add_node(GRAPH_ENTRY_NODE, _entry)
    workflow.add_node("reviewer", _reviewer)
    workflow.set_entry_point(GRAPH_ENTRY_NODE)
    workflow.add_edge(GRAPH_ENTRY_NODE, "reviewer")
    workflow.add_edge("reviewer", END)

    checkpointer = MemorySaver()
    graph = workflow.compile(checkpointer=checkpointer, interrupt_after=["reviewer"])
    config = {"configurable": {"thread_id": "advanced"}}

    await graph.aupdate_state(
        config,
        {"email_id": "advanced", "attachment_tokens": [], "pdf_token": None},
        as_node="__start__",
    )
    await graph.ainvoke(None, config)
    state = await graph.aget_state(config)
    assert state.next == ()
    assert resolve_bookkeeping_as_node(state) is None

    await graph.aupdate_state(
        config,
        {"pdf_token": "pdf-2", "attachment_tokens": ["pdf-2"]},
        as_node=resolve_bookkeeping_as_node(state),
    )
    confirmed = await graph.aget_state(config)
    assert confirmed.values["pdf_token"] == "pdf-2"
    # Must stay parked at the reviewer interrupt, not get rewound to the
    # entry point.
    assert confirmed.next == ()
    assert confirmed.values["approval_status"] == "pending_review"
