from __future__ import annotations

from src.graph.state import AgentState
from src.safety.manual_review import build_manual_review_delta


async def enter_manual_review(state: AgentState) -> AgentState:
    """Normalize every fail-closed branch into one bounded terminal State."""
    return build_manual_review_delta(
        state,
        state.get("safe_error_summary"),
        classification=state.get("classification"),
        review_result=state.get("review_result"),
    )
