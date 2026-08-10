import logging
from functools import partial
from typing import Awaitable, Callable

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from langgraph.checkpoint.memory import MemorySaver
from src.graph.state import AgentState
from src.graph.state_factory import GRAPH_ENTRY_NODE
from src.graph.dependencies import GraphDependencies
from src.nodes.categorizer import categorize_email
from src.nodes.retriever_node import retrieve_context
from src.nodes.drafter import generate_draft
from src.nodes.reviewer import review_draft
from src.nodes.sender import send_final_email
from src.nodes.manual_review import enter_manual_review
from src.domain.errors import DatabaseOperationError
from src.router.decision import RouteDecision

logger = logging.getLogger(__name__)


class GraphNodeExecutionError(RuntimeError):
    """Fixed-size exception safe for LangGraph's ``__error__`` pending write."""

    def __init__(self, node_name: str):
        super().__init__(f"graph_node_failed:{node_name}")


def _guard_graph_node(
    node_name: str,
    node: Callable[..., Awaitable[AgentState]],
    *,
    pass_config: bool = False,
) -> Callable[..., Awaitable[AgentState]]:
    async def invoke(state: AgentState, config: RunnableConfig | None = None):
        try:
            if pass_config:
                return await node(state, config=config)
            return await node(state)
        except DatabaseOperationError:
            raise
        except Exception as exc:
            logger.error(
                "Graph node failed: node=%s error_type=%s",
                node_name,
                type(exc).__name__,
            )
            raise GraphNodeExecutionError(node_name) from None

    invoke.__name__ = f"guarded_{node_name}"
    if not pass_config:
        async def invoke_without_config(state: AgentState):
            return await invoke(state)

        invoke_without_config.__name__ = invoke.__name__
        return invoke_without_config
    return invoke


def build_graph(
    checkpointer=None,
    *,
    dependencies: GraphDependencies,
):
    """
    构建 LangGraph 工作流。
    """
    workflow = StateGraph(AgentState)

    workflow.add_node(
        "categorizer",
        _guard_graph_node(
            "categorizer",
            partial(categorize_email, dependencies=dependencies),
        ),
    )
    workflow.add_node(
        "retriever",
        _guard_graph_node(
            "retriever",
            partial(retrieve_context, dependencies=dependencies),
        ),
    )
    workflow.add_node(
        "drafter",
        _guard_graph_node(
            "drafter",
            partial(generate_draft, dependencies=dependencies),
        ),
    )
    workflow.add_node(
        "reviewer",
        _guard_graph_node(
            "reviewer",
            partial(review_draft, dependencies=dependencies),
        ),
    )
    workflow.add_node(
        "sender",
        _guard_graph_node(
            "sender",
            partial(send_final_email, dependencies=dependencies),
            pass_config=True,
        ),
    )
    workflow.add_node(
        "manual_review",
        _guard_graph_node("manual_review", enter_manual_review),
    )

    workflow.set_entry_point(GRAPH_ENTRY_NODE)

    def route_after_categorizer(state: AgentState):
        raw = state.get("route_decision")
        if raw is None:
            return "manual_review"
        route = RouteDecision.model_validate(raw).route.value
        if route in {"reply", "forward"}:
            return "retriever"
        if route == "manual_review":
            return "manual_review"
        return "end"

    workflow.add_conditional_edges(
        "categorizer",
        route_after_categorizer,
        {
            "retriever": "retriever",
            "manual_review": "manual_review",
            "end": END,
        },
    )

    def route_after_retriever(state: AgentState):
        raw = state.get("route_decision")
        if raw is None:
            return "manual_review"
        route = RouteDecision.model_validate(raw).route.value
        if route in {"reply", "forward"}:
            return "drafter"
        if route == "manual_review":
            return "manual_review"
        return "end"

    workflow.add_conditional_edges(
        "retriever",
        route_after_retriever,
        {
            "drafter": "drafter",
            "manual_review": "manual_review",
            "end": END,
        },
    )

    workflow.add_conditional_edges(
        "drafter",
        lambda state: (
            "manual_review"
            if state.get("next_step") == "manual_review"
            else "reviewer"
        ),
        {
            "manual_review": "manual_review",
            "reviewer": "reviewer",
        },
    )

    def route_after_review(state: AgentState):
        if state.get("next_step") == "manual_review":
            return "manual_review"
        if state.get("next_step") == "drafter":
            return "drafter"
        # If approved (updated via human action), go to sender
        if state.get("approval_status") == "approved":
            return "sender"
        # Otherwise finish (wait for approval)
        return "continue"

    workflow.add_conditional_edges(
        "reviewer",
        route_after_review,
        {
            "manual_review": "manual_review",
            "drafter": "drafter",
            "continue": END,
            "sender": "sender",
        },
    )

    workflow.add_edge("sender", END)
    workflow.add_edge("manual_review", END)

    if checkpointer is None:
        checkpointer = MemorySaver()

    return workflow.compile(
        checkpointer=checkpointer,
        interrupt_after=["reviewer"]
    )
