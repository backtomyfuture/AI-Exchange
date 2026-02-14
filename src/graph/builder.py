from langgraph.graph import StateGraph, END

from langgraph.checkpoint.memory import MemorySaver
from src.graph.state import AgentState
from src.nodes.categorizer import categorize_email
from src.nodes.retriever_node import retrieve_context
from src.nodes.drafter import generate_draft
from src.nodes.reviewer import review_draft
from src.nodes.sender import send_final_email

def build_graph(checkpointer=None):
    """
    构建 LangGraph 工作流。
    """
    workflow = StateGraph(AgentState)

    workflow.add_node("categorizer", categorize_email)
    workflow.add_node("retriever", retrieve_context)
    workflow.add_node("drafter", generate_draft)
    workflow.add_node("reviewer", review_draft)
    workflow.add_node("sender", send_final_email)

    workflow.set_entry_point("categorizer")

    workflow.add_conditional_edges(
        "categorizer",
        lambda state: state["classification"]["need_reply"],
        {
            True: "retriever",
            False: END
        }
    )

    workflow.add_edge("retriever", "drafter")
    workflow.add_edge("drafter", "reviewer")

    def route_after_review(state: AgentState):
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
        {"drafter": "drafter", "continue": END, "sender": "sender"}
    )

    workflow.add_edge("sender", END)

    if checkpointer is None:
        checkpointer = MemorySaver()

    return workflow.compile(
        checkpointer=checkpointer,
        interrupt_after=["reviewer"]
    )
