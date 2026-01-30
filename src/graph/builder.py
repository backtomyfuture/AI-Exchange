from langgraph.graph import StateGraph, END

from langgraph.checkpoint.memory import MemorySaver
from src.graph.state import AgentState
from src.nodes.categorizer import categorize_email
from src.nodes.retriever_node import retrieve_context
from src.nodes.drafter import generate_draft
from src.nodes.sender import send_final_email

def build_graph(checkpointer=None):
    """
    构建 LangGraph 工作流。
    """
    # 初始化状态图
    workflow = StateGraph(AgentState)

    # 添加节点
    workflow.add_node("categorizer", categorize_email)
    workflow.add_node("retriever", retrieve_context)
    workflow.add_node("drafter", generate_draft)
    workflow.add_node("sender", send_final_email)

    # 设置入口点
    workflow.set_entry_point("categorizer")

    # 设置边
    # 分类后，如果需要回复则进入检索，否则结束
    workflow.add_conditional_edges(
        "categorizer",
        lambda state: state["classification"]["need_reply"],
        {
            True: "retriever",
            False: END
        }
    )

    workflow.add_edge("retriever", "drafter")

    # 在 drafter 之后，我们希望中断以进行人工审批
    # 审批后的条件边
    def route_after_approval(state: AgentState):
        status = state.get("approval_status", "pending")
        if status == "approved":
            return "sender"
        elif status == "modify":
            return "drafter" # 回到拟稿节点重新生成
        else:
            return END # 拒绝或其他情况

    workflow.add_conditional_edges(
        "drafter",
        route_after_approval,
        {
            "sender": "sender",
            "drafter": "drafter",
            END: END
        }
    )

    workflow.add_edge("sender", END)

    # 使用传入的 Checkpointer 或默认使用 MemorySaver
    if checkpointer is None:
        checkpointer = MemorySaver()

    # 编译图，设置在 drafter 节点之后中断
    return workflow.compile(
        checkpointer=checkpointer,
        interrupt_after=["drafter"]
    )
