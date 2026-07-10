import operator
from typing import TypedDict, List, Optional, Annotated

class AgentState(TypedDict):
    """
    LangGraph 状态定义，用于在节点间传递信息。
    List 字段使用 operator.add reducer 以支持增量合并。
    """
    # --- 基础数据层 ---
    email: dict
    classification: dict
    context: Annotated[List[dict], operator.add]
    draft: str

    # --- 路由与 Skill 控制层 ---
    active_skills: Annotated[List[str], operator.add]
    routing_log: Annotated[List[str], operator.add]
    priority_level: int
    system_prompt_modifier: Optional[str]
    tool_calls: Annotated[List[dict], operator.add]

    # --- 执行与状态层 ---
    approval_status: str
    feedback: Optional[str]
    next_step: str
    pdf_token: Optional[str]
    attachment_tokens: Annotated[List[str], operator.add]
    metadata: Optional[dict]

    # --- Memory 层 ---
    reply_examples: Annotated[List[dict], operator.add]
