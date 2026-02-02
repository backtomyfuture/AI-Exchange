from typing import TypedDict, List, Optional, Any

class AgentState(TypedDict):
    """
    LangGraph 状态定义，用于在节点间传递信息。
    """
    # 原始邮件数据，包含 subject, body, sender, date 等
    email: dict
    # 分类结果，包含 priority, need_reply, intent, reasoning
    classification: dict
    # 检索到的上下文信息（从向量数据库中获取）
    context: List[dict]
    # 生成的邮件回复草稿
    draft: str
    # 审批状态：pending, approved, rejected, modify
    approval_status: str
    # 用户提供的修改反馈
    feedback: Optional[str]
    # 下一步执行的节点名称
    next_step: str
    # 可选的其他元数据
    metadata: Optional[dict]
