from typing import Any, TypedDict

class AgentState(TypedDict, total=False):
    """
    仅保存可安全进入 checkpoint 的小型元数据与持久化引用。

    列表刻意不使用 append reducer；各节点必须返回经过统一上限处理的完整值。
    """
    # --- 基础数据层 ---
    email_id: str
    email: dict[str, str]
    content_ref: dict[str, Any]
    classification: dict
    context_summaries: list[dict[str, str]]
    draft_id: str | None
    draft_to: list[str]
    draft_cc: list[str]

    # --- 规范路由决策层 ---
    routing_log: list[str]
    routing_stage: str
    route_decision: dict[str, Any]
    priority_level: int
    system_prompt_modifier: str | None
    tool_calls: list[dict[str, str]]

    # --- 执行与状态层 ---
    approval_status: str
    next_step: str
    pdf_token: str | None
    attachment_tokens: list[str]
    metadata: dict[str, Any]
    review_result: dict[str, Any] | None
    safe_error_summary: str | None
    recipient_ui: dict[str, dict[str, Any]]
