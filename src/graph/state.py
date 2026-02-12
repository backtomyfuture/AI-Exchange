from typing import TypedDict, List, Optional, Any

class AgentState(TypedDict):
    """
    LangGraph 状态定义，用于在节点间传递信息。
    """
    # --- 基础数据层 ---
    # 原始邮件数据，包含 subject, body, sender, date, id 等
    email: dict
    # 分类结果，包含 priority, need_reply, intent, reasoning, summary, card_type
    classification: dict
    # 检索到的上下文信息（从向量数据库中获取）
    context: List[dict]
    # 生成的邮件回复草稿
    draft: str
    
    # --- 路由与 Skill 控制层 (新增) ---
    # 当前激活的 Skill ID 列表
    active_skills: List[str]
    # 路由决策日志，记录每一层的选择路径
    routing_log: List[str]
    # 数值型优先级 (0-10)，用于精细化调度
    priority_level: int
    # 由 Skill 动态注入的 System Prompt 修饰符
    system_prompt_modifier: Optional[str]
    # 待执行的工具调用列表 (遵循类似 OpenAI tool_calls 结构)
    tool_calls: List[dict]
    
    # --- 执行与状态层 ---
    # 审批状态：pending, approved, rejected, modify
    approval_status: str
    # 用户提供的修改反馈
    feedback: Optional[str]
    # 下一步执行的节点名称
    next_step: str
    # PDF 文件 Token，用于后续删除
    pdf_token: Optional[str]
    # 所有相关附件的 Token 列表（含 PDF 和原始附件），用于统一清理
    attachment_tokens: List[str]
    # 可选的其他元数据
    metadata: Optional[dict]
    
    # --- Memory 层 ---
    # 用户的历史回复示例，用于风格参考
    reply_examples: List[dict]
