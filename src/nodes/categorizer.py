
import os
from typing import Literal
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from src.graph.state import AgentState
from src.utils.rate_limiter import llm_rate_limiter

class EmailClassification(BaseModel):
    """邮件分类结果的结构化定义"""
    priority: Literal["P0", "P1", "P2", "P3"] = Field(description="邮件优先级：P0最高，P3最低")
    need_reply: bool = Field(description="是否需要回复这封邮件")
    intent: Literal["咨询", "审批", "通知", "垃圾邮件"] = Field(description="邮件的主要意图")
    summary: str = Field(description="根据邮件的标题和内容，生成一个简短的总结")
    reasoning: str = Field(description="简短的分类理由")

async def categorize_email(state: AgentState) -> AgentState:
    """
    分类节点：根据邮件内容进行优先级和意图分类。
    """
    email = state.get("email", {})
    subject = email.get("subject", "")
    body = email.get("body", "")

    # 初始化 LLM
    from src.utils.llm_factory import LLMFactory
    llm = LLMFactory.create_llm(temperature=0)
    
    # Use JsonOutputParser for robust parsing of LLM output
    parser = JsonOutputParser(pydantic_object=EmailClassification)

    prompt = ChatPromptTemplate.from_messages([
        ("system", "你是一个专业的邮件助手。请根据提供的邮件主题和正文，对邮件进行分类。\n{format_instructions}\n请只输出 JSON，不要包含 markdown 代码块或其他解释。"),
        ("user", "邮件主题: {subject}\n\n邮件正文:\n{body}\n\n{image_info}")
    ]).partial(format_instructions=parser.get_format_instructions())

    chain = prompt | llm | parser

    # 调用 LLM 进行分类
    from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
    import time
    
    @retry(
        wait=wait_random_exponential(multiplier=2, max=120), # 更激进的等待
        stop=stop_after_attempt(12), # 增加尝试次数
        retry=retry_if_exception_type(Exception), # 可以更具体，但暂定捕获所有 LLM 异常
        reraise=True
    )
    async def invoke_with_retry(payload):
        # 在调用前获取全局限流许可
        await llm_rate_limiter.acquire()
        return await chain.ainvoke(payload) # 使用异步调用

    try:
        # Expected result is a dict because parser converts it
        image_analysis = email.get("image_analysis", "")
        image_info = f"【注意：该邮件包含图片附件，以下是图片内容的解析结果】:\n{image_analysis}" if image_analysis else ""
        
        result = await invoke_with_retry({"subject": subject, "body": body, "image_info": image_info})
        classification_result = EmailClassification(**result)
        print(f"Classification success: {classification_result}")
    except Exception as e:
        print(f"Classification failed (Parsing Error or Max Retries): {e}")
        # Fallback default
        classification_result = EmailClassification(
            priority="P3", 
            need_reply=False, 
            intent="通知", 
            reasoning=f"Auto-fallback due to error: {str(e)[:50]}"
        )

    # 更新状态
    return {
        **state,
        "classification": classification_result.model_dump(),
        "next_step": "rag_search" if classification_result.need_reply else "end"
    }
