import asyncio
import sys
import os

# 将 src 目录添加到路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.router.engine import RoutingEngine
from src.graph.state import AgentState

async def test_router():
    engine = RoutingEngine()
    
    # 场景 1: VIP 邮件 (Tier 1 匹配)
    email_vip = {
        "sender": "ceo@company.com",
        "subject": "急需审批合同",
        "body": "请看一下附件中的合同。"
    }
    state = AgentState(
        email=email_vip,
        classification={},
        context=[],
        draft="",
        active_skills=[],
        routing_log=[],
        priority_level=0,
        system_prompt_modifier=None,
        tool_calls=[],
        approval_status="pending",
        feedback=None,
        next_step="",
        metadata={}
    )
    
    print("\n--- Testing VIP Mail (T1) ---")
    result = await engine.execute_router(state)
    print(f"Active Skills: {result['active_skills']}")
    print(f"Routing Log: {result['routing_log']}")
    print(f"Classification Priority: {result['classification'].get('priority')}")

    # 场景 2: 项目邮件 (Tier 1 匹配)
    email_project = {
        "sender": "worker@company.com",
        "subject": "关于项目 P-2026 的进展",
        "body": "P-2026 进展顺利。"
    }
    state_p = AgentState(
        email=email_project,
        classification={},
        context=[],
        draft="",
        active_skills=[],
        routing_log=[],
        priority_level=0,
        system_prompt_modifier=None,
        tool_calls=[],
        approval_status="pending",
        feedback=None,
        next_step="",
        metadata={}
    )
    
    print("\n--- Testing Project Mail ---")
    result_p = await engine.execute_router(state_p)
    print(f"Active Skills: {result_p['active_skills']}")
    print(f"Metadata Projects: {result_p['metadata'].get('detected_projects')}")

if __name__ == "__main__":
    asyncio.run(test_router())
