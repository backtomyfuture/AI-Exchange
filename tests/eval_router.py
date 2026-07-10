import os
import sys
from typing import List, Dict, Any

# 补丁：将项目根目录添加到 python 路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from langsmith import Client
from src.router.engine import RoutingEngine
from src.graph.state import AgentState

# 设置环境变量以启用 LangSmith (可选，如果用户已配置)
# os.environ["LANGCHAIN_TRACING_V2"] = "true"
# os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
# os.environ["LANGCHAIN_API_KEY"] = "your_key"

class SkillEvaluator:
    """
    Skill 级别评估器：验证路由准确率与 Skill 执行效果。
    """
    def __init__(self, project_name: str = "Email-Agent-Eval"):
        self.client = Client()
        self.project_name = project_name
        self.engine = RoutingEngine()

    async def evaluate_routing_accuracy(self, dataset: List[Dict[str, Any]]):
        """
        评估路由准确率。
        Dataset 格式: [{"email": {...}, "expected_skills": ["..."]}]
        """
        correct = 0
        total = len(dataset)
        
        results = []
        for i, item in enumerate(dataset):
            email = item["email"]
            expected = set(item["expected_skills"])
            
            # 初始化状态
            state = self._init_state(email)
            
            # 运行路由
            final_state = await self.engine.execute_router(state)
            actual = set(final_state.get("active_skills", []))
            
            is_correct = expected == actual
            if is_correct:
                correct += 1
            
            results.append({
                "input": email["subject"],
                "expected": list(expected),
                "actual": list(actual),
                "correct": is_correct,
                "routing_log": final_state.get("routing_log")
            })
            
            print(f"[{i+1}/{total}] Subject: {email['subject'][:30]}... | Match: {is_correct}")

        accuracy = correct / total if total > 0 else 0
        print(f"\nFinal Routing Accuracy: {accuracy:.2%}")
        return accuracy, results

    def _init_state(self, email: Dict) -> AgentState:
        return AgentState(
            email=email,
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

# 黄金测试集示例
GOLDEN_DATASET = [
    {
        "email": {"sender": "ceo@company.com", "subject": "Contract Approval", "body": "Please sign this."},
        "expected_skills": ["skill_vip_handling", "skill_finance_invoice"]
    },
    {
        "email": {"sender": "staff@company.com", "subject": "Update on P-1002", "body": "Project moving forward."},
        "expected_skills": ["skill_project_tracker"]
    },
    {
        "email": {"sender": "staff@company.com", "subject": "Important Report Q1", "body": "FYI"},
        "expected_skills": ["skill_leadership_tone"]
    },
    {
        "email": {"sender": "external@gmail.com", "subject": "Hello", "body": "How are you?"},
        "expected_skills": []
    }
]

if __name__ == "__main__":
    import asyncio
    evaluator = SkillEvaluator()
    asyncio.run(evaluator.evaluate_routing_accuracy(GOLDEN_DATASET))
