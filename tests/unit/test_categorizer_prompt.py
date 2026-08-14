from src.nodes import categorizer


def test_system_prompt_template_contains_priority_rubric():
    # categorizer 在调用时把评级标准放进 system prompt；
    # 用源码字符串校验评级标准已写入，避免触发 LLM。
    import inspect
    src = inspect.getsource(categorizer.categorize_email)
    assert "P0" in src and "领导" in src
    assert "P1" in src and "P2" in src and "P3" in src
    assert "判断 need_reply" not in src
    assert "need_reply" not in categorizer.EmailClassification.model_fields
