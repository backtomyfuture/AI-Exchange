"""
LLM 工厂单元测试

测试 Provider 系统的 LLM 创建功能（含向后兼容的 LLMFactory 包装）
"""

import pytest
from unittest.mock import patch
from src.utils.llm_factory import LLMFactory


class TestLLMFactory:
    """测试 LLM 工厂"""

    def test_create_llm_default_params(self):
        """测试使用默认参数创建 LLM"""
        with patch("src.providers.factory.ChatOpenAI") as mock_chat:
            LLMFactory.create_llm()

            mock_chat.assert_called_once()
            call_args = mock_chat.call_args[1]
            assert "temperature" in call_args

    def test_create_llm_custom_temperature(self):
        """测试自定义 temperature"""
        with patch("src.providers.factory.ChatOpenAI") as mock_chat:
            LLMFactory.create_llm(temperature=0.5)

            call_args = mock_chat.call_args[1]
            assert call_args["temperature"] == 0.5

    def test_create_llm_custom_model(self):
        """测试自定义模型"""
        with patch("src.providers.factory.ChatOpenAI") as mock_chat:
            LLMFactory.create_llm(model_name="gpt-4")

            call_args = mock_chat.call_args[1]
            assert call_args["model"] == "gpt-4"
