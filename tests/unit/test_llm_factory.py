"""
LLM工厂单元测试

测试 src/utils/llm_factory.py 的LLM创建功能
"""

import pytest
from unittest.mock import patch
from src.utils.llm_factory import LLMFactory


class TestLLMFactory:
    """测试LLM工厂"""
    
    def test_create_llm_default_params(self):
        """测试使用默认参数创建LLM"""
        with patch('src.utils.llm_factory.ChatOpenAI') as mock_chat:
            LLMFactory.create_llm()
            
            # 验证ChatOpenAI被调用
            mock_chat.assert_called_once()
            call_args = mock_chat.call_args[1]
            
            # 默认参数应该包含temperature
            assert 'temperature' in call_args
    
    
    def test_create_llm_custom_temperature(self):
        """测试自定义temperature"""
        with patch('src.utils.llm_factory.ChatOpenAI') as mock_chat:
            LLMFactory.create_llm(temperature=0.5)
            
            call_args = mock_chat.call_args[1]
            assert call_args['temperature'] == 0.5
    
    
    def test_create_llm_custom_model(self):
        """测试自定义模型"""
        with patch('src.utils.llm_factory.ChatOpenAI') as mock_chat:
            # 参数名是model_name
            LLMFactory.create_llm(model_name="gpt-4")
            
            call_args = mock_chat.call_args[1]
            # 但传给ChatOpenAI的key是'model'
            assert call_args['model'] == "gpt-4"
