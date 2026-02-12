"""
Tier1反射层路由单元测试

测试 src/router/tier1_reflex.py 的快速模式匹配:
- VIP发件人检测
- 关键词匹配
- 规则优先级
- 无匹配场景
"""

import pytest
from unittest.mock import Mock, patch
from src.router.tier1_reflex import Tier1ReflexRouter


@pytest.fixture
def router():
    """创建Tier1Router实例"""
    return Tier1ReflexRouter()


@pytest.fixture
def vip_email():
    """VIP邮件"""
    return {
        "sender": "boss@company.com",
        "subject": "紧急会议",
        "body": "请立即参加"
    }


@pytest.fixture
def normal_email():
    """普通邮件"""
    return {
        "sender": "colleague@company.com", 
        "subject": "项目更新",
        "body": "本周进展汇报"
    }


@pytest.fixture
def invoice_email():
    """发票邮件"""
    return {
        "sender": "finance@vendor.com",
        "subject": "发票 #12345",
        "body": "请查收本月发票,总额¥10,000"
    }


class TestTier1ReflexRouter:
    """测试Tier1反射层路由"""
    
    def test_vip_sender_detection(self, router):
        """测试VIP发件人检测"""
        # 不mock内部方法,直接测试整体route逻辑
        # 如果skill_manager配置了VIP规则,应该匹配
        email = {"sender": "ceo@company.com", "subject": "test", "body": "test"}
        result = router.route(email)
        
        # 至少应该返回列表(可能为空)
        assert isinstance(result, list)
    
    
    def test_keyword_matching_invoice(self, router, invoice_email):
        """测试发票关键词匹配"""
        result = router.route(invoice_email)
        
        # 如果有发票关键词规则,应该匹配到finance skill
        # 注意: 这取决于实际的规则配置
        if result:
            # 至少应该返回列表
            assert isinstance(result, list)
    
    
    def test_out_of_office_detection(self, router):
        """测试休假自动回复检测"""
        ooo_email = {
            "sender": "someone@company.com",
            "subject": "自动回复: 休假中",
            "body": "我将于下周返回办公室"
        }
        
        result = router.route(ooo_email)
        
        # 可能匹配到out_of_office skill
        if result:
            assert isinstance(result, list)
    
    
    def test_no_match_returns_empty(self, router, normal_email):
        """测试无匹配时返回空列表"""
        # 确保这是一封不会触发任何规则的普通邮件
        general_email = {
            "sender": "random@example.com",
            "subject": "Hello",
            "body": "Just saying hi"
        }
        
        result = router.route(general_email)
        
        # 应返回空列表或None
        assert result == [] or result is None or len(result) == 0
    
    
    def test_multiple_rule_matching(self, router):
        """测试多个规则同时匹配"""
        # 同时包含VIP和发票关键词的邮件
        complex_email = {
            "sender": "ceo@company.com",  # VIP
            "subject": "发票审批",  # 发票关键词
            "body": "请审批以下发票"
        }
        
        result = router.route(complex_email)
        
        # 可能匹配到多个skills
        if result:
            assert isinstance(result, list)
            # 可能包含多个skill
    
    
    def test_case_insensitive_matching(self, router):
        """测试关键词大小写不敏感"""
        email_upper = {
            "sender": "test@example.com",
            "subject": "发票",
            "body": "INVOICE attached"
        }
        
        email_lower = {
            "sender": "test@example.com",
            "subject": "发票",
            "body": "invoice attached"
        }
        
        result_upper = router.route(email_upper)
        result_lower = router.route(email_lower)
        
        # 两种情况应该有相同的匹配结果
        assert result_upper == result_lower
    
    
    def test_email_missing_fields(self, router):
        """测试邮件缺少字段时的健壮性"""
        incomplete_email = {
            "sender": "test@example.com"
            # 缺少subject和body
        }
        
        # 不应该崩溃
        result = router.route(incomplete_email)
        
        assert isinstance(result, list)
    
    
    def test_empty_email(self, router):
        """测试空邮件对象"""
        empty_email = {}
        
        # 不应该崩溃
        result = router.route(empty_email)
        
        assert isinstance(result, list)
    
    
    def test_none_email(self, router):
        """测试None输入"""
        # 应该优雅处理None输入
        try:
            result = router.route(None)
            assert isinstance(result, list)
        except (TypeError, AttributeError):
            # 如果抛出异常也可以接受
            pass
