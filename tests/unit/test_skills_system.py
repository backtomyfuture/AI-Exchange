"""
Skills系统通用测试

测试所有Skills的基础功能
"""

import pytest
from unittest.mock import Mock, AsyncMock
import sys
import os

# 添加skills_registry到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../skills_registry'))


class TestSkillsSystem:
    """测试Skills系统基础功能"""
    
    @pytest.mark.asyncio
    async def test_skill_vip_handling_execute(self):
        """测试VIP处理技能"""
        try:
            from skill_vip_handling import SkillVIPHandling
            
            skill = SkillVIPHandling()
            state = {
                "email": {"sender": "boss@company.com", "subject": "重要会议"},
                "metadata": {}
            }
            
            result = await skill.execute(state)
            
            # 应该返回更新后的状态
            assert isinstance(result, dict)
        except ImportError:
            pytest.skip("skill_vip_handling not found")
    
    
    @pytest.mark.asyncio
    async def test_skill_leadership_tone_execute(self):
        """测试领导语气技能"""
        try:
            from skill_leadership_tone import SkillLeadershipTone
            
            skill = SkillLeadershipTone()
            state = {
                "email": {"body": "测试内容"},
                "metadata": {}
            }
            
            result = await skill.execute(state)
            
            assert isinstance(result, dict)
        except ImportError:
            pytest.skip("skill_leadership_tone not found")
    
    
    @pytest.mark.asyncio  
    async def test_skill_finance_invoice_execute(self):
        """测试财务发票技能"""
        try:
            from skill_finance_invoice import SkillFinanceInvoice
            
            skill = SkillFinanceInvoice()
            state = {
                "email": {"subject": "发票 #12345", "body": "请查收"},
                "metadata": {}
            }
            
            result = await skill.execute(state)
            
            assert isinstance(result, dict)
        except ImportError:
            pytest.skip("skill_finance_invoice not found")
    
    
    @pytest.mark.asyncio
    async def test_skill_project_tracker_execute(self):
        """测试项目跟踪技能"""
        try:
            from skill_project_tracker import SkillProjectTracker
            
            skill = SkillProjectTracker()
            state = {
                "email": {"subject": "项目更新", "body": "本周进展"},
                "metadata": {}
            }
            
            result = await skill.execute(state)
            
            assert isinstance(result, dict)
        except ImportError:
            pytest.skip("skill_project_tracker not found")
    
    
    @pytest.mark.asyncio
    async def test_skill_out_of_office_execute(self):
        """测试休假自动回复技能"""
        try:
            from skill_out_of_office import SkillOutOfOffice
            
            skill = SkillOutOfOffice()
            state = {
                "email": {"subject": "自动回复: 休假中"},
                "metadata": {}
            }
            
            result = await skill.execute(state)
            
            assert isinstance(result, dict)
        except ImportError:
            pytest.skip("skill_out_of_office not found")
