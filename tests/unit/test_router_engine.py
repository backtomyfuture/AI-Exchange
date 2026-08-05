"""
路由引擎单元测试

测试 src/router/engine.py 的分层路由逻辑:
- Tier 1: 反射层路由
- Tier 2: 语义层路由(集成在retriever中)
- Tier 3: LLM推理路由
- Skill应用和状态合并
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from src.router.engine import RoutingEngine


@pytest.fixture
def mock_settings(monkeypatch):
    """Mock环境配置"""
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("LARK_CHAT_ID", "test-chat-id")
    monkeypatch.setenv("EXCHANGE_API_URL", "http://test")


@pytest.fixture
def sample_state():
    """测试用的标准状态"""
    return {
        "email": {
            "id": "test-123",
            "subject": "测试邮件",
            "body": "这是测试邮件内容",
            "sender": "test@example.com"
        },
        "classification": {},
        "active_skills": [],
        "routing_log": [],
        "metadata": {}
    }


@pytest.fixture
def vip_email_state():
    """VIP邮件状态(应触发Tier 1)"""
    return {
        "email": {
            "id": "vip-456",
            "subject": "重要会议",
            "body": "请查收",
            "sender": "boss@company.com"  # 假设这是VIP
        },
        "classification": {},
        "active_skills": [],
        "routing_log": [],
        "metadata": {}
    }


class TestRoutingEngineTier1:
    """测试Tier 1反射层路由"""
    
    @pytest.mark.asyncio
    async def test_tier1_vip_match(self, vip_email_state, mock_settings):
        """测试VIP邮件触发Tier 1路由"""
        engine = RoutingEngine()
        
        # Mock Tier1Router返回VIP skill
        with patch.object(engine.t1_router, 'route', return_value=['skill_vip_handling']):
            # Mock skill execution
            with patch.object(engine, '_apply_skills', return_value=vip_email_state) as mock_apply:
                result = await engine.execute_router(vip_email_state)
                
                # 验证Tier 1路由被调用
                engine.t1_router.route.assert_called_once()
                
                # 验证Skills被应用
                mock_apply.assert_called_once_with(vip_email_state, ['skill_vip_handling'])
                
                # 验证routing_log记录
                assert "Tier 1 Match" in str(result.get("routing_log", []))
                assert 'skill_vip_handling' in result.get("active_skills", [])


    @pytest.mark.asyncio
    async def test_tier1_no_match_defers_tier3_until_retrieval(self, sample_state, mock_settings):
        """Tier 1 无匹配必须先等待检索层的 Tier 2。"""
        engine = RoutingEngine()
        
        # Mock Tier1返回空列表(无匹配)
        with patch.object(engine.t1_router, 'route', return_value=[]):
            # Mock Tier3返回skills
            with patch.object(engine, '_tier3_llm_route', new_callable=AsyncMock) as mock_t3:
                mock_t3.return_value = ['skill_project_tracker']
                
                with patch.object(engine, '_apply_skills', return_value=sample_state):
                    result = await engine.execute_router(sample_state)
                    
                    mock_t3.assert_not_awaited()
                    log = result.get("routing_log", [])
                    assert any("Tier 1 No match" in str(entry) for entry in log)
                    assert result["routing_stage"] == "pending"


class TestRoutingEngineTier3:
    """测试Tier 3 LLM路由"""
    
    @pytest.mark.asyncio
    async def test_tier3_llm_skill_selection(self, sample_state, mock_settings):
        """测试Tier 3通过LLM选择合适的Skill"""
        engine = RoutingEngine()
        
        # Mock skill manager返回可用技能
        mock_skill = Mock()
        mock_skill.manifest.description = "处理项目相关邮件"
        
        with patch.object(engine.skill_manager, 'get_all_skills', return_value={
            'skill_project_tracker': mock_skill
        }):
            mock_llm = AsyncMock()
            mock_response = Mock()
            mock_response.content = "skill_project_tracker"
            mock_llm.ainvoke.return_value = mock_response

            with patch('src.providers.factory.get_llm_for_role', return_value=mock_llm):
                result = await engine._tier3_llm_route(sample_state)
                
                assert 'skill_project_tracker' in result
                assert len(result) == 1


    @pytest.mark.asyncio
    async def test_tier3_llm_no_match(self, sample_state, mock_settings):
        """测试Tier 3 LLM返回NONE时无匹配"""
        engine = RoutingEngine()
        
        with patch.object(engine.skill_manager, 'get_all_skills', return_value={}):
            mock_llm = AsyncMock()
            mock_response = Mock()
            mock_response.content = "NONE"
            mock_llm.ainvoke.return_value = mock_response

            with patch('src.providers.factory.get_llm_for_role', return_value=mock_llm):
                result = await engine._tier3_llm_route(sample_state)
                assert result == []


    @pytest.mark.asyncio
    async def test_tier3_llm_multiple_skills(self, sample_state, mock_settings):
        """测试Tier 3 LLM返回多个技能"""
        engine = RoutingEngine()
        
        mock_skill1 = Mock()
        mock_skill1.manifest.description = "VIP处理"
        mock_skill2 = Mock()
        mock_skill2.manifest.description = "项目管理"
        
        with patch.object(engine.skill_manager, 'get_all_skills', return_value={
            'skill_vip_handling': mock_skill1,
            'skill_project_tracker': mock_skill2
        }):
            mock_llm = AsyncMock()
            mock_response = Mock()
            mock_response.content = "skill_vip_handling, skill_project_tracker"
            mock_llm.ainvoke.return_value = mock_response

            with patch('src.providers.factory.get_llm_for_role', return_value=mock_llm):
                result = await engine._tier3_llm_route(sample_state)
                
                assert len(result) == 2
                assert 'skill_vip_handling' in result
                assert 'skill_project_tracker' in result


    @pytest.mark.skip(reason="Mock配置问题,get_all_skills的side_effect需要调整")
    @pytest.mark.asyncio
    async def test_tier3_llm_invalid_skill_filtered(self, sample_state, mock_settings):
        """测试Tier 3过滤掉无效的skill ID"""
        engine = RoutingEngine()
        
        mock_skill = Mock()
        mock_skill.manifest.description = "测试"
        
        # get_all_skills应该返回所有有效的skills
        with patch.object(engine.skill_manager, 'get_all_skills', return_value={
            'skill_valid': mock_skill
        }):
            mock_llm = AsyncMock()
            mock_response = Mock()
            mock_response.content = "skill_valid, skill_invalid, skill_nonexistent"
            mock_llm.ainvoke.return_value = mock_response

            with patch('src.providers.factory.get_llm_for_role', return_value=mock_llm):
                result = await engine._tier3_llm_route(sample_state)
                
                assert 'skill_valid' in result
                assert len(result) == 1


class TestRoutingEngineReplacementLists:
    """List fields are explicitly merged now that Graph reducers are removed."""

    @pytest.mark.asyncio
    async def test_execute_router_preserves_existing_routing_log(self, mock_settings):
        engine = RoutingEngine()
        state = {
            "email": {"id": "x", "subject": "s", "body": "b", "sender": "u@x.com"},
            "classification": {},
            "active_skills": ["already_active"],
            "routing_log": ["existing entry"],
            "metadata": {},
        }

        with patch.object(engine.t1_router, "route", return_value=["skill_new"]), \
             patch.object(engine, "_apply_skills", return_value=state):
            result = await engine.execute_router(state)

        assert result["routing_log"] == [
            "existing entry",
            "Tier 1 Match: ['skill_new']"
        ]
        assert result["active_skills"] == ["already_active", "skill_new"]

    @pytest.mark.asyncio
    async def test_execute_router_filters_already_active_skills(self, mock_settings):
        engine = RoutingEngine()
        state = {
            "email": {"id": "x", "subject": "s", "body": "b", "sender": "u@x.com"},
            "classification": {},
            "active_skills": ["skill_already"],
            "routing_log": [],
            "metadata": {},
        }

        with patch.object(engine.t1_router, "route", return_value=["skill_already", "skill_fresh"]), \
             patch.object(engine, "_apply_skills", return_value=state):
            result = await engine.execute_router(state)

        assert result["active_skills"] == ["skill_already", "skill_fresh"]


class TestRoutingEngineSkillApplication:
    """测试Skill应用和状态合并"""
    
    @pytest.mark.asyncio
    async def test_apply_single_skill(self, sample_state, mock_settings):
        """测试应用单个Skill"""
        engine = RoutingEngine()
        
        # Mock skill
        mock_skill = Mock()
        mock_skill.execute = AsyncMock(return_value={
            "classification": {"priority": "P0"},
            "metadata": {"vip": True}
        })
        
        with patch.object(engine.skill_manager, 'get_skill', return_value=mock_skill):
            with patch('src.router.dependency.resolve_skill_order', return_value=['skill_test']):
                result = await engine._apply_skills(sample_state, ['skill_test'])
                
                # 验证skill被执行
                mock_skill.execute.assert_called_once()
                
                # 验证状态被合并
                assert result["classification"]["priority"] == "P0"
                assert result["metadata"]["vip"] is True


    @pytest.mark.asyncio
    async def test_apply_multiple_skills_in_order(self, sample_state, mock_settings):
        """测试按顺序应用多个Skills"""
        engine = RoutingEngine()
        
        # Mock两个skills
        skill1 = Mock()
        skill1.execute = AsyncMock(return_value={"metadata": {"step": 1}})
        
        skill2 = Mock()
        skill2.execute = AsyncMock(return_value={"metadata": {"step": 2, "final": True}})
        
        def get_skill_side_effect(skill_id):
            if skill_id == 'skill_1':
                return skill1
            elif skill_id == 'skill_2':
                return skill2
            return None
        
        with patch.object(engine.skill_manager, 'get_skill', side_effect=get_skill_side_effect):
            with patch('src.router.dependency.resolve_skill_order', return_value=['skill_1', 'skill_2']):
                result = await engine._apply_skills(sample_state, ['skill_1', 'skill_2'])
                
                # 验证两个skills都被执行
                skill1.execute.assert_called_once()
                skill2.execute.assert_called_once()
                
                # 验证状态合并是顺序的(skill2覆盖skill1的step)
                assert result["metadata"]["step"] == 2
                assert result["metadata"]["final"] is True


    @pytest.mark.asyncio
    async def test_apply_skills_with_error_handling(self, sample_state, mock_settings):
        """Skill执行错误必须整体失败，不能保留或继续部分路由。"""
        engine = RoutingEngine()
        
        # Mock一个会抛异常的skill
        failing_skill = Mock()
        failing_skill.execute = AsyncMock(side_effect=Exception("Skill execution failed"))
        
        # Mock一个正常的skill
        normal_skill = Mock()
        normal_skill.execute = AsyncMock(return_value={"metadata": {"success": True}})
        
        def get_skill_side_effect(skill_id):
            if skill_id == 'failing':
                return failing_skill
            elif skill_id == 'normal':
                return normal_skill
            return None
        
        with patch.object(engine.skill_manager, 'get_skill', side_effect=get_skill_side_effect):
            with patch('src.router.dependency.resolve_skill_order', return_value=['failing', 'normal']):
                with pytest.raises(RuntimeError, match="router_skill_failed"):
                    await engine._apply_skills(sample_state, ['failing', 'normal'])

                failing_skill.execute.assert_called_once()
                normal_skill.execute.assert_not_called()
                assert "success" not in sample_state.get("metadata", {})


    @pytest.mark.asyncio
    async def test_state_immutability(self, sample_state, mock_settings):
        """测试状态更新的不可变性(不修改原状态)"""
        engine = RoutingEngine()
        
        original_email = sample_state["email"].copy()
        
        mock_skill = Mock()
        mock_skill.execute = AsyncMock(return_value={
            "new_field": "new_value"
        })
        
        with patch.object(engine.skill_manager, 'get_skill', return_value=mock_skill):
            with patch('src.router.dependency.resolve_skill_order', return_value=['skill_test']):
                result = await engine._apply_skills(sample_state, ['skill_test'])
                
                # 验证原状态的email未被修改
                assert sample_state["email"] == original_email
                
                # 验证返回的是新状态
                assert "new_field" in result
                assert result["new_field"] == "new_value"
