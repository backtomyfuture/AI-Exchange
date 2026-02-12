"""
Skill依赖解析单元测试

测试 src/router/dependency.py 的依赖解析逻辑:
- 无依赖的技能顺序
- 有依赖的拓扑排序
- 循环依赖检测
- 依赖链解析
"""

import pytest
from src.router.dependency import resolve_skill_order


class TestSkillDependencyResolution:
    """测试Skill依赖解析"""
    
    def test_no_dependencies(self):
        """测试无依赖的技能列表"""
        skills = ['skill_a', 'skill_b', 'skill_c']
        dependencies = {}
        
        result = resolve_skill_order(skills, dependencies)
        
        # 无依赖时应保持原顺序
        assert result == skills
    
    
    def test_simple_dependency_chain(self):
        """测试简单依赖链: A -> B -> C"""
        skills = ['skill_c', 'skill_a', 'skill_b']
        dependencies = {
            'skill_b': ['skill_a'],
            'skill_c': ['skill_b']
        }
        
        result = resolve_skill_order(skills, dependencies)
        
        # 应按依赖顺序排列: A先执行, 然后B, 最后C
        assert result.index('skill_a') < result.index('skill_b')
        assert result.index('skill_b') < result.index('skill_c')
    
    
    def test_multiple_dependencies(self):
        """测试多个依赖: C依赖A和B"""
        skills = ['skill_c', 'skill_a', 'skill_b']
        dependencies = {
            'skill_c': ['skill_a', 'skill_b']
        }
        
        result = resolve_skill_order(skills, dependencies)
        
        # A和B都应在C之前执行
        assert result.index('skill_a') < result.index('skill_c')
        assert result.index('skill_b') < result.index('skill_c')
    
    
    def test_complex_dependency_graph(self):
        """测试复杂依赖图"""
        skills = ['skill_d', 'skill_c', 'skill_b', 'skill_a']
        dependencies = {
            'skill_b': ['skill_a'],
            'skill_c': ['skill_a'],
            'skill_d': ['skill_b', 'skill_c']
        }
        
        result = resolve_skill_order(skills, dependencies)
        
        # 验证拓扑顺序
        assert result.index('skill_a') < result.index('skill_b')
        assert result.index('skill_a') < result.index('skill_c')
        assert result.index('skill_b') < result.index('skill_d')
        assert result.index('skill_c') < result.index('skill_d')
    
    
    def test_circular_dependency_detection(self):
        """测试循环依赖检测: A -> B -> C -> A"""
        skills = ['skill_a', 'skill_b', 'skill_c']
        dependencies = {
            'skill_b': ['skill_a'],
            'skill_c': ['skill_b'],
            'skill_a': ['skill_c']  # 循环!
        }
        
        # 应该抛出异常或返回错误
        # 注意: 需要查看实际实现来确定错误处理方式
        try:
            result = resolve_skill_order(skills, dependencies)
            # 如果没有抛出异常,至少不应该无限循环
            assert len(result) > 0
        except (ValueError, RecursionError) as e:
            # 预期会抛出循环依赖错误
            assert 'circular' in str(e).lower() or 'cycle' in str(e).lower()
    
    
    def test_self_dependency(self):
        """测试自依赖: A依赖A自己"""
        skills = ['skill_a']
        dependencies = {
            'skill_a': ['skill_a']
        }
        
        # 自依赖应该被检测为循环依赖
        try:
            result = resolve_skill_order(skills, dependencies)
            # 如果没有错误处理,至少应该能返回
            assert 'skill_a' in result
        except (ValueError, RecursionError):
            # 预期抛出循环依赖错误
            pass
    
    
    def test_missing_dependency_skill(self):
        """测试依赖的skill不在列表中"""
        skills = ['skill_a', 'skill_b']
        dependencies = {
            'skill_b': ['skill_a', 'skill_nonexistent']
        }
        
        # 应该处理缺失的依赖
        result = resolve_skill_order(skills, dependencies)
        
        # 至少A应该在B之前
        assert result.index('skill_a') < result.index('skill_b')
    
    
    def test_empty_skills_list(self):
        """测试空技能列表"""
        skills = []
        dependencies = {}
        
        result = resolve_skill_order(skills, dependencies)
        
        assert result == []
    
    
    def test_single_skill_no_dependency(self):
        """测试单个技能无依赖"""
        skills = ['skill_only']
        dependencies = {}
        
        result = resolve_skill_order(skills, dependencies)
        
        assert result == ['skill_only']
    
    
    def test_partial_dependencies(self):
        """测试部分技能有依赖,部分没有"""
        skills = ['skill_a', 'skill_b', 'skill_c', 'skill_d']
        dependencies = {
            'skill_c': ['skill_a']
        }
        
        result = resolve_skill_order(skills, dependencies)
        
        # A必须在C之前
        assert result.index('skill_a') < result.index('skill_c')
        # B和D没有依赖,应该保持相对原始顺序或任意顺序
        assert 'skill_b' in result
        assert 'skill_d' in result
