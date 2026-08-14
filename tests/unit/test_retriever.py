"""
检索器(Retriever)单元测试

测试 src/utils/retriever.py 的RAG检索功能:
- 向量搜索
- 线程搜索
- 混合搜索(文本+发件人)
- 错误处理
"""

import pytest
from unittest.mock import Mock, patch
from src.utils.retriever import EmailRetriever


@pytest.fixture
def mock_qdrant_client():
    """Mock Qdrant客户端"""
    return Mock()


@pytest.fixture
def mock_openai_client():
    """Mock OpenAI客户端"""
    return Mock()


@pytest.fixture
def retriever(mock_qdrant_client, mock_openai_client):
    """创建Retriever实例"""
    with patch('src.utils.retriever.QdrantClient', return_value=mock_qdrant_client):
        with patch('src.utils.retriever.OpenAI', return_value=mock_openai_client):
            return EmailRetriever(
                collection_name="test_emails",
                qdrant_url="http://test:6333",
                embedding_api_key="test-key"
            )


class TestEmailRetriever:
    """测试EmailRetriever"""
    
    def test_search_with_results(self, retriever, mock_qdrant_client, mock_openai_client):
        """测试成功搜索返回结果"""
        # Mock embedding生成
        mock_embedding_response = Mock()
        mock_embedding_response.data = [Mock(embedding=[0.1, 0.2, 0.3])]
        mock_openai_client.embeddings.create.return_value = mock_embedding_response
        
        # Mock Qdrant搜索结果
        mock_hit1 = Mock()
        mock_hit1.payload = {"subject": "Test 1", "body": "Content 1"}
        mock_hit2 = Mock()
        mock_hit2.payload = {"subject": "Test 2", "body": "Content 2"}
        
        mock_search_result = Mock()
        mock_search_result.points = [mock_hit1, mock_hit2]
        mock_qdrant_client.query_points.return_value = mock_search_result
        
        # 执行搜索
        results = retriever.search("test query", limit=2)
        
        # 验证
        assert len(results) == 2
        assert results[0]["subject"] == "Test 1"
        assert results[1]["subject"] == "Test 2"
        mock_openai_client.embeddings.create.assert_called_once()
        mock_qdrant_client.query_points.assert_called_once()
    
    
    def test_search_with_sender_filter(self, retriever, mock_qdrant_client, mock_openai_client):
        """测试带发件人过滤的搜索"""
        # Mock embedding
        mock_embedding_response = Mock()
        mock_embedding_response.data = [Mock(embedding=[0.1, 0.2])]
        mock_openai_client.embeddings.create.return_value = mock_embedding_response
        
        # Mock搜索结果
        mock_search_result = Mock()
        mock_search_result.points = []
        mock_qdrant_client.query_points.return_value = mock_search_result
        
        # 执行搜索
        retriever.search("test", sender="boss@company.com")
        
        # 验证filter被传递
        call_args = mock_qdrant_client.query_points.call_args
        assert call_args[1]['query_filter'] is not None

    def test_search_excludes_the_current_email_from_semantic_context(
        self,
        retriever,
        mock_qdrant_client,
        mock_openai_client,
    ):
        mock_embedding_response = Mock()
        mock_embedding_response.data = [Mock(embedding=[0.1, 0.2])]
        mock_openai_client.embeddings.create.return_value = mock_embedding_response
        current = Mock(payload={"id": "mail-current", "subject": "current"})
        old_one = Mock(payload={"id": "mail-old-1", "subject": "old one"})
        old_two = Mock(payload={"id": "mail-old-2", "subject": "old two"})
        mock_qdrant_client.query_points.return_value = Mock(
            points=[current, old_one, old_two]
        )

        results = retriever.search(
            "query",
            limit=2,
            exclude_email_id="mail-current",
        )

        assert [item["id"] for item in results] == ["mail-old-1", "mail-old-2"]
        assert mock_qdrant_client.query_points.call_args.kwargs["limit"] == 7
    
    
    def test_search_embedding_failure(self, retriever, mock_openai_client):
        """测试embedding生成失败时的处理"""
        # Mock embedding失败 - 返回空embedding而不是抛异常
        mock_embedding_response = Mock()
        mock_embedding_response.data = [Mock(embedding=[])]
        mock_openai_client.embeddings.create.return_value = mock_embedding_response
        
        # 应该捕获并返回空列表
        results = retriever.search("test query")
        assert results == []
    
    
    def test_search_qdrant_failure(self, retriever, mock_qdrant_client, mock_openai_client):
        """测试Qdrant搜索失败时的处理"""
        # Mock embedding成功
        mock_embedding_response = Mock()
        mock_embedding_response.data = [Mock(embedding=[0.1])]
        mock_openai_client.embeddings.create.return_value = mock_embedding_response
        
        # Mock Qdrant连接错误
        mock_qdrant_client.query_points.side_effect = ConnectionError("Connection failed")
        
        # 应该捕获异常并返回空列表
        results = retriever.search("test")
        assert results == []
    
    
    def test_search_by_thread_success(self, retriever, mock_qdrant_client):
        """测试按线程搜索成功"""
        # Mock scroll结果
        mock_point1 = Mock()
        mock_point1.payload = {"thread_id": "thread-123", "subject": "Email 1"}
        mock_point2 = Mock()
        mock_point2.payload = {"thread_id": "thread-123", "subject": "Email 2"}
        
        mock_qdrant_client.scroll.return_value = ([mock_point1, mock_point2], None)
        
        # 执行搜索
        results = retriever.search_by_thread("thread-123")
        
        # 验证
        assert len(results) == 2
        assert results[0]["thread_id"] == "thread-123"
        mock_qdrant_client.scroll.assert_called_once()

    def test_search_by_thread_excludes_the_current_email(
        self,
        retriever,
        mock_qdrant_client,
    ):
        current = Mock(payload={"id": "mail-current", "thread_id": "thread-123"})
        old = Mock(payload={"id": "mail-old", "thread_id": "thread-123"})
        mock_qdrant_client.scroll.return_value = ([current, old], None)

        results = retriever.search_by_thread(
            "thread-123",
            limit=2,
            exclude_email_id="mail-current",
        )

        assert results == [{"id": "mail-old", "thread_id": "thread-123"}]
        assert mock_qdrant_client.scroll.call_args.kwargs["limit"] == 7

    def test_search_excludes_hits_not_older_than_the_current_email(
        self,
        retriever,
        mock_qdrant_client,
        mock_openai_client,
    ):
        mock_embedding_response = Mock()
        mock_embedding_response.data = [Mock(embedding=[0.1, 0.2])]
        mock_openai_client.embeddings.create.return_value = mock_embedding_response
        newer = Mock(payload={"id": "mail-new", "received_at": "2026-02-02T00:00:00+00:00"})
        older = Mock(payload={"id": "mail-old", "received_at": "2026-01-01T00:00:00+00:00"})
        mock_qdrant_client.query_points.return_value = Mock(points=[newer, older])

        results = retriever.search(
            "query",
            limit=2,
            received_before="2026-02-01T00:00:00+00:00",
        )

        assert [item["id"] for item in results] == ["mail-old"]

    def test_search_by_thread_empty_thread_id(self, retriever):
        """测试空线程ID时返回空列表"""
        results = retriever.search_by_thread("")
        assert results == []
        
        results = retriever.search_by_thread(None)
        assert results == []
    
    
    def test_search_by_thread_failure(self, retriever, mock_qdrant_client):
        """测试线程搜索失败时的处理"""
        # Mock scroll失败
        mock_qdrant_client.scroll.side_effect = Exception("DB Error")
        
        # 应该返回空列表
        results = retriever.search_by_thread("thread-123")
        
        assert results == []
