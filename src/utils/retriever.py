import logging
from typing import List, Optional, Any
from openai import OpenAI, APIError, APIConnectionError

from src.config import get_settings
from src.security.redaction import fingerprint_identifier

logger = logging.getLogger(__name__)

# Keep patchable symbols for tests while retaining lazy import behavior.
QdrantClient = None
models = None

class EmailRetriever:
    """
    Email retrieval utility class, responsible for searching relevant historical emails from Qdrant.
    """
    def __init__(
        self,
        collection_name: str = "emails",
        qdrant_url: Optional[str] = None,
        embedding_api_key: Optional[str] = None,
        embedding_base_url: Optional[str] = None,
        embedding_model: Optional[str] = None
    ):
        settings = get_settings()
        self._qdrant_url = qdrant_url or settings.QDRANT_URL
        self.client: Optional[Any] = None
        # Capture patchable class at construction time for test compatibility.
        self._qdrant_client_cls = QdrantClient
        self.collection_name = collection_name

        api_base = embedding_base_url or settings.EMBEDDING_BASE_URL or "http://localhost:11434/v1"
        from src.config import resolve_secret
        api_key = embedding_api_key or resolve_secret(settings.EMBEDDING_API_KEY) or "ollama"

        self.openai_client = OpenAI(
            base_url=api_base,
            api_key=api_key
        )
        self.embedding_model = embedding_model or settings.EMBEDDING_MODEL

    def _get_client(self):
        if self.client is None:
            # Lazy import to avoid hard crash during module import in constrained envs.
            qdrant_cls = self._qdrant_client_cls
            if qdrant_cls is None:
                from qdrant_client import QdrantClient as qdrant_cls_import

                qdrant_cls = qdrant_cls_import
            self.client = qdrant_cls(url=self._qdrant_url)
        return self.client

    def _get_embedding(self, text: str) -> List[float]:
        try:
            response = self.openai_client.embeddings.create(
                input=text,
                model=self.embedding_model
            )
            return response.data[0].embedding
        except APIConnectionError as exc:
            logger.error("Embedding service unavailable: error_type=%s", type(exc).__name__)
            return []
        except APIError as exc:
            logger.error("Embedding API failed: error_type=%s", type(exc).__name__)
            return []

    def search(
        self,
        query_text: str,
        sender: Optional[str] = None,
        limit: int = 5
    ) -> List[dict]:
        """
        Hybrid search based on text content and sender.
        """
        # 1. Vectorize query text
        query_vector = self._get_embedding(query_text)
        if not query_vector:
            return []

        # 2. Build filter
        query_filter = None
        if sender:
            qdrant_models = models
            if qdrant_models is None:
                from qdrant_client import models as qdrant_models_import

                qdrant_models = qdrant_models_import

            query_filter = qdrant_models.Filter(
                must=[
                    qdrant_models.FieldCondition(
                        key="sender",
                        match=qdrant_models.MatchValue(value=sender)
                    )
                ]
            )

        try:
            client = self._get_client()
            search_result = client.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                query_filter=query_filter,
                limit=limit,
                with_payload=True
            )
            return [hit.payload for hit in search_result.points]
        except Exception as exc:
            # Keep broad handling to avoid hard dependency on qdrant exception classes.
            if exc.__class__.__name__ == "UnexpectedResponse":
                logger.error("Qdrant search failed: error_type=UnexpectedResponse")
                return []
            if isinstance(exc, ConnectionError):
                logger.error("Qdrant search connection failed: error_type=%s", type(exc).__name__)
                return []
            logger.error("Qdrant search failed: error_type=%s", type(exc).__name__)
            return []

    def search_by_thread(self, thread_id: str, limit: int = 20) -> List[dict]:
        """Search all emails in the same conversation thread."""
        if not thread_id:
            return []

        qdrant_models = models
        if qdrant_models is None:
            from qdrant_client import models as qdrant_models_import

            qdrant_models = qdrant_models_import

        query_filter = qdrant_models.Filter(
            must=[
                qdrant_models.FieldCondition(
                    key="thread_id",
                    match=qdrant_models.MatchValue(value=thread_id),
                )
            ]
        )

        try:
            client = self._get_client()
            points, _ = client.scroll(
                collection_name=self.collection_name,
                scroll_filter=query_filter,
                limit=limit,
                with_payload=True,
            )
            return [point.payload for point in points]
        except Exception as exc:
            logger.error(
                "Thread search failed: thread=%s error_type=%s",
                fingerprint_identifier(thread_id, namespace="thread"),
                type(exc).__name__,
            )
            return []


_retriever_instance: Optional[EmailRetriever] = None


def get_retriever() -> EmailRetriever:
    """获取 EmailRetriever 全局单例。"""
    global _retriever_instance
    if _retriever_instance is None:
        _retriever_instance = EmailRetriever()
    return _retriever_instance
