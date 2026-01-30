import os
import logging
from typing import List, Optional
from qdrant_client import QdrantClient, models
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

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
        self.client = QdrantClient(url=qdrant_url or os.getenv("QDRANT_URL", "http://localhost:6333"))
        self.collection_name = collection_name

        # Priority: explicit arg > ENV > default
        api_base = embedding_base_url or os.getenv("EMBEDDING_BASE_URL") or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        api_key = embedding_api_key or os.getenv("EMBEDDING_API_KEY", "ollama")

        self.openai_client = OpenAI(
            base_url=api_base,
            api_key=api_key
        )
        self.embedding_model = embedding_model or os.getenv("EMBEDDING_MODEL", "qwen3-embedding:4b")

    def _get_embedding(self, text: str) -> List[float]:
        try:
            response = self.openai_client.embeddings.create(
                input=text,
                model=self.embedding_model
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
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
            query_filter = models.Filter(
                must=[
                    models.FieldCondition(
                        key="sender",
                        match=models.MatchValue(value=sender)
                    )
                ]
            )

        # 3. Execute search
        try:
            search_result = self.client.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                query_filter=query_filter,
                limit=limit,
                with_payload=True
            )
            # 4. Extract payload
            return [hit.payload for hit in search_result.points]
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
