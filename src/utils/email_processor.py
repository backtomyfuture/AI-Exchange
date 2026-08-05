
import hashlib
import logging
import uuid
from typing import List, Dict, Any, Optional

from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse
from openai import OpenAI, APIError, APIConnectionError, RateLimitError
from langchain_text_splitters import RecursiveCharacterTextSplitter
from tenacity import retry, stop_after_attempt, wait_random_exponential

from src.config import get_settings
from src.utils.email_body_projection import project_email_body_for_model
from src.security.redaction import fingerprint_identifier

logger = logging.getLogger(__name__)

MAX_INDEX_BODY_CHARS = 50_000
MAX_QUOTED_HISTORY_FALLBACK_CHARS = 4_000
MAX_QUOTED_HISTORY_PREVIEW_CHARS = 2_000


class EmailProcessor:
    def __init__(
        self, 
        qdrant_url: Optional[str] = None, 
        embedding_api_key: Optional[str] = None,
        embedding_base_url: Optional[str] = None,
        embedding_model: Optional[str] = None,
        collection_name: str = "emails"
    ):
        settings = get_settings()
        self.qdrant_client = QdrantClient(url=qdrant_url or settings.QDRANT_URL)
        
        # Priority: explicit arg > Settings
        api_base = embedding_base_url or settings.EMBEDDING_BASE_URL
        from src.config import resolve_secret
        api_key = embedding_api_key or resolve_secret(settings.EMBEDDING_API_KEY)
        
        self.openai_client = OpenAI(
            base_url=api_base,
            api_key=api_key,
            timeout=5.0
        )
        self.embedding_model = embedding_model or settings.EMBEDDING_MODEL
        
        self.collection_name = collection_name
        
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000, 
            chunk_overlap=100,
            separators=["\n\n", "\n", " ", ""]
        )
        
        # Lazy load dimension
        self._embedding_dim = None

    @property
    def embedding_dim(self) -> int:
        if self._embedding_dim is None:
            self._embedding_dim = self._get_embedding_dim()
        return self._embedding_dim

    def _get_embedding_dim(self) -> int:
        try:
            logger.info(f"Checking embedding dimension for model: {self.embedding_model}...")
            test_resp = self.openai_client.embeddings.create(input="test", model=self.embedding_model)
            dim = len(test_resp.data[0].embedding)
            logger.info(f"Detected embedding dimension: {dim}")
            return dim
        except APIConnectionError as exc:
            logger.error(
                "Failed to connect to embedding service: error_type=%s",
                type(exc).__name__,
            )
            raise
        except APIError as exc:
            logger.error(
                "Embedding dimension API failed: error_type=%s",
                type(exc).__name__,
            )
            raise

    def init_collection(self):
        try:
            self.qdrant_client.get_collection(self.collection_name)
            logger.info(f"Collection {self.collection_name} exists.")
        except UnexpectedResponse:
            logger.info(f"Collection {self.collection_name} does not exist. Creating...")
            self.qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(size=self.embedding_dim, distance=models.Distance.COSINE),
            )

    @staticmethod
    def generate_deterministic_uuid(content: str) -> str:
        """
        Generate a deterministic UUID based on string content.
        """
        hash_object = hashlib.md5(content.encode('utf-8'))
        return str(uuid.UUID(hash_object.hexdigest()))

    def _get_embedding_safe(self, text: str) -> List[float]:
        """Get embedding vector, returning empty list on failure."""
        try:
            response = self.openai_client.embeddings.create(
                input=text, model=self.embedding_model,
            )
            return response.data[0].embedding
        except Exception as exc:
            logger.error("Embedding failed: error_type=%s", type(exc).__name__)
            return []

    def process_email(self, email: Dict[str, Any]) -> bool:
        """
        Process a single email. Returns True on success.
        """
        return self.process_batch([email]) > 0
        

    @retry(stop=stop_after_attempt(2), wait=wait_random_exponential(multiplier=1, max=10))
    def process_batch(self, batch_emails: List[Dict[str, Any]]) -> int:
        """
        Embed and upsert a batch of emails. Returns number of points upserted.
        """
        if not batch_emails:
            return 0
            
        # Ensure collection exists
        self.init_collection()

        points = []
        for email in batch_emails:
            # --- 1. Process Attachments Metadata (Lazy Image Analysis) ---
            attachments = email.get("attachments", [])
            attachment_summaries = []
            valid_attachments_metadata = []

            for att in attachments:
                name = att.get("name", "unknown")
                ftype = att.get("content_type", "application/octet-stream")
                size = att.get("size", 0)
                # Metadata summary for embedding intent
                meta_str = f"附件: {name} ({ftype}, {size} bytes)"
                attachment_summaries.append(meta_str)
                
                # Store structured metadata for payload
                valid_attachments_metadata.append({
                    "name": name,
                    "content_type": ftype,
                    "size": size
                })

            # --- 2. Construct Full Text ---
            # Structure: Subject + Attachment Metadata + Body (no image descriptions at this stage)
            
            parts = [f"Subject: {email.get('subject', '')}"]
            
            if attachment_summaries:
                parts.append("【包含的附件列表】:\n" + "\n".join(attachment_summaries))
                
            body_projection = project_email_body_for_model(
                email.get("body", ""),
                unique_body=email.get("unique_body"),
            )
            projected_body = body_projection.current_text
            if len(projected_body) > MAX_INDEX_BODY_CHARS:
                projected_body = (
                    projected_body[:MAX_INDEX_BODY_CHARS] + "\n...[truncated]"
                )

            if projected_body:
                parts.append("【本轮新增正文】:\n" + projected_body)
                body_source = "current_message"
            elif body_projection.has_quoted_history:
                # A bare forward/reply has no delta to embed.  Retain a bounded
                # fallback so it remains semantically searchable, without
                # re-embedding an arbitrarily deep nested thread.
                history_fallback = body_projection.quoted_text[
                    :MAX_QUOTED_HISTORY_FALLBACK_CHARS
                ]
                parts.append(
                    "【引用历史（本轮无新增正文）】:\n" + history_fallback
                )
                body_source = "quoted_history_fallback"
            else:
                parts.append("【本轮新增正文】:\n")
                body_source = "current_message"

            quoted_history_preview = body_projection.quoted_text[
                :MAX_QUOTED_HISTORY_PREVIEW_CHARS
            ]
            
            full_text = "\n\n".join(parts)
            
            chunks = self.text_splitter.split_text(full_text)
            
            if not chunks:
                continue

            base_id = email.get("id") or email.get("message_id") or str(uuid.uuid4())
            
            try:
                embeddings = []
                # Batch request to avoid embedding API timeouts
                for chunk_idx in range(0, len(chunks), 50):
                    batch_chunks = chunks[chunk_idx:chunk_idx+50]
                    responses = self.openai_client.embeddings.create(
                        input=batch_chunks, 
                        model=self.embedding_model,
                        timeout=30.0
                    )
                    embeddings.extend([data.embedding for data in responses.data])
                
                for i, (chunk, vector) in enumerate(zip(chunks, embeddings)):
                    chunk_id = self.generate_deterministic_uuid(f"{base_id}_{i}_{chunk[:20]}")
                    
                    payload = email.copy()
                    # Legacy callers may still include image payload copies.  Never
                    # persist those bytes to Qdrant, while keeping the caller's
                    # dictionary untouched.
                    payload.pop("_image_attachments", None)
                    payload.pop("image_analysis", None)
                    # Never retain provider-supplied raw alternative bodies in
                    # Qdrant: both canonical and Gateway aliases may contain
                    # HTML/data URIs.  ``body`` below is the bounded, safe
                    # current-message projection.
                    payload.pop("unique_body", None)
                    payload.pop("uniqueBody", None)
                    payload.pop("UniqueBody", None)
                    thread_id = payload.get("thread_id") or payload.get(
                        "conversation_id"
                    )
                    if thread_id:
                        payload["thread_id"] = thread_id
                    payload["body"] = projected_body
                    payload["body_source"] = body_source
                    payload["has_quoted_history"] = (
                        body_projection.has_quoted_history
                    )
                    if quoted_history_preview:
                        payload["quoted_history_preview"] = quoted_history_preview
                    payload["attachments_metadata"] = valid_attachments_metadata
                    
                    if "attachments" in payload:
                        del payload["attachments"]
                    payload["chunk_index"] = i
                    payload["chunk_text"] = chunk
                    
                    if "body" in payload and isinstance(payload["body"], str) and len(payload["body"]) > 2000:
                        payload["body_preview"] = payload["body"][:2000]
                        payload["body"] = payload["body"][:2000] + "... [truncated]"
                    
                    points.append(models.PointStruct(
                        id=chunk_id,
                        vector=vector,
                        payload=payload
                    ))
            except RateLimitError as exc:
                logger.warning(
                    "Email embedding rate limited: email=%s error_type=%s",
                    fingerprint_identifier(base_id, namespace="email"),
                    type(exc).__name__,
                )
                continue
            except APIConnectionError as exc:
                logger.error(
                    "Email embedding connection failed: email=%s error_type=%s",
                    fingerprint_identifier(base_id, namespace="email"),
                    type(exc).__name__,
                )
                continue
            except APIError as exc:
                logger.error(
                    "Email embedding API failed: email=%s error_type=%s",
                    fingerprint_identifier(base_id, namespace="email"),
                    type(exc).__name__,
                )
                continue

        if points:
            try:
                # Upsert in smaller chunks to avoid payload size limits (33MB limit)
                chunk_size = 100
                total_upserted = 0
                for i in range(0, len(points), chunk_size):
                    batch_points = points[i:i+chunk_size]
                    self.qdrant_client.upsert(
                        collection_name=self.collection_name,
                        points=batch_points,
                        wait=False
                    )
                    total_upserted += len(batch_points)
                logger.info(f"Successfully upserted {total_upserted} points to Qdrant.")
                return total_upserted
            except UnexpectedResponse as exc:
                logger.error("Qdrant upsert failed: error_type=%s", type(exc).__name__)
                return 0
            except ConnectionError as exc:
                logger.error("Qdrant upsert connection failed: error_type=%s", type(exc).__name__)
                return 0
        return 0

    def update_email_labels(
        self,
        email_id: str,
        active_skills: Optional[List[str]] = None,
        priority: Optional[str] = None,
        intent: Optional[str] = None,
        need_reply: Optional[bool] = None,
    ) -> bool:
        """
        Write classification/routing labels back into Qdrant payload for already-ingested emails.

        Used by Tier 2 (semantic routing) to vote on past similar emails' skills.
        Returns True if at least one point was updated.
        """
        if not email_id:
            return False

        payload_update: Dict[str, Any] = {}
        if active_skills is not None:
            payload_update["active_skills"] = list(active_skills)
        if priority is not None:
            payload_update["priority"] = priority
        if intent is not None:
            payload_update["intent"] = intent
        if need_reply is not None:
            payload_update["need_reply"] = bool(need_reply)
        if not payload_update:
            return False

        try:
            point_filter = models.Filter(
                must=[models.FieldCondition(key="id", match=models.MatchValue(value=email_id))]
            )
            self.qdrant_client.set_payload(
                collection_name=self.collection_name,
                payload=payload_update,
                points=point_filter,
                wait=False,
            )
            logger.info(
                "Updated Qdrant labels: email=%s field_count=%d",
                fingerprint_identifier(email_id, namespace="email"),
                len(payload_update),
            )
            return True
        except UnexpectedResponse as exc:
            logger.error(
                "Qdrant set-payload failed: email=%s error_type=%s",
                fingerprint_identifier(email_id, namespace="email"),
                type(exc).__name__,
            )
            return False
        except ConnectionError as exc:
            logger.error(
                "Qdrant set-payload connection failed: email=%s error_type=%s",
                fingerprint_identifier(email_id, namespace="email"),
                type(exc).__name__,
            )
            return False
        except Exception as exc:
            logger.error(
                "Qdrant set-payload failed: email=%s error_type=%s",
                fingerprint_identifier(email_id, namespace="email"),
                type(exc).__name__,
            )
            return False

    def process_sent_email(self, original_email_data: dict, reply_content: str, reply_id: str = None) -> bool:
        """
        Create a synthetic email object for the sent reply and index it into Qdrant.
        """
        if not reply_id:
            reply_id = str(uuid.uuid4())
            
        subject = f"Re: {original_email_data.get('subject', 'No Subject')}"
        
        sent_email = {
            "id": reply_id,
            "subject": subject,
            "body": reply_content,
            "sender": "me",
            "to": original_email_data.get("sender"),
            "recipients": original_email_data.get("sender"),
            "received_at": "Now",
            "type": "sent_reply",
            "attachments": []
        }
        
        logger.info(
            "Indexing sent reply: email=%s",
            fingerprint_identifier(reply_id, namespace="email"),
        )
        return self.process_email(sent_email)
