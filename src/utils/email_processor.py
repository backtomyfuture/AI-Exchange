
import hashlib
import logging
import uuid
from typing import List, Dict, Any, Optional

from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse
from openai import OpenAI, APIError, APIConnectionError, RateLimitError
from langchain_text_splitters import RecursiveCharacterTextSplitter
from tenacity import retry, stop_after_attempt, wait_random_exponential
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage

from src.config import get_settings

logger = logging.getLogger(__name__)

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
        except APIConnectionError as e:
            logger.error(f"Failed to connect to embedding service: {e}")
            raise
        except APIError as e:
            logger.error(f"API error retrieving embedding dimension: {e}")
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

    def process_email(self, email: Dict[str, Any]) -> bool:
        """
        Process a single email. Returns True on success.
        """
        return self.process_batch([email]) > 0
        

    def _describe_image(self, base64_content: str, mime_type: str = "image/jpeg") -> str:
        """
        Use an LLM to generate a text description for an image.
        """
        try:
            from src.providers.factory import get_llm
            llm = get_llm(temperature=0.3)
            
            message = HumanMessage(
                content=[
                    {"type": "text", "text": "请详细描述这张图片的内容，捕捉关键信息、文字和视觉元素。如果是一张图表或文档，请总结其核心数据或要点。请用中文回答。"},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:{mime_type};base64,{base64_content}"
                        },
                    },
                ]
            )
            
            @retry(
                wait=wait_random_exponential(multiplier=1, max=60),
                stop=stop_after_attempt(5),
                reraise=True
            )
            def invoke_with_retry(msg):
                return llm.invoke([msg])

            response = invoke_with_retry(message)
            return response.content
        except RateLimitError as e:
            logger.warning(f"Image analysis rate limited: {e}")
            return "[图片解析失败: 请求限制]"
        except APIConnectionError as e:
            logger.error(f"Image analysis connection failed: {e}")
            return "[图片解析失败: 连接错误]"
        except APIError as e:
            logger.error(f"Image analysis API error: {e}")
            return "[图片解析失败]"

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
            image_attachments = []  # 暂存图片数据，延迟到拟稿阶段分析
            valid_attachments_metadata = []

            for att in attachments:
                name = att.get("name", "unknown")
                ftype = att.get("content_type", "application/octet-stream")
                size = att.get("size", 0)
                content_b64 = att.get("content", "")
                
                # Metadata summary for embedding intent
                meta_str = f"附件: {name} ({ftype}, {size} bytes)"
                attachment_summaries.append(meta_str)
                
                # Store structured metadata for payload
                valid_attachments_metadata.append({
                    "name": name,
                    "content_type": ftype,
                    "size": size
                })

                # Collect image attachments for deferred analysis (no Vision API call here)
                if content_b64 and ftype.startswith("image/"):
                    image_attachments.append({
                        "name": name,
                        "content": content_b64,
                        "mime_type": ftype
                    })
                    logger.info(f"Collected image attachment for deferred analysis: {name} ({ftype})")

            # --- 2. Construct Full Text ---
            # Structure: Subject + Attachment Metadata + Body (no image descriptions at this stage)
            
            parts = [f"Subject: {email.get('subject', '')}"]
            
            if attachment_summaries:
                parts.append("【包含的附件列表】:\n" + "\n".join(attachment_summaries))
                
            parts.append("【邮件正文】:\n" + email.get('body', ''))
            
            full_text = "\n\n".join(parts)
            
            # Store image attachment data for deferred analysis by retriever_node
            if image_attachments:
                email["_image_attachments"] = image_attachments
                logger.info(f"Stored {len(image_attachments)} image(s) for deferred analysis.")
            
            chunks = self.text_splitter.split_text(full_text)
            
            if not chunks:
                continue

            base_id = email.get("id") or email.get("message_id") or str(uuid.uuid4())
            
            try:
                responses = self.openai_client.embeddings.create(input=chunks, model=self.embedding_model)
                embeddings = [data.embedding for data in responses.data]
                
                for i, (chunk, vector) in enumerate(zip(chunks, embeddings)):
                    chunk_id = self.generate_deterministic_uuid(f"{base_id}_{i}_{chunk[:20]}")
                    
                    payload = email.copy()
                    payload["attachments_metadata"] = valid_attachments_metadata
                    if "attachments" in payload:
                        del payload["attachments"]
                        
                    payload["chunk_index"] = i
                    payload["chunk_text"] = chunk
                    if len(payload.get("body", "")) > 5000:
                        payload["body_preview"] = payload["body"][:2000]
                    
                    points.append(models.PointStruct(
                        id=chunk_id,
                        vector=vector,
                        payload=payload
                    ))
            except RateLimitError as e:
                logger.warning(f"Rate limited while embedding email {base_id}: {e}")
                continue
            except APIConnectionError as e:
                logger.error(f"Connection error embedding email {base_id}: {e}")
                continue
            except APIError as e:
                logger.error(f"API error embedding email {base_id}: {e}")
                continue

        if points:
            try:
                self.qdrant_client.upsert(
                    collection_name=self.collection_name,
                    points=points,
                    wait=False
                )
                logger.info(f"Successfully upserted {len(points)} points to Qdrant.")
                return len(points)
            except UnexpectedResponse as e:
                logger.error(f"Qdrant upsert failed (unexpected response): {e}")
                return 0
            except ConnectionError as e:
                logger.error(f"Qdrant connection error during upsert: {e}")
                return 0
        return 0

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
        
        logger.info(f"Indexing sent reply: {reply_id} (Subject: {subject})")
        return self.process_email(sent_email)
