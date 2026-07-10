"""
Memory Consolidator — 定期分析邮件处理模式，提取经验洞察。

借鉴 nanobot 的 MemoryStore 合并模式，但集成 Qdrant 向量检索和 Tier 路由系统。

数据流:
  1. 从 emails_log 查询近期处理记录
  2. LLM 分析模式：发件人规律、意图分布、处理偏好
  3. 结构化洞察存入 Qdrant email_experience 集合 (Tier 2 可检索)
  4. 高置信度规律可生成 Tier 1 候选规则建议
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any


logger = logging.getLogger(__name__)

EXPERIENCE_COLLECTION = "email_experience"

_CONSOLIDATE_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "save_experience",
            "description": "Save the email processing experience consolidation result.",
            "parameters": {
                "type": "object",
                "properties": {
                    "insights": {
                        "type": "array",
                        "description": "List of experience insights extracted from processing records.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "pattern": {
                                    "type": "string",
                                    "description": "A concise description of the observed pattern, "
                                    "e.g. 'Emails from finance@corp.com about invoices are always P2/need_reply'",
                                },
                                "category": {
                                    "type": "string",
                                    "enum": ["sender_pattern", "intent_pattern", "priority_pattern",
                                             "response_style", "routing_hint"],
                                    "description": "Category of the insight.",
                                },
                                "confidence": {
                                    "type": "number",
                                    "description": "Confidence score 0.0 to 1.0 based on sample size and consistency.",
                                },
                                "suggested_action": {
                                    "type": "string",
                                    "description": "Recommended handling for emails matching this pattern.",
                                },
                                "sample_count": {
                                    "type": "integer",
                                    "description": "Number of emails this pattern was observed in.",
                                },
                            },
                            "required": ["pattern", "category", "confidence"],
                        },
                    },
                    "summary": {
                        "type": "string",
                        "description": "Overall summary of email processing trends in the analyzed period.",
                    },
                },
                "required": ["insights", "summary"],
            },
        },
    }
]


class MemoryConsolidator:
    """Analyzes processed email records and extracts reusable experience insights."""

    def __init__(self, db_manager, email_processor=None):
        self.db_manager = db_manager
        self.email_processor = email_processor

    async def consolidate(self, days: int = 7, min_records: int = 5) -> dict[str, Any]:
        """Run a consolidation cycle.

        Args:
            days: Number of days to look back.
            min_records: Minimum records needed to trigger consolidation.

        Returns:
            Dict with keys 'insights_count', 'summary', 'stored'.
        """
        records = await self._fetch_recent_records(days)
        if len(records) < min_records:
            logger.info(
                "Memory consolidation skipped: only %d records (need %d).",
                len(records), min_records,
            )
            return {"insights_count": 0, "summary": "Not enough data", "stored": False}

        existing_experience = await self._load_existing_experience()

        insights_data = await self._analyze_with_llm(records, existing_experience)
        if not insights_data:
            return {"insights_count": 0, "summary": "LLM analysis failed", "stored": False}

        insights = insights_data.get("insights", [])
        summary = insights_data.get("summary", "")

        stored = False
        if insights and self.email_processor:
            stored = await self._store_insights(insights, summary)

        logger.info(
            "Memory consolidation complete: %d insights, stored=%s",
            len(insights), stored,
        )
        return {
            "insights_count": len(insights),
            "summary": summary,
            "stored": stored,
        }

    async def _fetch_recent_records(self, days: int) -> list[dict]:
        """Fetch recent processed email records from the database."""
        try:
            async with self.db_manager.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT id, subject, sender, status, classification,
                               processed_at, updated_at
                        FROM emails_log
                        WHERE processed_at >= CURRENT_DATE - INTERVAL '%s days'
                          AND status NOT IN ('pending', 'error')
                        ORDER BY processed_at DESC
                        LIMIT 200
                        """,
                        (days,),
                    )
                    return await cur.fetchall()
        except Exception as e:
            logger.error("Failed to fetch records for consolidation: %s", e)
            return []

    async def _load_existing_experience(self) -> str:
        """Load existing experience from Qdrant for context."""
        if not self.email_processor:
            return ""
        try:
            from qdrant_client.http.exceptions import UnexpectedResponse

            client = self.email_processor.qdrant_client
            try:
                client.get_collection(EXPERIENCE_COLLECTION)
            except (UnexpectedResponse, Exception):
                return ""

            points, _ = client.scroll(
                collection_name=EXPERIENCE_COLLECTION,
                limit=20,
                with_payload=True,
            )
            if not points:
                return ""

            lines = []
            for p in points:
                pattern = p.payload.get("pattern", "")
                category = p.payload.get("category", "")
                confidence = p.payload.get("confidence", 0)
                lines.append(f"- [{category}] (conf={confidence:.1f}) {pattern}")
            return "\n".join(lines)
        except Exception as e:
            logger.warning("Failed to load existing experience: %s", e)
            return ""

    async def _analyze_with_llm(
        self,
        records: list[dict],
        existing_experience: str,
    ) -> dict[str, Any] | None:
        """Use LLM to analyze records and extract patterns via structured tool call."""
        from src.providers.factory import get_llm_for_role

        lines = []
        for r in records[:100]:
            cls = r.get("classification")
            if isinstance(cls, str):
                try:
                    cls = json.loads(cls)
                except (json.JSONDecodeError, TypeError):
                    cls = {}
            cls = cls or {}

            priority = cls.get("priority", "?")
            intent = cls.get("intent", "?")
            need_reply = cls.get("need_reply", "?")
            summary_text = cls.get("summary", "")[:80]

            lines.append(
                f"- sender={r.get('sender', '?')}, subject=\"{(r.get('subject') or '')[:60]}\", "
                f"status={r.get('status')}, priority={priority}, intent={intent}, "
                f"need_reply={need_reply}, summary=\"{summary_text}\""
            )

        existing_ctx = ""
        if existing_experience:
            existing_ctx = f"\n## Existing Experience\n{existing_experience}\n"

        prompt = f"""Analyze these email processing records and extract reusable patterns.
Focus on:
1. Sender patterns (recurring senders and their typical email types)
2. Intent distribution (common intents and how they map to priorities)
3. Response style preferences (what gets approved vs rejected)
4. Routing hints (patterns that could speed up future classification)

Only report patterns with clear evidence (3+ occurrences). Be specific.
Call the save_experience tool with your analysis.
{existing_ctx}
## Recent Email Processing Records ({len(records)} emails, last {len(lines)} shown)
{chr(10).join(lines)}"""

        try:
            llm = get_llm_for_role("consolidator", temperature=0)

            from langchain_core.messages import HumanMessage, SystemMessage
            messages = [
                SystemMessage(content=(
                    "You are an email processing pattern analyst. "
                    "Analyze the records and call save_experience with structured insights."
                )),
                HumanMessage(content=prompt),
            ]

            tools_schema = _CONSOLIDATE_TOOL
            llm_with_tools = llm.bind_tools(tools_schema)
            response = await llm_with_tools.ainvoke(messages)

            if response.tool_calls:
                args = response.tool_calls[0].get("args", {})
                return args

            if response.additional_kwargs.get("tool_calls"):
                tc = response.additional_kwargs["tool_calls"][0]
                args_str = tc.get("function", {}).get("arguments", "{}")
                return json.loads(args_str)

            logger.warning("Memory consolidation: LLM did not call save_experience")
            return None

        except Exception as e:
            logger.error("Memory consolidation LLM analysis failed: %s", e)
            return None

    async def _store_insights(self, insights: list[dict], summary: str) -> bool:
        """Store insights into Qdrant email_experience collection."""
        import asyncio
        try:
            from qdrant_client import models as qdrant_models
            from qdrant_client.http.exceptions import UnexpectedResponse

            client = self.email_processor.qdrant_client

            try:
                client.get_collection(EXPERIENCE_COLLECTION)
            except (UnexpectedResponse, Exception):
                dim = self.email_processor.embedding_dim
                client.create_collection(
                    collection_name=EXPERIENCE_COLLECTION,
                    vectors_config=qdrant_models.VectorParams(
                        size=dim, distance=qdrant_models.Distance.COSINE,
                    ),
                )
                logger.info("Created Qdrant collection: %s", EXPERIENCE_COLLECTION)

            points = []
            timestamp = datetime.now().isoformat()

            for i, insight in enumerate(insights):
                pattern = insight.get("pattern", "")
                if not pattern:
                    continue

                embedding_text = (
                    f"{insight.get('category', '')}: {pattern} "
                    f"action: {insight.get('suggested_action', '')}"
                )

                embedding = await asyncio.to_thread(
                    self.email_processor._get_embedding_safe, embedding_text,
                )
                if not embedding:
                    continue

                point_id = self.email_processor.generate_deterministic_uuid(
                    f"experience_{timestamp}_{i}_{pattern[:30]}"
                )

                payload = {
                    "pattern": pattern,
                    "category": insight.get("category", ""),
                    "confidence": insight.get("confidence", 0.0),
                    "suggested_action": insight.get("suggested_action", ""),
                    "sample_count": insight.get("sample_count", 0),
                    "summary": summary,
                    "consolidated_at": timestamp,
                    "type": "experience_insight",
                }

                points.append(qdrant_models.PointStruct(
                    id=point_id, vector=embedding, payload=payload,
                ))

            if points:
                client.upsert(
                    collection_name=EXPERIENCE_COLLECTION,
                    points=points,
                    wait=False,
                )
                logger.info(
                    "Stored %d experience insights to Qdrant.", len(points),
                )
                return True

            return False

        except Exception as e:
            logger.error("Failed to store experience insights: %s", e)
            return False


async def search_experience(
    query_text: str,
    email_processor,
    limit: int = 3,
    min_confidence: float = 0.5,
) -> list[dict]:
    """Search relevant experience insights from Qdrant.

    Used by retriever_node to inject experience context before categorization.
    """
    import asyncio
    try:
        from qdrant_client import models as qdrant_models
        from qdrant_client.http.exceptions import UnexpectedResponse

        client = email_processor.qdrant_client

        try:
            client.get_collection(EXPERIENCE_COLLECTION)
        except (UnexpectedResponse, Exception):
            return []

        embedding = await asyncio.to_thread(
            email_processor._get_embedding_safe, query_text,
        )
        if not embedding:
            return []

        confidence_filter = qdrant_models.Filter(
            must=[
                qdrant_models.FieldCondition(
                    key="confidence",
                    range=qdrant_models.Range(gte=min_confidence),
                )
            ]
        )

        result = await asyncio.to_thread(
            client.query_points,
            collection_name=EXPERIENCE_COLLECTION,
            query=embedding,
            query_filter=confidence_filter,
            limit=limit,
            with_payload=True,
        )

        return [hit.payload for hit in result.points]

    except Exception as e:
        logger.debug("Experience search failed (non-critical): %s", e)
        return []
