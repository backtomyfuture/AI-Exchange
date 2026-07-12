"""
User Preference Learner — learns drafting preferences from user edits.

Analyzes diffs between original AI-generated drafts and user-edited final versions
to extract patterns like tone adjustments, phrase replacements, and structural preferences.
Rejection reasons are also mined for explicit negative feedback.

Data flow:
  1. Query emails_log for records where original_draft != final_draft
  2. Also query for rejection_reason IS NOT NULL
  3. LLM extracts preference patterns via tool calling
  4. Structured preferences stored in Qdrant `user_preferences` collection
  5. Retrievable by context for future draft generation
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

PREFERENCES_COLLECTION = "user_preferences"

_PREFERENCE_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "save_preferences",
            "description": "Save the extracted user drafting preferences.",
            "parameters": {
                "type": "object",
                "properties": {
                    "preferences": {
                        "type": "array",
                        "description": "List of user preference patterns extracted from draft modifications.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "pattern": {
                                    "type": "string",
                                    "description": "A concise description of the preference, "
                                    "e.g. 'User always replaces 您好 with Hi when writing to internal colleagues'",
                                },
                                "category": {
                                    "type": "string",
                                    "enum": [
                                        "tone_preference",
                                        "phrase_replacement",
                                        "structure_preference",
                                        "rejection_pattern",
                                    ],
                                    "description": "Category of the preference.",
                                },
                                "confidence": {
                                    "type": "number",
                                    "description": "Confidence score 0.0 to 1.0 based on consistency and sample size.",
                                },
                                "example_before": {
                                    "type": "string",
                                    "description": "Example text from the original AI draft.",
                                },
                                "example_after": {
                                    "type": "string",
                                    "description": "Example text from the user-edited version.",
                                },
                            },
                            "required": ["pattern", "category", "confidence", "example_before", "example_after"],
                        },
                    },
                },
                "required": ["preferences"],
            },
        },
    }
]


class UserPreferenceLearner:
    """Learns user drafting preferences from historical draft modifications."""

    def __init__(self, db_manager, email_processor=None):
        self.db_manager = db_manager
        self.email_processor = email_processor

    async def learn(self, days: int = 14) -> dict:
        """Run a preference learning cycle.

        Args:
            days: Number of days to look back for draft modifications.

        Returns:
            Dict with keys ``preferences_count`` and ``stored``.
        """
        modified_drafts = await self._fetch_modified_drafts(days)
        rejections = await self._fetch_rejections(days)

        if not modified_drafts and not rejections:
            logger.info("Preference learning skipped: no modified drafts or rejections found.")
            return {"preferences_count": 0, "stored": False}

        preferences_data = await self._analyze_with_llm(modified_drafts, rejections)
        if not preferences_data:
            return {"preferences_count": 0, "stored": False}

        preferences = preferences_data.get("preferences", [])

        stored = False
        if preferences and self.email_processor:
            stored = await self._store_preferences(preferences)

        logger.info(
            "Preference learning complete: %d preferences, stored=%s",
            len(preferences), stored,
        )
        return {"preferences_count": len(preferences), "stored": stored}

    async def get_preferences(self, context: str = "", limit: int = 5) -> list[dict]:
        """Search Qdrant for relevant user preferences.

        Args:
            context: Text context to match preferences against.
            limit: Maximum number of preferences to return.

        Returns:
            List of preference payload dicts.
        """
        if not self.email_processor:
            return []

        try:
            from qdrant_client.http.exceptions import UnexpectedResponse

            client = self.email_processor.qdrant_client

            try:
                await asyncio.to_thread(client.get_collection, PREFERENCES_COLLECTION)
            except (UnexpectedResponse, Exception):
                return []

            if not context:
                points, _ = await asyncio.to_thread(
                    client.scroll,
                    collection_name=PREFERENCES_COLLECTION,
                    limit=limit,
                    with_payload=True,
                )
                return [p.payload for p in points] if points else []

            embedding = await asyncio.to_thread(
                self.email_processor._get_embedding_safe, context,
            )
            if not embedding:
                return []

            result = await asyncio.to_thread(
                client.query_points,
                collection_name=PREFERENCES_COLLECTION,
                query=embedding,
                limit=limit,
                with_payload=True,
            )
            return [hit.payload for hit in result.points]

        except Exception as exc:
            logger.debug(
                "Preference search failed: error_type=%s",
                type(exc).__name__,
            )
            return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_modified_drafts(self, days: int) -> list[dict]:
        """Fetch records where original_draft and final_draft both exist and differ."""
        try:
            async with self.db_manager.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT id, subject, sender, original_draft, final_draft
                        FROM emails_log
                        WHERE processed_at >= CURRENT_DATE - INTERVAL '%s days'
                          AND original_draft IS NOT NULL
                          AND final_draft IS NOT NULL
                          AND original_draft <> final_draft
                        ORDER BY processed_at DESC
                        LIMIT 100
                        """,
                        (days,),
                    )
                    return await cur.fetchall()
        except Exception as exc:
            logger.error(
                "Failed to fetch modified drafts: error_type=%s",
                type(exc).__name__,
            )
            return []

    async def _fetch_rejections(self, days: int) -> list[dict]:
        """Fetch records where a rejection reason was recorded."""
        try:
            async with self.db_manager.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT id, subject, sender, rejection_reason, original_draft
                        FROM emails_log
                        WHERE processed_at >= CURRENT_DATE - INTERVAL '%s days'
                          AND rejection_reason IS NOT NULL
                        ORDER BY processed_at DESC
                        LIMIT 50
                        """,
                        (days,),
                    )
                    return await cur.fetchall()
        except Exception as exc:
            logger.error(
                "Failed to fetch rejection records: error_type=%s",
                type(exc).__name__,
            )
            return []

    async def _analyze_with_llm(
        self,
        modified_drafts: list[dict],
        rejections: list[dict],
    ) -> dict[str, Any] | None:
        """Use LLM tool calling to extract preference patterns."""
        from src.providers.factory import get_llm_for_role

        draft_lines: list[str] = []
        for r in modified_drafts[:50]:
            original = (r.get("original_draft") or "")[:300]
            final = (r.get("final_draft") or "")[:300]
            draft_lines.append(
                f"- sender={r.get('sender', '?')}, subject=\"{(r.get('subject') or '')[:60]}\"\n"
                f"  ORIGINAL: {original}\n"
                f"  EDITED:   {final}"
            )

        rejection_lines: list[str] = []
        for r in rejections[:30]:
            reason = (r.get("rejection_reason") or "")[:200]
            original = (r.get("original_draft") or "")[:200]
            rejection_lines.append(
                f"- sender={r.get('sender', '?')}, subject=\"{(r.get('subject') or '')[:60]}\"\n"
                f"  REASON: {reason}\n"
                f"  DRAFT:  {original}"
            )

        prompt_parts = [
            "Analyze the following draft modifications and rejections to extract the user's "
            "writing preferences. Focus on:\n"
            "1. Tone preferences (formal vs casual, 您 vs 你, etc.)\n"
            "2. Phrase replacements (specific words/phrases the user consistently changes)\n"
            "3. Structure preferences (greeting style, closing style, paragraph structure)\n"
            "4. Rejection patterns (what causes the user to reject drafts)\n\n"
            "Only report patterns with clear evidence. Call the save_preferences tool with your analysis.",
        ]

        if draft_lines:
            prompt_parts.append(
                f"\n## Modified Drafts ({len(modified_drafts)} total, showing {len(draft_lines)})\n"
                + "\n".join(draft_lines)
            )

        if rejection_lines:
            prompt_parts.append(
                f"\n## Rejected Drafts ({len(rejections)} total, showing {len(rejection_lines)})\n"
                + "\n".join(rejection_lines)
            )

        prompt = "\n".join(prompt_parts)

        try:
            llm = get_llm_for_role("consolidator", temperature=0)

            from langchain_core.messages import HumanMessage, SystemMessage
            messages = [
                SystemMessage(content=(
                    "You are a writing style analyst. Analyze draft modifications and rejections "
                    "to extract the user's writing preferences. Call save_preferences with structured results."
                )),
                HumanMessage(content=prompt),
            ]

            llm_with_tools = llm.bind_tools(_PREFERENCE_TOOL)
            response = await llm_with_tools.ainvoke(messages)

            if response.tool_calls:
                return response.tool_calls[0].get("args", {})

            if response.additional_kwargs.get("tool_calls"):
                tc = response.additional_kwargs["tool_calls"][0]
                args_str = tc.get("function", {}).get("arguments", "{}")
                return json.loads(args_str)

            logger.warning("Preference learning: LLM did not call save_preferences")
            return None

        except Exception as exc:
            logger.error(
                "Preference learning LLM analysis failed: error_type=%s",
                type(exc).__name__,
            )
            return None

    async def _store_preferences(self, preferences: list[dict]) -> bool:
        """Store preference patterns into Qdrant user_preferences collection."""
        try:
            from qdrant_client import models as qdrant_models
            from qdrant_client.http.exceptions import UnexpectedResponse

            client = self.email_processor.qdrant_client

            try:
                await asyncio.to_thread(client.get_collection, PREFERENCES_COLLECTION)
            except (UnexpectedResponse, Exception):
                dim = self.email_processor.embedding_dim
                await asyncio.to_thread(
                    client.create_collection,
                    collection_name=PREFERENCES_COLLECTION,
                    vectors_config=qdrant_models.VectorParams(
                        size=dim, distance=qdrant_models.Distance.COSINE,
                    ),
                )
                logger.info("Created Qdrant collection: %s", PREFERENCES_COLLECTION)

            points = []
            timestamp = datetime.now().isoformat()

            for i, pref in enumerate(preferences):
                pattern = pref.get("pattern", "")
                if not pattern:
                    continue

                embedding_text = (
                    f"{pref.get('category', '')}: {pattern} "
                    f"before: {pref.get('example_before', '')} "
                    f"after: {pref.get('example_after', '')}"
                )

                embedding = await asyncio.to_thread(
                    self.email_processor._get_embedding_safe, embedding_text,
                )
                if not embedding:
                    continue

                point_id = self.email_processor.generate_deterministic_uuid(
                    f"preference_{timestamp}_{i}_{pattern[:30]}"
                )

                payload = {
                    "pattern": pattern,
                    "category": pref.get("category", ""),
                    "confidence": pref.get("confidence", 0.0),
                    "example_before": pref.get("example_before", ""),
                    "example_after": pref.get("example_after", ""),
                    "learned_at": timestamp,
                    "type": "user_preference",
                }

                points.append(qdrant_models.PointStruct(
                    id=point_id, vector=embedding, payload=payload,
                ))

            if points:
                await asyncio.to_thread(
                    client.upsert,
                    collection_name=PREFERENCES_COLLECTION,
                    points=points,
                    wait=False,
                )
                logger.info("Stored %d user preferences to Qdrant.", len(points))
                return True

            return False

        except Exception as exc:
            logger.error(
                "Failed to store user preferences: error_type=%s",
                type(exc).__name__,
            )
            return False
