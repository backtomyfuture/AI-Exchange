"""
Style Profiler — extracts writing style DNA from historical sent emails.

Analyzes sent replies stored in Qdrant to build a multi-register style profile
that captures the user's voice across different relationship tiers (e.g. internal
colleagues, external partners, executives).

Data flow:
  1. Scroll Qdrant `emails` collection for type=sent_reply records
  2. Group by recipient domain/sender to infer relationship tiers
  3. LLM analyzes style: sentence length, formality, greetings, Chinese/English mixing
  4. Profile stored in Qdrant `style_profiles` collection (single versioned document)
  5. get_style_guidance() returns formatted prompt injection for drafter
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from src.config import get_settings

logger = logging.getLogger(__name__)

STYLES_COLLECTION = "style_profiles"
EMAILS_COLLECTION = "emails"
PROFILE_REBUILD_INTERVAL_HOURS = 72

_STYLE_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "save_style_profile",
            "description": "Save the analyzed writing style profile.",
            "parameters": {
                "type": "object",
                "properties": {
                    "general_style": {
                        "type": "string",
                        "description": "Overall summary of the user's writing style "
                        "(sentence length, tone, Chinese/English mixing habits, etc.).",
                    },
                    "register_profiles": {
                        "type": "array",
                        "description": "Style profiles segmented by relationship tier.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "tier": {
                                    "type": "string",
                                    "description": "Relationship tier, e.g. 'internal_colleague', "
                                    "'external_partner', 'executive', 'vendor'.",
                                },
                                "style_notes": {
                                    "type": "string",
                                    "description": "Style notes specific to this tier.",
                                },
                                "sample_phrases": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Representative phrases/greetings/closings used for this tier.",
                                },
                            },
                            "required": ["tier", "style_notes", "sample_phrases"],
                        },
                    },
                    "common_replacements": {
                        "type": "array",
                        "description": "Common phrase substitutions the user prefers.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "original": {
                                    "type": "string",
                                    "description": "Phrase that should be avoided.",
                                },
                                "preferred": {
                                    "type": "string",
                                    "description": "Phrase the user prefers instead.",
                                },
                            },
                            "required": ["original", "preferred"],
                        },
                    },
                },
                "required": ["general_style", "register_profiles", "common_replacements"],
            },
        },
    }
]


class StyleProfiler:
    """Extracts and caches a writing style profile from historical sent emails."""

    def __init__(self, email_processor=None):
        self.email_processor = email_processor
        self._cached_profile: dict | None = None
        self._cached_at: datetime | None = None

    async def build_profile(self) -> dict:
        """Build (or rebuild) the writing style profile.

        Returns:
            Style profile dict with keys ``general_style``, ``register_profiles``,
            and ``common_replacements``.
        """
        if not _memory_learning_enabled():
            logger.info("Style profiling skipped: Memory Learning is disabled.")
            return _empty_profile()

        sent_emails = await self._fetch_sent_replies()
        if not sent_emails:
            logger.info("Style profiling skipped: no sent replies found.")
            return _empty_profile()

        grouped = self._group_by_recipient(sent_emails)
        profile = await self._analyze_with_llm(sent_emails, grouped)

        if not profile:
            return _empty_profile()

        if self.email_processor:
            await self._store_profile(profile)

        self._cached_profile = profile
        self._cached_at = datetime.now()

        logger.info(
            "Style profile built: %d register tiers, %d replacements",
            len(profile.get("register_profiles", [])),
            len(profile.get("common_replacements", [])),
        )
        return profile

    async def get_style_guidance(self, sender_email: str = "") -> str:
        """Return formatted style guidance for LLM prompt injection.

        Args:
            sender_email: The email sender address to match a register tier.

        Returns:
            A formatted string suitable for appending to an LLM system prompt.
        """
        profile = await self._get_or_build_profile()
        if not profile or not profile.get("general_style"):
            return ""

        parts = [f"## User Writing Style\n{profile['general_style']}"]

        if sender_email and profile.get("register_profiles"):
            matched_tier = self._match_tier(sender_email, profile["register_profiles"])
            if matched_tier:
                parts.append(
                    f"\n### Register for this sender ({matched_tier['tier']})\n"
                    f"{matched_tier['style_notes']}"
                )
                if matched_tier.get("sample_phrases"):
                    parts.append(
                        "Sample phrases: " + " | ".join(matched_tier["sample_phrases"][:5])
                    )

        if profile.get("common_replacements"):
            replacement_lines = [
                f"  - \"{r['original']}\" → \"{r['preferred']}\""
                for r in profile["common_replacements"][:10]
            ]
            parts.append("\n### Preferred Replacements\n" + "\n".join(replacement_lines))

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_or_build_profile(self) -> dict | None:
        """Return cached profile if fresh enough, otherwise rebuild."""
        if (
            self._cached_profile
            and self._cached_at
            and (datetime.now() - self._cached_at) < timedelta(hours=PROFILE_REBUILD_INTERVAL_HOURS)
        ):
            return self._cached_profile

        stored = await self._load_stored_profile()
        if stored:
            consolidated_at = stored.get("consolidated_at", "")
            if consolidated_at:
                try:
                    ts = datetime.fromisoformat(consolidated_at)
                    if (datetime.now() - ts) < timedelta(hours=PROFILE_REBUILD_INTERVAL_HOURS):
                        self._cached_profile = stored
                        self._cached_at = ts
                        return stored
                except (ValueError, TypeError):
                    pass

        if not _memory_learning_enabled():
            return stored
        return await self.build_profile()

    async def _load_stored_profile(self) -> dict | None:
        """Load the most recent style profile from Qdrant."""
        if not self.email_processor:
            return None
        try:
            from qdrant_client.http.exceptions import UnexpectedResponse

            client = self.email_processor.qdrant_client

            try:
                await asyncio.to_thread(client.get_collection, STYLES_COLLECTION)
            except (UnexpectedResponse, Exception):
                return None

            points, _ = await asyncio.to_thread(
                client.scroll,
                collection_name=STYLES_COLLECTION,
                limit=1,
                with_payload=True,
            )
            if points:
                return points[0].payload
            return None
        except Exception as exc:
            logger.debug(
                "Failed to load stored style profile: error_type=%s",
                type(exc).__name__,
            )
            return None

    async def _fetch_sent_replies(self) -> list[dict]:
        """Scroll Qdrant emails collection for sent_reply records."""
        if not self.email_processor:
            return []
        try:
            from qdrant_client import models as qdrant_models
            from qdrant_client.http.exceptions import UnexpectedResponse

            client = self.email_processor.qdrant_client

            try:
                await asyncio.to_thread(client.get_collection, EMAILS_COLLECTION)
            except (UnexpectedResponse, Exception):
                return []

            type_filter = qdrant_models.Filter(
                must=[
                    qdrant_models.FieldCondition(
                        key="type",
                        match=qdrant_models.MatchValue(value="sent_reply"),
                    )
                ]
            )

            points, _ = await asyncio.to_thread(
                client.scroll,
                collection_name=EMAILS_COLLECTION,
                scroll_filter=type_filter,
                limit=50,
                with_payload=True,
            )
            return [p.payload for p in points] if points else []

        except Exception as exc:
            logger.warning(
                "Failed to fetch sent replies for style profiling: error_type=%s",
                type(exc).__name__,
            )
            return []

    @staticmethod
    def _group_by_recipient(emails: list[dict]) -> dict[str, list[dict]]:
        """Group emails by recipient domain to infer relationship tiers."""
        grouped: dict[str, list[dict]] = defaultdict(list)
        for email in emails:
            recipient = email.get("to") or email.get("recipients") or ""
            if isinstance(recipient, list):
                recipient = recipient[0] if recipient else ""
            domain = ""
            if "@" in str(recipient):
                domain = str(recipient).split("@")[-1].strip().lower().rstrip(">")
            key = domain or "unknown"
            grouped[key].append(email)
        return dict(grouped)

    async def _analyze_with_llm(
        self,
        emails: list[dict],
        grouped: dict[str, list[dict]],
    ) -> dict[str, Any] | None:
        """Use LLM tool calling to extract style patterns."""
        from src.providers.factory import get_llm_for_role

        sample_lines: list[str] = []
        for email in emails[:40]:
            body = (email.get("body") or email.get("chunk_text") or "")[:400]
            recipient = email.get("to") or email.get("recipients") or "?"
            sample_lines.append(
                f"- to={recipient}, subject=\"{(email.get('subject') or '')[:60]}\"\n"
                f"  BODY: {body}"
            )

        domain_summary = []
        for domain, items in grouped.items():
            domain_summary.append(f"  - {domain}: {len(items)} email(s)")

        prompt = (
            "Analyze the following sent email replies to extract the user's writing style DNA.\n"
            "Focus on:\n"
            "1. General style: average sentence length, formality level, Chinese/English mixing\n"
            "2. Greeting and closing patterns\n"
            "3. Register differences by recipient domain/tier\n"
            "4. Common phrases and replacements the user favors\n\n"
            "Call the save_style_profile tool with your analysis.\n\n"
            f"## Recipient Domains ({len(grouped)} domains)\n"
            + "\n".join(domain_summary)
            + f"\n\n## Sent Replies ({len(emails)} total, showing {len(sample_lines)})\n"
            + "\n".join(sample_lines)
        )

        try:
            llm = get_llm_for_role("consolidator", temperature=0)

            from langchain_core.messages import HumanMessage, SystemMessage
            messages = [
                SystemMessage(content=(
                    "You are a writing style analyst. Analyze the user's sent email replies "
                    "and call save_style_profile with a structured style profile."
                )),
                HumanMessage(content=prompt),
            ]

            llm_with_tools = llm.bind_tools(_STYLE_TOOL)
            response = await llm_with_tools.ainvoke(messages)

            if response.tool_calls:
                return response.tool_calls[0].get("args", {})

            if response.additional_kwargs.get("tool_calls"):
                tc = response.additional_kwargs["tool_calls"][0]
                args_str = tc.get("function", {}).get("arguments", "{}")
                return json.loads(args_str)

            logger.warning("Style profiling: LLM did not call save_style_profile")
            return None

        except Exception as exc:
            logger.error(
                "Style profiling LLM analysis failed: error_type=%s",
                type(exc).__name__,
            )
            return None

    async def _store_profile(self, profile: dict) -> bool:
        """Store the style profile as a single versioned document in Qdrant."""
        try:
            from qdrant_client import models as qdrant_models
            from qdrant_client.http.exceptions import UnexpectedResponse

            client = self.email_processor.qdrant_client

            try:
                await asyncio.to_thread(client.get_collection, STYLES_COLLECTION)
            except (UnexpectedResponse, Exception):
                dim = self.email_processor.embedding_dim
                await asyncio.to_thread(
                    client.create_collection,
                    collection_name=STYLES_COLLECTION,
                    vectors_config=qdrant_models.VectorParams(
                        size=dim, distance=qdrant_models.Distance.COSINE,
                    ),
                )
                logger.info("Created Qdrant collection: %s", STYLES_COLLECTION)

            timestamp = datetime.now().isoformat()
            embedding_text = (
                f"writing style profile: {profile.get('general_style', '')[:200]}"
            )

            embedding = await asyncio.to_thread(
                self.email_processor._get_embedding_safe, embedding_text,
            )
            if not embedding:
                logger.warning("Could not generate embedding for style profile.")
                return False

            point_id = self.email_processor.generate_deterministic_uuid(
                "style_profile_latest"
            )

            payload = {
                "general_style": profile.get("general_style", ""),
                "register_profiles": profile.get("register_profiles", []),
                "common_replacements": profile.get("common_replacements", []),
                "consolidated_at": timestamp,
                "type": "style_profile",
            }

            point = qdrant_models.PointStruct(
                id=point_id, vector=embedding, payload=payload,
            )

            await asyncio.to_thread(
                client.upsert,
                collection_name=STYLES_COLLECTION,
                points=[point],
                wait=False,
            )
            logger.info("Stored style profile to Qdrant (consolidated_at=%s).", timestamp)
            return True

        except Exception as exc:
            logger.error(
                "Failed to store style profile: error_type=%s",
                type(exc).__name__,
            )
            return False

    @staticmethod
    def _match_tier(
        sender_email: str,
        register_profiles: list[dict],
    ) -> dict | None:
        """Best-effort match of sender email to a register tier."""
        if not sender_email or not register_profiles:
            return None

        domain = ""
        if "@" in sender_email:
            domain = sender_email.split("@")[-1].strip().lower()

        settings = get_settings()
        own_domain = ""
        if settings.EXCHANGE_ACCOUNT_EMAIL and "@" in settings.EXCHANGE_ACCOUNT_EMAIL:
            own_domain = settings.EXCHANGE_ACCOUNT_EMAIL.split("@")[-1].strip().lower()

        for rp in register_profiles:
            tier_lower = rp.get("tier", "").lower()
            if domain and own_domain and domain == own_domain and "internal" in tier_lower:
                return rp
            if domain and own_domain and domain != own_domain and "external" in tier_lower:
                return rp

        return register_profiles[0] if register_profiles else None


def _memory_learning_enabled() -> bool:
    return bool(getattr(get_settings(), "MEMORY_LEARNING_ENABLED", False))


def _empty_profile() -> dict:
    return {
        "general_style": "",
        "register_profiles": [],
        "common_replacements": [],
    }
