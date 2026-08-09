from __future__ import annotations

import inspect
import hashlib
from collections.abc import Awaitable, Callable, Mapping
from email.utils import parseaddr
from typing import Any, Literal

from pydantic import Field

from src.handoff.models import CanonicalDTO, EvidenceSource, HandoffPlan

Adapter = Callable[[Mapping[str, Any], int], list[dict[str, Any]] | Awaitable[list[dict[str, Any]]]]
BeforeSource = Callable[[EvidenceSource], None | Awaitable[None]]


class EvidenceItem(CanonicalDTO):
    schema_version: Literal[1] = 1
    source: EvidenceSource
    source_id: str = Field(min_length=1, max_length=512)
    content: str = Field(min_length=1)
    sender: str = Field(default="", max_length=512)
    subject: str = Field(default="", max_length=1_024)


class EvidencePack(CanonicalDTO):
    schema_version: Literal[1] = 1
    profile_id: str
    items: tuple[EvidenceItem, ...] = ()


class EvidenceAdapterRegistry:
    """Closed adapter set; callers can substitute implementations, not names."""

    _ALLOWED = frozenset({"mail_thread", "semantic_history", "exchange_contact"})

    def __init__(self, **adapters: Adapter) -> None:
        unknown = set(adapters) - self._ALLOWED
        if unknown:
            raise ValueError(f"adapter not registered: {sorted(unknown)[0]}")
        self._adapters = dict(adapters)

    @classmethod
    def from_email_retriever(cls, retriever: Any) -> "EvidenceAdapterRegistry":
        """Bind the repository's existing Qdrant reader without adding credentials."""

        def mail_thread(request: Mapping[str, Any], limit: int) -> list[dict[str, Any]]:
            thread_id = str(request.get("thread_id") or request.get("conversation_id") or "")
            return retriever.search_by_thread(
                thread_id=thread_id,
                limit=limit,
                exclude_email_id=request.get("email_id"),
            )

        def semantic_history(request: Mapping[str, Any], limit: int) -> list[dict[str, Any]]:
            query = str(request.get("query_text") or request.get("body") or "")
            return retriever.search(
                query_text=query,
                sender=request.get("sender"),
                limit=limit,
                exclude_email_id=request.get("email_id"),
            )

        return cls(mail_thread=mail_thread, semantic_history=semantic_history)

    @classmethod
    def from_runtime(
        cls,
        *,
        retriever: Any,
        exchange_client: Any,
    ) -> "EvidenceAdapterRegistry":
        """Bind the closed production reader set using existing credentials."""

        history = cls.from_email_retriever(retriever)

        async def exchange_contact(
            request: Mapping[str, Any],
            _limit: int,
        ) -> list[dict[str, Any]]:
            sender = parseaddr(str(request.get("sender") or ""))[1].strip().lower()
            if not sender:
                return []
            name = await exchange_client.resolve_contact(sender)
            if not isinstance(name, str) or not name.strip():
                return []
            source_id = hashlib.sha256(sender.encode("utf-8")).hexdigest()
            return [
                {
                    "id": f"exchange-contact:{source_id}",
                    "content": (
                        "Exchange directory verified the sender identity: "
                        f"display name {name.strip()}; mailbox {sender}."
                    ),
                    "sender": sender,
                    "subject": "Verified Exchange directory identity",
                }
            ]

        return cls(
            **history._adapters,
            exchange_contact=exchange_contact,
        )

    def resolve(self, name: str) -> Adapter:
        if name not in self._ALLOWED or name not in self._adapters:
            raise ValueError(f"adapter not registered: {name}")
        return self._adapters[name]


class WritingEvidenceRetriever:
    """Collect writing-only evidence; its output schema cannot encode routing."""

    def __init__(
        self,
        adapters: EvidenceAdapterRegistry,
        *,
        before_source: BeforeSource | None = None,
    ) -> None:
        self._adapters = adapters
        self._before_source = before_source

    async def retrieve(self, plan: HandoffPlan, request: Mapping[str, Any]) -> EvidencePack:
        items: list[EvidenceItem] = []
        for source in (*plan.required_sources, *plan.optional_sources):
            if self._before_source is not None:
                authorization = self._before_source(source)
                if inspect.isawaitable(authorization):
                    await authorization
            try:
                result = self._adapters.resolve(source)(request, plan.max_items_per_source)
                rows = await result if inspect.isawaitable(result) else result
            except Exception:
                if source in plan.required_sources:
                    raise RuntimeError(f"required_evidence_missing:{source}") from None
                continue
            source_items = tuple(self._to_item(source, row) for row in rows if isinstance(row, Mapping))
            source_items = tuple(item for item in source_items if item is not None)
            if source in plan.required_sources and not source_items:
                raise RuntimeError(f"required_evidence_missing:{source}")
            items.extend(source_items)
        return EvidencePack(profile_id=plan.profile_id, items=tuple(items))

    @staticmethod
    def _to_item(source: EvidenceSource, row: Mapping[str, Any]) -> EvidenceItem | None:
        content = str(
            row.get("content") or row.get("body") or row.get("chunk_text") or ""
        ).strip()
        source_id = str(row.get("id") or row.get("email_id") or "").strip()
        if not content or not source_id:
            return None
        return EvidenceItem(
            source=source,
            source_id=source_id,
            content=content,
            sender=str(row.get("sender") or "")[:512],
            subject=str(row.get("subject") or "")[:1_024],
        )


__all__ = ["EvidenceAdapterRegistry", "EvidenceItem", "EvidencePack", "WritingEvidenceRetriever"]
