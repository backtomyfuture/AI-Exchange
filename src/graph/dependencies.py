from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.storage import ContentStore


class DraftStore(Protocol):
    async def save_draft(self, email_id: str, content: str) -> str:
        raise NotImplementedError

    async def load_draft(self, draft_id: str) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class GraphDependencies:
    content_store: ContentStore
    drafts: DraftStore
