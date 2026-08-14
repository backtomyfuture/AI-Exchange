from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

EvidenceSource = Literal["mail_thread", "semantic_history", "exchange_contact"]
WriterMode = Literal["llm", "fixed"]


class HandoffDisposition(StrEnum):
    READY = "ready"
    MANUAL_REVIEW = "manual_review"


class CanonicalDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def canonical_digest(self) -> str:
        encoded = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, ensure_ascii=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


class HandoffPlan(CanonicalDTO):
    schema_version: Literal[1] = 1
    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_]*_v[1-9][0-9]*$")
    required_sources: tuple[EvidenceSource, ...] = ()
    optional_sources: tuple[EvidenceSource, ...] = ()
    max_items_per_source: int = Field(default=5, ge=1, le=20)
    writer_mode: WriterMode = "llm"
    prompt_modifier: str | None = Field(default=None, max_length=2_048)
    fixed_draft: str | None = Field(default=None, max_length=16_384)

    @model_validator(mode="after")
    def _sources_are_disjoint(self) -> "HandoffPlan":
        if set(self.required_sources) & set(self.optional_sources):
            raise ValueError("evidence source cannot be both required and optional")
        if self.writer_mode == "fixed" and not self.fixed_draft:
            raise ValueError("fixed writer requires fixed_draft")
        if self.writer_mode != "fixed" and self.fixed_draft is not None:
            raise ValueError("only fixed writer may contain fixed_draft")
        return self


class HandoffProfile(CanonicalDTO):
    schema_version: Literal[1] = 1
    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_]*_v[1-9][0-9]*$")
    required_sources: tuple[EvidenceSource, ...] = ()
    optional_sources: tuple[EvidenceSource, ...] = ()
    writer_mode: WriterMode = "llm"
    prompt_modifier: str | None = Field(default=None, max_length=2_048)
    fixed_draft: str | None = Field(default=None, max_length=16_384)
    readonly: Literal[True] = True

    def build_plan(self) -> HandoffPlan:
        return HandoffPlan(
            profile_id=self.profile_id,
            required_sources=self.required_sources,
            optional_sources=self.optional_sources,
            writer_mode=self.writer_mode,
            prompt_modifier=self.prompt_modifier,
            fixed_draft=self.fixed_draft,
        )
