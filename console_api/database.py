"""Read-only database adapter for Pipeline Trace projections."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from console_api.models import (
    EmailListItem,
    EmailListResponse,
    PipelineTrace,
    TraceEdge,
    TraceNode,
)
from console_api.settings import ConsoleSettings
from src.router.tier1.dsl import EmailView, UNKNOWN


class ConsoleDatabaseError(RuntimeError):
    """Safe failure raised when the console cannot read its projection."""


_TRACE_TABLES = frozenset(
    {
        "event_inbox",
        "intake_decisions",
        "emails",
        "tier1_decisions",
        "handoff_runs",
        "handoff_executions",
        "execution_payload_revisions",
        "approved_execution_envelopes",
        "audit_events",
        "emails_log",
    }
)


def _authoritative_event_order() -> sql.SQL:
    """Prefer the event that owns business processing over sync projections.

    ``event_inbox`` contains both the original create event and later
    metadata-only update events.  The ``emails.processing_inbox_id`` foreign
    key points at the exact create event used for business processing.  The
    console must follow that ownership relation instead of treating the
    newest inbox event as the pipeline's current trace.
    """

    return sql.SQL(
        """
        CASE
            WHEN email.processing_inbox_id IS NOT NULL THEN 0
            WHEN inbox.change_kind = 'create' THEN 1
            ELSE 2
        END,
        CASE
            WHEN inbox.change_kind = 'create' THEN inbox.received_at
        END ASC NULLS LAST,
        inbox.received_at DESC,
        inbox.id
        """
    )


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _json_list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _mailbox_address(value: object) -> str | None:
    text = _text(value)
    if not text:
        return None
    match = re.search(r"<([^<>@\s]+@[^<>@\s]+)>", text)
    if match:
        return match.group(1)
    if "@" in text and not any(char in text for char in "{}[]"):
        return text
    parsed = _json_mapping(value)
    return _text(parsed.get("address"))


def _addresses_or_unknown(value: object) -> list[str] | object:
    if isinstance(value, Mapping):
        value = value.get("addresses")
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return UNKNOWN


_CONTENT_KEYS = frozenset(
    {
        "body",
        "content",
        "content_ref",
        "draft",
        "draft_content",
        "fixed_draft",
        "html",
        "prompt_modifier",
        "snippet",
        "text",
    }
)
_CONTENT_KEY_PARTS = ("attachment", "body", "content", "draft", "html", "prompt", "snippet", "text")


def _is_content_key(key: object) -> bool:
    normalized = str(key).casefold()
    return normalized in _CONTENT_KEYS or any(part in normalized for part in _CONTENT_KEY_PARTS)


def _safe_projection(value: object, *, depth: int = 0) -> object:
    """Keep trace metadata bounded and exclude message/draft content."""

    if depth > 4:
        return "[bounded]"
    if isinstance(value, Mapping):
        return {
            str(key): _safe_projection(item, depth=depth + 1)
            for key, item in value.items()
            if not _is_content_key(key)
        }
    if isinstance(value, list):
        return [_safe_projection(item, depth=depth + 1) for item in value[:32]]
    if isinstance(value, tuple):
        return [_safe_projection(item, depth=depth + 1) for item in value[:32]]
    if isinstance(value, str):
        return value[:512]
    return value


class ConsoleDatabase:
    """A bounded, read-only adapter with trace assembly behind a small interface."""

    def __init__(self, settings: ConsoleSettings):
        self._settings = settings
        self._schema = settings.schema_name
        self._dsn = settings.resolved_database_url()
        if not self._dsn:
            raise ConsoleDatabaseError("console_database_url_unconfigured")

    @asynccontextmanager
    async def _connection(self):
        try:
            async with await psycopg.AsyncConnection.connect(
                self._dsn,
                autocommit=False,
                row_factory=dict_row,
                prepare_threshold=0,
            ) as connection:
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute("SELECT current_user AS console_user")
                        identity = await cursor.fetchone()
                        if not identity or identity["console_user"] != self._settings.database_role:
                            raise ConsoleDatabaseError("console_database_identity_invalid")
                        await cursor.execute(
                            "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
                            (str(self._settings.statement_timeout_ms),),
                        )
                        await cursor.execute("SET LOCAL default_transaction_read_only = on")
                        yield cursor
        except ConsoleDatabaseError:
            raise
        except Exception as exc:
            raise ConsoleDatabaseError("console_database_read_failed") from exc

    def _relation(self, name: str) -> sql.Identifier:
        if name not in _TRACE_TABLES:
            raise ValueError("console_relation_not_allowed")
        return sql.Identifier(self._schema, name)

    async def list_emails(
        self,
        *,
        page: int,
        page_size: int,
        status: str | None = None,
        sender: str | None = None,
        received_from: datetime | None = None,
        received_to: datetime | None = None,
    ) -> EmailListResponse:
        offset = (page - 1) * page_size
        predicates = ["inbox.account_id = %s"]
        params: list[object] = [self._settings.account_id]
        if status:
            predicates.append("email.status = %s")
            params.append(status)
        if sender:
            predicates.append("log.sender ILIKE %s")
            params.append(f"%{sender}%")
        if received_from:
            predicates.append("COALESCE(inbox.received_at, log.received_at) >= %s")
            params.append(received_from)
        if received_to:
            predicates.append("COALESCE(inbox.received_at, log.received_at) < %s")
            params.append(received_to)
        where = sql.SQL(" AND ").join(sql.SQL(item) for item in predicates)
        inbox = self._relation("event_inbox")
        emails = self._relation("emails")
        decisions = self._relation("tier1_decisions")
        logs = self._relation("emails_log")
        event_order = _authoritative_event_order()
        query = sql.SQL(
            """
            WITH latest AS (
                SELECT DISTINCT ON (inbox.external_email_id)
                    inbox.external_email_id,
                    inbox.id AS inbox_id,
                    inbox.received_at,
                    inbox.account_id,
                    email.status AS email_status,
                    email.updated_at AS email_updated_at,
                    decision.route,
                    decision.tier,
                    log.subject,
                    log.sender,
                    log.received_at AS log_received_at,
                    log.updated_at AS log_updated_at
                FROM {inbox} AS inbox
                LEFT JOIN {emails} AS email
                  ON email.processing_inbox_id = inbox.id
                 AND email.account_id = inbox.account_id
                LEFT JOIN {decisions} AS decision
                  ON decision.inbox_id = inbox.id
                LEFT JOIN {logs} AS log
                  ON log.id = inbox.external_email_id
                WHERE {where}
                ORDER BY inbox.external_email_id, {event_order}
            )
            SELECT *, COUNT(*) OVER () AS total_count
            FROM latest
            ORDER BY COALESCE(received_at, log_received_at) DESC NULLS LAST,
                     external_email_id DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            inbox=inbox,
            emails=emails,
            decisions=decisions,
            logs=logs,
            where=where,
            event_order=event_order,
        )
        async with self._connection() as cursor:
            await cursor.execute(query, (*params, page_size, offset))
            rows = await cursor.fetchall()
        total = int(rows[0]["total_count"]) if rows else 0
        items = [
            EmailListItem(
                external_email_id=str(row["external_email_id"]),
                inbox_id=_text(row.get("inbox_id")),
                subject=_text(row.get("subject")),
                sender=_text(row.get("sender")),
                received_at=row.get("received_at") or row.get("log_received_at"),
                status=str(row.get("email_status") or "unknown"),
                route=_text(row.get("route")),
                tier=_text(row.get("tier")),
                updated_at=row.get("email_updated_at") or row.get("log_updated_at"),
            )
            for row in rows
        ]
        return EmailListResponse(items=items, page=page, page_size=page_size, total=total)

    async def trace(self, external_email_id: str) -> PipelineTrace | None:
        if not external_email_id.strip() or len(external_email_id) > 1024:
            return None
        inbox = self._relation("event_inbox")
        intake = self._relation("intake_decisions")
        emails = self._relation("emails")
        decisions = self._relation("tier1_decisions")
        handoffs = self._relation("handoff_runs")
        executions = self._relation("handoff_executions")
        revisions = self._relation("execution_payload_revisions")
        envelopes = self._relation("approved_execution_envelopes")
        audits = self._relation("audit_events")
        logs = self._relation("emails_log")
        event_order = _authoritative_event_order()
        async with self._connection() as cursor:
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT
                        inbox.id AS inbox_id,
                        inbox.external_email_id,
                        inbox.status AS inbox_status,
                        inbox.received_at AS inbox_received_at,
                        inbox.processing_started_at AS inbox_processing_started_at,
                        inbox.safe_error_code AS inbox_error_code,
                        email.status AS email_status,
                        email.updated_at AS email_updated_at,
                        email.safe_error_code AS email_error_code,
                        log.subject,
                        log.sender,
                        log.rejection_reason,
                        log.received_at AS log_received_at,
                        decision.decision_digest,
                        decision.decision_json,
                        decision.outcome AS decision_outcome,
                        decision.route,
                        decision.tier,
                        decision.created_at AS decision_created_at,
                        intake.disposition AS intake_disposition,
                        intake.reason_code AS intake_reason_code,
                        intake.created_at AS intake_created_at,
                        handoff.plan_json,
                        handoff.plan_digest,
                        handoff.evidence_json,
                        handoff.evidence_digest,
                        handoff.state AS handoff_state,
                        handoff.payload_revision,
                        handoff.created_at AS handoff_created_at,
                        handoff.updated_at AS handoff_updated_at,
                        execution.state AS execution_state,
                        execution.safe_error_code AS execution_error_code,
                        execution.updated_at AS execution_updated_at
                    FROM {inbox} AS inbox
                    LEFT JOIN {emails} AS email
                      ON email.processing_inbox_id = inbox.id
                     AND email.account_id = inbox.account_id
                    LEFT JOIN {logs} AS log
                      ON log.id = inbox.external_email_id
                    LEFT JOIN {decisions} AS decision
                      ON decision.inbox_id = inbox.id
                    LEFT JOIN {intake} AS intake
                      ON intake.inbox_id = inbox.id
                     AND intake.execution_epoch = inbox.execution_epoch
                    LEFT JOIN {handoffs} AS handoff
                      ON handoff.inbox_id = inbox.id
                    LEFT JOIN {executions} AS execution
                      ON execution.inbox_id = inbox.id
                    WHERE inbox.account_id = %s
                      AND inbox.external_email_id = %s
                    ORDER BY {event_order}
                    LIMIT 1
                    """
                ).format(
                    inbox=inbox,
                    intake=intake,
                    emails=emails,
                    logs=logs,
                    decisions=decisions,
                    handoffs=handoffs,
                    executions=executions,
                    event_order=event_order,
                ),
                (self._settings.account_id, external_email_id),
            )
            row = await cursor.fetchone()
            if row is None:
                await cursor.execute(
                    sql.SQL(
                        "SELECT id, subject, sender, status, received_at, updated_at "
                        "FROM {} WHERE id = %s LIMIT 1"
                    ).format(logs),
                    (external_email_id,),
                )
                legacy = await cursor.fetchone()
                if legacy is None:
                    return None
                return _trace_from_legacy(legacy)
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT revision, payload_digest, draft_digest, draft_ref,
                           editor, edited_at, created_at
                    FROM {revisions}
                    WHERE inbox_id = %s
                    ORDER BY revision ASC
                    """
                ).format(revisions=revisions),
                (row["inbox_id"],),
            )
            revision_rows = await cursor.fetchall()
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT payload_revision, payload_digest, approver, approved_at,
                           created_at
                    FROM {envelopes}
                    WHERE inbox_id = %s
                    ORDER BY payload_revision ASC
                    """
                ).format(envelopes=envelopes),
                (row["inbox_id"],),
            )
            envelope_rows = await cursor.fetchall()
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT action, result, actor, reason, safe_metadata, created_at
                    FROM {audits}
                    WHERE account_id = %s
                      AND (
                        email_id = %s
                        OR safe_metadata ->> 'inbox_id' = %s
                      )
                    ORDER BY created_at ASC
                    LIMIT 200
                    """
                ).format(audits=audits),
                (self._settings.account_id, row["inbox_id"], str(row["inbox_id"])),
            )
            audit_rows = await cursor.fetchall()
        return _assemble_trace(row, revision_rows, envelope_rows, audit_rows)

    async def historical_email_view(self, external_email_id: str) -> EmailView | None:
        """Return the bounded matcher projection, never raw attachments or HTML."""

        logs = self._relation("emails_log")
        async with self._connection() as cursor:
            await cursor.execute(
                sql.SQL(
                    "SELECT id, subject, sender, classification FROM {} "
                    "WHERE id = %s LIMIT 1"
                ).format(logs),
                (external_email_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        sender = _mailbox_address(row.get("sender"))
        classification = _json_mapping(row.get("classification"))
        email = _json_mapping(classification.get("email"))
        body = _json_mapping(email.get("body"))
        return EmailView(
            sender_address=sender or "",
            to_addresses=_addresses_or_unknown(email.get("to")),
            cc_addresses=_addresses_or_unknown(email.get("cc")),
            subject=_text(row.get("subject")) or "",
            body_current_text=(
                _text(body.get("current_text"))
                if body.get("current_text") is not None
                else UNKNOWN
            ),
            body_full_text=(
                _text(body.get("full_text"))
                if body.get("full_text") is not None
                else UNKNOWN
            ),
        )


def _node(
    node_id: str,
    label: str,
    kind: str,
    status: str,
    timestamp: object = None,
    safe_error_code: object = None,
    detail: Mapping[str, Any] | None = None,
) -> TraceNode:
    safe_status = status if status in {"pending", "active", "completed", "failed", "skipped", "unknown"} else "unknown"
    return TraceNode(
        id=node_id,
        label=label,
        kind=kind,
        status=safe_status,
        timestamp=timestamp if isinstance(timestamp, datetime) else None,
        safe_error_code=_text(safe_error_code),
        detail=dict(detail or {}),
    )


def _assemble_trace(row, revisions, envelopes, audits) -> PipelineTrace:
    decision = _json_mapping(row.get("decision_json"))
    provenance = _json_mapping(decision.get("provenance"))
    handoff_state = _text(row.get("handoff_state"))
    execution_state = _text(row.get("execution_state"))
    intake_disposition = _text(row.get("intake_disposition"))
    route = _text(row.get("route"))
    email_status = _text(row.get("email_status"))
    inbox_status = _text(row.get("inbox_status"))
    plan = _safe_projection(_json_mapping(row.get("plan_json")))
    evidence = _safe_projection(_json_mapping(row.get("evidence_json")))
    revision_detail = [
        {
            "revision": int(item["revision"]),
            "payload_digest": _text(item.get("payload_digest")),
            "draft_digest": _text(item.get("draft_digest")),
            "draft_ref": _safe_projection(_json_mapping(item.get("draft_ref"))),
            "editor": _text(item.get("editor")),
            "edited_at": item.get("edited_at"),
        }
        for item in revisions
    ]
    approval_detail = [
        {
            "payload_revision": int(item["payload_revision"]),
            "payload_digest": _text(item.get("payload_digest")),
            "approver": _text(item.get("approver")),
            "approved_at": item.get("approved_at"),
        }
        for item in envelopes
    ]
    audit_detail = [
        {
            "action": _text(item.get("action")),
            "result": _text(item.get("result")),
            "actor": _text(item.get("actor")),
            "reason": _text(item.get("reason")),
            "safe_metadata": _safe_projection(_json_mapping(item.get("safe_metadata"))),
            "created_at": item.get("created_at"),
        }
        for item in audits
    ]
    has_send = bool(execution_state or approval_detail)
    route_status = "completed" if route else "pending"
    route_status = "failed" if _text(row.get("decision_outcome")) == "error" else route_status
    handoff_status = (
        "pending"
        if not handoff_state
        else "failed"
        if handoff_state == "failed"
        else "completed"
    )
    approval_status = (
        "completed"
        if approval_detail or handoff_state in {"approved", "rejected"}
        else "pending"
    )
    send_status = (
        "pending"
        if not has_send
        else "failed"
        if execution_state == "failed"
        else "completed"
        if execution_state == "completed"
        else "active"
    )
    nodes = [
        _node(
            "ingestion",
            "Ingestion",
            "ingestion",
            "completed" if inbox_status in {"completed", "manual_review", "leased"} else "active",
            row.get("inbox_received_at") or row.get("log_received_at"),
            row.get("inbox_error_code"),
            {
                "inbox_id": str(row["inbox_id"]),
                "external_email_id": str(row["external_email_id"]),
                "status": inbox_status,
            },
        ),
        _node(
            "intake_guard",
            "Intake Guard",
            "intake_guard",
            "completed" if intake_disposition else "pending",
            row.get("intake_created_at"),
            None,
            {"disposition": intake_disposition, "reason_code": _text(row.get("intake_reason_code"))},
        ),
        _node(
            "route_decision",
            "Route Decision",
            "route_decision",
            route_status,
            row.get("decision_created_at"),
            row.get("email_error_code"),
            {
                "route": route,
                "tier": _text(row.get("tier")) or _text(provenance.get("tier")),
                "rule_ids": [str(value) for value in provenance.get("rule_ids", [])][:32],
                "evidence_ids": [str(value) for value in provenance.get("evidence_ids", [])][:16],
                "confidence": provenance.get("confidence"),
                "decision_digest": _text(row.get("decision_digest")),
            },
        ),
        _node(
            "handoff",
            "Handoff Plan / Evidence",
            "handoff",
            handoff_status,
            row.get("handoff_updated_at") or row.get("handoff_created_at"),
            row.get("email_error_code") if handoff_state == "failed" else None,
            {
                "state": handoff_state,
                "plan_digest": _text(row.get("plan_digest")),
                "evidence_digest": _text(row.get("evidence_digest")),
                "plan": plan,
                "evidence": evidence,
            },
        ),
        _node(
            "draft",
            "Draft Revisions",
            "draft",
            "completed" if revision_detail else "skipped" if route in {"read_only", "no_action"} else "pending",
            revision_detail[-1].get("edited_at") if revision_detail else None,
            None,
            {"revisions": revision_detail, "current_revision": row.get("payload_revision")},
        ),
        _node(
            "approval",
            "Human Approval",
            "approval",
            approval_status if route not in {"read_only", "no_action"} else "skipped",
            approval_detail[-1].get("approved_at") if approval_detail else None,
            None,
            {
                "state": handoff_state,
                "approvals": approval_detail,
                "rejection_reason": _text(row.get("rejection_reason")),
            },
        ),
        _node(
            "send",
            "Send Outcome",
            "send",
            send_status if route not in {"read_only", "no_action"} else "skipped",
            row.get("execution_updated_at"),
            _text(row.get("execution_error_code")),
            {"state": execution_state, "audit_events": audit_detail},
        ),
    ]
    return PipelineTrace(
        external_email_id=str(row["external_email_id"]),
        inbox_id=str(row["inbox_id"]),
        subject=_text(row.get("subject")),
        sender=_text(row.get("sender")),
        current_status=email_status or inbox_status,
        nodes=nodes,
        edges=[
            TraceEdge(source=left, target=right)
            for left, right in zip(
                ("ingestion", "intake_guard", "route_decision", "handoff", "draft", "approval"),
                ("intake_guard", "route_decision", "handoff", "draft", "approval", "send"),
            )
        ],
    )


def _trace_from_legacy(row) -> PipelineTrace:
    status = _text(row.get("status")) or "unknown"
    nodes = [
        _node("ingestion", "Ingestion", "ingestion", "completed", row.get("received_at"), detail={"source": "emails_log"}),
        _node("intake_guard", "Intake Guard", "intake_guard", "unknown"),
        _node("route_decision", "Route Decision", "route_decision", "unknown"),
        _node("handoff", "Handoff Plan / Evidence", "handoff", "unknown"),
        _node("draft", "Draft Revisions", "draft", "unknown"),
        _node("approval", "Human Approval", "approval", "unknown"),
        _node("send", "Send Outcome", "send", "failed" if status.endswith("failed") else "unknown"),
    ]
    return PipelineTrace(
        external_email_id=str(row["id"]),
        subject=_text(row.get("subject")),
        sender=_text(row.get("sender")),
        current_status=status,
        nodes=nodes,
        edges=[
            TraceEdge(source=left, target=right)
            for left, right in zip(
                ("ingestion", "intake_guard", "route_decision", "handoff", "draft", "approval"),
                ("intake_guard", "route_decision", "handoff", "draft", "approval", "send"),
            )
        ],
    )
