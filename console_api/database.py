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
    RouteDecisionDetail,
    RouteEvaluationStep,
    SenderInfo,
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
        "route_evaluation_traces",
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


def _text(value: object, *, limit: int = 512) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result[:limit] or None


_MAILBOX_RE = re.compile(
    r"name=(?P<name>'[^']*'|\"[^\"]*\").*?"
    r"(?:email_address|address)=(?P<address>'[^']*'|\"[^\"]*\")",
    re.IGNORECASE,
)


def _mailbox_info(value: object) -> SenderInfo | None:
    """Normalize Exchange's Mailbox repr and legacy JSON into safe fields."""

    text = _text(value)
    parsed = _json_mapping(value)
    name = _text(parsed.get("name"))
    address = _text(parsed.get("address") or parsed.get("email_address"))
    if text:
        match = _MAILBOX_RE.search(text)
        if match:
            name = name or match.group("name")[1:-1].strip()
            address = address or match.group("address")[1:-1].strip()
        if not address:
            angle = re.search(r"<([^<>@\s]+@[^<>@\s]+)>", text)
            if angle:
                address = angle.group(1)
            elif "@" in text and not any(char in text for char in "{}[]"):
                address = text
    if not name and text and address and "<" in text:
        name = text.split("<", 1)[0].strip() or None
    if not name and not address:
        return None
    return SenderInfo(name=name, address=address)


def _mailbox_address(value: object) -> str | None:
    info = _mailbox_info(value)
    return info.address if info else None


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


def _duration_ms(started_at: object, finished_at: object) -> int | None:
    if not isinstance(started_at, datetime) or not isinstance(finished_at, datetime):
        return None
    return max(0, round((finished_at - started_at).total_seconds() * 1000))


def _route_evaluation_rows(value: object) -> list[dict[str, Any]]:
    rows = value if isinstance(value, list) else _json_list(value)
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _matched_rule_count(value: object) -> int | None:
    decision = _json_mapping(value)
    provenance = _json_mapping(decision.get("provenance"))
    rule_ids = provenance.get("rule_ids")
    if not isinstance(rule_ids, list):
        return None
    return min(32, len([item for item in rule_ids if str(item).strip()]))


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
        query_text: str | None = None,
        route: str | None = None,
        tier: str | None = None,
        requires_human: bool | None = None,
        has_anomaly: bool | None = None,
        received_from: datetime | None = None,
        received_to: datetime | None = None,
    ) -> EmailListResponse:
        offset = (page - 1) * page_size
        predicates = ["inbox.account_id = %s"]
        params: list[object] = [self._settings.account_id]
        if status:
            predicates.append("COALESCE(email.status, inbox.status, log.status) = %s")
            params.append(status)
        if sender:
            predicates.append("log.sender ILIKE %s")
            params.append(f"%{sender}%")
        if query_text:
            predicates.append(
                "(COALESCE(log.subject, '') ILIKE %s OR COALESCE(log.sender, '') ILIKE %s)"
            )
            params.extend((f"%{query_text}%", f"%{query_text}%"))
        if route:
            predicates.append("decision.route = %s")
            params.append(route)
        if tier:
            predicates.append("decision.tier = %s")
            params.append(tier)
        if received_from:
            predicates.append("COALESCE(inbox.received_at, log.received_at) >= %s")
            params.append(received_from)
        if received_to:
            predicates.append("COALESCE(inbox.received_at, log.received_at) < %s")
            params.append(received_to)
        outer_predicates: list[str] = []
        if requires_human is not None:
            outer_predicates.append("requires_human = %s")
            params.append(requires_human)
        if has_anomaly is not None:
            outer_predicates.append("has_anomaly = %s")
            params.append(has_anomaly)
        where = sql.SQL(" AND ").join(sql.SQL(item) for item in predicates)
        outer_where = (
            sql.SQL("WHERE ")
            + sql.SQL(" AND ").join(sql.SQL(item) for item in outer_predicates)
            if outer_predicates
            else sql.SQL("")
        )
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
                    inbox.status AS inbox_status,
                    email.status AS email_status,
                    email.updated_at AS email_updated_at,
                    decision.route,
                    decision.tier,
                    decision.decision_json,
                    log.subject,
                    log.sender,
                    log.status AS log_status,
                    log.received_at AS log_received_at,
                    log.updated_at AS log_updated_at,
                    CASE
                        WHEN COALESCE(email.status, inbox.status, log.status)
                             IN ('waiting_approval', 'manual_review', 'dead_letter',
                                 'send_unknown', 'send_failed')
                        THEN TRUE
                        ELSE FALSE
                    END AS requires_human,
                    CASE
                        WHEN inbox.safe_error_code IS NOT NULL
                          OR email.safe_error_code IS NOT NULL
                          OR COALESCE(email.status, inbox.status, log.status)
                             IN ('dead_letter', 'send_unknown', 'send_failed')
                          OR (
                              COALESCE(email.status, inbox.status, log.status) IN
                              ('sent', 'completed')
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM {executions} AS execution_check
                                  WHERE execution_check.inbox_id = inbox.id
                                    AND execution_check.state = 'completed'
                              )
                          )
                        THEN TRUE
                        ELSE FALSE
                    END AS has_anomaly
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
            {outer_where}
            ORDER BY COALESCE(received_at, log_received_at) DESC NULLS LAST,
                     external_email_id DESC
            LIMIT %s OFFSET %s
            """
        ).format(
            inbox=inbox,
            emails=emails,
            decisions=decisions,
            logs=logs,
            executions=self._relation("handoff_executions"),
            where=where,
            outer_where=outer_where,
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
                sender=_mailbox_info(row.get("sender")),
                received_at=row.get("received_at") or row.get("log_received_at"),
                status=str(
                    row.get("email_status")
                    or row.get("inbox_status")
                    or row.get("log_status")
                    or "unknown"
                ),
                route=_text(row.get("route")),
                tier=_text(row.get("tier")),
                matched_rule_count=_matched_rule_count(row.get("decision_json")),
                requires_human=bool(row.get("requires_human")),
                has_anomaly=bool(row.get("has_anomaly")),
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
        route_traces = self._relation("route_evaluation_traces")
        event_order = _authoritative_event_order()
        async with self._connection() as cursor:
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT
                        inbox.id AS inbox_id,
                        inbox.external_email_id,
                        inbox.source,
                        inbox.status AS inbox_status,
                        inbox.received_at AS inbox_received_at,
                        inbox.processing_started_at AS inbox_processing_started_at,
                        inbox.effect_started_at AS inbox_effect_started_at,
                        inbox.updated_at AS inbox_updated_at,
                        inbox.safe_error_code AS inbox_error_code,
                        email.status AS email_status,
                        email.processing_started_at AS email_processing_started_at,
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
                        intake.decision_digest AS intake_decision_digest,
                        intake.policy_version AS intake_policy_version,
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
            await cursor.execute(
                sql.SQL(
                    """
                    SELECT sequence, tier, outcome, matched_rule_ids,
                           candidate_routes, evidence_refs, confidence,
                           continue_reason, safe_reason, started_at, finished_at,
                           safe_detail_json
                    FROM {route_traces}
                    WHERE inbox_id = %s
                    ORDER BY sequence ASC
                    """
                ).format(route_traces=route_traces),
                (row["inbox_id"],),
            )
            route_trace_rows = await cursor.fetchall()
        return _assemble_trace(
            row,
            revision_rows,
            envelope_rows,
            audit_rows,
            route_trace_rows,
        )

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
    *,
    summary: str | None = None,
    started_at: object = None,
    finished_at: object = None,
    data_quality: str = "ok",
    business_detail: Mapping[str, Any] | None = None,
    input_output: Mapping[str, Any] | None = None,
    technical_detail: Mapping[str, Any] | None = None,
) -> TraceNode:
    allowed_statuses = {
        "pending",
        "active",
        "waiting",
        "human_action",
        "completed",
        "not_triggered",
        "skipped",
        "failed",
        "unknown",
    }
    safe_status = status if status in allowed_statuses else "unknown"
    safe_quality = data_quality if data_quality in {"ok", "missing", "inconsistent"} else "missing"
    safe_business = dict(business_detail or {})
    safe_input_output = dict(input_output or {})
    safe_technical = dict(technical_detail or {})
    compatibility_detail = dict(detail or {})
    return TraceNode(
        id=node_id,
        label=label,
        kind=kind,
        status=safe_status,
        timestamp=timestamp if isinstance(timestamp, datetime) else None,
        summary=_text(summary),
        started_at=started_at if isinstance(started_at, datetime) else None,
        finished_at=finished_at if isinstance(finished_at, datetime) else None,
        duration_ms=_duration_ms(started_at, finished_at),
        data_quality=safe_quality,
        safe_error_code=_text(safe_error_code),
        business_detail=safe_business,
        input_output=safe_input_output,
        technical_detail=safe_technical,
        detail=compatibility_detail,
    )


def _route_step_status(outcome: str) -> str:
    if outcome in {"error", "failed"}:
        return "failed"
    if outcome == "conflict":
        return "human_action"
    if outcome == "skipped":
        return "skipped"
    if outcome in {"partial", "unavailable", "unknown"}:
        return "unknown"
    return "completed"


def _route_step_summary(tier: str, outcome: str, safe_reason: object, continue_reason: object) -> str:
    reason = _text(safe_reason) or _text(continue_reason)
    if reason:
        return reason
    if outcome == "matched":
        return f"{tier.upper()} 已形成候选结果"
    if outcome == "abstain":
        return f"{tier.upper()} 未命中，继续评估"
    if outcome == "conflict":
        return f"{tier.upper()} 发现冲突，需要人工确认"
    if outcome == "skipped":
        return f"{tier.upper()} 未触发"
    if outcome in {"unavailable", "partial"}:
        return f"{tier.upper()} 证据不可用或不完整"
    return f"{tier.upper()} 评估结果未知"


def _route_step_from_row(row: Mapping[str, Any]) -> RouteEvaluationStep:
    tier = _text(row.get("tier")) or "tier1"
    if tier not in {"tier1", "tier2", "tier3"}:
        tier = "tier1"
    outcome = _text(row.get("outcome")) or "unknown"
    detail = _safe_projection(_json_mapping(row.get("safe_detail_json")))
    detail_mapping = detail if isinstance(detail, Mapping) else {}
    matched_rules = _safe_projection(_json_list(row.get("matched_rule_ids")))
    candidates = _safe_projection(_json_list(row.get("candidate_routes")))
    evidence = _safe_projection(_json_list(row.get("evidence_refs")))
    matched_rules = [
        dict(item) if isinstance(item, Mapping) else {"rule_id": _text(item)}
        for item in matched_rules
        if isinstance(item, Mapping) or _text(item)
    ]
    candidates = [
        dict(item) if isinstance(item, Mapping) else {"route": _text(item)}
        for item in candidates
        if isinstance(item, Mapping) or _text(item)
    ]
    evidence = [
        dict(item) if isinstance(item, Mapping) else {"id": _text(item)}
        for item in evidence
        if isinstance(item, Mapping) or _text(item)
    ]
    model_result = detail_mapping.get("model_result")
    if not isinstance(model_result, Mapping):
        model_result = None
    data_quality = (
        _text(detail_mapping.get("data_quality"))
        if isinstance(detail_mapping, Mapping)
        else None
    ) or "ok"
    if outcome in {"partial", "unavailable", "unknown"} and data_quality == "ok":
        data_quality = "missing"
    return RouteEvaluationStep(
        tier=tier,
        status=_route_step_status(outcome),
        summary=_route_step_summary(
            tier,
            outcome,
            row.get("safe_reason"),
            row.get("continue_reason"),
        ),
        continue_reason=_text(row.get("continue_reason")),
        matched_rules=matched_rules if isinstance(matched_rules, list) else [],
        candidates=candidates if isinstance(candidates, list) else [],
        evidence=evidence if isinstance(evidence, list) else [],
        model_result=dict(model_result) if isinstance(model_result, Mapping) else None,
        safe_error_code=_text(detail_mapping.get("safe_error_code"))
        if isinstance(detail_mapping, Mapping)
        else None,
        started_at=row.get("started_at") if isinstance(row.get("started_at"), datetime) else None,
        finished_at=row.get("finished_at") if isinstance(row.get("finished_at"), datetime) else None,
        duration_ms=_duration_ms(row.get("started_at"), row.get("finished_at")),
        data_quality=data_quality if data_quality in {"ok", "missing", "inconsistent"} else "missing",
    )


def _route_decision_detail(
    row: Mapping[str, Any],
    route_trace_rows: list[Mapping[str, Any]],
) -> RouteDecisionDetail:
    decision = _json_mapping(row.get("decision_json"))
    provenance = _json_mapping(decision.get("provenance"))
    final_tier = _text(row.get("tier")) or _text(provenance.get("tier"))
    final_route = _text(row.get("route")) or _text(decision.get("route"))
    by_tier = {
        str(_text(item.get("tier"))): _route_step_from_row(item)
        for item in route_trace_rows
        if _text(item.get("tier")) in {"tier1", "tier2", "tier3"}
    }
    tier_order = ("tier1", "tier2", "tier3")
    final_index = tier_order.index(final_tier) if final_tier in tier_order else None
    steps: list[RouteEvaluationStep] = []
    for index, tier in enumerate(tier_order):
        if tier in by_tier:
            steps.append(by_tier[tier])
            continue
        if final_index is not None and index > final_index:
            steps.append(
                RouteEvaluationStep(
                    tier=tier,
                    status="not_triggered",
                    summary=f"{tier.upper()} 未触发",
                    data_quality="ok",
                )
            )
        else:
            steps.append(
                RouteEvaluationStep(
                    tier=tier,
                    status="unknown",
                    summary=f"{tier.upper()} 历史评估记录缺失",
                    data_quality="missing",
                )
            )
    quality = "ok" if route_trace_rows and all(step.data_quality == "ok" for step in steps) else "missing"
    return RouteDecisionDetail(
        final_route=final_route,
        final_tier=final_tier,
        confidence=provenance.get("confidence"),
        reason_code=_text(decision.get("reason_code")),
        steps=steps,
        decision_digest=_text(row.get("decision_digest")),
        decision_data_quality=quality,
    )


def _assemble_trace(row, revisions, envelopes, audits, route_trace_rows) -> PipelineTrace:
    decision = _json_mapping(row.get("decision_json"))
    provenance = _json_mapping(decision.get("provenance"))
    handoff_state = _text(row.get("handoff_state"))
    execution_state = _text(row.get("execution_state"))
    intake_disposition = _text(row.get("intake_disposition"))
    route = _text(row.get("route"))
    email_status = _text(row.get("email_status"))
    inbox_status = _text(row.get("inbox_status"))
    route_detail = _route_decision_detail(row, route_trace_rows)
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
    has_execution = bool(execution_state)
    route_status = (
        "failed"
        if _text(row.get("decision_outcome")) == "error"
        else "human_action"
        if _text(row.get("decision_outcome")) == "conflict"
        else "completed"
        if route
        else "waiting"
        if inbox_status in {"leased", "pending", "retry_wait"}
        else "unknown"
    )
    ingestion_status = (
        "failed"
        if _text(row.get("inbox_error_code"))
        else "completed"
        if inbox_status in {"completed", "manual_review", "leased", "dead_letter", "failed"}
        else "active"
    )
    handoff_status = (
        "waiting"
        if not handoff_state
        else "failed"
        if handoff_state == "failed"
        else "human_action"
        if handoff_state in {"manual_review", "approval_pending", "rejected"}
        else "active"
        if handoff_state == "executing"
        else "completed"
    )
    approval_status = (
        "completed"
        if approval_detail or handoff_state in {"approved", "rejected"}
        else "human_action"
        if handoff_state in {"manual_review", "approval_pending"}
        else "waiting"
        if revision_detail
        else "not_triggered"
    )
    send_status = (
        "waiting"
        if not has_execution and handoff_state in {"approved", "executing"}
        else "failed"
        if execution_state == "failed"
        else "completed"
        if execution_state == "completed"
        else "active"
        if execution_state == "effect_committed"
        else "not_triggered"
    )
    send_quality = "ok"
    if execution_state == "completed" and not any(
        _text(item.get("action")) in {"send", "forward", "archive", "mark_read"}
        and _text(item.get("result")) in {"completed", "success", "confirmed"}
        for item in audit_detail
    ):
        send_quality = "inconsistent"
    nodes = [
        _node(
            "ingestion",
            "Ingestion",
            "ingestion",
            ingestion_status,
            row.get("inbox_received_at") or row.get("log_received_at"),
            row.get("inbox_error_code"),
            {
                "inbox_id": str(row["inbox_id"]),
                "external_email_id": str(row["external_email_id"]),
                "status": inbox_status,
            },
            summary=(
                "邮件已进入 Durable Inbox"
                if inbox_status
                else "接入事实缺失"
            ),
            started_at=row.get("inbox_received_at"),
            finished_at=row.get("inbox_processing_started_at"),
            data_quality="ok" if inbox_status else "missing",
            business_detail={
                "source": _text(row.get("source")) or "Exchange",
                "received_at": row.get("inbox_received_at") or row.get("log_received_at"),
                "status": inbox_status,
            },
            input_output={
                "subject": _text(row.get("subject"), limit=256),
                "sender": _mailbox_info(row.get("sender")).model_dump(mode="json")
                if _mailbox_info(row.get("sender"))
                else None,
            },
            technical_detail={
                "inbox_id": str(row["inbox_id"]),
                "external_email_id": str(row["external_email_id"]),
                "safe_error_code": _text(row.get("inbox_error_code")),
            },
        ),
        _node(
            "intake_guard",
            "Intake Guard",
            "intake_guard",
            "completed" if intake_disposition else "waiting",
            row.get("intake_created_at"),
            None,
            {"disposition": intake_disposition, "reason_code": _text(row.get("intake_reason_code"))},
            summary=(
                "接入检查已完成"
                if intake_disposition
                else "等待接入检查结果"
            ),
            started_at=row.get("intake_created_at"),
            finished_at=row.get("intake_created_at"),
            data_quality="ok" if intake_disposition else "missing",
            business_detail={
                "disposition": intake_disposition,
                "reason": _text(row.get("intake_reason_code")),
            },
            input_output={
                "allowed_for_automation": intake_disposition == "pass",
            },
            technical_detail={
                "decision_digest": _text(row.get("intake_decision_digest")),
                "policy_version": _text(row.get("intake_policy_version")),
            },
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
            summary=(
                f"最终路由：{route}"
                if route
                else "尚未形成最终路由"
            ),
            started_at=row.get("decision_created_at"),
            finished_at=row.get("decision_created_at"),
            data_quality=route_detail.decision_data_quality,
            business_detail={
                "final_route": route,
                "final_tier": _text(row.get("tier")) or _text(provenance.get("tier")),
                "reason_code": _text(decision.get("reason_code")),
            },
            input_output={
                "route_decision": route_detail.model_dump(mode="json"),
            },
            technical_detail={
                "decision_digest": _text(row.get("decision_digest")),
                "artifact_digest": _text(provenance.get("artifact_digest")),
                "source_version": _text(provenance.get("source_version")),
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
            summary=(
                "处理计划与证据已准备"
                if handoff_state
                else "处理计划尚未生成"
            ),
            started_at=row.get("handoff_created_at"),
            finished_at=row.get("handoff_updated_at"),
            data_quality="ok" if handoff_state or route in {"read_only", "no_action"} else "missing",
            business_detail={
                "state": handoff_state,
                "plan_available": bool(plan),
                "evidence_available": bool(evidence),
            },
            input_output={
                "plan": plan,
                "evidence": evidence,
            },
            technical_detail={
                "plan_digest": _text(row.get("plan_digest")),
                "evidence_digest": _text(row.get("evidence_digest")),
            },
        ),
        _node(
            "draft",
            "Draft Revisions",
            "draft",
            "completed" if revision_detail else "skipped" if route in {"read_only", "no_action"} else "waiting",
            revision_detail[-1].get("edited_at") if revision_detail else None,
            None,
            {"revisions": revision_detail, "current_revision": row.get("payload_revision")},
            summary=(
                "已有草稿修订版本"
                if revision_detail
                else "该路由不需要草稿"
                if route in {"read_only", "no_action"}
                else "草稿尚未生成"
            ),
            started_at=revision_detail[0].get("edited_at") if revision_detail else None,
            finished_at=revision_detail[-1].get("edited_at") if revision_detail else None,
            data_quality="ok" if revision_detail or route in {"read_only", "no_action"} else "missing",
            business_detail={
                "revision_count": len(revision_detail),
                "current_revision": row.get("payload_revision"),
                "content_protected": True,
            },
            input_output={"revisions": revision_detail},
            technical_detail={
                "current_revision": row.get("payload_revision"),
            },
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
            summary=(
                "已完成人工审批"
                if approval_detail
                else "等待人工审批"
                if approval_status == "human_action"
                else "该路由不需要审批"
                if route in {"read_only", "no_action"}
                else "审批尚未开始"
            ),
            started_at=approval_detail[0].get("approved_at") if approval_detail else None,
            finished_at=approval_detail[-1].get("approved_at") if approval_detail else None,
            data_quality="ok" if approval_detail or route in {"read_only", "no_action"} else "missing",
            business_detail={
                "state": handoff_state,
                "approval_count": len(approval_detail),
                "rejection_reason": _text(row.get("rejection_reason")),
            },
            input_output={"approvals": approval_detail},
            technical_detail={"payload_revision": row.get("payload_revision")},
        ),
        _node(
            "send",
            "Send Outcome",
            "send",
            send_status if route not in {"read_only", "no_action"} else "skipped",
            row.get("execution_updated_at"),
            _text(row.get("execution_error_code")),
            {"state": execution_state, "audit_events": audit_detail},
            summary=(
                "执行已完成"
                if execution_state == "completed"
                else "执行失败"
                if execution_state == "failed"
                else "已提交执行，等待结果"
                if execution_state == "effect_committed"
                else "该路由不需要执行"
                if route in {"read_only", "no_action"}
                else "执行尚未开始"
            ),
            started_at=row.get("execution_updated_at") if execution_state else None,
            finished_at=row.get("execution_updated_at") if execution_state == "completed" else None,
            data_quality=send_quality if route not in {"read_only", "no_action"} else "ok",
            business_detail={
                "state": execution_state,
                "effect_count": len(audit_detail),
            },
            input_output={"audit_events": audit_detail},
            technical_detail={
                "safe_error_code": _text(row.get("execution_error_code")),
            },
        ),
    ]
    return PipelineTrace(
        external_email_id=str(row["external_email_id"]),
        inbox_id=str(row["inbox_id"]),
        subject=_text(row.get("subject")),
        sender=_mailbox_info(row.get("sender")),
        current_status=email_status or inbox_status,
        nodes=nodes,
        edges=[
            TraceEdge(source=left, target=right)
            for left, right in zip(
                ("ingestion", "intake_guard", "route_decision", "handoff", "draft", "approval"),
                ("intake_guard", "route_decision", "handoff", "draft", "approval", "send"),
            )
        ],
        route_decision=route_detail,
        updated_at=(
            row.get("email_updated_at")
            or row.get("inbox_updated_at")
            or row.get("execution_updated_at")
        ),
    )


def _trace_from_legacy(row) -> PipelineTrace:
    status = _text(row.get("status")) or "unknown"
    nodes = [
        _node("ingestion", "Ingestion", "ingestion", "completed", row.get("received_at"), detail={"source": "emails_log"}),
        _node("intake_guard", "Intake Guard", "intake_guard", "unknown", summary="历史数据不足"),
        _node("route_decision", "Route Decision", "route_decision", "unknown", summary="历史数据不足", data_quality="missing"),
        _node("handoff", "Handoff Plan / Evidence", "handoff", "unknown", summary="历史数据不足"),
        _node("draft", "Draft Revisions", "draft", "unknown", summary="历史数据不足"),
        _node("approval", "Human Approval", "approval", "unknown", summary="历史数据不足"),
        _node("send", "Send Outcome", "send", "failed" if status.endswith("failed") else "unknown"),
    ]
    return PipelineTrace(
        external_email_id=str(row["id"]),
        subject=_text(row.get("subject")),
        sender=_mailbox_info(row.get("sender")),
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
