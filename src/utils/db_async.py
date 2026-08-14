"""
Async Database Manager - 使用 psycopg (v3) 异步版本
提供与同步 DatabaseManager 兼容的接口，但使用异步操作
"""
import json
import logging
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from src.domain.email_state import InitialEmailWriteResult
from src.domain.errors import DatabaseOperationError
from src.graph.state_factory import content_ref_from_json, content_ref_to_json
from src.handoff.evidence import EvidencePack
from src.handoff.models import HandoffPlan
from src.ingestion.processing import ProcessingEffectScope
from src.router.decision import RouteDecision
from src.router.observability import validate_route_evaluation
from src.safety.execution_gate import ApprovedExecutionEnvelope
from src.storage import ContentRef

logger = logging.getLogger(__name__)


class ApprovalCommitStatus(StrEnum):
    CREATED = "created"
    ALREADY_APPROVED_EXACT = "already_approved_exact"


@dataclass(frozen=True, slots=True)
class ApprovalCommitResult:
    status: ApprovalCommitStatus
    envelope: dict[str, object]
    envelope_digest: str


def _digest_json(value: object) -> str:
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()

CAS_ONLY_EMAIL_STATUSES = frozenset(
    {"approved", "sending", "send_unknown", "sent"}
)
EMAIL_STATUS_CAS_TRANSITIONS = frozenset(
    {
        ("waiting_approval", "approved"),
        ("waiting_approval", "rejected"),
        ("waiting_approval", "saving_draft"),
        ("approved", "sending"),
        ("sending", "sent"),
        ("saving_draft", "draft_saved"),
    }
)
HANDOFF_TRANSITIONS = frozenset(
    {
        ("planned", "effect_committed"),
        ("planned", "completed"),
        ("planned", "failed"),
        ("effect_committed", "completed"),
        ("effect_committed", "failed"),
    }
)


def normalize_timestamp_input(value: Any) -> Any:
    """Normalize timestamp input before DB write."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


class AsyncDatabaseManager:
    """
    Manages PostgreSQL async connection and operations for email tracking.
    """

    def __init__(self, settings):
        self._dsn = settings.database_url
        self._pool: Optional[AsyncConnectionPool] = None

    @property
    def dsn(self) -> str:
        return self._dsn

    async def open(self):
        """Open the connection pool without mutating database schema."""
        try:
            if self._pool is None:
                self._pool = AsyncConnectionPool(
                    conninfo=self._dsn,
                    min_size=2,
                    max_size=10,
                    open=False,
                    kwargs={"autocommit": True, "row_factory": dict_row},
                )
                await self._pool.open()
                logger.info("AsyncDatabaseManager connection pool opened (min=2, max=10).")
        except psycopg.OperationalError as exc:
            logger.error(
                "Failed to open PostgreSQL connection pool: error_type=%s",
                type(exc).__name__,
            )
            raise

    @asynccontextmanager
    async def get_connection(self):
        if self._pool is None:
            await self.open()
        async with self._pool.connection() as conn:
            yield conn

    async def log_initial_email(
        self, email_data: Dict[str, Any]
    ) -> InitialEmailWriteResult:
        """
        Record a new email in the database.
        Return a typed result that distinguishes creation from duplication.
        """
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                    INSERT INTO emails_log (id, subject, sender, received_at, status)
                    VALUES (%s, %s, %s, %s, 'pending')
                    ON CONFLICT (id) DO NOTHING
                """, (
                    email_data.get("id"),
                    email_data.get("subject"),
                    str(email_data.get("sender")),
                    normalize_timestamp_input(email_data.get("received_at"))
                ))
                    if cur.rowcount > 0:
                        return InitialEmailWriteResult.CREATED
                    return InitialEmailWriteResult.DUPLICATE
        except psycopg.Error as exc:
            logger.error(
                "Failed to log initial email: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="log_initial_email",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="initial email persistence failed",
            ) from None

    async def persist_route_decision(
        self,
        *,
        scope: ProcessingEffectScope,
        decision_raw: object,
    ) -> RouteDecision:
        """Create one immutable decision and its mutable handoff exactly once."""
        if not isinstance(scope, ProcessingEffectScope):
            raise ValueError("scope must be a ProcessingEffectScope")
        try:
            decision = RouteDecision.model_validate(decision_raw)
        except Exception:
            raise DatabaseOperationError(
                operation="persist_route_decision",
                retryable=False,
                message="route decision is invalid",
            ) from None
        payload = decision.model_dump(mode="json")
        digest = decision.canonical_digest()
        try:
            async with self.get_connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        if await self._route_for_attempt(cur, scope, lock=True) is None:
                            raise DatabaseOperationError(
                                operation="persist_route_decision",
                                retryable=False,
                                message="route authority is stale",
                            )
                        await cur.execute(
                            """
                            INSERT INTO tier1_decisions (
                                inbox_id, account_id, external_email_id,
                                decision_digest, decision_json, outcome, route,
                                tier, artifact_digest
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (inbox_id) DO NOTHING
                            """,
                            (
                                scope.inbox_id,
                                scope.account_id,
                                scope.external_email_id,
                                digest,
                                Jsonb(payload),
                                decision.outcome.value,
                                decision.route.value if decision.route else None,
                                decision.provenance.tier.value,
                                decision.provenance.artifact_digest,
                            ),
                        )
                        await cur.execute(
                            """
                            SELECT decision_digest, decision_json
                            FROM tier1_decisions
                            WHERE inbox_id = %s
                            """,
                            (scope.inbox_id,),
                        )
                        row = await cur.fetchone()
                        existing_json = row.get("decision_json") if row else None
                        if isinstance(existing_json, str):
                            existing_json = json.loads(existing_json)
                        if (
                            row is None
                            or row.get("decision_digest") != digest
                            or existing_json != payload
                        ):
                            raise DatabaseOperationError(
                                operation="persist_route_decision",
                                retryable=False,
                                message="immutable route decision conflict",
                            )
                        await cur.execute(
                            """
                            INSERT INTO handoff_executions (
                                inbox_id, decision_digest, state
                            ) VALUES (%s, %s, 'planned')
                            ON CONFLICT (inbox_id) DO NOTHING
                            """,
                            (scope.inbox_id, digest),
                        )
                        await cur.execute(
                            """
                            SELECT decision_digest FROM handoff_executions
                            WHERE inbox_id = %s
                            """,
                            (scope.inbox_id,),
                        )
                        handoff = await cur.fetchone()
                        if handoff is None or handoff.get("decision_digest") != digest:
                            raise DatabaseOperationError(
                                operation="persist_route_decision",
                                retryable=False,
                                message="handoff decision conflict",
                            )
        except DatabaseOperationError:
            raise
        except (psycopg.Error, ValueError, TypeError):
            raise DatabaseOperationError(
                operation="persist_route_decision",
                retryable=False,
                message="route decision persistence failed",
            ) from None
        return decision

    async def persist_route_evaluation_trace(
        self,
        *,
        scope: ProcessingEffectScope,
        sequence: int,
        evaluation: object,
    ) -> None:
        """Append one bounded, non-authoritative routing observation.

        The write is fenced to the current inbox lease, but its failure never
        changes the canonical route decision.  The table has no update path.
        """

        if not isinstance(scope, ProcessingEffectScope):
            raise ValueError("scope must be a ProcessingEffectScope")
        try:
            trace = validate_route_evaluation(
                evaluation,
                inbox_id=scope.inbox_id,
                sequence=sequence,
            )
        except Exception:
            raise DatabaseOperationError(
                operation="persist_route_evaluation_trace",
                retryable=False,
                message="route evaluation projection is invalid",
            ) from None
        try:
            async with self.get_connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            SELECT id
                            FROM event_inbox
                            WHERE id = %s
                              AND account_id = %s
                              AND external_email_id = %s
                              AND generation = %s
                              AND fencing_token = %s
                              AND execution_epoch = %s
                              AND authority_epoch = %s
                              AND capability_hash = %s
                              AND lease_session_id = %s
                              AND lease_owner = %s
                              AND status = 'leased'
                              AND lease_until > statement_timestamp()
                            LIMIT 1
                            """,
                            (
                                scope.inbox_id,
                                scope.account_id,
                                scope.external_email_id,
                                scope.generation,
                                scope.fencing_token,
                                scope.execution_epoch,
                                scope.authority_epoch,
                                scope.capability_hash,
                                scope.lease_session_id,
                                scope.lease_owner,
                            ),
                        )
                        if await cur.fetchone() is None:
                            raise DatabaseOperationError(
                                operation="persist_route_evaluation_trace",
                                retryable=False,
                                message="route evaluation authority is stale",
                            )
                        await cur.execute(
                            """
                            INSERT INTO route_evaluation_traces (
                                inbox_id, sequence, tier, outcome,
                                matched_rule_ids, candidate_routes, evidence_refs,
                                confidence, continue_reason, safe_reason,
                                started_at, finished_at, safe_detail_json
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s
                            )
                            ON CONFLICT (inbox_id, sequence) DO NOTHING
                            """,
                            (
                                trace.inbox_id,
                                trace.sequence,
                                trace.tier,
                                trace.outcome,
                                Jsonb(trace.matched_rule_ids),
                                Jsonb(trace.candidate_routes),
                                Jsonb(trace.evidence_refs),
                                trace.confidence,
                                trace.continue_reason,
                                trace.safe_reason,
                                trace.started_at,
                                trace.finished_at,
                                Jsonb(trace.safe_detail_json),
                            ),
                        )
                        await cur.execute(
                            """
                            SELECT tier, outcome, safe_detail_json
                            FROM route_evaluation_traces
                            WHERE inbox_id = %s AND sequence = %s
                            """,
                            (trace.inbox_id, trace.sequence),
                        )
                        existing = await cur.fetchone()
                        existing_detail = existing.get("safe_detail_json") if existing else None
                        if isinstance(existing_detail, str):
                            try:
                                existing_detail = json.loads(existing_detail)
                            except (TypeError, ValueError):
                                existing_detail = None
                        if (
                            existing is None
                            or existing.get("tier") != trace.tier
                            or existing.get("outcome") != trace.outcome
                            or existing_detail != trace.safe_detail_json
                        ):
                            raise DatabaseOperationError(
                                operation="persist_route_evaluation_trace",
                                retryable=False,
                                message="route evaluation projection conflict",
                            )
        except DatabaseOperationError:
            raise
        except (psycopg.Error, ValueError, TypeError):
            raise DatabaseOperationError(
                operation="persist_route_evaluation_trace",
                retryable=False,
                message="route evaluation projection persistence failed",
            ) from None

    async def get_route_decision_for_attempt(
        self,
        *,
        scope: ProcessingEffectScope,
    ) -> RouteDecision | None:
        """Recover a route only while the exact processing authority is live."""
        if not isinstance(scope, ProcessingEffectScope):
            raise ValueError("scope must be a ProcessingEffectScope")
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    row = await self._route_for_attempt(cur, scope, lock=True)
        if row is None:
            raise DatabaseOperationError(
                operation="get_route_decision_for_attempt",
                retryable=False,
                message="route authority is stale",
            )
        raw = row.get("decision_json")
        return RouteDecision.model_validate(raw) if raw is not None else None

    async def get_route_decision_diagnostic(
        self, *, inbox_id: str | None = None, external_email_id: str | None = None
    ) -> RouteDecision | None:
        """Unfenced read for diagnostics; processing code must not call this."""
        if (inbox_id is None) == (external_email_id is None):
            raise ValueError("provide_exactly_one_route_identity")
        column, value = ("inbox_id", inbox_id) if inbox_id else (
            "external_email_id", external_email_id
        )
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT decision_json FROM tier1_decisions WHERE {column} = %s", (value,)
                )
                row = await cur.fetchone()
        return RouteDecision.model_validate(row["decision_json"]) if row else None

    @staticmethod
    async def _route_for_attempt(
        cur: object,
        scope: ProcessingEffectScope,
        *,
        lock: bool,
    ) -> dict[str, object] | None:
        lock_clause = "FOR UPDATE OF i, e" if lock else ""
        await cur.execute(
            f"""
            SELECT d.decision_json
            FROM event_inbox i
            JOIN emails e ON e.processing_inbox_id = i.id
            JOIN pipeline_runtime_instances runtime
              ON runtime.session_id = i.lease_session_id
             AND runtime.account_id = i.account_id
             AND runtime.generation = i.generation
             AND runtime.fencing_token = i.fencing_token
             AND runtime.authority_epoch = i.authority_epoch
             AND runtime.capability_hash = i.capability_hash
            JOIN pipeline_ownership ownership
              ON ownership.account_id = i.account_id
             AND ownership.pipeline_name = i.pipeline_name
             AND ownership.generation = i.generation
             AND ownership.fencing_token = i.fencing_token
            LEFT JOIN tier1_decisions d ON d.inbox_id = i.id
            WHERE i.id = %s
              AND i.account_id = %s
              AND i.external_email_id = %s
              AND i.status = 'leased'
              AND i.generation = %s
              AND i.fencing_token = %s
              AND i.execution_epoch = %s
              AND i.authority_epoch = %s
              AND i.capability_hash = %s
              AND i.lease_session_id = %s
              AND i.lease_owner = %s
              AND i.lease_until > statement_timestamp()
              AND ownership.state IN ('current_ingress','quiescing','draining')
              AND runtime.instance_id = i.lease_owner
              AND runtime.workload = 'web'
              AND runtime.lifecycle = 'active'
              AND runtime.lease_until > statement_timestamp()
              AND e.id = %s
              AND e.account_id = i.account_id
              AND e.external_email_id = i.external_email_id
              AND e.status = 'processing'
              AND e.version = %s
              AND e.owner_generation = i.generation
              AND e.owner_fencing_token = i.fencing_token
              AND e.owner_authority_epoch = i.authority_epoch
              AND e.owner_capability_hash = i.capability_hash
              AND e.processing_execution_epoch = i.execution_epoch
            {lock_clause}
            """,
            (
                scope.inbox_id,
                scope.account_id,
                scope.external_email_id,
                scope.generation,
                scope.fencing_token,
                scope.execution_epoch,
                scope.authority_epoch,
                scope.capability_hash,
                scope.lease_session_id,
                scope.lease_owner,
                scope.email_id,
                scope.expected_email_version,
            ),
        )
        return await cur.fetchone()

    async def persist_handoff_plan(
        self, *, inbox_id: str, decision_digest: str, plan: object
    ) -> dict[str, object]:
        typed_plan = HandoffPlan.model_validate(plan)
        plan = typed_plan.model_dump(mode="json")
        digest = typed_plan.canonical_digest()
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """INSERT INTO handoff_runs (inbox_id,decision_digest,plan_json,plan_digest)
                    VALUES (%s,%s,%s,%s) ON CONFLICT (inbox_id) DO NOTHING""",
                    (inbox_id, decision_digest, Jsonb(plan), digest),
                )
                await cur.execute("SELECT * FROM handoff_runs WHERE inbox_id=%s", (inbox_id,))
                row = await cur.fetchone()
        if not row or row["decision_digest"] != decision_digest or row["plan_digest"] != digest:
            raise DatabaseOperationError(operation="persist_handoff_plan", retryable=False, message="immutable handoff conflict")
        return row

    async def persist_handoff_evidence(
        self, *, inbox_id: str, expected_version: int, evidence: object
    ) -> bool:
        typed_evidence = EvidencePack.model_validate(evidence)
        evidence = typed_evidence.model_dump(mode="json")
        digest = typed_evidence.canonical_digest()
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """UPDATE handoff_runs SET evidence_json=%s,evidence_digest=%s,
                    state='evidence_ready',version=version+1,updated_at=CURRENT_TIMESTAMP
                    WHERE inbox_id=%s AND version=%s AND state='planned'""",
                    (Jsonb(evidence), digest, inbox_id, expected_version),
                )
                return cur.rowcount == 1

    async def get_handoff_run(self, inbox_id: str) -> dict[str, object] | None:
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM handoff_runs WHERE inbox_id=%s", (inbox_id,))
                return await cur.fetchone()

    async def transition_handoff_manual_review(
        self, *, inbox_id: str, expected_version: int
    ) -> bool:
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """UPDATE handoff_runs SET state='manual_review',version=version+1,
                    updated_at=CURRENT_TIMESTAMP WHERE inbox_id=%s AND version=%s
                    AND state IN ('planned','evidence_ready','approval_pending','approved',
                                  'draft_saving')""",
                    (inbox_id, expected_version),
                )
                return cur.rowcount == 1

    async def create_payload_revision(
        self,
        *,
        inbox_id: str,
        expected_version: int,
        expected_payload_revision: int | None,
        expected_payload_digest: str | None,
        payload: dict[str, object],
    ) -> int:
        """Append a frozen editable payload and invalidate any older approval."""
        required = {"decision_digest", "plan_digest", "evidence_digest", "draft_digest", "editor", "edited_at"}
        if not required <= payload.keys():
            raise ValueError("incomplete_execution_payload")
        if (expected_payload_revision is None) != (expected_payload_digest is None):
            raise ValueError("incomplete_expected_payload_binding")
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT payload_revision, decision_digest, plan_digest,
                        evidence_digest, state
                        FROM handoff_runs WHERE inbox_id=%s AND version=%s
                        AND state IN ('evidence_ready','approval_pending') FOR UPDATE""",
                        (inbox_id, expected_version),
                    )
                    run = await cur.fetchone()
                    if not run:
                        raise DatabaseOperationError(operation="create_payload_revision", retryable=False, message="handoff CAS conflict")
                    if any(
                        payload[key] != run[key]
                        for key in ("decision_digest", "plan_digest", "evidence_digest")
                    ):
                        raise DatabaseOperationError(
                            operation="create_payload_revision",
                            retryable=False,
                            message="payload authority digest conflict",
                        )
                    canonical_payload = {
                        key: payload.get(key)
                        for key in (
                            "decision_digest", "plan_digest", "evidence_digest",
                            "draft_digest", "draft_content", "draft_ref", "to", "cc",
                            "attachment_refs", "attachment_digests",
                            "external_recipient_acknowledged",
                        )
                    }
                    payload_digest = _digest_json(canonical_payload)
                    if expected_payload_revision is None:
                        binding_matches = (
                            run["state"] == "evidence_ready"
                            and run["payload_revision"] is None
                        )
                    else:
                        binding_matches = (
                            run["state"] == "approval_pending"
                            and run["payload_revision"] == expected_payload_revision
                        )
                        if binding_matches:
                            await cur.execute(
                                """SELECT 1 FROM execution_payload_revisions
                                WHERE inbox_id=%s AND revision=%s
                                AND payload_digest=%s""",
                                (
                                    inbox_id,
                                    expected_payload_revision,
                                    expected_payload_digest,
                                ),
                            )
                            binding_matches = await cur.fetchone() is not None
                        if (
                            not binding_matches
                            and run["state"] == "approval_pending"
                            and run["payload_revision"]
                            == expected_payload_revision + 1
                        ):
                            # The durable append may have committed before a
                            # best-effort draft/checkpoint/card projection
                            # failed. An exact replay may recover that one
                            # successor, but must never create N+2 from the
                            # stale N callback.
                            await cur.execute(
                                """SELECT 1 FROM execution_payload_revisions
                                WHERE inbox_id=%s AND revision=%s
                                AND payload_digest=%s""",
                                (
                                    inbox_id,
                                    expected_payload_revision,
                                    expected_payload_digest,
                                ),
                            )
                            predecessor_matches = await cur.fetchone() is not None
                            await cur.execute(
                                """SELECT payload_digest,decision_digest,
                                plan_digest,evidence_digest,draft_digest,draft_content,
                                draft_ref,to_recipients,cc_recipients,attachment_refs,
                                attachment_digests,external_recipient_acknowledged
                                FROM execution_payload_revisions
                                WHERE inbox_id=%s AND revision=%s""",
                                (inbox_id, run["payload_revision"]),
                            )
                            successor = await cur.fetchone()
                            if predecessor_matches and successor:
                                successor_payload = {
                                    "decision_digest": successor["decision_digest"],
                                    "plan_digest": successor["plan_digest"],
                                    "evidence_digest": successor["evidence_digest"],
                                    "draft_digest": successor["draft_digest"],
                                    "draft_content": successor["draft_content"],
                                    "draft_ref": successor["draft_ref"],
                                    "to": successor["to_recipients"],
                                    "cc": successor["cc_recipients"],
                                    "attachment_refs": successor["attachment_refs"],
                                    "attachment_digests": successor[
                                        "attachment_digests"
                                    ],
                                    "external_recipient_acknowledged": successor[
                                        "external_recipient_acknowledged"
                                    ],
                                }
                                successor_digest = _digest_json(successor_payload)
                                if (
                                    successor_digest == payload_digest
                                    and successor_digest
                                    == successor["payload_digest"]
                                ):
                                    return int(run["payload_revision"])
                    if not binding_matches:
                        raise DatabaseOperationError(
                            operation="create_payload_revision",
                            retryable=False,
                            message="stale payload edit",
                        )
                    revision = (run["payload_revision"] or 0) + 1
                    await cur.execute(
                        """INSERT INTO execution_payload_revisions
                        (inbox_id,revision,payload_digest,decision_digest,plan_digest,evidence_digest,draft_digest,
                         draft_content,draft_ref,to_recipients,cc_recipients,attachment_refs,
                         attachment_digests,external_recipient_acknowledged,editor,edited_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (inbox_id, revision, payload_digest, payload["decision_digest"], payload["plan_digest"],
                         payload["evidence_digest"], payload["draft_digest"], payload.get("draft_content"),
                         Jsonb(payload.get("draft_ref")), Jsonb(payload.get("to", [])),
                         Jsonb(payload.get("cc", [])), Jsonb(payload.get("attachment_refs", [])),
                         Jsonb(payload.get("attachment_digests", [])),
                         bool(payload.get("external_recipient_acknowledged", False)),
                         payload["editor"], payload["edited_at"]),
                    )
                    await cur.execute(
                        """UPDATE handoff_runs SET payload_revision=%s,state='approval_pending',
                        version=version+1,updated_at=CURRENT_TIMESTAMP
                        WHERE inbox_id=%s AND version=%s""", (revision, inbox_id, expected_version)
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(operation="create_payload_revision", retryable=False, message="handoff CAS conflict")
        return revision

    async def is_current_payload_revision(
        self,
        *,
        inbox_id: str,
        revision: int,
        payload_digest: str,
    ) -> bool:
        """Validate a card binding against the current editable revision."""
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """SELECT 1 FROM handoff_runs h
                    JOIN execution_payload_revisions p
                      ON p.inbox_id=h.inbox_id
                     AND p.revision=h.payload_revision
                    WHERE h.inbox_id=%s AND h.state='approval_pending'
                      AND h.payload_revision=%s AND p.payload_digest=%s""",
                    (inbox_id, revision, payload_digest),
                )
                return await cur.fetchone() is not None

    async def get_payload_revision_binding(
        self, *, inbox_id: str, revision: int
    ) -> dict[str, object] | None:
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT payload_digest FROM execution_payload_revisions WHERE inbox_id=%s AND revision=%s",
                    (inbox_id, revision),
                )
                row = await cur.fetchone()
        return {"inbox_id": inbox_id, "payload_revision": revision,
                "payload_digest": row["payload_digest"]} if row else None

    async def get_payload_revision_snapshot(
        self, *, inbox_id: str, revision: int
    ) -> dict[str, object] | None:
        """Read one immutable payload revision for projection or a claimed effect."""
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """SELECT p.*, d.external_email_id, l.subject
                    FROM execution_payload_revisions p
                    JOIN tier1_decisions d ON d.inbox_id=p.inbox_id
                    JOIN emails_log l ON l.id=d.external_email_id
                    WHERE p.inbox_id=%s AND p.revision=%s""",
                    (inbox_id, revision),
                )
                return await cur.fetchone()

    async def get_current_payload_revision_snapshot(
        self, *, inbox_id: str
    ) -> dict[str, object] | None:
        """Read the current editable payload and its binding in one statement."""
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """SELECT p.*, d.external_email_id, l.subject
                    FROM handoff_runs h
                    JOIN execution_payload_revisions p
                      ON p.inbox_id=h.inbox_id AND p.revision=h.payload_revision
                    JOIN tier1_decisions d ON d.inbox_id=h.inbox_id
                    JOIN emails_log l ON l.id=d.external_email_id
                    WHERE h.inbox_id=%s AND h.state='approval_pending'""",
                    (inbox_id,),
                )
                return await cur.fetchone()

    @staticmethod
    def _validate_existing_approved_envelope(
        existing: dict[str, object],
        *,
        inbox_id: str,
        revision: int,
        payload_digest: str,
        approver: str,
    ) -> ApprovalCommitResult:
        """Validate all immutable truth before treating a callback as a replay."""
        if int(existing.get("envelope_count") or 0) != 1:
            raise DatabaseOperationError(
                operation="approve_payload_revision",
                retryable=False,
                message="approved envelope cardinality conflict",
            )
        raw_envelope = existing.get("envelope_json")
        recorded_digest = str(existing.get("envelope_digest") or "")
        try:
            envelope = ApprovedExecutionEnvelope.model_validate(raw_envelope)
            route_decision = RouteDecision.model_validate(existing.get("decision_json"))
        except Exception:
            raise DatabaseOperationError(
                operation="approve_payload_revision",
                retryable=False,
                message="approved envelope validation conflict",
            ) from None
        expected_pairs = {
            "payload_revision": revision,
            "envelope_payload_digest": payload_digest,
            "payload_payload_digest": payload_digest,
            "approver": approver,
            "decision_digest": envelope.decision_digest,
            "plan_digest": envelope.plan_digest,
            "evidence_digest": envelope.evidence_digest,
            "draft_digest": envelope.draft_digest,
            "payload_decision_digest": envelope.decision_digest,
            "payload_plan_digest": envelope.plan_digest,
            "payload_evidence_digest": envelope.evidence_digest,
            "payload_draft_digest": envelope.draft_digest,
        }
        if (
            envelope.inbox_id != inbox_id
            or envelope.payload_revision != revision
            or envelope.approver != approver
            or envelope.canonical_digest() != recorded_digest
            or str(existing.get("inbox_id")) != inbox_id
            or envelope.account_id != existing.get("account_id")
            or envelope.email_id != existing.get("external_email_id")
            or envelope.route_decision != route_decision
            or any(existing.get(key) != value for key, value in expected_pairs.items())
        ):
            raise DatabaseOperationError(
                operation="approve_payload_revision",
                retryable=False,
                message="approved envelope replay conflict",
            )
        return ApprovalCommitResult(
            status=ApprovalCommitStatus.ALREADY_APPROVED_EXACT,
            envelope=envelope.model_dump(mode="json"),
            envelope_digest=recorded_digest,
        )

    async def approve_payload_revision(
        self, *, inbox_id: str, revision: int, expected_version: int,
        payload_digest: str, approver: str, approved_at: object
    ) -> ApprovalCommitResult:
        """Freeze one envelope only when approving the current payload revision."""
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT e.envelope_json,e.envelope_digest,e.inbox_id,
                               e.payload_revision,
                               e.payload_digest AS envelope_payload_digest,
                               e.approver,e.decision_digest,
                               e.plan_digest,e.evidence_digest,e.draft_digest,
                               p.payload_digest AS payload_payload_digest,
                               p.decision_digest AS payload_decision_digest,
                               p.plan_digest AS payload_plan_digest,
                               p.evidence_digest AS payload_evidence_digest,
                               p.draft_digest AS payload_draft_digest,
                               d.decision_json,d.external_email_id,d.account_id,
                               COUNT(*) OVER () AS envelope_count
                        FROM approved_execution_envelopes e
                        JOIN execution_payload_revisions p
                          ON p.inbox_id=e.inbox_id
                         AND p.revision=e.payload_revision
                        JOIN handoff_runs h ON h.inbox_id=e.inbox_id
                        JOIN tier1_decisions d ON d.inbox_id=e.inbox_id
                        JOIN emails_log l ON l.id=d.external_email_id
                        WHERE e.inbox_id=%s
                          AND h.payload_revision=e.payload_revision
                          AND h.state IN ('approved','executing','completed')
                          AND l.status IN ('approved','sending','sent')
                        ORDER BY e.payload_revision LIMIT 1""",
                        (inbox_id,),
                    )
                    existing = await cur.fetchone()
                    if existing is not None:
                        return self._validate_existing_approved_envelope(
                            existing,
                            inbox_id=inbox_id,
                            revision=revision,
                            payload_digest=payload_digest,
                            approver=approver,
                        )
                    await cur.execute(
                        """SELECT p.*, d.decision_json, d.external_email_id, d.account_id
                        FROM execution_payload_revisions p JOIN handoff_runs h
                        ON h.inbox_id=p.inbox_id JOIN tier1_decisions d ON d.inbox_id=h.inbox_id
                        JOIN emails_log l ON l.id=d.external_email_id
                        WHERE p.inbox_id=%s AND p.revision=%s AND p.payload_digest=%s
                        AND h.payload_revision=p.revision AND h.version=%s AND h.state='approval_pending'
                        AND l.status='waiting_approval' FOR UPDATE OF h,l""",
                        (inbox_id, revision, payload_digest, expected_version)
                    )
                    payload = await cur.fetchone()
                    if not payload:
                        raise DatabaseOperationError(operation="approve_payload_revision", retryable=False, message="stale payload approval")
                    envelope = {
                        "schema_version": 1,
                        "inbox_id": inbox_id,
                        "account_id": payload["account_id"],
                        "email_id": payload["external_email_id"],
                        "payload_revision": revision,
                        "payload_digest": payload_digest,
                        "route_decision": payload["decision_json"],
                        "decision_digest": payload["decision_digest"],
                        "plan_digest": payload["plan_digest"],
                        "evidence_digest": payload["evidence_digest"],
                        "draft_digest": payload["draft_digest"],
                        "draft_content": payload["draft_content"],
                        "draft_ref": payload["draft_ref"],
                        "to": payload["to_recipients"],
                        "cc": payload["cc_recipients"],
                        "attachment_refs": payload["attachment_refs"],
                        "attachment_digests": payload["attachment_digests"],
                        "external_recipient_acknowledged": payload[
                            "external_recipient_acknowledged"
                        ],
                        "approver": approver,
                        "approved_at": str(approved_at),
                    }
                    typed_envelope = ApprovedExecutionEnvelope.model_validate(envelope)
                    envelope = typed_envelope.model_dump(mode="json")
                    digest = typed_envelope.canonical_digest()
                    await cur.execute(
                        """INSERT INTO approved_execution_envelopes
                        (inbox_id,payload_revision,payload_digest,envelope_json,envelope_digest,decision_digest,
                         plan_digest,evidence_digest,draft_digest,approver,approved_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (inbox_id, revision, payload_digest, Jsonb(envelope), digest, payload["decision_digest"],
                         payload["plan_digest"], payload["evidence_digest"], payload["draft_digest"],
                         approver, approved_at),
                    )
                    await cur.execute(
                        """UPDATE handoff_runs SET state='approved',version=version+1,
                        updated_at=CURRENT_TIMESTAMP WHERE inbox_id=%s AND version=%s
                        AND payload_revision=%s AND state='approval_pending'""",
                        (inbox_id, expected_version, revision),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(operation="approve_payload_revision", retryable=False, message="approval CAS conflict")
                    await cur.execute(
                        """UPDATE emails_log SET status='approved',approver_user_id=%s,
                        approval_at=%s,final_draft=%s,updated_at=CURRENT_TIMESTAMP
                        WHERE id=%s AND status='waiting_approval'""",
                        (approver, approved_at, payload["draft_content"], payload["external_email_id"]),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(operation="approve_payload_revision", retryable=False, message="email approval CAS conflict")
        return ApprovalCommitResult(
            status=ApprovalCommitStatus.CREATED,
            envelope=envelope,
            envelope_digest=digest,
        )

    async def reject_payload_revision(
        self,
        *,
        inbox_id: str,
        revision: int,
        payload_digest: str,
        expected_version: int,
        approver: str,
        reason: str,
    ) -> bool:
        """Reject exactly the immutable payload shown on the durable card."""
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT d.external_email_id
                        FROM handoff_runs h
                        JOIN execution_payload_revisions p
                          ON p.inbox_id=h.inbox_id
                         AND p.revision=h.payload_revision
                        JOIN tier1_decisions d ON d.inbox_id=h.inbox_id
                        JOIN emails_log l ON l.id=d.external_email_id
                        WHERE h.inbox_id=%s AND h.version=%s
                          AND h.state='approval_pending'
                          AND h.payload_revision=%s AND p.payload_digest=%s
                          AND l.status='waiting_approval'
                        FOR UPDATE OF h,l""",
                        (inbox_id, expected_version, revision, payload_digest),
                    )
                    row = await cur.fetchone()
                    if not row:
                        return False
                    await cur.execute(
                        """UPDATE handoff_runs SET state='rejected',version=version+1,
                        updated_at=CURRENT_TIMESTAMP WHERE inbox_id=%s AND version=%s
                        AND state='approval_pending' AND payload_revision=%s""",
                        (inbox_id, expected_version, revision),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="reject_payload_revision",
                            retryable=False,
                            message="handoff rejection CAS conflict",
                        )
                    await cur.execute(
                        """UPDATE emails_log SET status='rejected',approver_user_id=%s,
                        rejection_reason=%s,updated_at=CURRENT_TIMESTAMP
                        WHERE id=%s AND status='waiting_approval'""",
                        (approver, reason or None, row["external_email_id"]),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="reject_payload_revision",
                            retryable=False,
                            message="email rejection CAS conflict",
                        )
        return True

    async def claim_payload_draft_save(
        self,
        *,
        inbox_id: str,
        revision: int,
        payload_digest: str,
        expected_version: int,
    ) -> dict[str, object] | None:
        """Claim draft creation and return only the frozen payload to the worker."""
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """SELECT p.*,d.external_email_id,l.subject
                        FROM handoff_runs h
                        JOIN execution_payload_revisions p
                          ON p.inbox_id=h.inbox_id
                         AND p.revision=h.payload_revision
                        JOIN tier1_decisions d ON d.inbox_id=h.inbox_id
                        JOIN emails_log l ON l.id=d.external_email_id
                        WHERE h.inbox_id=%s AND h.version=%s
                          AND h.state='approval_pending'
                          AND h.payload_revision=%s AND p.payload_digest=%s
                          AND l.status='waiting_approval'
                        FOR UPDATE OF h,l""",
                        (inbox_id, expected_version, revision, payload_digest),
                    )
                    payload = await cur.fetchone()
                    if not payload:
                        return None
                    await cur.execute(
                        """UPDATE handoff_runs SET state='draft_saving',version=version+1,
                        updated_at=CURRENT_TIMESTAMP WHERE inbox_id=%s AND version=%s
                        AND state='approval_pending' AND payload_revision=%s""",
                        (inbox_id, expected_version, revision),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="claim_payload_draft_save",
                            retryable=False,
                            message="handoff draft-save CAS conflict",
                        )
                    await cur.execute(
                        """UPDATE emails_log SET status='saving_draft',
                        updated_at=CURRENT_TIMESTAMP
                        WHERE id=%s AND status='waiting_approval'""",
                        (payload["external_email_id"],),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="claim_payload_draft_save",
                            retryable=False,
                            message="email draft-save CAS conflict",
                        )
                    result = dict(payload)
                    result["handoff_version"] = expected_version + 1
                    return result

    async def complete_payload_draft_save(
        self, *, inbox_id: str, expected_version: int
    ) -> bool:
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """UPDATE handoff_runs SET state='draft_saved',version=version+1,
                        updated_at=CURRENT_TIMESTAMP WHERE inbox_id=%s AND version=%s
                        AND state='draft_saving' RETURNING inbox_id""",
                        (inbox_id, expected_version),
                    )
                    if await cur.fetchone() is None:
                        return False
                    await cur.execute(
                        """UPDATE emails_log l SET status='draft_saved',
                        updated_at=CURRENT_TIMESTAMP FROM tier1_decisions d
                        WHERE d.inbox_id=%s AND d.external_email_id=l.id
                        AND l.status='saving_draft'""",
                        (inbox_id,),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="complete_payload_draft_save",
                            retryable=False,
                            message="email draft-save completion conflict",
                        )
        return True

    async def fail_payload_draft_save(
        self, *, inbox_id: str, expected_version: int, error_code: str
    ) -> bool:
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """UPDATE handoff_runs SET state='manual_review',version=version+1,
                        updated_at=CURRENT_TIMESTAMP WHERE inbox_id=%s AND version=%s
                        AND state='draft_saving' RETURNING inbox_id""",
                        (inbox_id, expected_version),
                    )
                    if await cur.fetchone() is None:
                        return False
                    await cur.execute(
                        """UPDATE emails_log l SET status='manual_review',error_message=%s,
                        updated_at=CURRENT_TIMESTAMP FROM tier1_decisions d
                        WHERE d.inbox_id=%s AND d.external_email_id=l.id
                        AND l.status='saving_draft'""",
                        (error_code, inbox_id),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="fail_payload_draft_save",
                            retryable=False,
                            message="email draft-save quarantine conflict",
                        )
        return True

    async def get_approved_execution_envelope(
        self, *, inbox_id: str, revision: int
    ) -> dict[str, object] | None:
        async with self.get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """SELECT envelope_json, envelope_digest
                    FROM approved_execution_envelopes
                    WHERE inbox_id=%s AND payload_revision=%s""",
                    (inbox_id, revision),
                )
                row = await cur.fetchone()
        return (
            {
                "envelope": row["envelope_json"],
                "envelope_digest": row["envelope_digest"],
            }
            if row
            else None
        )

    async def claim_execution(
        self, *, inbox_id: str, revision: int, expected_version: int, claim_id: str
    ) -> bool:
        """Exactly-once deterministic execution gate."""
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """UPDATE emails_log l SET status='sending',updated_at=CURRENT_TIMESTAMP
                        FROM tier1_decisions d WHERE d.inbox_id=%s
                        AND d.external_email_id=l.id AND l.status='approved'""",
                        (inbox_id,),
                    )
                    if cur.rowcount != 1:
                        return False
                    await cur.execute(
                    """UPDATE handoff_runs h SET state='executing',execution_claim_id=%s,
                    execution_claimed_at=CURRENT_TIMESTAMP,version=version+1,updated_at=CURRENT_TIMESTAMP
                    WHERE h.inbox_id=%s AND h.payload_revision=%s AND h.version=%s
                    AND h.state='approved' AND h.execution_claim_id IS NULL
                    AND EXISTS (SELECT 1 FROM approved_execution_envelopes e
                    WHERE e.inbox_id=h.inbox_id AND e.payload_revision=h.payload_revision)""",
                        (claim_id, inbox_id, revision, expected_version),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="claim_execution",
                            retryable=False,
                            message="execution claim conflict",
                        )
        return True

    async def complete_execution(
        self, *, inbox_id: str, expected_version: int, sent: bool
    ) -> bool:
        target = "completed" if sent else "failed"
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """UPDATE handoff_runs SET state=%s,version=version+1,
                        updated_at=CURRENT_TIMESTAMP WHERE inbox_id=%s AND version=%s
                        AND state='executing'""",
                        (target, inbox_id, expected_version),
                    )
                    if cur.rowcount != 1:
                        return False
                    if sent:
                        await cur.execute(
                            """UPDATE emails_log l SET status='sent',updated_at=CURRENT_TIMESTAMP
                            FROM tier1_decisions d WHERE d.inbox_id=%s
                            AND d.external_email_id=l.id AND l.status='sending'""",
                            (inbox_id,),
                        )
                        if cur.rowcount != 1:
                            raise DatabaseOperationError(
                                operation="complete_execution",
                                retryable=False,
                                message="send completion conflict",
                            )
        return True

    async def advance_handoff_execution(
        self,
        *,
        inbox_id: str,
        expected_state: str,
        next_state: str,
        error_code: str | None = None,
    ) -> None:
        """Advance the mutable handoff row using an exact compare-and-set."""
        if (expected_state, next_state) not in HANDOFF_TRANSITIONS:
            raise ValueError("invalid_handoff_transition")
        if error_code is not None and (
            not isinstance(error_code, str)
            or not error_code
            or len(error_code.encode("utf-8")) > 128
        ):
            raise ValueError("invalid_handoff_error_code")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE handoff_executions
                        SET state = %s,
                            safe_error_code = %s,
                            version = version + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE inbox_id = %s AND state = %s
                        """,
                        (next_state, error_code, inbox_id, expected_state),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="advance_handoff_execution",
                            retryable=False,
                            message="handoff state conflict",
                        )
        except DatabaseOperationError:
            raise
        except psycopg.Error as exc:
            raise DatabaseOperationError(
                operation="advance_handoff_execution",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="handoff transition failed",
            ) from None

    async def get_email_approval_record(self, email_id: str) -> dict[str, Any] | None:
        """Return the fields needed to score a human approval outcome."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT id, status, original_draft, final_draft, classification,
                               updated_at, approval_at
                        FROM emails_log
                        WHERE id = %s
                        """,
                        (email_id,),
                    )
                    return await cur.fetchone()
        except psycopg.Error as exc:
            logger.error(
                "Failed to get approval record: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="get_email_approval_record",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="approval record read failed",
            ) from None

    async def get_email_status(self, email_id: str) -> str | None:
        """Return the persisted processing status for an email, if present."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT status FROM emails_log WHERE id = %s", (email_id,)
                    )
                    row = await cur.fetchone()
                    return row["status"] if row else None
        except psycopg.Error as exc:
            logger.error(
                "Failed to get email status: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="get_email_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="email status read failed",
            ) from None

    async def set_content_ref(self, email_id: str, ref: ContentRef) -> None:
        payload = content_ref_to_json(ref)
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET content_ref = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (Jsonb(payload), email_id),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="set_content_ref",
                            retryable=False,
                            message="email row missing",
                        )
        except DatabaseOperationError:
            raise
        except psycopg.Error as exc:
            logger.error(
                "Content reference persistence failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="set_content_ref",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="content reference persistence failed",
            ) from None

    async def set_content_ref_if_absent(
        self,
        email_id: str,
        ref: ContentRef,
    ) -> bool:
        """Atomically claim an empty content_ref slot for concurrent retries."""
        payload = content_ref_to_json(ref)
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET content_ref = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND content_ref IS NULL
                        """,
                        (Jsonb(payload), email_id),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Content reference claim failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="set_content_ref_if_absent",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="content reference claim failed",
            ) from None

    async def get_content_ref(self, email_id: str) -> ContentRef | None:
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT content_ref FROM emails_log WHERE id = %s",
                        (email_id,),
                    )
                    row = await cur.fetchone()
        except psycopg.Error as exc:
            logger.error(
                "Content reference read failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="get_content_ref",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="content reference read failed",
            ) from None

        if row is None or row.get("content_ref") is None:
            return None
        raw_ref = row["content_ref"]
        if isinstance(raw_ref, str):
            try:
                raw_ref = json.loads(raw_ref)
            except json.JSONDecodeError:
                from src.storage import ContentStoreReferenceError

                raise ContentStoreReferenceError("invalid_content_ref") from None
        return content_ref_from_json(raw_ref)

    async def save_draft(self, email_id: str, content: str) -> str:
        if not isinstance(email_id, str) or not email_id or not isinstance(content, str):
            raise DatabaseOperationError(
                operation="save_draft",
                retryable=False,
                message="invalid draft input",
            )
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET draft_content = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (content, email_id),
                    )
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="save_draft",
                            retryable=False,
                            message="email row missing",
                        )
        except DatabaseOperationError:
            raise
        except psycopg.Error as exc:
            logger.error(
                "Draft persistence failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="save_draft",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="draft persistence failed",
            ) from None
        return email_id

    async def save_draft_if_status(self, email_id: str, content: str) -> bool:
        """Update a draft only while the email still awaits approval."""
        if (
            not isinstance(email_id, str)
            or not email_id.strip()
            or not isinstance(content, str)
            or not content.strip()
        ):
            raise ValueError("invalid_draft_edit")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET draft_content = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND status = %s
                        """,
                        (content, email_id, "waiting_approval"),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Conditional draft persistence failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="save_draft_if_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="conditional draft persistence failed",
            ) from None

    async def load_draft(self, draft_id: str) -> str:
        if not isinstance(draft_id, str) or not draft_id:
            raise DatabaseOperationError(
                operation="load_draft",
                retryable=False,
                message="invalid draft identifier",
            )
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT draft_content FROM emails_log WHERE id = %s",
                        (draft_id,),
                    )
                    row = await cur.fetchone()
        except psycopg.Error as exc:
            logger.error(
                "Draft read failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="load_draft",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="draft read failed",
            ) from None

        if row is None or not isinstance(row.get("draft_content"), str):
            raise DatabaseOperationError(
                operation="load_draft",
                retryable=False,
                message="draft not found",
            )
        return row["draft_content"]

    async def update_status(self, email_id: str, status: Optional[str], **kwargs):
        """Update the status and optional fields of an email log.

        Args:
            email_id: The email ID.
            status: New status string, or None to skip status change (metadata-only update).
            **kwargs: Additional columns to update.
        """
        ALLOWED_COLUMNS = {
            "classification", "summary", "priority", "need_reply",
            "card_type", "draft", "draft_content", "message_id", "intent",
            "reasoning", "error_message",
            "routing_log",
            "original_draft", "final_draft", "draft_diff",
            "approver_user_id", "rejection_reason",
        }
        JSONB_COLUMNS = {"classification", "routing_log"}
        if status is not None and (
            not isinstance(status, str)
            or not status.strip()
            or len(status.encode("utf-8")) > 64
        ):
            raise ValueError("invalid_email_status")
        if status in CAS_ONLY_EMAIL_STATUSES:
            raise ValueError("status_requires_compare_and_set")
        try:
            async with self.get_connection() as conn:
                update_fields = ["updated_at = CURRENT_TIMESTAMP"]
                params: list = []

                if status is not None:
                    update_fields.insert(0, "status = %s")
                    params.append(status)

                for key, value in kwargs.items():
                    if key not in ALLOWED_COLUMNS:
                        logger.warning(f"Rejected update_status column: {key}")
                        continue
                    if key in JSONB_COLUMNS:
                        update_fields.append(f"{key} = %s")
                        params.append(json.dumps(value) if not isinstance(value, str) else value)
                    else:
                        update_fields.append(f"{key} = %s")
                        params.append(value)

                if not update_fields:
                    return

                params.append(email_id)
                if status is not None:
                    params.append(sorted(CAS_ONLY_EMAIL_STATUSES))
                    query = (
                        f"UPDATE emails_log SET {', '.join(update_fields)} "
                        "WHERE id = %s AND status <> ALL(%s)"
                    )
                else:
                    query = (
                        f"UPDATE emails_log SET {', '.join(update_fields)} "
                        "WHERE id = %s"
                    )

                async with conn.cursor() as cur:
                    await cur.execute(query, tuple(params))
                    if cur.rowcount != 1:
                        raise DatabaseOperationError(
                            operation="update_status",
                            retryable=False,
                            message=f"Email {email_id} was not updated",
                        )

            if status is not None:
                try:
                    from src.observability.metrics import record_email_status
                    record_email_status(status)
                except Exception:
                    pass
        except psycopg.Error as exc:
            logger.error(
                "Failed to update email status: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="update_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="email status update failed",
            ) from None

    async def compare_and_set_status(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        target: str,
    ) -> bool:
        """Atomically transition an email when its current status is expected."""
        if (
            not isinstance(email_id, str)
            or not email_id.strip()
            or type(expected) is not frozenset
            or len(expected) != 1
            or not isinstance(target, str)
            or not target
        ):
            raise ValueError("invalid_email_status_transition")
        source = next(iter(expected))
        if (
            not isinstance(source, str)
            or (source, target) not in EMAIL_STATUS_CAS_TRANSITIONS
        ):
            raise ValueError("email_status_transition_not_allowed")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE emails_log SET status=%s, updated_at=CURRENT_TIMESTAMP "
                        "WHERE id=%s AND status=ANY(%s)",
                        (target, email_id, list(expected)),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Failed to compare and set email status: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="compare_and_set_status",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="email status compare-and-set failed",
            ) from None

    async def list_expired_approvals(
        self,
        *,
        older_than,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return waiting_approval rows older than the SLA cutoff."""
        if type(limit) is not int or limit < 1:
            raise ValueError("invalid_expired_approval_limit")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT l.id,
                               l.updated_at,
                               l.original_draft,
                               l.final_draft,
                               l.classification,
                               d.inbox_id,
                               d.decision_json,
                               h.version AS handoff_version,
                               h.payload_revision
                        FROM emails_log l
                        LEFT JOIN tier1_decisions d ON d.external_email_id = l.id
                        LEFT JOIN handoff_runs h ON h.inbox_id = d.inbox_id
                        WHERE l.status = 'waiting_approval'
                          AND l.updated_at < %s
                        ORDER BY l.updated_at ASC
                        LIMIT %s
                        """,
                        (older_than, limit),
                    )
                    return list(await cur.fetchall())
        except psycopg.Error as exc:
            logger.error(
                "Expired approval scan failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="list_expired_approvals",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="expired approval scan failed",
            ) from None

    async def compare_and_set_manual_review(
        self,
        email_id: str,
        *,
        expected: frozenset[str],
        error_code: str,
    ) -> bool:
        """Atomically enter manual review and persist its bounded reason."""
        if (
            not isinstance(email_id, str)
            or not email_id.strip()
            or not isinstance(error_code, str)
            or not error_code
            or len(error_code.encode("utf-8")) > 256
        ):
            raise ValueError("invalid_manual_review_transition")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET status = %s,
                            error_message = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND status = ANY(%s)
                        """,
                        (
                            "manual_review",
                            error_code,
                            email_id,
                            list(expected),
                        ),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Manual-review transition failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="compare_and_set_manual_review",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="manual-review transition failed",
            ) from None

    async def compare_and_set_send_unknown(
        self,
        email_id: str,
        *,
        error_code: str,
    ) -> bool:
        """Quarantine one started send without making it retryable."""
        if (
            not isinstance(email_id, str)
            or not email_id.strip()
            or not isinstance(error_code, str)
            or not error_code
            or len(error_code.encode("utf-8")) > 256
        ):
            raise ValueError("invalid_send_unknown_transition")
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET status = %s,
                            error_message = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND status = %s
                        """,
                        (
                            "send_unknown",
                            error_code,
                            email_id,
                            "sending",
                        ),
                    )
                    return cur.rowcount == 1
        except psycopg.Error as exc:
            logger.error(
                "Send-unknown transition failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="compare_and_set_send_unknown",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="send-unknown transition failed",
            ) from None

    async def recover_incomplete_approval_states(self) -> int:
        """Fail closed for approval/send transitions left ambiguous at restart."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        UPDATE emails_log
                        SET status = CASE status
                                WHEN %s THEN %s
                                ELSE %s
                            END,
                            error_message = CASE status
                                WHEN %s THEN %s
                                WHEN %s THEN %s
                                WHEN %s THEN %s
                                WHEN %s THEN %s
                                ELSE error_message
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE status = ANY(%s)
                          AND (status <> 'approved' OR NOT EXISTS (
                            SELECT 1 FROM tier1_decisions d
                            JOIN handoff_runs h ON h.inbox_id=d.inbox_id
                            JOIN approved_execution_envelopes e
                              ON e.inbox_id=h.inbox_id
                             AND e.payload_revision=h.payload_revision
                            WHERE d.external_email_id=emails_log.id
                              AND h.state='approved'
                          ))
                        """,
                        (
                            "sending",
                            "send_unknown",
                            "manual_review",
                            "approved",
                            "approval_handoff_incomplete",
                            "sending",
                            "send_outcome_unknown",
                            "saving_draft",
                            "draft_save_outcome_unknown",
                            "recovering",
                            "self_healing_interrupted",
                            ["approved", "sending", "saving_draft", "recovering"],
                        ),
                    )
                    return cur.rowcount
        except psycopg.Error as exc:
            logger.error(
                "Approval state recovery failed: error_type=%s",
                type(exc).__name__,
            )
            raise DatabaseOperationError(
                operation="recover_incomplete_approval_states",
                retryable=isinstance(exc, psycopg.OperationalError),
                message="approval state recovery failed",
            ) from None

    async def recover_durable_handoff_states(self) -> int:
        """Quarantine cross-table durable approval/execution inconsistencies."""
        async with self.get_connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """WITH inconsistent AS (
                            SELECT h.inbox_id,h.state,d.external_email_id,
                                   (e.inbox_id IS NOT NULL) AS has_envelope
                            FROM handoff_runs h
                            JOIN tier1_decisions d ON d.inbox_id=h.inbox_id
                            LEFT JOIN approved_execution_envelopes e
                              ON e.inbox_id=h.inbox_id
                             AND e.payload_revision=h.payload_revision
                            JOIN emails_log l ON l.id=d.external_email_id
                            WHERE (h.state='approved' AND (e.inbox_id IS NULL OR l.status<>'approved'))
                               OR h.state IN ('executing','draft_saving')
                               OR l.status IN ('sending','saving_draft')
                        ), handoff_fixed AS (
                            UPDATE handoff_runs h SET
                              state=CASE WHEN i.state='executing' THEN 'failed' ELSE 'manual_review' END,
                              version=h.version+1,updated_at=CURRENT_TIMESTAMP
                            FROM inconsistent i WHERE h.inbox_id=i.inbox_id
                            RETURNING h.inbox_id
                        )
                        UPDATE emails_log l SET
                          status=CASE WHEN i.state='executing' OR l.status='sending'
                                      THEN 'send_unknown' ELSE 'manual_review' END,
                          error_message=CASE WHEN i.state='executing' OR l.status='sending'
                                      THEN 'send_outcome_unknown'
                                      WHEN i.state='draft_saving' OR l.status='saving_draft'
                                      THEN 'draft_save_outcome_unknown'
                                      ELSE 'approval_handoff_incomplete' END,
                          updated_at=CURRENT_TIMESTAMP
                        FROM inconsistent i WHERE l.id=i.external_email_id"""
                    )
                    return cur.rowcount

    async def check_email_exists(self, email_id: str) -> bool:
        """Check if an email ID has already been logged/processed."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1 FROM emails_log WHERE id = %s", (email_id,))
                    return await cur.fetchone() is not None
        except psycopg.Error as exc:
            logger.error(
                "Failed to check email existence: error_type=%s",
                type(exc).__name__,
            )
            return False

    async def get_processed_count(self) -> int:
        """Get total count of processed emails for reporting."""
        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT count(*) as cnt FROM emails_log")
                    row = await cur.fetchone()
                    return row["cnt"] if row else 0
        except psycopg.Error:
            return 0

    async def mark_as_processed(self, email_id: str):
        """Quick shortcut for dedup, sets status to 'ingested'."""
        await self.update_status(email_id, "ingested")

    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("AsyncDatabaseManager connection pool closed.")
            self._pool = None
