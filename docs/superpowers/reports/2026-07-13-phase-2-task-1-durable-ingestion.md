# Phase 2 Task 1C Durable Ingestion Foundation Closeout Report

**Date:** 2026-07-13

**Branch:** `codex/ai-exchange-remediation`

**Base:** `eff733b`

**Status:** Implementation and current-tree automated/container verification complete in the enclosing Task 1C changeset. Nothing in this report is a deployment or cutover approval.

## Scope Delivered in the Current Uncommitted Batch

- Added the forward-only `20260710_0003` durable-ingestion migration with six PostgreSQL fact tables: `pipeline_ownership`, `event_inbox`, `sync_cursors`, `emails`, `audit_events`, and `pipeline_shadow_comparisons`.
- Added immutable ingestion DTOs with bounded text/JSON, timezone-aware timestamps, UUID/SHA-256 validation, defensive copies, explicit enums, and safe error-code constraints.
- Added an exact physical schema manifest covering tables, columns, constraints, indexes, triggers, foreign keys, ownership, persistence/access methods, RLS/policies, and unexpected-object rejection.
- Extended the database trust boundary to four distinct non-superuser, `NOINHERIT` identities: migration owner, runtime, checkpoint maintenance, and checkpoint auditor. Exact database/schema/table/column/function/type/sequence/default-privilege contracts fail closed on drift.
- Split checkpoint cleanup into a read-only auditor plan path and a maintenance execute path guarded by an externally signed Ed25519 backup receipt.
- Added double-layer checkpoint maintenance fencing: a dedicated runtime shared advisory lock plus per-pool-session shared locks. Every checkpoint mutation rechecks the dedicated guard; runtime checkpoint DDL is forbidden; loss of the guard fails closed.
- Applied the same runtime database revision, schema, role, and fence preflight to the web service and the reprocessing entry point. Shutdown closes the application context and pool before releasing the dedicated fence, with a bounded fail-stop path for cancellation-resistant shutdown.
- Kept reconciliation capability fail closed: enabling `SYNC_RECONCILIATION_ENABLED` before its implementation is available is rejected instead of silently advertising a working scheduler.
- Added pinned CI/build inputs, hash-locked and wheel-only Python dependency installation, real-PostgreSQL CI gates, critical-module coverage gates, and an explicit production build-context allowlist.
- Removed legacy real-message/PDF fixtures from the current tree and replaced the mail-rendering path with a synthetic `example.test` fixture. Security tests enforce an exact mail-artifact allowlist under `tests/fixtures` (only `synthetic_notification.eml`; no PDF or mailbox binaries) and reject real-looking addresses or routing headers in the allowed message.

## Security and Reliability Invariants

1. PostgreSQL is the durable business fact source; a successful future Webhook `202` may only follow an Inbox commit.
2. Runtime credentials cannot run Alembic or LangGraph setup, own the protected objects, create objects in the application schema, or inherit migration/maintenance/auditor authority.
3. The auditor can read only the bounded columns required to prepare a cleanup plan. It cannot execute cleanup. The maintenance identity can perform only the explicitly contracted checkpoint cleanup operations and cannot replace the migration owner or runtime.
4. Public and unmanaged-role privileges, grant options, role memberships, owner-rights views, unexpected triggers/policies/indexes/constraints, and schema drift cause startup or maintenance preflight rejection.
5. Checkpoint maintenance requires the runtime shared fence to be absent, a valid unexpired plan, and a valid externally signed backup receipt. Runtime checkpoint writers cannot resume silently across a maintenance lock transition.
6. A lost dedicated fence, invalid runtime boundary, incomplete shutdown, or unavailable sync capability fails closed; these paths do not downgrade to an in-memory queue or an unfenced writer.
7. Raw mailbox content, credentials, identifiers, and legacy fixture data are not included in this report or in the new synthetic fixture contract.

## Verification Evidence

| Gate | Result |
|---|---|
| Unit, security, and consolidation suites | `1763 passed, 12 skipped, 4 warnings` |
| Focused real-PostgreSQL ingestion/role/checkpoint/Alembic suites | `263 passed` |
| Full explicit real-PostgreSQL suite | `2060 passed, 12 skipped, 4 warnings` |
| Critical modules | Each module passed the 90% gate; measured range `93.84%` to `100%` |
| Durable ingestion package | `src/ingestion/*`: `97.51%` |
| Database role/schema/bootstrap aggregate | `src/db/roles.py`, `src/db/schema_contract.py`, and `src/db/bootstrap.py`: `96.63%` |
| Static dependency/code gates | Frozen lock/sync, package consistency, Ruff, compile, and diff-whitespace checks passed |
| Production image | Content ID `sha256:37c8e1e9b32acb16893b61a776406f98d412dc50263cd2542f2a8e0b4a40ad77`; `linux/arm64`, non-root `appuser`, Python 3.12.13, package consistency, imports, and maintenance CLI passed; `/app` contains no tests, EML, or PDF fixtures |
| Compose configuration | Default configuration and the migration, checkpoint-maintenance, and checkpoint-maintenance-execute profiles passed; the merged configuration contains exactly six services |
| Independent review | Schema and checkpoint/supply-chain reviews reported no remaining Critical or Important findings in this batch |

The four warnings in the broad suites are existing dependency deprecations. Full-repository coverage was measured at `79.21%`; this is below the final master-plan 80% acceptance ratchet and therefore is not represented as whole-project completion. Reliability-critical Phase 2 modules independently exceed their 90% threshold.

## Explicit Non-Actions and Residual Follow-up

- No production database migration was applied, no live Inbox traffic was enabled, no feature flag was cut over, and no deployment was performed.
- `/Users/jarod/Documents/exchange-feishu-extension` remained read-only: it was not modified, committed, started, restarted, or used for a live capability change.
- Deleting the legacy fixtures from the current tree does not remove their blobs from earlier Git history. Rewriting shared history and coordinating old clones/remotes is destructive and requires separate, explicit authorization; it is not part of this batch.

## Next Task

Phase 2 Task 2 will implement canonical Webhook/Sync normalization and stable dedupe keys under TDD. The signed Webhook body will be authoritative, headers will only provide consistency assertions, validation errors will expose fixed safe codes, sync cursors will participate in sync dedupe identity, and callers will be required to choose an explicit processing policy. Ignored-folder durability semantics and historical/cold-start suppression must be locked before intake is activated.

The target intake remains hybrid: Webhook supplies the low-latency trigger, periodic `/emails/sync` supplies reconciliation, and both converge on the same durable `event_inbox` before downstream processing.
