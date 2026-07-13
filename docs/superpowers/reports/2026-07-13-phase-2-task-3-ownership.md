# Phase 2 Task 3 Pipeline Ownership and Fencing Closeout Report

**Date:** 2026-07-13

**Branch:** `codex/ai-exchange-remediation`

**Base:** `e817d78`

**Status:** Implementation, scoped verification, real-PostgreSQL verification, full-repository regression, independent code review, and frozen-plan review are complete. This report is not a deployment, migration, activation, or cutover approval.

## Scope Delivered

- Added the transactional `PipelineOwnershipRepository` boundary for first-generation bootstrap, exact ownership reads, current-ingress lookup, next-generation inspection, diagnostic fence checks, existing-lease executability, quiescing, and guarded retirement.
- Added privacy-safe `StaleFence(ErrorKind.INTERNAL_INVARIANT)` with a fixed code, summary, representation, and no raw account, pipeline, actor, reason, database, or mailbox detail.
- Serialized ownership control operations with a stable per-account transaction advisory lock and bounded local lock, statement, and idle-in-transaction timeouts.
- Made bootstrap concurrent and idempotent only for the exact initial pipeline. Ownership history without an exact current generation fails closed and never silently recreates authority.
- Made `current_ingress -> quiescing` the only public Phase-2 transition. Replays return the existing row without creating a new generation or unbounded duplicate audit facts.
- Added private transaction-local handoff primitives for later governed activation. They bind to the database top-level transaction ID, reject reuse across commits, reacquire the account lock for every mutation, repeat exact state/generation/fence predicates, and re-read persisted draining state before inserting a successor.
- Closed both transaction-object hazards found in review: a savepoint rollback cannot leave phantom Python draining authority, and one helper object cannot split lock/drain/insert across multiple committed transactions.
- Added a static architecture gate that forbids production `src/` or `scripts/` code from calling the transaction-local handoff primitives. The transaction helper is not exported from the package boundary, and no public `switch`, `promote`, or `create_next` API exists.
- Kept standalone `assert_fence()` explicitly diagnostic. Claim, renewal, effect-start, completion, and other mutations must repeat the fence predicate in their own CAS statement or locked transaction in later tasks.
- Added fail-closed retirement. The default guard always denies because Outbox and high-water evidence do not exist at schema head `0004`; missing future evidence is never interpreted as zero.
- Locked the current exact retirement blockers. Inbox blocks on `pending/retry_wait/leased/manual_review/dead_letter`; email blocks on `ingested/processing/retry_wait/manual_review/waiting_approval/send_queued/sending/accepted/send_unknown/dead_letter`. Outcome-known `send_failed` and `delivery_failed` terminal projections do not block by themselves.
- Added atomic transition audits and regression checks proving failed handoffs do not retain ownership or audit writes, while successful bootstrap/quiesce/handoff/retire actions append the expected bounded facts.
- Made repository row parsing compatible with both default tuple rows and the production application's `autocommit + dict_row` pool configuration. A real-role full-lifecycle test locks this production-compatible path.

## Frozen Plan Corrections

- Removed Task 3's invalid dependency on the future Inbox repository while retaining the existing immutable `InboxLease` contract for `can_execute()` tests.
- Moved new-claim, lease renewal, and completion CAS authority to Task 4, where the repository SQL exists.
- Made retirement success explicitly dependent on a later complete Inbox/Outbox/high-water guard instead of treating absent future tables as empty.
- Added an explicit non-executable `reserved` ownership state to Task 10/`0007`, with only `reserved -> current_ingress` promotion or `reserved -> retired` cancellation/expiry.
- Required Task 10 to revoke runtime's raw ownership `INSERT`/`UPDATE` privileges and replace bootstrap, quiesce, preparation, promotion, cancellation, and retirement with narrow, source-digest-locked, fixed-search-path database entry points.
- Added `src/domain/email_state.py`, `src/ingestion/ownership.py`, their tests, ACL behavior tests, and exact function-source digests to the Task 10 migration scope.
- Corrected every Phase-2 real-PostgreSQL verification command and fixture description to use `TEST_POSTGRES_ADMIN_URL` plus `TEST_POSTGRES_ROLE_DDL=1`, preventing skipped integration tests from appearing as a false green gate.

## Verification Evidence

| Gate | Result |
|---|---|
| Task 3 unit, real-PostgreSQL, and architecture gate | `57 passed` |
| Ownership module coverage gate | `95.21%` (`376` statements, `18` missed; required `>=95%`) |
| Unit, security, and architecture regression | `1975 passed`, `12 skipped`, `4 warnings` |
| Full real-PostgreSQL repository regression | `2317 passed`, `12 skipped`, `4 warnings` in `362.85s` |
| Real-role ACL regression | `41 passed` during independent frozen-plan review and included again in the full repository gate |
| Static gates | Full Ruff check, changed-file format check, `compileall`, and `git diff --check` passed |
| Independent code review | Critical `0`, Important `0`, Minor `0` |
| Independent frozen-plan review | Critical `0`, Important `0`, Minor `0` |

The four warnings are existing third-party deprecations from Lark/protobuf/websockets imports. No test failure, new warning class, or skipped Task 3 real-PostgreSQL case remained.

## Explicit Non-Actions and Remaining Gates

- No production database was migrated or mutated; no Webhook, polling, Sync, worker, or scheduler authority was enabled; no generation was switched in a live environment.
- `/Users/jarod/Documents/exchange-feishu-extension` remained read-only and clean. It was not modified, committed, started, restarted, or used for a live capability change.
- Runtime still has schema-head `0004` ownership DML needed by this dormant implementation. Production activation remains forbidden until Task 10/`0007` revokes that raw authority and installs/verifies the governed database operations.
- Production retirement remains impossible with the default guard. The allowing guard exists only in tests to prove the atomic terminal transition after complete evidence is supplied by later tasks.
- New Inbox claims, lease renewal/completion CAS, retries, dead letters, and durable worker effects are not implemented here. They remain Task 4 and later work.
- The stable target architecture remains hybrid: Webhook is the low-latency trigger, periodic `/emails/sync` is reconciliation, and both converge on one durable `event_inbox` under exact generation/fence ownership.

## Next Task

Phase 2 Task 4 implements the Inbox repository, disjoint `SKIP LOCKED` claims, renewable leases, repeatable effect-start markers, bounded retries, expired-lease recovery, dead letters, and mutation-local ownership fencing.
