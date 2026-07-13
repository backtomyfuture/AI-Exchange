# Phase 2 Task 2 Ingress Normalization Closeout Report

**Date:** 2026-07-13

**Branch:** `codex/ai-exchange-remediation`

**Base:** `9ee04f8`

**Status:** Implementation, scoped verification, real-PostgreSQL verification, full-repository regression, combined final review, staged-scope inspection, and the Task 2 branch commit are complete. This report is not a deployment or cutover approval.

## Scope Delivered in the Current Uncommitted Batch

- Added a strict normalization boundary shared by verified Webhook delivery and future Exchange Sync reconciliation.
- Added one policy-free `validate_sync_change_contract()` boundary shared by the future Sync HTTP client and the normalizer. Untrusted HTTP mappings have exact outer and item manifests; typed internal DTOs retain a deliberately narrower compatibility layer without creating a second validator.
- Locked the public interfaces: `normalize_webhook_event` is entirely keyword-only; `normalize_sync_change` accepts its first four arguments positionally and requires `processing_policy` as a keyword-only argument.
- Removed the fail-open `ProcessingPolicy.FULL` default from `NormalizedIngressEvent`. Both normalizers require an actual `ProcessingPolicy` enum from every caller.
- Added `ProcessingPolicy.IGNORED` so a verified event excluded by routing policy can still receive a durable, auditable Inbox receipt instead of disappearing before persistence.
- Added the forward-only `20260713_0004` Alembic revision. It expands only `ck_event_inbox_processing_policy`, admits `ignored`, rejects unknown values, and intentionally provides no downgrade path. A non-empty `event_inbox` now fails closed under the same `ACCESS EXCLUSIVE` migration transaction, leaving the `0003` revision and every existing row untouched.
- Locked a code-first-only bridge: new code with all Phase 2 flags off verifies exact `0003`, bootstrap requires an inactive/empty Inbox and advances to `0004`, exact schema and all four role gates pass, and only then may intake be enabled. Migration-first is not supported because the previous application revision does not recognize `0004`.
- Corrected bootstrap ordering for an already-versioned database: the exact business schema and every already-present checkpoint object are first checked for incompatible drift; checkpoint migrations may then create missing checkpoint objects; the resulting full schema is verified exactly before any Alembic write. A corrupt `0003` database therefore remains at `0003` instead of being mislabeled `0004`; the separately tested fresh-database recovery path still permits the intentionally recoverable `0004`-before-checkpoint case.
- Added a closed `IngressValidationCode` allowlist and privacy-safe `IngressValidationError` with a fixed summary and representation. Validation output does not include raw body data, mailbox identifiers, folder, version, cursor, arbitrary caller text, or chained causes.

## Locked Normalization Contract

### Webhook Authority and Validation

1. The verified raw body is authoritative. It must be bytes containing one strict UTF-8 JSON object; duplicate keys, non-finite numbers, malformed JSON, non-object roots and invalid UTF-8 fail closed.
2. The separately supplied `payload` is only a consistency assertion. Its canonical JSON must exactly equal the parsed signed body; it cannot add, remove, repair, or override signed fields.
3. `event` and `event_type` come from the signed body. If both are present they must be non-null and exactly equal. The optional `header_event` may only assert equality; it cannot supply a missing event or override a body value.
4. Only `NewMailEvent`/`CreatedEvent`, `ModifiedEvent`, and `DeletedEvent` map to create, update, and delete respectively. Aliases and unsupported events fail closed.
5. The body account must be a positive PostgreSQL BIGINT and cannot be a boolean. External ID candidates, folder assertions, and changekey candidates must agree when more than one representation is present; malformed high-priority fields cannot be bypassed by a lower-priority fallback.
6. `parent_folder_id.id` is required. Standard folders are canonicalized while custom folder spelling and case are preserved after boundary trimming.
7. Only a timezone-aware signed-body timestamp or bounded epoch is trusted as `source_event_at`. No unsigned timestamp is accepted.
8. Opaque email IDs, source versions and Sync cursors are exact tokens: leading/trailing whitespace is rejected instead of silently changing dedupe or cursor identity. Only folder boundaries may be trimmed.
9. The complete validated signed object becomes the normalized payload. NUL keys/values and unpaired Unicode surrogates—even when introduced through a legal ASCII JSON escape—fail closed because PostgreSQL UTF-8 text/`jsonb` cannot represent them. Payload sizing includes PostgreSQL fixed-point expansion of exponent-form numbers, and `payload_for_storage()` recursively returns built-in dict/list values suitable for psycopg `Jsonb` without weakening the immutable public DTO.

### Stable Dedupe Identity

The dedupe key is SHA-256 over schema-versioned UTF-8 canonical JSON with sorted keys, compact separators, Unicode preservation and non-finite-number rejection. The common identity includes account, source, raw event type, change kind, external ID, normalized folder, source version, cursor, trusted source time and raw-body digest.

Webhook selects exactly one retry discriminator in strict priority order:

1. Source version, with trusted time and raw-body digest excluded from identity.
2. Otherwise trusted signed-body time, with raw-body digest excluded.
3. Otherwise the exact raw-body SHA-256 digest.

Header assertions, delivery metadata, normalized payload content and processing policy are not part of dedupe identity. Consequently, retry metadata or an ignored/full routing decision cannot create a second durable identity for the same versioned source event.

### Sync Contract

- Sync accepts only create, update, and delete. Create/update require an item; delete requires no item. An inner item ID, when present, must be an exact non-null string matching the outer change ID.
- A raw HTTP change has exactly `change_type`, `id`, and `item`. Raw create/update items have exactly `id`, `subject`, `sender`, `received_time`, `is_read`, and `has_attachments`; transport `source_version`, unknown fields, nested values, coercion, invalid booleans, hostile mapping protocols, NUL/surrogate text, and oversized JSONB representations fail closed with fixed safe errors.
- The current service constructs a naive datetime at whole-second precision before JSON encoding. The boundary therefore accepts only the exact display format `YYYY-MM-DDTHH:MM:SS`; Python's broader date-only, week-date, arbitrary-separator, fractional-second, and timezone ISO forms are rejected. This field remains display-only and is never promoted to trusted event time.
- Read-state changes remain update events; they are not promoted to a synthetic read event.
- The cursor and change type always participate in Sync identity, preventing collisions across batches or change kinds.
- Explicit ASCII standard folder aliases canonicalize to one uppercase vocabulary. Custom punctuation, spacing, case and non-ASCII spelling are preserved, so values such as `s_e_n_t` or `ſent` cannot collide with `SENT`.
- Sync currently emits `source_event_at=None`. The upstream service exposes a naive display timestamp without a timezone or trusted-source-time contract, so neither `received_time` nor another item field is treated as authoritative until that cross-repository contract is versioned and locked.
- Processing policy is explicit and excluded from identity. `IGNORED` therefore records the routing decision without changing source-event identity.

## Verification Evidence

| Gate | Result |
|---|---|
| Normalization and immutable-model suites | `232 passed` |
| `src.ingestion` coverage gate | `96.28%` (`726` statements, `27` missed; required `>=90%`) |
| Unit and security gate | `1943 passed`, `12 skipped`, `4 warnings` |
| Real-PostgreSQL ingestion gate | `110 passed` |
| Full repository regression | `2260 passed`, `12 skipped`, `4 warnings` |
| Independent default-environment regression | `1971 passed`, `301 skipped`, `4 warnings` after reviewer repaired isolated local binary dependencies; no failures |
| Static gates | Full Ruff check passed; changed-file format check passed; `compileall` and code `git diff --check` passed |
| Independent code review | Critical `0`, Important `0`, Minor `0`; reviewer also passed `476` Task 2 combined unit tests and `110` real-PostgreSQL ingestion tests |
| Combined plan review | Critical `0`, Important `0`, Minor `0` after the frozen successor-consumption, target-reservation, capability/live-barrier, `0015` dual-path and destructive-drift review |

These are current-tree results from the exact uncommitted batch described here. Both code and plan reviews are signed off; the complete staged scope must still be inspected before commit.

## Explicit Non-Actions and Remaining Gates

- No production database migration was applied, no live Webhook or Sync traffic was enabled, no feature flag was cut over, and no deployment was performed.
- `/Users/jarod/Documents/exchange-feishu-extension` remained read-only: it was not modified, committed, started, restarted, or used for a live capability change.
- Read-only cross-repository review proved that the current `/emails/sync` endpoint is not yet compatible with the future strict AI client: `max_changes_returned` is an EWS page size while the service eagerly consumes every generator page into one unbounded list, and EWS `read_flag_change` is returned with `item=None` although this boundary deliberately accepts only the versioned three-kind contract. The AI implementation may continue and be accepted while dormant, but `SYNC_RECONCILIATION` and production `durable_active` activation remain blocked on a versioned `exchange_sync_contract_v2`, a real capability probe, and subsequent ActivationService evidence.
- The current project still lacks durable Inbox repository insertion, ownership/fencing operations, worker execution, atomic Sync cursor advancement, Webhook `202` integration and scheduler activation; those remain later Phase 2 tasks.
- Task 2 is closed at the repository level; production migration, traffic enablement and activation remain explicitly deferred to their later gated tasks.

## Next Task

After the pending Task 2 closeout gates pass, Phase 2 Task 3 implements durable pipeline ownership and fencing. The target architecture remains hybrid: Webhook supplies the low-latency trigger, periodic `/emails/sync` supplies reconciliation, and both converge on the same durable `event_inbox` before downstream processing.
