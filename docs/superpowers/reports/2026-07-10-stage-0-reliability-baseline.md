# Stage 0 Reliability Baseline

- Branch: feat/lark-push-filtering
- Pre-existing tracked modifications: 49 files
- Baseline tests: 378 passed, 12 skipped
- Coverage: 53 percent
- Application RSS: approximately 1.88 GiB of 2 GiB
- PostgreSQL size: approximately 1.99 GiB
- checkpoint_blobs: approximately 1.29 GiB
- checkpoint_writes: approximately 679 MiB
- Known lock state: uv.lock is stale

## Preserved behavioral changes

- Exchange Sent/Drafts Chinese and English folder aliases
- Explicit Lark SDK imports
- Existing formatter and lint cleanup

## Safety rule

No later task may reset or overwrite this baseline commit. Each task stages only its declared paths.

## Phase 1 P0 gate after Task 10 (2026-07-12)

- Remediation branch: `codex/ai-exchange-remediation`
- Full isolated-PostgreSQL test gate: 1286 passed, 12 skipped
- Coverage: 76 percent
- Ruff, `uv lock --check`, `pip check`, `git diff --check`: passed
- Production and development Compose rendering: passed
- Dependency lock: current, 100 packages resolved
- Remaining warnings: four third-party Lark/WebSocket deprecations

### Guarded checkpoint cleanup

- Terminal allowlist is limited to `sent`, `rejected`, and `draft_saved`.
- A candidate must be strictly older than 24 hours, use only the default
  namespace, prove the current slim checkpoint shape, and have empty
  `attachment_tokens` and `pdf_token` cleanup handles.
- Plans are canonical, content-addressed, owner-only artifacts with one-hour
  expiry and hard limits of 100 threads, 500 physical rows, and 64 MiB of
  estimated logical bytes.
- Non-dry execution additionally requires an exact plan, one-shot claim,
  quiesced service declaration, cluster-bound database fingerprint, and an
  HMAC-signed full-database backup receipt bound to the plan and both schema
  revisions.
- Real isolated PostgreSQL tests prove all three checkpoint tables delete in
  one explicit per-thread transaction. A failure injected into the second
  delete rolls the first delete back completely.
- PostgreSQL `DELETE` estimates are reported only as logical bytes. No VACUUM
  or immediate disk-reclamation claim is made.

### Live migration and read-only dry-run evidence

- The existing business database was aligned with the already-tested,
  forward-only Alembic revision `20260710_0002`; the legacy dataset contained
  894 `emails_log` rows. LangGraph migrations remained at complete revision 9.
- Live plan ID:
  `403dbd256e512ae4cac760e47053bf983505e664e449a77fc16e3643bc75b981`.
- Verified plan artifact SHA-256:
  `a1bf886871a1a36b6ad06a6c558b94dbe2ffb769b508fce91fa5471d1024ad6b`.
- The dry-run scanned 894 business rows, selected zero candidates, protected
  862 nonterminal/unknown rows, and failed closed on 32 legacy checkpoint
  shapes.
- Before and after the dry-run, row counts and ordered primary-key/xmin
  metadata digests were identical: 3,246 `checkpoints`, 6,653
  `checkpoint_blobs`, and 12,092 `checkpoint_writes`.
- The report recorded `deleted_thread_count=0`, `vacuum_performed=false`, and
  no error. No live `execute` command was run.
- Private artifacts are stored under
  `~/.local/state/ai-exchange/checkpoint-cleanup` with directory mode 0700 and
  file mode 0600.

### Remaining boundary

This gate proves safe planning, dry-run, and an isolated destructive protocol.
It does not claim automatic online retention, backlog exhaustion, verified
backup-media existence, approval expiry, or resumable cleanup. Those remain in
the Phase 4 and Phase 6 plans. The related
`/Users/jarod/Documents/exchange-feishu-extension` repository remained
read-only throughout Task 10.
