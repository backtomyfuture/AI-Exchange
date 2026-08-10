---
status: accepted
---

# Pipeline Trace is built from business tables, not LangGraph checkpoint history

A Pipeline Trace renders `event_inbox` -> `emails` -> `tier1_decisions` ->
`handoff_runs`/`handoff_executions` -> `execution_payload_revisions` ->
`approved_execution_envelopes` -> `audit_events`, joined by `inbox_id`/`external_email_id`,
as its node graph — not `aget_state_history`/raw `checkpoints`/`checkpoint_blobs` rows
from the Postgres Checkpointer. No code in this repo has ever read checkpoint history
this way, and `tier1_decisions` already carries the tier, matched rule IDs, confidence,
and evidence IDs a trace needs, while `handoff_runs.state` is already a state machine
(`planned -> evidence_ready -> manual_review/approval_pending -> approved/rejected ->
draft_saving -> draft_saved -> executing -> completed/failed`) that maps cleanly onto
graph nodes without any new backend instrumentation. This trades literal fidelity to the
`categorizer -> retriever -> drafter -> reviewer` LangGraph node names for a
business-meaning trace (why a route was chosen, which evidence backed it) built entirely
on data that already exists and is already the answer to "what happened to this email,"
which is what motivated the console in the first place. If node-level LangGraph fidelity
is wanted later, it is a separate, additive data source behind the same trace view, not
a replacement for it.
