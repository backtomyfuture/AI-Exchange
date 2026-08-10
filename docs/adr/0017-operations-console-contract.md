---
status: accepted
---

# Operations Console contract and read-only role

The Operations Console is implemented as two local modules inside this
repository: `console_api/` is a small FastAPI application and `console-web/`
is a Vite/React single-page application. The API binds only to loopback in
the documented run command and is not mounted into `src.server` or shipped in
the production Compose image.

The API exposes two read-only projection interfaces:

- `GET /api/emails` lists one bounded row per durable inbox identity.
- `GET /api/emails/{external_email_id}/trace` assembles the fixed business-stage
  graph from `event_inbox`, `intake_decisions`, `emails`, `tier1_decisions`,
  `handoff_runs`, `handoff_executions`, `execution_payload_revisions`,
  `approved_execution_envelopes`, `audit_events`, and the legacy
  `emails_log` enrichment row.

The rule interface reads and writes only the local `tier1_rules/` working tree.
`POST /api/rules/validate` runs the real `compile_registry()` pipeline in a
temporary copy and does not publish an artifact. `POST /api/rules/compile` may
write a digest-addressed local artifact, but neither endpoint restarts,
signals, or hot-reloads the production process. An enabled Rule Draft cannot
be saved unless that same compiler accepts the complete registry.

The API uses a dedicated `ai_exchange_operations_console` PostgreSQL login.
`scripts/provision_operations_console.py` creates the role explicitly from an
administrator-owned DSN file, grants `SELECT` only on the trace tables, and
writes the resulting DSN to a private local file. The role is not added to
the production container environment, the runtime role set, or migration
credentials. If the postcondition proves the effective privilege scope is
wider than the allowlist, provisioning fails closed.
