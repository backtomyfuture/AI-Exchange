---
status: accepted
---

# Operations Console runs locally with read-only production access

The Operations Console is a local-only tool the operator runs on their own machine, not
a route on the production FastAPI app and not a separately deployed service. It reads
production Postgres through a new, dedicated read-only role scoped to the tables a
Pipeline Trace needs, distinct from the runtime and migration/maintenance identities
already kept apart per CLAUDE.md §3.5. For Tier 1 rules it writes directly to the
operator's local `tier1_rules/*.yaml` working tree — the same files an operator edits by
hand today — and leaves committing and deploying to the operator; it does not run `git`
on their behalf and does not gain any write path into the production container or
database. This keeps a convenience tool for a single operator from becoming new
production attack surface or a second place rule files can live, at the cost of the
console only being usable from the machine that has the repo checked out and network
access to a read replica or the primary. If multiple people ever need it, or it needs to
be reachable without that machine, this should be revisited as a standalone service
rather than assumed to generalize for free.
