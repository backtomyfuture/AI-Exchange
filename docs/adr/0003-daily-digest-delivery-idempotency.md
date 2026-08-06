---
status: accepted
---

# Give each daily digest its own idempotent execution record

Each Daily Digest Execution is persisted separately from email-processing and pipeline-command receipts, with a logical identity for its Daily Digest Reporting Window and delivery scope. A Daily Digest Delivery Bundle may contain one plain-text message or several ordered, numbered parts, each beginning with a fixed Daily Digest Header; each part has durable confirmation, so a failed bundle retries only unconfirmed parts, and recovery automatically resumes a still-pending execution without requiring an operator command. When an outcome remains unknown after the Feishu request-deduplication window, Digest Delivery Reconciliation reads only bot-authored digest messages with the matching header in the configured chat before any retry. Only one confirmed bundle may be recorded for a digest. Any late delivery preserves its original reporting window and is explicitly marked as a backfill. The retry window ends when the next 18:00 digest window begins; an older unconfirmed execution becomes a Missed Daily Digest and is reported as an exception in the next digest rather than delivered separately. This prevents scheduler replay, restart recovery, or send retries from duplicating a group notification while keeping a failed daily report recoverable.
