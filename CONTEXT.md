# AI Email Assistance

This context turns an inbound email into machine reasoning and human review material without confusing embedded presentation assets with business attachments.

## Language

**Inbound Email**:
The complete message received from Exchange, including its envelope, body, inline images, and business attachments.

**Historical Email**:
A previously sent or received email, including its message-thread context, used as past evidence for email assistance and live RAG retrieval. It excludes instant-message conversations such as Feishu or WeChat.
_Avoid_: Chat history, instant-message history

**Historical Email Import**:
An explicitly initiated, normally one-time initialization operation that loads a bounded set of Historical Email into Qdrant. It is manual and never a service-scheduled or automatic synchronization.
_Avoid_: Mailbox sync, background ingestion

**Greenfield Deployment**:
An independently initialized AI Email Assistance installation with no retained application database or runtime state. Historical Email, when wanted, enters it only through an explicit Historical Email Import.
_Avoid_: In-place upgrade, implicit data migration

**Memory Learning**:
An explicitly enabled, operator-initiated analysis that derives Preference Memory, Style Profile, or processing experience from stored email evidence. It is not performed implicitly while processing an Inbound Email.
_Avoid_: Automatic adaptation, implicit profiling

**Discovered Skill Candidate**:
A proposed Declarative Tier 1 Skill inferred from Historical Email, which may include a proposed forward action and fixed recipient. It remains outside `tier1_rules` and does not participate in email processing until manually promoted.
_Avoid_: Enabled Skill, automatic rule

**Time-Split Validation**:
A chronological check of a Discovered Skill Candidate: the earliest 80% of Historical Email is used for discovery, and the newest 20% is replayed to show matches, observed reply rate, and examples. It informs operator selection but has no automatic pass threshold.
_Avoid_: Random split, automatic approval

**Historical Skill Discovery**:
A manually initiated, normally one-time analysis of Historical Email that uses the configured LLM by default to infer Discovered Skill Candidates. It does not itself enable any routing rule.
_Avoid_: Continuous learning, automatic rule creation

**Historical RAG Context**:
The shared Qdrant corpus of Historical Email used to retrieve relevant prior examples while processing a new Inbound Email. It is not limited to skill discovery.
_Avoid_: Discovery-only corpus, separate historical index

**Declarative Tier 1 Skill**:
A production routing rule represented by bounded data rather than executable `handler.py`. It may match an email and declare one Canonical Route Decision, including a versioned Handoff Profile for a writing route; it never sends email or names arbitrary executable code.
_Avoid_: Generated code, automatic sending

**Intake Guard**:
A conservative deterministic gate after durable normalization and deduplication but before business routing, retrieval, or model calls. It returns only pass, suppress, or quarantine; it does not classify business intent or perform fuzzy spam detection.
_Avoid_: Tier 1 no_action, spam classifier

**Intake Decision**:
The append-only result of one Intake Guard evaluation for one inbox execution epoch. A quarantine release creates a new execution epoch and release record instead of rewriting the original decision.
_Avoid_: Retry status, mutable quarantine flag

**Canonical Route Decision**:
The single immutable answer from Tier 1, Historical Route Consensus, or Tier 3 that states reply, forward, read_only, no_action, or manual_review. It is persisted before profile, retrieval, model, graph, or user-visible effects and cannot be changed by downstream failure.
_Avoid_: Classification projection, graph next_step

**Historical Route Consensus**:
Tier 2 voting over immutable Canonical Route Decision labels from distinct Historical Email. It runs only after Tier 1 abstains; duplicate evidence contributes at most one vote, conflicting labels fail closed, and it is separate from writing retrieval.
_Avoid_: Historical RAG Context, writing style retrieval

**Routing Assessment**:
A bounded advisory assessment of the current Inbound Email, the mailbox owner's recipient
relationship, Tier 1 abstention, and Tier 2 Historical RAG evidence. It is supplied only to
the Tier 3 fallback and cannot authorize a route, handoff profile, or recipient.
_Avoid_: Canonical Route Decision, Evidence Pack

**Routing Evidence Bundle**:
The one bounded retrieval result shared by Historical Route Consensus and Tier 3. It preserves
thread and semantic Historical Email snippets even when no historical route consensus exists,
and records partial or unavailable retrieval instead of treating it as empty history.
_Avoid_: Evidence Pack, Historical Route Consensus

**Handoff Profile**:
A read-only, versioned writing contract selected by a reply or forward Canonical Route Decision. It names only registered evidence sources and a bounded writer mode; it cannot point to arbitrary Python, URLs, credentials, or scripts.
_Avoid_: Executable Tier 1 handler, RAG result

**Handoff Plan**:
The immutable per-email expansion of a Handoff Profile, persisted with a canonical digest before evidence collection. It controls which registered evidence sources and writer behavior are permitted without acquiring routing authority.
_Avoid_: Route decision, mutable graph state

**Evidence Pack**:
The immutable, digest-addressed facts collected by registered read-only adapters under a Handoff Plan. It may support drafting and draft review but cannot encode or change route, profile, or recipients.
_Avoid_: Tier 2 vote, prompt-owned facts

**Execution Payload Revision**:
An append-only frozen approval payload containing the exact decision, plan and evidence bindings, draft, To/Cc recipients, and attachment manifest shown for approval. Any edit creates a new revision and invalidates approval actions bound to an older revision.
_Avoid_: Mutable checkpoint draft, card display state

**Approved Execution Envelope**:
The immutable envelope created transactionally when a human approves an exact Execution Payload Revision. The sender may consume this envelope only, never mutable graph draft or recipient fields.
_Avoid_: Approval status flag, reconstructed send request

**Draft Quality Gate**:
The pre-approval rule and LLM review of a draft against the original email and Evidence Pack. It may mark the draft ready, request a rewrite, or require manual review, but cannot alter route, profile, or recipients.
_Avoid_: Tier 4, execution authorization

**Execution Gate**:
The deterministic validation immediately before an approved external send. It verifies the frozen envelope and digests and may block or require manual review, but never rewrites human-approved content or reroutes the email.
_Avoid_: LLM reviewer, final routing tier

**Proposed Forward Target**:
A forward action and fixed recipient inferred from Historical Email as part of a Discovered Skill Candidate. It is only a suggestion until the operator confirms or edits it during Skill Promotion.
_Avoid_: Approved recipient, automatic forwarding

**Candidate Review**:
The conversational presentation of every field that would become part of a Declarative Tier 1 Skill: triggers, priority, reply requirement, action, and fixed recipients. The operator may edit those fields before selection, and promotion uses exactly the reviewed values.
_Avoid_: Hidden inference, name-only confirmation

**Skill Promotion**:
An operator's explicit selection of a reviewed, time-split-validated Discovered Skill Candidate in a conversation, which then makes it an enabled Declarative Tier 1 Skill.
_Avoid_: Auto-confirmation, discovery run, separate promotion CLI

**Skill Promotion Conflict**:
A promotion attempt whose target rule ID already exists in `tier1_rules`. It stops and is shown to the operator; it never overwrites or merges the existing rule automatically.
_Avoid_: Silent overwrite, automatic merge

**Skill Activation**:
The loading of any `tier1_rules/` change — from Skill Promotion or from a directly authored Rule Draft — at a planned service restart. No path hot-reloads rules into a running email-processing service.
_Avoid_: Hot reload, mid-processing rule switch

**Rule Draft**:
A Declarative Tier 1 Skill created or edited directly through the Operations Console, independent of a Discovered Skill Candidate. It may be saved incomplete, but must pass the same schema, fixture, and conflict validation as any other rule before its status can become enabled.
_Avoid_: Discovered Skill Candidate, proposed candidate

**Operations Console**:
A local-only, single-operator web tool that renders an Inbound Email's Pipeline Trace and authors Rule Drafts. It has read-only access to production data, writes rule changes only to the same `tier1_rules/` files an operator would otherwise hand-edit, and never restarts or hot-reloads the email-processing service.
_Avoid_: Admin dashboard, production console, hosted panel

**Pipeline Trace**:
The assembled, business-stage view of one Inbound Email's journey — ingestion, Intake Guard, Canonical Route Decision, Handoff Plan and Evidence Pack, draft revisions, approval, and send outcome — read from existing durable tables and replayed in the Operations Console. It is not a live stream of LangGraph node execution.
_Avoid_: LangGraph execution trace, live stream, checkpoint replay

**Bounded Recovery**:
Recovery of a processing attempt within explicit durable state boundaries. It may retry a known-safe transient internal failure or be operator-triggered, but it never blindly replays an outcome-unknown Feishu or Exchange external effect.
_Avoid_: SelfHealer, blind reprocessing

**Daily Email Operations Digest**:
A daily operational account of the email-assistance service for one Daily Digest Reporting Window, covering processed volume, pending approvals, failures or backlog, and service health. It uses the Daily Digest Layout and is emitted even when no email was processed in that window.
_Avoid_: Daily Summary (legacy), health notification

**Daily Digest Layout**:
The concise plain-text presentation of a Daily Email Operations Digest: an overview, a direct list of Digest Attention Items, then a compact every-email list. It is not a card or a thematic summary.
_Avoid_: Card dashboard

**Daily Digest Reporting Window**:
The half-open, 24-hour interval from the previous 18:00 inclusive to the current 18:00 exclusive in Asia/Shanghai. Every Inbound Email belongs to exactly one such window.
_Avoid_: Calendar date, rolling day

**Daily Digest Execution**:
A durable record of one Daily Email Operations Digest for one Daily Digest Reporting Window and delivery scope. It permits retries after a failed Daily Digest Delivery Bundle but permits at most one confirmed bundle.
_Avoid_: Scheduler run, email-processing receipt

**Daily Digest Delivery Bundle**:
The ordered set of one or more plain-text Feishu messages that delivers one Daily Email Operations Digest. Each part begins with a Daily Digest Header; a split bundle numbers its parts and is confirmed only after all parts are delivered, while retries send only parts without durable confirmation.
_Avoid_: Independent digest, duplicate notification

**Daily Digest Header**:
The fixed, readable first line of a Daily Digest Delivery Bundle part. It identifies the Daily Digest Reporting Window and, when split, the part number, serving both the reader and delivery reconciliation.
_Avoid_: Opaque machine token, card header

**Digest Delivery Reconciliation**:
A read-only check of bot-authored Daily Digest Delivery Bundle messages in the configured Feishu chat, matched by Daily Digest Header, to resolve an unknown part before retrying. It does not import, analyze, or expose unrelated chat history.
_Avoid_: Chat-history import, message analytics

**Backfilled Daily Digest**:
A Daily Email Operations Digest delivered after its scheduled time. It identifies its original Daily Digest Reporting Window and is explicitly marked as a backfill.
_Avoid_: Current daily digest

**Missed Daily Digest**:
A Daily Digest Execution that did not achieve confirmed delivery before the next scheduled digest window. It is not delivered separately; the following Daily Email Operations Digest reports it as an operational exception.
_Avoid_: Backfilled Daily Digest

**Digest Aggregate**:
A non-identifying count or safe status code included in a Daily Email Operations Digest. It does not contain an individual email's subject, sender, recipients, body, attachments, or content snippet.
_Avoid_: Mail detail, email summary

**Digest Email Item**:
A concise one-line entry in the every-email list for one Inbound Email in a Daily Digest Reporting Window. It states its received or sent time, sender, subject, current status, and processing outcome or next action, but not the email body or attachments.
_Avoid_: Email body, Review Material

**Digest Attention Item**:
An unresolved Inbound Email shown directly in the attention section with its status and next action. It may come from the current Daily Digest Reporting Window or be Historical Backlog; an explicitly saved draft is handled, not a Digest Attention Item, and this is not a thematic or model-generated summary.
_Avoid_: Topic summary, LLM summary

**Historical Backlog**:
An unresolved Inbound Email received before the current Daily Digest Reporting Window. It appears in 需关注事项 with that label but is not repeated in the current window's every-email list.
_Avoid_: Duplicate email item, lost pending email

**Inline Image**:
A visual asset referenced from the email body and intended to be read in that body position, such as a chart, signature, or logo. It is not a Business Attachment.
_Avoid_: Image attachment, embedded attachment

**Business Attachment**:
A file intentionally attached as a separate document for the recipient to open or download.
_Avoid_: Inline image, embedded image

**Visual Summary**:
A bounded textual interpretation of selected email images produced to help the assistant understand their material content. It is derived evidence, not a replacement for the original images.
_Avoid_: Image OCR, image body

**Reply-Required Email**:
An Inbound Email classified as requiring a reply or forward, and therefore eligible for drafting and Visual Summary generation.
_Avoid_: Important email, approval email

**Feishu Delivery**:
A user-facing Feishu message that surfaces an Inbound Email or a Daily Email Operations Digest, as an Email Feishu Delivery or concise daily digest message.
_Avoid_: Visual analysis, model result

**Email Feishu Delivery**:
A user-facing Feishu card for exactly one Inbound Email, as a Read Notification, Draft Approval, or Manual Review Notification. It excludes Daily Digest Delivery, inbound Feishu events, and card-action handling.
_Avoid_: Daily Digest Delivery, Lark event handling

**Email Delivery Outcome**:
The durable result of attempting one Email Feishu Delivery: confirmed delivery, known failure, or unknown outcome. It is distinct from the Exchange read state; only confirmed delivery can advance delivery state, and an unknown outcome is not safe to replay automatically.
_Avoid_: Email processing outcome, mark-as-read result

**Delivery Resource**:
A rendered Review Material PDF or uploaded Business Attachment referenced by one Email Feishu Delivery. A production Email Feishu Delivery requires its Review Material PDF; staging, preservation, replacement, and cleanup belong to that delivery lifecycle rather than the email-processing graph.
_Avoid_: Graph state, generic temporary file

**Read Notification**:
A Feishu Delivery that asks the recipient to read an email but does not contain a reply draft.
_Avoid_: Draft approval, skipped email

**Draft Approval**:
A Feishu Delivery containing a proposed reply or forward for human review and decision.
_Avoid_: Read notification, automatic reply

**Manual Review Notification**:
An Email Feishu Delivery that presents an unresolved or unsafe processing outcome for human inspection, without offering a draft-approval action or uploading Business Attachments.
_Avoid_: Draft approval, automatic retry

**Review Material**:
The faithful, safely rendered original email presented to a human decision-maker, including Inline Images in their original context.
_Avoid_: Visual summary, attachment list
