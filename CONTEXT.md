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
A production routing rule represented by bounded data rather than executable `handler.py`. It may match emails and specify priority, whether a response is needed, a forward action with fixed recipients, or a tone directive; it only prepares a routing or approval plan and never sends email.
_Avoid_: Generated code, automatic sending

**Proposed Forward Target**:
A forward action and fixed recipient inferred from Historical Email as part of a Discovered Skill Candidate. It is only a suggestion until the operator confirms or edits it during Skill Promotion.
_Avoid_: Approved recipient, automatic forwarding

**Candidate Review**:
The conversational presentation of every field that would become part of a Declarative Tier 1 Skill: triggers, priority, reply requirement, tone, action, and fixed recipients. The operator may edit those fields before selection, and promotion uses exactly the reviewed values.
_Avoid_: Hidden inference, name-only confirmation

**Skill Promotion**:
An operator's explicit selection of a reviewed, time-split-validated Discovered Skill Candidate in a conversation, which then makes it an enabled Declarative Tier 1 Skill.
_Avoid_: Auto-confirmation, discovery run, separate promotion CLI

**Skill Promotion Conflict**:
A promotion attempt whose target rule ID already exists in `tier1_rules`. It stops and is shown to the operator; it never overwrites or merges the existing rule automatically.
_Avoid_: Silent overwrite, automatic merge

**Skill Activation**:
The loading of a promoted Declarative Tier 1 Skill at a planned service restart. Promotion does not hot-reload rules into a running email-processing service.
_Avoid_: Hot reload, mid-processing rule switch

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
