# AI Email Assistance

This context turns an inbound email into machine reasoning and human review material without confusing embedded presentation assets with business attachments.

## Language

**Inbound Email**:
The complete message received from Exchange, including its envelope, body, inline images, and business attachments.

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
A user-facing Feishu card that surfaces an Inbound Email, either as a Read Notification or a Draft Approval.
_Avoid_: Visual analysis, model result

**Read Notification**:
A Feishu Delivery that asks the recipient to read an email but does not contain a reply draft.
_Avoid_: Draft approval, skipped email

**Draft Approval**:
A Feishu Delivery containing a proposed reply or forward for human review and decision.
_Avoid_: Read notification, automatic reply

**Review Material**:
The faithful, safely rendered original email presented to a human decision-maker, including Inline Images in their original context.
_Avoid_: Visual summary, attachment list
