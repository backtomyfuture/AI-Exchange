---
status: accepted
---

# Generate visual summaries lazily while preserving inline images for review

Classify an inbound email from its normalized textual content first, and generate a bounded Visual Summary only after it is classified as a Reply-Required Email. The summary is supplied to drafting and reused by draft review, but raw image bytes and data URIs never enter model text prompts, graph checkpoints, or the vector index. Human recipients receive the original Inline Images in safely rendered Review Material; Inline Images are not duplicated as standalone Drive attachments. Business Attachments are uploaded only after the delivery policy chooses a Feishu Delivery, for both Read Notifications and Draft Approvals, and are skipped when no Feishu card will be sent. This preserves human evidence and drafting context while avoiding unnecessary vision calls, oversized prompts, signature and logo noise, and premature external effects.

Treat an explicit `is_inline` value as authoritative: `false` remains a Business Attachment even when Exchange also supplies a `content_id`. When that flag is absent, classify an attachment as inline only when its content ID is actually referenced by the email body. Upload and card rendering must share this classification rule.
