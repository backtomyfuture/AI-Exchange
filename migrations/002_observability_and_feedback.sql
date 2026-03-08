-- Migration 002: Routing observability + feedback loop
-- Covers: Phase 1 (routing), Phase 2 (feedback), Phase 5 (style)
--
-- Run with: psql -U user -d email_agent -f migrations/002_observability_and_feedback.sql
-- Idempotent: safe to run multiple times.

BEGIN;

-- Phase 1: Routing observability ------------------------------------------------
ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS routing_log JSONB;
ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS active_skills JSONB;

-- Phase 2: Feedback loop ---------------------------------------------------------
ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS original_draft TEXT;
ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS final_draft TEXT;
ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS draft_diff TEXT;
ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS approver_user_id TEXT;
ALTER TABLE emails_log ADD COLUMN IF NOT EXISTS rejection_reason TEXT;

-- Index for feedback analysis queries
CREATE INDEX IF NOT EXISTS idx_emails_log_status_processed
    ON emails_log (status, processed_at DESC);

COMMIT;
