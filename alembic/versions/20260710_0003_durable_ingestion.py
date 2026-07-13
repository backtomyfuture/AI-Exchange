"""Add the expand-only durable ingestion core schema."""

from __future__ import annotations

from alembic import op


revision = "20260710_0003"
down_revision = "20260710_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE pipeline_ownership (
            account_id pg_catalog.int8 NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            pipeline_name pg_catalog.text NOT NULL,
            state pg_catalog.text NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            created_by pg_catalog.text NOT NULL,
            reason pg_catalog.text,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_ownership
                PRIMARY KEY (account_id, generation),
            CONSTRAINT uq_pipeline_ownership_fence
                UNIQUE (account_id, fencing_token),
            CONSTRAINT uq_pipeline_ownership_generation_fence
                UNIQUE (account_id, generation, fencing_token),
            CONSTRAINT uq_pipeline_ownership_event_identity
                UNIQUE (account_id, generation, fencing_token, pipeline_name),
            CONSTRAINT ck_pipeline_ownership_positive_identity CHECK (
                account_id > 0 AND generation > 0 AND fencing_token > 0
            ),
            CONSTRAINT ck_pipeline_ownership_pipeline_name CHECK (
                pg_catalog.btrim(pipeline_name) <> ''
                AND pg_catalog.char_length(pipeline_name) <= 64
            ),
            CONSTRAINT ck_pipeline_ownership_state CHECK (
                state IN (
                    'current_ingress', 'quiescing', 'draining', 'retired'
                )
            ),
            CONSTRAINT ck_pipeline_ownership_created_by CHECK (
                pg_catalog.btrim(created_by) <> ''
                AND pg_catalog.char_length(created_by) <= 128
            ),
            CONSTRAINT ck_pipeline_ownership_reason CHECK (
                reason IS NULL OR (
                    pg_catalog.btrim(reason) <> ''
                    AND pg_catalog.char_length(reason) <= 512
                )
            )
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_pipeline_current_ingress
        ON pipeline_ownership(account_id)
        WHERE state = 'current_ingress'
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_pipeline_ownership()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path FROM CURRENT
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.state <> 'current_ingress' THEN
                    RAISE EXCEPTION 'pipeline ownership must start current';
                END IF;
                RETURN NEW;
            END IF;

            IF TG_OP = 'UPDATE' THEN
                IF NEW.account_id IS DISTINCT FROM OLD.account_id
                   OR NEW.generation IS DISTINCT FROM OLD.generation
                   OR NEW.pipeline_name IS DISTINCT FROM OLD.pipeline_name
                   OR NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
                   OR NEW.created_by IS DISTINCT FROM OLD.created_by
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'pipeline ownership identity is immutable';
                END IF;
                IF NOT (
                    (OLD.state = 'current_ingress' AND NEW.state = 'quiescing')
                    OR (OLD.state = 'quiescing' AND NEW.state = 'draining')
                    OR (OLD.state = 'draining' AND NEW.state = 'retired')
                ) THEN
                    RAISE EXCEPTION 'pipeline ownership transition rejected';
                END IF;
                RETURN NEW;
            END IF;

            RAISE EXCEPTION 'pipeline ownership history is immutable';
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_ownership_guard_row
        BEFORE INSERT OR UPDATE OR DELETE ON pipeline_ownership
        FOR EACH ROW EXECUTE FUNCTION guard_pipeline_ownership()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_ownership_guard_truncate
        BEFORE TRUNCATE ON pipeline_ownership
        FOR EACH STATEMENT EXECUTE FUNCTION guard_pipeline_ownership()
        """
    )

    op.execute(
        """
        CREATE TABLE event_inbox (
            id pg_catalog.uuid NOT NULL,
            account_id pg_catalog.int8 NOT NULL,
            external_email_id pg_catalog.text NOT NULL,
            folder_key pg_catalog.text NOT NULL,
            source pg_catalog.text NOT NULL,
            raw_event_type pg_catalog.text NOT NULL,
            change_kind pg_catalog.text NOT NULL,
            dedupe_key pg_catalog.bpchar(64) NOT NULL,
            source_version pg_catalog.text,
            source_event_at pg_catalog.timestamptz,
            payload pg_catalog.jsonb NOT NULL DEFAULT '{}'::pg_catalog.jsonb,
            processing_policy pg_catalog.text NOT NULL,
            pipeline_name pg_catalog.text NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            status pg_catalog.text NOT NULL,
            lease_owner pg_catalog.text,
            lease_until pg_catalog.timestamptz,
            attempts pg_catalog.int8 NOT NULL DEFAULT 0,
            available_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            processing_started_at pg_catalog.timestamptz,
            effect_started_at pg_catalog.timestamptz,
            safe_error_code pg_catalog.text,
            safe_error_summary pg_catalog.text,
            received_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_event_inbox PRIMARY KEY (id),
            CONSTRAINT uq_event_inbox_dedupe UNIQUE (dedupe_key),
            CONSTRAINT uq_event_inbox_processing_identity UNIQUE (
                id,
                account_id,
                external_email_id,
                generation,
                fencing_token
            ),
            CONSTRAINT fk_event_inbox_pipeline_ownership FOREIGN KEY (
                account_id,
                generation,
                fencing_token,
                pipeline_name
            ) REFERENCES pipeline_ownership (
                account_id,
                generation,
                fencing_token,
                pipeline_name
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_event_inbox_positive_identity CHECK (
                account_id > 0 AND generation > 0 AND fencing_token > 0
            ),
            CONSTRAINT ck_event_inbox_external_email_id CHECK (
                pg_catalog.btrim(external_email_id) <> ''
                AND pg_catalog.char_length(external_email_id) <= 1024
            ),
            CONSTRAINT ck_event_inbox_folder_key CHECK (
                pg_catalog.btrim(folder_key) <> ''
                AND pg_catalog.char_length(folder_key) <= 512
            ),
            CONSTRAINT ck_event_inbox_source CHECK (
                source IN ('webhook', 'sync', 'backfill')
            ),
            CONSTRAINT ck_event_inbox_raw_event_type CHECK (
                pg_catalog.btrim(raw_event_type) <> ''
                AND pg_catalog.char_length(raw_event_type) <= 128
            ),
            CONSTRAINT ck_event_inbox_change_kind CHECK (
                change_kind IN ('create', 'update', 'read', 'delete')
            ),
            CONSTRAINT ck_event_inbox_dedupe_key CHECK (
                dedupe_key::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
            ),
            CONSTRAINT ck_event_inbox_source_version CHECK (
                source_version IS NULL OR (
                    pg_catalog.btrim(source_version) <> ''
                    AND pg_catalog.char_length(source_version) <= 512
                )
            ),
            CONSTRAINT ck_event_inbox_payload CHECK (
                pg_catalog.jsonb_typeof(payload) = 'object'
                AND pg_catalog.octet_length(payload::pg_catalog.text) <= 262144
            ),
            CONSTRAINT ck_event_inbox_processing_policy CHECK (
                processing_policy IN (
                    'full', 'archive', 'metadata_only', 'historical_suppressed'
                )
            ),
            CONSTRAINT ck_event_inbox_pipeline_name CHECK (
                pg_catalog.btrim(pipeline_name) <> ''
                AND pg_catalog.char_length(pipeline_name) <= 64
            ),
            CONSTRAINT ck_event_inbox_status CHECK (
                status IN (
                    'pending', 'retry_wait', 'leased', 'completed',
                    'dead_letter', 'manual_review'
                )
            ),
            CONSTRAINT ck_event_inbox_lease CHECK (
                (
                    status = 'leased'
                    AND lease_owner IS NOT NULL
                    AND lease_until IS NOT NULL
                    AND pg_catalog.btrim(lease_owner) <> ''
                    AND pg_catalog.char_length(lease_owner) <= 128
                ) OR (
                    status <> 'leased'
                    AND lease_owner IS NULL
                    AND lease_until IS NULL
                )
            ),
            CONSTRAINT ck_event_inbox_attempts CHECK (attempts >= 0),
            CONSTRAINT ck_event_inbox_effect_order CHECK (
                effect_started_at IS NULL OR (
                    processing_started_at IS NOT NULL
                    AND processing_started_at <= effect_started_at
                    AND status IN ('leased', 'completed', 'manual_review')
                )
            ),
            CONSTRAINT ck_event_inbox_error CHECK (
                safe_error_summary IS NULL OR safe_error_code IS NOT NULL
            ),
            CONSTRAINT ck_event_inbox_error_state CHECK (
                (
                    status IN ('retry_wait', 'dead_letter', 'manual_review')
                    AND safe_error_code IS NOT NULL
                    AND pg_catalog.btrim(safe_error_code) <> ''
                    AND pg_catalog.char_length(safe_error_code) <= 64
                    AND (
                        safe_error_summary IS NULL
                        OR (
                            pg_catalog.btrim(safe_error_summary) <> ''
                            AND pg_catalog.char_length(safe_error_summary) <= 256
                        )
                    )
                ) OR (
                    status IN ('pending', 'leased', 'completed')
                    AND safe_error_code IS NULL
                    AND safe_error_summary IS NULL
                )
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_event_inbox_claim
        ON event_inbox(
            pipeline_name, status, available_at, received_at, id
        )
        WHERE status IN ('pending', 'retry_wait')
        """
    )
    op.execute(
        """
        CREATE INDEX ix_event_inbox_expired_lease
        ON event_inbox(lease_until, id)
        WHERE status = 'leased'
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_event_inbox_update()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path FROM CURRENT
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.account_id IS DISTINCT FROM OLD.account_id
               OR NEW.external_email_id IS DISTINCT FROM OLD.external_email_id
               OR NEW.folder_key IS DISTINCT FROM OLD.folder_key
               OR NEW.source IS DISTINCT FROM OLD.source
               OR NEW.raw_event_type IS DISTINCT FROM OLD.raw_event_type
               OR NEW.change_kind IS DISTINCT FROM OLD.change_kind
               OR NEW.dedupe_key IS DISTINCT FROM OLD.dedupe_key
               OR NEW.source_version IS DISTINCT FROM OLD.source_version
               OR NEW.source_event_at IS DISTINCT FROM OLD.source_event_at
               OR NEW.payload IS DISTINCT FROM OLD.payload
               OR NEW.processing_policy IS DISTINCT FROM OLD.processing_policy
               OR NEW.pipeline_name IS DISTINCT FROM OLD.pipeline_name
               OR NEW.generation IS DISTINCT FROM OLD.generation
               OR NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
               OR NEW.received_at IS DISTINCT FROM OLD.received_at THEN
                RAISE EXCEPTION 'event inbox identity is immutable';
            END IF;
            IF OLD.processing_started_at IS NOT NULL
               AND NEW.processing_started_at IS DISTINCT FROM OLD.processing_started_at THEN
                RAISE EXCEPTION 'processing marker is immutable once set';
            END IF;
            IF OLD.effect_started_at IS NOT NULL
               AND NEW.effect_started_at IS DISTINCT FROM OLD.effect_started_at THEN
                RAISE EXCEPTION 'effect marker is immutable once set';
            END IF;
            IF NEW.attempts < OLD.attempts THEN
                RAISE EXCEPTION 'event attempts cannot decrease';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_event_inbox_guard_update
        BEFORE UPDATE ON event_inbox
        FOR EACH ROW EXECUTE FUNCTION guard_event_inbox_update()
        """
    )

    op.execute(
        """
        CREATE TABLE sync_cursors (
            account_id pg_catalog.int8 NOT NULL,
            folder_key pg_catalog.text NOT NULL,
            cursor pg_catalog.text,
            status pg_catalog.text NOT NULL,
            blocked_reason_code pg_catalog.text,
            contract_fingerprint pg_catalog.bpchar(64),
            blocked_at pg_catalog.timestamptz,
            version pg_catalog.int8 NOT NULL DEFAULT 0,
            last_success_at pg_catalog.timestamptz,
            last_attempt_at pg_catalog.timestamptz,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_sync_cursors PRIMARY KEY (account_id, folder_key),
            CONSTRAINT ck_sync_cursors_account CHECK (account_id > 0),
            CONSTRAINT ck_sync_cursors_folder_key CHECK (
                pg_catalog.btrim(folder_key) <> ''
                AND pg_catalog.char_length(folder_key) <= 512
            ),
            CONSTRAINT ck_sync_cursors_cursor CHECK (
                cursor IS NULL OR (
                    pg_catalog.btrim(cursor) <> ''
                    AND pg_catalog.char_length(cursor) <= 8192
                )
            ),
            CONSTRAINT ck_sync_cursors_status CHECK (
                status IN (
                    'active', 'reset_required', 'cold_start_pending',
                    'blocked_contract'
                )
            ),
            CONSTRAINT ck_sync_cursors_reason CHECK (
                blocked_reason_code IS NULL OR (
                    pg_catalog.btrim(blocked_reason_code) <> ''
                    AND pg_catalog.char_length(blocked_reason_code) <= 64
                )
            ),
            CONSTRAINT ck_sync_cursors_fingerprint CHECK (
                contract_fingerprint IS NULL OR (
                    contract_fingerprint::pg_catalog.text
                        OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                )
            ),
            CONSTRAINT ck_sync_cursors_version CHECK (version >= 0),
            CONSTRAINT ck_sync_cursors_state_matrix CHECK (
                (
                    status = 'active'
                    AND cursor IS NOT NULL
                    AND last_success_at IS NOT NULL
                    AND blocked_reason_code IS NULL
                    AND contract_fingerprint IS NULL
                    AND blocked_at IS NULL
                    AND (
                        last_attempt_at IS NULL
                        OR last_attempt_at >= last_success_at
                    )
                ) OR (
                    status = 'reset_required'
                    AND cursor IS NOT NULL
                    AND blocked_reason_code IS NOT NULL
                    AND last_attempt_at IS NOT NULL
                    AND contract_fingerprint IS NULL
                    AND blocked_at IS NULL
                ) OR (
                    status = 'cold_start_pending'
                    AND cursor IS NULL
                    AND blocked_reason_code IS NOT NULL
                    AND contract_fingerprint IS NULL
                    AND blocked_at IS NULL
                ) OR (
                    status = 'blocked_contract'
                    AND blocked_reason_code IS NOT NULL
                    AND contract_fingerprint IS NOT NULL
                    AND blocked_at IS NOT NULL
                )
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_sync_cursors_status_attempt
        ON sync_cursors(status, last_attempt_at)
        """
    )

    op.execute(
        """
        CREATE TABLE emails (
            id pg_catalog.uuid NOT NULL,
            account_id pg_catalog.int8 NOT NULL,
            external_email_id pg_catalog.text NOT NULL,
            source_folder_key pg_catalog.text NOT NULL,
            status pg_catalog.text NOT NULL,
            version pg_catalog.int8 NOT NULL DEFAULT 0,
            owner_generation pg_catalog.int8 NOT NULL,
            owner_fencing_token pg_catalog.int8 NOT NULL,
            processing_inbox_id pg_catalog.uuid,
            create_seen_at pg_catalog.timestamptz,
            processing_started_at pg_catalog.timestamptz,
            source_deleted_at pg_catalog.timestamptz,
            external_effects_started_at pg_catalog.timestamptz,
            safe_error_code pg_catalog.text,
            safe_error_summary pg_catalog.text,
            content_ref pg_catalog.jsonb,
            is_read pg_catalog.bool,
            is_read_refresh_required pg_catalog.bool NOT NULL DEFAULT false,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_emails PRIMARY KEY (id),
            CONSTRAINT uq_email_external UNIQUE (account_id, external_email_id),
            CONSTRAINT uq_emails_account_id UNIQUE (account_id, id),
            CONSTRAINT uq_emails_outbox_identity UNIQUE (
                id, account_id, owner_generation, owner_fencing_token
            ),
            CONSTRAINT fk_emails_pipeline_ownership FOREIGN KEY (
                account_id, owner_generation, owner_fencing_token
            ) REFERENCES pipeline_ownership (
                account_id, generation, fencing_token
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT fk_emails_processing_inbox FOREIGN KEY (
                processing_inbox_id,
                account_id,
                external_email_id,
                owner_generation,
                owner_fencing_token
            ) REFERENCES event_inbox (
                id,
                account_id,
                external_email_id,
                generation,
                fencing_token
            ) MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_emails_positive_identity CHECK (
                account_id > 0
                AND owner_generation > 0
                AND owner_fencing_token > 0
            ),
            CONSTRAINT ck_emails_external_email_id CHECK (
                pg_catalog.btrim(external_email_id) <> ''
                AND pg_catalog.char_length(external_email_id) <= 1024
            ),
            CONSTRAINT ck_emails_source_folder_key CHECK (
                pg_catalog.btrim(source_folder_key) <> ''
                AND pg_catalog.char_length(source_folder_key) <= 512
            ),
            CONSTRAINT ck_emails_status CHECK (
                status IN (
                    'ingested', 'processing', 'retry_wait', 'manual_review',
                    'waiting_approval', 'notified_readonly', 'send_queued',
                    'sending', 'accepted', 'sent', 'send_failed',
                    'delivery_failed', 'send_unknown', 'no_action', 'archived',
                    'rejected', 'draft_saved', 'expired', 'cancelled',
                    'dead_letter'
                )
            ),
            CONSTRAINT ck_emails_version CHECK (version >= 0),
            CONSTRAINT ck_emails_error CHECK (
                safe_error_summary IS NULL OR safe_error_code IS NOT NULL
            ),
            CONSTRAINT ck_emails_processing_state CHECK (
                (
                    status = 'processing'
                    AND processing_inbox_id IS NOT NULL
                    AND safe_error_code IS NULL
                    AND safe_error_summary IS NULL
                ) OR (
                    status IN ('retry_wait', 'manual_review', 'dead_letter')
                    AND processing_inbox_id IS NOT NULL
                    AND safe_error_code IS NOT NULL
                    AND pg_catalog.btrim(safe_error_code) <> ''
                    AND pg_catalog.char_length(safe_error_code) <= 64
                    AND (
                        safe_error_summary IS NULL
                        OR (
                            pg_catalog.btrim(safe_error_summary) <> ''
                            AND pg_catalog.char_length(safe_error_summary) <= 256
                        )
                    )
                ) OR (
                    status NOT IN (
                        'processing', 'retry_wait', 'manual_review', 'dead_letter'
                    )
                    AND processing_inbox_id IS NULL
                    AND safe_error_code IS NULL
                    AND safe_error_summary IS NULL
                )
            ),
            CONSTRAINT ck_emails_content_ref CHECK (
                content_ref IS NULL
                OR pg_catalog.jsonb_typeof(content_ref) = 'object'
            ),
            CONSTRAINT ck_emails_read_projection CHECK (
                is_read IS NOT NULL OR is_read_refresh_required
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_emails_account_status
        ON emails(account_id, status, updated_at, id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_emails_owner_status
        ON emails(account_id, owner_generation, status)
        """
    )
    op.execute(
        """
        CREATE FUNCTION enforce_email_processing_owner()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path FROM CURRENT
        AS $$
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF NEW.id IS DISTINCT FROM OLD.id
                   OR NEW.account_id IS DISTINCT FROM OLD.account_id
                   OR NEW.external_email_id IS DISTINCT FROM OLD.external_email_id
                   OR NEW.owner_generation IS DISTINCT FROM OLD.owner_generation
                   OR NEW.owner_fencing_token IS DISTINCT FROM OLD.owner_fencing_token
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'email ownership identity is immutable';
                END IF;
                IF OLD.status <> 'ingested' AND NEW.status = 'ingested' THEN
                    RAISE EXCEPTION 'email cannot return to ingested state';
                END IF;
                IF (OLD.create_seen_at IS NOT NULL
                       AND NEW.create_seen_at IS DISTINCT FROM OLD.create_seen_at)
                   OR (OLD.processing_started_at IS NOT NULL
                       AND NEW.processing_started_at IS DISTINCT FROM OLD.processing_started_at)
                   OR (OLD.source_deleted_at IS NOT NULL
                       AND NEW.source_deleted_at IS DISTINCT FROM OLD.source_deleted_at)
                   OR (OLD.external_effects_started_at IS NOT NULL
                       AND NEW.external_effects_started_at
                           IS DISTINCT FROM OLD.external_effects_started_at) THEN
                    RAISE EXCEPTION 'email processing facts are immutable once recorded';
                END IF;
                IF OLD.processing_inbox_id IS NOT NULL
                   AND NEW.processing_inbox_id IS NOT NULL
                   AND NEW.processing_inbox_id IS DISTINCT FROM OLD.processing_inbox_id THEN
                    RAISE EXCEPTION 'processing owner cannot be replaced';
                END IF;
                IF OLD.processing_inbox_id IS NULL
                   AND NEW.processing_inbox_id IS NOT NULL
                   AND NOT (
                       OLD.status = 'ingested' AND NEW.status = 'processing'
                   ) THEN
                    RAISE EXCEPTION 'terminal email cannot be reopened';
                END IF;
            END IF;

            IF NEW.processing_inbox_id IS NOT NULL AND NOT EXISTS (
                SELECT 1
                FROM event_inbox AS inbox
                WHERE inbox.id = NEW.processing_inbox_id
                  AND inbox.account_id = NEW.account_id
                  AND inbox.external_email_id = NEW.external_email_id
                  AND inbox.generation = NEW.owner_generation
                  AND inbox.fencing_token = NEW.owner_fencing_token
                  AND inbox.change_kind = 'create'
            ) THEN
                RAISE EXCEPTION 'processing owner must be a create event';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER trg_emails_processing_owner
        AFTER INSERT OR UPDATE ON emails
        FOR EACH ROW EXECUTE FUNCTION enforce_email_processing_owner()
        """
    )

    op.execute(
        """
        CREATE TABLE audit_events (
            id pg_catalog.uuid NOT NULL,
            event_key pg_catalog.bpchar(64) NOT NULL,
            account_id pg_catalog.int8 NOT NULL,
            email_id pg_catalog.uuid,
            object_type pg_catalog.text NOT NULL,
            object_fingerprint pg_catalog.bpchar(64) NOT NULL,
            action pg_catalog.text NOT NULL,
            result pg_catalog.text NOT NULL,
            actor pg_catalog.text NOT NULL,
            reason pg_catalog.text,
            safe_metadata pg_catalog.jsonb NOT NULL DEFAULT '{}'::pg_catalog.jsonb,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_audit_events PRIMARY KEY (id),
            CONSTRAINT uq_audit_events_event_key UNIQUE (event_key),
            CONSTRAINT fk_audit_events_email FOREIGN KEY (account_id, email_id)
                REFERENCES emails(account_id, id)
                MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_audit_events_account CHECK (account_id > 0),
            CONSTRAINT ck_audit_events_event_key CHECK (
                event_key::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
            ),
            CONSTRAINT ck_audit_events_object_type CHECK (
                pg_catalog.btrim(object_type) <> ''
                AND pg_catalog.char_length(object_type) <= 64
            ),
            CONSTRAINT ck_audit_events_object_fingerprint CHECK (
                object_fingerprint::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
            ),
            CONSTRAINT ck_audit_events_action CHECK (
                pg_catalog.btrim(action) <> ''
                AND pg_catalog.char_length(action) <= 64
            ),
            CONSTRAINT ck_audit_events_result CHECK (
                pg_catalog.btrim(result) <> ''
                AND pg_catalog.char_length(result) <= 64
            ),
            CONSTRAINT ck_audit_events_actor CHECK (
                pg_catalog.btrim(actor) <> ''
                AND pg_catalog.char_length(actor) <= 128
            ),
            CONSTRAINT ck_audit_events_reason CHECK (
                reason IS NULL OR (
                    pg_catalog.btrim(reason) <> ''
                    AND pg_catalog.char_length(reason) <= 512
                )
            ),
            CONSTRAINT ck_audit_events_safe_metadata CHECK (
                pg_catalog.jsonb_typeof(safe_metadata) = 'object'
                AND pg_catalog.octet_length(
                    safe_metadata::pg_catalog.text
                ) <= 16384
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_audit_events_account_time
        ON audit_events(account_id, created_at DESC, id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_audit_events_email_time
        ON audit_events(email_id, created_at DESC, id)
        WHERE email_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_audit_events_mutation()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path FROM CURRENT
        AS $$
        BEGIN
            RAISE EXCEPTION 'audit history is append only';
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_audit_events_guard_row
        BEFORE UPDATE OR DELETE ON audit_events
        FOR EACH ROW EXECUTE FUNCTION reject_audit_events_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_audit_events_guard_truncate
        BEFORE TRUNCATE ON audit_events
        FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_events_mutation()
        """
    )

    op.execute(
        """
        CREATE TABLE pipeline_shadow_comparisons (
            id pg_catalog.uuid NOT NULL,
            account_id pg_catalog.int8 NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            pipeline_name pg_catalog.text NOT NULL,
            candidate_pipeline_name pg_catalog.text NOT NULL,
            candidate_build_id pg_catalog.text NOT NULL,
            candidate_config_hash pg_catalog.bpchar(64) NOT NULL,
            event_key pg_catalog.bpchar(64) NOT NULL,
            input_hash pg_catalog.bpchar(64) NOT NULL,
            legacy_status pg_catalog.text NOT NULL,
            shadow_status pg_catalog.text NOT NULL,
            comparison_status pg_catalog.text NOT NULL,
            legacy_decision_hash pg_catalog.bpchar(64),
            legacy_failure_code pg_catalog.text,
            shadow_decision_hash pg_catalog.bpchar(64),
            shadow_failure_code pg_catalog.text,
            safe_metadata pg_catalog.jsonb NOT NULL DEFAULT '{}'::pg_catalog.jsonb,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_shadow_comparisons PRIMARY KEY (id),
            CONSTRAINT uq_pipeline_shadow_candidate_event UNIQUE (
                account_id,
                generation,
                pipeline_name,
                candidate_pipeline_name,
                candidate_build_id,
                candidate_config_hash,
                event_key
            ),
            CONSTRAINT fk_pipeline_shadow_ownership FOREIGN KEY (
                account_id,
                generation,
                fencing_token,
                pipeline_name
            ) REFERENCES pipeline_ownership (
                account_id,
                generation,
                fencing_token,
                pipeline_name
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_pipeline_shadow_positive_identity CHECK (
                account_id > 0 AND generation > 0 AND fencing_token > 0
            ),
            CONSTRAINT ck_pipeline_shadow_pipeline_names CHECK (
                pg_catalog.btrim(pipeline_name) <> ''
                AND pg_catalog.char_length(pipeline_name) <= 64
                AND pg_catalog.btrim(candidate_pipeline_name) <> ''
                AND pg_catalog.char_length(candidate_pipeline_name) <= 64
            ),
            CONSTRAINT ck_pipeline_shadow_build_id CHECK (
                pg_catalog.btrim(candidate_build_id) <> ''
                AND pg_catalog.char_length(candidate_build_id) <= 128
            ),
            CONSTRAINT ck_pipeline_shadow_hashes CHECK (
                candidate_config_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                AND event_key::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                AND input_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                AND (
                    legacy_decision_hash IS NULL
                    OR legacy_decision_hash::pg_catalog.text
                        OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                )
                AND (
                    shadow_decision_hash IS NULL
                    OR shadow_decision_hash::pg_catalog.text
                        OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                )
            ),
            CONSTRAINT ck_pipeline_shadow_legacy_state CHECK (
                (legacy_status = 'pending'
                    AND legacy_decision_hash IS NULL
                    AND legacy_failure_code IS NULL)
                OR (legacy_status = 'completed'
                    AND legacy_decision_hash IS NOT NULL
                    AND legacy_failure_code IS NULL)
                OR (legacy_status = 'failed'
                    AND legacy_decision_hash IS NULL
                    AND legacy_failure_code IS NOT NULL)
            ),
            CONSTRAINT ck_pipeline_shadow_shadow_state CHECK (
                (shadow_status = 'pending'
                    AND shadow_decision_hash IS NULL
                    AND shadow_failure_code IS NULL)
                OR (shadow_status = 'completed'
                    AND shadow_decision_hash IS NOT NULL
                    AND shadow_failure_code IS NULL)
                OR (shadow_status = 'failed'
                    AND shadow_decision_hash IS NULL
                    AND shadow_failure_code IS NOT NULL)
            ),
            CONSTRAINT ck_pipeline_shadow_comparison_state CHECK (
                (comparison_status = 'pending'
                    AND (legacy_status = 'pending' OR shadow_status = 'pending'))
                OR (comparison_status = 'matched'
                    AND legacy_status = 'completed'
                    AND shadow_status = 'completed'
                    AND legacy_decision_hash = shadow_decision_hash)
                OR (comparison_status = 'diverged'
                    AND legacy_status = 'completed'
                    AND shadow_status = 'completed'
                    AND legacy_decision_hash <> shadow_decision_hash)
                OR (comparison_status = 'incomplete'
                    AND legacy_status <> 'pending'
                    AND shadow_status <> 'pending'
                    AND (legacy_status = 'failed' OR shadow_status = 'failed'))
            ),
            CONSTRAINT ck_pipeline_shadow_failure_codes CHECK (
                (legacy_failure_code IS NULL OR (
                    pg_catalog.btrim(legacy_failure_code) <> ''
                    AND pg_catalog.char_length(legacy_failure_code) <= 64
                ))
                AND (shadow_failure_code IS NULL OR (
                    pg_catalog.btrim(shadow_failure_code) <> ''
                    AND pg_catalog.char_length(shadow_failure_code) <= 64
                ))
            ),
            CONSTRAINT ck_pipeline_shadow_safe_metadata CHECK (
                pg_catalog.jsonb_typeof(safe_metadata) = 'object'
                AND pg_catalog.octet_length(
                    safe_metadata::pg_catalog.text
                ) <= 16384
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_pipeline_shadow_pending
        ON pipeline_shadow_comparisons(comparison_status, created_at, id)
        WHERE comparison_status = 'pending'
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_pipeline_shadow_comparison()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path FROM CURRENT
        AS $$
        BEGIN
            IF TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION 'shadow comparison history is immutable';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'shadow comparison history is immutable';
            END IF;
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.account_id IS DISTINCT FROM OLD.account_id
               OR NEW.generation IS DISTINCT FROM OLD.generation
               OR NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
               OR NEW.pipeline_name IS DISTINCT FROM OLD.pipeline_name
               OR NEW.candidate_pipeline_name IS DISTINCT FROM OLD.candidate_pipeline_name
               OR NEW.candidate_build_id IS DISTINCT FROM OLD.candidate_build_id
               OR NEW.candidate_config_hash IS DISTINCT FROM OLD.candidate_config_hash
               OR NEW.event_key IS DISTINCT FROM OLD.event_key
               OR NEW.input_hash IS DISTINCT FROM OLD.input_hash
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'shadow comparison identity is immutable';
            END IF;
            IF OLD.legacy_status <> 'pending'
               AND NEW.legacy_status IS DISTINCT FROM OLD.legacy_status THEN
                RAISE EXCEPTION 'legacy decision is final';
            END IF;
            IF OLD.shadow_status <> 'pending'
               AND NEW.shadow_status IS DISTINCT FROM OLD.shadow_status THEN
                RAISE EXCEPTION 'shadow decision is final';
            END IF;
            IF OLD.comparison_status <> 'pending'
               AND NEW.comparison_status IS DISTINCT FROM OLD.comparison_status THEN
                RAISE EXCEPTION 'comparison decision is final';
            END IF;
            IF OLD.legacy_decision_hash IS NOT NULL
               AND NEW.legacy_decision_hash IS DISTINCT FROM OLD.legacy_decision_hash THEN
                RAISE EXCEPTION 'legacy decision hash is immutable';
            END IF;
            IF OLD.shadow_decision_hash IS NOT NULL
               AND NEW.shadow_decision_hash IS DISTINCT FROM OLD.shadow_decision_hash THEN
                RAISE EXCEPTION 'shadow decision hash is immutable';
            END IF;
            IF OLD.legacy_failure_code IS NOT NULL
               AND NEW.legacy_failure_code IS DISTINCT FROM OLD.legacy_failure_code THEN
                RAISE EXCEPTION 'legacy failure is immutable';
            END IF;
            IF OLD.shadow_failure_code IS NOT NULL
               AND NEW.shadow_failure_code IS DISTINCT FROM OLD.shadow_failure_code THEN
                RAISE EXCEPTION 'shadow failure is immutable';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_shadow_guard_row
        BEFORE UPDATE OR DELETE ON pipeline_shadow_comparisons
        FOR EACH ROW EXECUTE FUNCTION guard_pipeline_shadow_comparison()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_shadow_guard_truncate
        BEFORE TRUNCATE ON pipeline_shadow_comparisons
        FOR EACH STATEMENT EXECUTE FUNCTION guard_pipeline_shadow_comparison()
        """
    )

    op.execute(
        """
        REVOKE ALL ON FUNCTION guard_pipeline_ownership() FROM PUBLIC;
        REVOKE ALL ON FUNCTION guard_event_inbox_update() FROM PUBLIC;
        REVOKE ALL ON FUNCTION enforce_email_processing_owner() FROM PUBLIC;
        REVOKE ALL ON FUNCTION reject_audit_events_mutation() FROM PUBLIC;
        REVOKE ALL ON FUNCTION guard_pipeline_shadow_comparison() FROM PUBLIC
        """
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
