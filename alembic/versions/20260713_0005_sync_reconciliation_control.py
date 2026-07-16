"""Add dormant Sync reconciliation and cold-start control state."""

from __future__ import annotations

from alembic import op


revision = "20260713_0005"
down_revision = "20260713_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE sync_cold_start_plans (
            plan_id pg_catalog.uuid NOT NULL,
            account_id pg_catalog.int8 NOT NULL,
            folder_key pg_catalog.text NOT NULL,
            expected_cursor_status pg_catalog.text NOT NULL,
            expected_cursor pg_catalog.text,
            expected_cursor_version pg_catalog.int8 NOT NULL,
            pipeline_name pg_catalog.text NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            state pg_catalog.text NOT NULL,
            version pg_catalog.int8 NOT NULL DEFAULT 0,
            preview_cursor pg_catalog.text,
            preview_cursor_version pg_catalog.int8 NOT NULL DEFAULT 0,
            boundary_cursor pg_catalog.text,
            boundary_cursor_version pg_catalog.int8,
            apply_cursor pg_catalog.text,
            apply_cursor_version pg_catalog.int8,
            cursor_binding_plan_id pg_catalog.uuid GENERATED ALWAYS AS (
                CASE
                    WHEN state = 'approved'
                         AND apply_cursor IS NOT NULL
                         AND apply_cursor_version IS NOT NULL
                    THEN plan_id
                    ELSE NULL
                END
            ) STORED,
            rolling_hash pg_catalog.bpchar(64),
            page_count pg_catalog.int8 NOT NULL DEFAULT 0,
            item_count pg_catalog.int8 NOT NULL DEFAULT 0,
            redacted_samples pg_catalog.jsonb NOT NULL
                DEFAULT '[]'::pg_catalog.jsonb,
            contract_fingerprint pg_catalog.bpchar(64) NOT NULL,
            folder_scope_config_hash pg_catalog.bpchar(64) NOT NULL,
            plan_hash pg_catalog.bpchar(64),
            actor pg_catalog.text NOT NULL,
            reason pg_catalog.text NOT NULL,
            blocked_reason_code pg_catalog.text,
            blocked_fingerprint pg_catalog.bpchar(64),
            expires_at pg_catalog.timestamptz NOT NULL,
            ready_at pg_catalog.timestamptz,
            approved_at pg_catalog.timestamptz,
            completed_at pg_catalog.timestamptz,
            blocked_at pg_catalog.timestamptz,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_sync_cold_start_plans PRIMARY KEY (plan_id),
            CONSTRAINT uq_sync_cold_start_plan_identity UNIQUE (
                plan_id, account_id, folder_key
            ),
            CONSTRAINT uq_sync_cold_start_plan_apply_binding UNIQUE (
                plan_id, account_id, folder_key,
                apply_cursor, apply_cursor_version, state
            ),
            CONSTRAINT fk_sync_cold_start_plan_ownership FOREIGN KEY (
                account_id, generation, fencing_token, pipeline_name
            ) REFERENCES pipeline_ownership (
                account_id, generation, fencing_token, pipeline_name
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_sync_cold_start_plans_positive_identity CHECK (
                account_id > 0 AND generation > 0 AND fencing_token > 0
            ),
            CONSTRAINT ck_sync_cold_start_plans_folder_key CHECK (
                pg_catalog.btrim(folder_key) <> ''
                AND pg_catalog.char_length(folder_key) <= 512
            ),
            CONSTRAINT ck_sync_cold_start_plans_expected_cursor CHECK (
                expected_cursor_status IN ('cold_start_pending', 'reset_required')
                AND expected_cursor_version >= 0
                AND (
                    (
                        expected_cursor_status = 'cold_start_pending'
                        AND expected_cursor IS NULL
                    ) OR (
                        expected_cursor_status = 'reset_required'
                        AND expected_cursor IS NOT NULL
                        AND pg_catalog.btrim(expected_cursor) <> ''
                        AND pg_catalog.char_length(expected_cursor) <= 8192
                    )
                )
            ),
            CONSTRAINT ck_sync_cold_start_plans_pipeline_name CHECK (
                pg_catalog.btrim(pipeline_name) <> ''
                AND pg_catalog.char_length(pipeline_name) <= 64
            ),
            CONSTRAINT ck_sync_cold_start_plans_state CHECK (
                state IN ('previewing', 'ready', 'approved', 'completed', 'blocked')
            ),
            CONSTRAINT ck_sync_cold_start_plans_versions CHECK (
                version >= 0
                AND preview_cursor_version >= 0
                AND (
                    boundary_cursor_version IS NULL
                    OR boundary_cursor_version >= 0
                )
                AND (
                    apply_cursor_version IS NULL OR apply_cursor_version >= 0
                )
            ),
            CONSTRAINT ck_sync_cold_start_plans_cursors CHECK (
                (
                    preview_cursor IS NULL OR (
                        pg_catalog.btrim(preview_cursor) <> ''
                        AND pg_catalog.char_length(preview_cursor) <= 8192
                    )
                ) AND (
                    boundary_cursor IS NULL OR (
                        pg_catalog.btrim(boundary_cursor) <> ''
                        AND pg_catalog.char_length(boundary_cursor) <= 8192
                    )
                ) AND (
                    apply_cursor IS NULL OR (
                        pg_catalog.btrim(apply_cursor) <> ''
                        AND pg_catalog.char_length(apply_cursor) <= 8192
                    )
                ) AND (
                    (boundary_cursor IS NULL AND boundary_cursor_version IS NULL)
                    OR (
                        boundary_cursor IS NOT NULL
                        AND boundary_cursor_version IS NOT NULL
                    )
                ) AND (
                    (apply_cursor IS NULL AND apply_cursor_version IS NULL)
                    OR (
                        apply_cursor IS NOT NULL
                        AND apply_cursor_version IS NOT NULL
                    )
                )
            ),
            CONSTRAINT ck_sync_cold_start_plans_hashes CHECK (
                (
                    rolling_hash IS NULL OR rolling_hash::pg_catalog.text
                        OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                )
                AND contract_fingerprint::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
                AND folder_scope_config_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                AND (
                    plan_hash IS NULL OR plan_hash::pg_catalog.text
                        OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                )
                AND (
                    blocked_fingerprint IS NULL
                    OR blocked_fingerprint::pg_catalog.text
                        OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                )
            ),
            CONSTRAINT ck_sync_cold_start_plans_counts CHECK (
                page_count >= 0 AND item_count >= 0
                AND preview_cursor_version = page_count
                AND (
                    (
                        page_count = 0
                        AND item_count = 0
                        AND preview_cursor IS NULL
                        AND rolling_hash IS NULL
                    ) OR (
                        page_count > 0
                        AND preview_cursor IS NOT NULL
                        AND rolling_hash IS NOT NULL
                    )
                )
            ),
            CONSTRAINT ck_sync_cold_start_plans_samples CHECK (
                pg_catalog.jsonb_typeof(redacted_samples) = 'array'
                AND pg_catalog.jsonb_array_length(redacted_samples) <= 20
                AND pg_catalog.octet_length(
                    redacted_samples::pg_catalog.text
                ) <= 16384
            ),
            CONSTRAINT ck_sync_cold_start_plans_operator CHECK (
                pg_catalog.btrim(actor) <> ''
                AND pg_catalog.char_length(actor) <= 128
                AND pg_catalog.btrim(reason) <> ''
                AND pg_catalog.char_length(reason) <= 512
            ),
            CONSTRAINT ck_sync_cold_start_plans_blocked_reason CHECK (
                blocked_reason_code IS NULL OR (
                    pg_catalog.btrim(blocked_reason_code) <> ''
                    AND pg_catalog.char_length(blocked_reason_code) <= 64
                )
            ),
            CONSTRAINT ck_sync_cold_start_plans_state_matrix CHECK (
                expires_at > created_at
                AND (
                    (
                        state = 'previewing'
                        AND boundary_cursor IS NULL
                        AND boundary_cursor_version IS NULL
                        AND apply_cursor IS NULL
                        AND apply_cursor_version IS NULL
                        AND plan_hash IS NULL
                        AND ready_at IS NULL
                        AND approved_at IS NULL
                        AND completed_at IS NULL
                        AND blocked_reason_code IS NULL
                        AND blocked_fingerprint IS NULL
                        AND blocked_at IS NULL
                    ) OR (
                        state = 'ready'
                        AND boundary_cursor IS NOT NULL
                        AND boundary_cursor IS NOT DISTINCT FROM preview_cursor
                        AND boundary_cursor_version = preview_cursor_version
                        AND apply_cursor IS NULL
                        AND apply_cursor_version IS NULL
                        AND plan_hash IS NOT NULL
                        AND ready_at IS NOT NULL
                        AND ready_at >= created_at
                        AND approved_at IS NULL
                        AND completed_at IS NULL
                        AND blocked_reason_code IS NULL
                        AND blocked_fingerprint IS NULL
                        AND blocked_at IS NULL
                    ) OR (
                        state = 'approved'
                        AND boundary_cursor IS NOT NULL
                        AND boundary_cursor IS NOT DISTINCT FROM preview_cursor
                        AND boundary_cursor_version = preview_cursor_version
                        AND plan_hash IS NOT NULL
                        AND ready_at IS NOT NULL
                        AND approved_at IS NOT NULL
                        AND approved_at >= ready_at
                        AND completed_at IS NULL
                        AND blocked_reason_code IS NULL
                        AND blocked_fingerprint IS NULL
                        AND blocked_at IS NULL
                        AND (
                            (apply_cursor IS NULL AND apply_cursor_version IS NULL)
                            OR (
                                apply_cursor IS NOT NULL
                                AND apply_cursor_version IS NOT NULL
                            )
                        )
                    ) OR (
                        state = 'completed'
                        AND boundary_cursor IS NOT NULL
                        AND boundary_cursor IS NOT DISTINCT FROM preview_cursor
                        AND boundary_cursor_version = preview_cursor_version
                        AND apply_cursor IS NOT NULL
                        AND apply_cursor_version IS NOT NULL
                        AND plan_hash IS NOT NULL
                        AND ready_at IS NOT NULL
                        AND approved_at IS NOT NULL
                        AND completed_at IS NOT NULL
                        AND approved_at >= ready_at
                        AND completed_at >= approved_at
                        AND blocked_reason_code IS NULL
                        AND blocked_fingerprint IS NULL
                        AND blocked_at IS NULL
                    ) OR (
                        state = 'blocked'
                        AND completed_at IS NULL
                        AND blocked_reason_code IS NOT NULL
                        AND blocked_fingerprint IS NOT NULL
                        AND blocked_at IS NOT NULL
                        AND blocked_at >= created_at
                        AND (
                            (
                                boundary_cursor IS NULL
                                AND boundary_cursor_version IS NULL
                                AND apply_cursor IS NULL
                                AND apply_cursor_version IS NULL
                                AND plan_hash IS NULL
                                AND ready_at IS NULL
                                AND approved_at IS NULL
                            ) OR (
                                boundary_cursor IS NOT NULL
                                AND boundary_cursor
                                    IS NOT DISTINCT FROM preview_cursor
                                AND boundary_cursor_version =
                                    preview_cursor_version
                                AND plan_hash IS NOT NULL
                                AND ready_at IS NOT NULL
                                AND ready_at >= created_at
                                AND blocked_at >= ready_at
                                AND (
                                    (
                                        approved_at IS NULL
                                        AND apply_cursor IS NULL
                                        AND apply_cursor_version IS NULL
                                    ) OR (
                                        approved_at IS NOT NULL
                                        AND approved_at >= ready_at
                                        AND blocked_at >= approved_at
                                        AND (
                                            (
                                                apply_cursor IS NULL
                                                AND apply_cursor_version IS NULL
                                            ) OR (
                                                apply_cursor IS NOT NULL
                                                AND apply_cursor_version IS NOT NULL
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_sync_cold_start_open_plan
        ON sync_cold_start_plans(account_id, folder_key)
        WHERE state IN ('previewing', 'ready', 'approved')
        """
    )
    op.execute(
        """
        CREATE INDEX ix_sync_cold_start_plans_state_expiry
        ON sync_cold_start_plans(state, expires_at, plan_id)
        """
    )

    op.execute(
        """
        ALTER TABLE sync_cursors
        DROP CONSTRAINT ck_sync_cursors_status,
        DROP CONSTRAINT ck_sync_cursors_state_matrix,
        ADD COLUMN transient_failures pg_catalog.int8 NOT NULL DEFAULT 0,
        ADD COLUMN retry_after_at pg_catalog.timestamptz,
        ADD COLUMN cold_start_plan_id pg_catalog.uuid,
        ADD COLUMN cold_start_plan_state pg_catalog.text,
        ADD CONSTRAINT ck_sync_cursors_status CHECK (
            status IN (
                'active', 'reset_required', 'cold_start_pending',
                'blocked_contract', 'cold_start_applying'
            )
        ),
        ADD CONSTRAINT ck_sync_cursors_transient_failures CHECK (
            transient_failures >= 0
        ),
        ADD CONSTRAINT ck_sync_cursors_retry CHECK (
            (transient_failures = 0 AND retry_after_at IS NULL)
            OR (transient_failures > 0 AND retry_after_at IS NOT NULL)
        ),
        ADD CONSTRAINT ck_sync_cursors_plan_binding CHECK (
            (
                status = 'cold_start_applying'
                AND cold_start_plan_id IS NOT NULL
                AND cold_start_plan_state = 'approved'
            ) OR (
                status <> 'cold_start_applying'
                AND cold_start_plan_id IS NULL
                AND cold_start_plan_state IS NULL
            )
        ),
        ADD CONSTRAINT ck_sync_cursors_state_matrix CHECK (
            (
                status = 'active'
                AND cursor IS NOT NULL
                AND last_success_at IS NOT NULL
                AND blocked_reason_code IS NULL
                AND contract_fingerprint IS NULL
                AND blocked_at IS NULL
                AND cold_start_plan_id IS NULL
                AND cold_start_plan_state IS NULL
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
                AND transient_failures = 0
                AND retry_after_at IS NULL
                AND cold_start_plan_id IS NULL
                AND cold_start_plan_state IS NULL
            ) OR (
                status = 'cold_start_pending'
                AND cursor IS NULL
                AND blocked_reason_code IS NOT NULL
                AND contract_fingerprint IS NULL
                AND blocked_at IS NULL
                AND transient_failures = 0
                AND retry_after_at IS NULL
                AND cold_start_plan_id IS NULL
                AND cold_start_plan_state IS NULL
            ) OR (
                status = 'blocked_contract'
                AND blocked_reason_code IS NOT NULL
                AND contract_fingerprint IS NOT NULL
                AND blocked_at IS NOT NULL
                AND transient_failures = 0
                AND retry_after_at IS NULL
                AND cold_start_plan_id IS NULL
                AND cold_start_plan_state IS NULL
            ) OR (
                status = 'cold_start_applying'
                AND cursor IS NOT NULL
                AND blocked_reason_code IS NULL
                AND contract_fingerprint IS NULL
                AND blocked_at IS NULL
                AND last_success_at IS NOT NULL
                AND last_attempt_at IS NOT NULL
                AND last_attempt_at >= last_success_at
                AND cold_start_plan_id IS NOT NULL
                AND cold_start_plan_state = 'approved'
            )
        ),
        ADD CONSTRAINT uq_sync_cursors_cold_start_binding UNIQUE (
            cold_start_plan_id, account_id, folder_key, cursor, version,
            cold_start_plan_state
        ),
        ADD CONSTRAINT fk_sync_cursors_cold_start_plan FOREIGN KEY (
            cold_start_plan_id, account_id, folder_key, cursor, version,
            cold_start_plan_state
        ) REFERENCES sync_cold_start_plans (
            plan_id, account_id, folder_key, apply_cursor, apply_cursor_version,
            state
        ) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
        """
    )
    op.execute(
        """
        ALTER TABLE sync_cold_start_plans
        ADD CONSTRAINT fk_sync_cold_start_plan_active_cursor FOREIGN KEY (
            cursor_binding_plan_id, account_id, folder_key, apply_cursor,
            apply_cursor_version, state
        ) REFERENCES sync_cursors (
            cold_start_plan_id, account_id, folder_key, cursor, version,
            cold_start_plan_state
        ) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_sync_cursors_cold_start_plan
        ON sync_cursors(cold_start_plan_id)
        WHERE cold_start_plan_id IS NOT NULL
        """
    )

    op.execute(
        """
        CREATE TABLE pipeline_command_receipts (
            id pg_catalog.uuid NOT NULL,
            account_id pg_catalog.int8 NOT NULL,
            command_name pg_catalog.text NOT NULL,
            idempotency_key_hash pg_catalog.bpchar(64) NOT NULL,
            canonical_payload_hash pg_catalog.bpchar(64) NOT NULL,
            outcome pg_catalog.text NOT NULL,
            result_type pg_catalog.text NOT NULL,
            result_id pg_catalog.text NOT NULL,
            result_hash pg_catalog.bpchar(64) NOT NULL,
            authority_epoch pg_catalog.int8 NOT NULL,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_command_receipts PRIMARY KEY (id),
            CONSTRAINT uq_pipeline_command_receipts_identity UNIQUE (
                account_id, command_name, idempotency_key_hash
            ),
            CONSTRAINT ck_pipeline_command_receipts_account CHECK (
                account_id > 0
            ),
            CONSTRAINT ck_pipeline_command_receipts_command_name CHECK (
                command_name IN (
                    'cold_start.preview',
                    'cold_start.approve',
                    'cold_start.apply_page'
                )
            ),
            CONSTRAINT ck_pipeline_command_receipts_hashes CHECK (
                idempotency_key_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
                AND canonical_payload_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
                AND result_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
            ),
            CONSTRAINT ck_pipeline_command_receipts_outcome CHECK (
                outcome = 'succeeded'
            ),
            CONSTRAINT ck_pipeline_command_receipts_result CHECK (
                result_type = 'sync_cold_start_plan'
                AND result_id OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ),
            CONSTRAINT ck_pipeline_command_receipts_authority_epoch CHECK (
                authority_epoch >= 0
            )
        )
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_pipeline_command_receipts_mutation()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path FROM CURRENT
        AS $$
        BEGIN
            RAISE EXCEPTION 'pipeline command receipts are append-only';
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_command_receipts_guard_row
        BEFORE UPDATE OR DELETE ON pipeline_command_receipts
        FOR EACH ROW EXECUTE FUNCTION reject_pipeline_command_receipts_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_command_receipts_guard_truncate
        BEFORE TRUNCATE ON pipeline_command_receipts
        FOR EACH STATEMENT
        EXECUTE FUNCTION reject_pipeline_command_receipts_mutation()
        """
    )
    op.execute(
        """
        CREATE VIEW cold_start_command_receipts AS
        SELECT
            id,
            account_id,
            command_name,
            idempotency_key_hash,
            canonical_payload_hash,
            outcome,
            result_type,
            result_id,
            result_hash,
            authority_epoch,
            created_at
        FROM pipeline_command_receipts
        WHERE command_name IN (
            'cold_start.preview',
            'cold_start.approve',
            'cold_start.apply_page'
        )
        WITH CASCADED CHECK OPTION
        """
    )
    op.execute(
        """
        REVOKE ALL ON FUNCTION reject_pipeline_command_receipts_mutation()
        FROM PUBLIC
        """
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
