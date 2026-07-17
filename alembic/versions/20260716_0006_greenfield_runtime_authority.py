"""Create the fresh-only greenfield runtime authority schema."""

from __future__ import annotations

from alembic import op


revision = "20260716_0006"
down_revision = "20260713_0005"
branch_labels = None
depends_on = None


_CAPABILITY_CHAIN_ROOT_HASH = (
    "95771c6d473119376654d5530f7fe189c77d83e56fe08e91179f48b1040df86f"
)


def upgrade() -> None:
    # This must remain the first migration statement.  A single fixed lock order
    # serializes the emptiness decision against every legacy/governed writer.
    op.execute(
        """
        LOCK TABLE
            emails_log,
            app_kv_store,
            pipeline_ownership,
            event_inbox,
            sync_cursors,
            emails,
            audit_events,
            pipeline_shadow_comparisons,
            sync_cold_start_plans,
            pipeline_command_receipts
        IN ACCESS EXCLUSIVE MODE
        """
    )
    op.execute(
        """
        DO $greenfield$
        BEGIN
            IF EXISTS (SELECT 1 FROM emails_log LIMIT 1)
               OR EXISTS (SELECT 1 FROM app_kv_store LIMIT 1)
               OR EXISTS (SELECT 1 FROM pipeline_ownership LIMIT 1)
               OR EXISTS (SELECT 1 FROM event_inbox LIMIT 1)
               OR EXISTS (SELECT 1 FROM sync_cursors LIMIT 1)
               OR EXISTS (SELECT 1 FROM emails LIMIT 1)
               OR EXISTS (SELECT 1 FROM audit_events LIMIT 1)
               OR EXISTS (
                    SELECT 1 FROM pipeline_shadow_comparisons LIMIT 1
               )
               OR EXISTS (SELECT 1 FROM sync_cold_start_plans LIMIT 1)
               OR EXISTS (SELECT 1 FROM pipeline_command_receipts LIMIT 1)
            THEN
                RAISE EXCEPTION 'greenfield_reinitialize_required'
                    USING ERRCODE = 'P0001';
            END IF;
        END
        $greenfield$
        """
    )

    op.execute("DROP TABLE pipeline_shadow_comparisons")
    op.execute("DROP FUNCTION guard_pipeline_shadow_comparison()")

    op.execute(
        f"""
        CREATE TABLE pipeline_runtime_capabilities (
            capability_hash pg_catalog.bpchar(64) NOT NULL,
            predecessor_hash pg_catalog.bpchar(64) NOT NULL,
            stage pg_catalog.text NOT NULL,
            stage_ordinal pg_catalog.int2 GENERATED ALWAYS AS (
                CASE stage
                    WHEN 'phase2_ingestion' THEN 1::pg_catalog.int2
                    WHEN 'phase3_approval_send' THEN 2::pg_catalog.int2
                    WHEN 'phase4_graph_projection' THEN 3::pg_catalog.int2
                    ELSE NULL::pg_catalog.int2
                END
            ) STORED,
            predecessor_stage_ordinal pg_catalog.int2 GENERATED ALWAYS AS (
                CASE stage
                    WHEN 'phase2_ingestion' THEN NULL::pg_catalog.int2
                    WHEN 'phase3_approval_send' THEN 1::pg_catalog.int2
                    WHEN 'phase4_graph_projection' THEN 2::pg_catalog.int2
                    ELSE NULL::pg_catalog.int2
                END
            ) STORED,
            schema_revision pg_catalog.text NOT NULL,
            schema_digest pg_catalog.bpchar(64) NOT NULL,
            protocol_version pg_catalog.int8 NOT NULL,
            minimum_build_id pg_catalog.text NOT NULL,
            config_hash pg_catalog.bpchar(64) NOT NULL,
            adapter_hash pg_catalog.bpchar(64) NOT NULL,
            policy_manifest_hash pg_catalog.bpchar(64) NOT NULL,
            evidence_manifest_hash pg_catalog.bpchar(64) NOT NULL,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_runtime_capabilities
                PRIMARY KEY (capability_hash),
            CONSTRAINT uq_pipeline_runtime_capabilities_stage_identity
                UNIQUE (capability_hash, stage_ordinal),
            CONSTRAINT uq_pipeline_runtime_capabilities_policy_identity
                UNIQUE (
                    capability_hash,
                    stage_ordinal,
                    policy_manifest_hash
                ),
            CONSTRAINT uq_pipeline_runtime_capabilities_runtime_contract
                UNIQUE (
                    capability_hash,
                    stage_ordinal,
                    schema_revision,
                    protocol_version,
                    minimum_build_id,
                    config_hash,
                    policy_manifest_hash
                ),
            CONSTRAINT uq_pipeline_runtime_capabilities_instance_contract
                UNIQUE (
                    capability_hash,
                    stage_ordinal,
                    schema_revision,
                    protocol_version,
                    minimum_build_id,
                    config_hash
                ),
            CONSTRAINT fk_pipeline_runtime_capabilities_predecessor FOREIGN KEY (
                predecessor_hash,
                predecessor_stage_ordinal
            ) REFERENCES pipeline_runtime_capabilities (
                capability_hash,
                stage_ordinal
            ) MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_pipeline_runtime_capabilities_hashes CHECK (
                capability_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{{64}}$'
                AND predecessor_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{{64}}$'
                AND schema_digest::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{{64}}$'
                AND config_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{{64}}$'
                AND adapter_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{{64}}$'
                AND policy_manifest_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{{64}}$'
                AND evidence_manifest_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{{64}}$'
            ),
            CONSTRAINT ck_pipeline_runtime_capabilities_stage CHECK (
                stage IN (
                    'phase2_ingestion',
                    'phase3_approval_send',
                    'phase4_graph_projection'
                )
                AND stage_ordinal BETWEEN 1 AND 3
            ),
            CONSTRAINT ck_pipeline_runtime_capabilities_predecessor CHECK (
                (
                    stage = 'phase2_ingestion'
                    AND predecessor_hash =
                        '{_CAPABILITY_CHAIN_ROOT_HASH}'::pg_catalog.bpchar
                    AND predecessor_stage_ordinal IS NULL
                ) OR (
                    stage IN (
                        'phase3_approval_send',
                        'phase4_graph_projection'
                    )
                    AND predecessor_hash <>
                        '{_CAPABILITY_CHAIN_ROOT_HASH}'::pg_catalog.bpchar
                    AND predecessor_stage_ordinal = stage_ordinal - 1
                )
            ),
            CONSTRAINT ck_pipeline_runtime_capabilities_schema CHECK (
                schema_revision OPERATOR(pg_catalog.~)
                    '^[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}$'
                AND (
                    stage <> 'phase2_ingestion'
                    OR schema_revision = '20260716_0006'
                )
            ),
            CONSTRAINT ck_pipeline_runtime_capabilities_protocol CHECK (
                protocol_version > 0
            ),
            CONSTRAINT ck_pipeline_runtime_capabilities_build CHECK (
                minimum_build_id OPERATOR(pg_catalog.~)
                    '^[A-Za-z0-9][A-Za-z0-9_.+-]{{0,127}}$'
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_pipeline_runtime_capabilities_stage
        ON pipeline_runtime_capabilities(stage_ordinal, created_at, capability_hash)
        """
    )

    op.execute(
        """
        ALTER TABLE pipeline_command_receipts
        DROP CONSTRAINT ck_pipeline_command_receipts_command_name,
        DROP CONSTRAINT ck_pipeline_command_receipts_result,
        DROP CONSTRAINT ck_pipeline_command_receipts_authority_epoch,
        ADD CONSTRAINT ck_pipeline_command_receipts_command_name CHECK (
            command_name IN (
                'cold_start.preview',
                'cold_start.approve',
                'cold_start.apply_page',
                'runtime.initialize',
                'runtime.pause',
                'runtime.resume_ingress',
                'inbox.requeue'
            )
        ),
        ADD CONSTRAINT ck_pipeline_command_receipts_result CHECK (
            (
                command_name IN (
                    'cold_start.preview',
                    'cold_start.approve',
                    'cold_start.apply_page'
                )
                AND result_type = 'sync_cold_start_plan'
                AND result_id OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ) OR (
                command_name = 'runtime.initialize'
                AND result_type = 'runtime_initialization'
                AND result_id OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ) OR (
                command_name IN ('runtime.pause', 'runtime.resume_ingress')
                AND result_type = 'runtime_authority'
                AND result_id OPERATOR(pg_catalog.~) '^[1-9][0-9]{0,18}$'
            ) OR (
                command_name = 'inbox.requeue'
                AND result_type = 'event_inbox'
                AND result_id OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
        ),
        ADD CONSTRAINT ck_pipeline_command_receipts_authority_epoch CHECK (
            (
                command_name IN (
                    'cold_start.preview',
                    'cold_start.approve',
                    'cold_start.apply_page'
                )
                AND authority_epoch >= 0
            ) OR (
                command_name IN (
                    'runtime.initialize',
                    'runtime.pause',
                    'runtime.resume_ingress',
                    'inbox.requeue'
                )
                AND authority_epoch > 0
            )
        ),
        ADD CONSTRAINT uq_pipeline_command_receipts_runtime_binding UNIQUE (
            id,
            account_id,
            command_name,
            authority_epoch
        )
        """
    )

    op.execute(
        """
        CREATE TABLE pipeline_initializations (
            initialization_id pg_catalog.uuid NOT NULL,
            command_receipt_id pg_catalog.uuid NOT NULL,
            receipt_command_name pg_catalog.text GENERATED ALWAYS AS (
                'runtime.initialize'::pg_catalog.text
            ) STORED,
            account_id pg_catalog.int8 NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            pipeline_name pg_catalog.text NOT NULL,
            authority_epoch pg_catalog.int8 NOT NULL,
            authority_version pg_catalog.int8 NOT NULL,
            capability_hash pg_catalog.bpchar(64) NOT NULL,
            capability_stage_ordinal pg_catalog.int2 NOT NULL DEFAULT 1,
            policy_manifest_hash pg_catalog.bpchar(64) NOT NULL,
            transaction_id pg_catalog.text NOT NULL,
            actor pg_catalog.text NOT NULL,
            reason pg_catalog.text NOT NULL,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_initializations PRIMARY KEY (initialization_id),
            CONSTRAINT uq_pipeline_initializations_account UNIQUE (account_id),
            CONSTRAINT uq_pipeline_initializations_receipt UNIQUE (
                command_receipt_id
            ),
            CONSTRAINT uq_pipeline_initializations_authority_binding UNIQUE (
                initialization_id,
                account_id,
                generation,
                fencing_token,
                pipeline_name,
                policy_manifest_hash
            ),
            CONSTRAINT uq_pipeline_initializations_policy_binding UNIQUE (
                initialization_id,
                account_id,
                policy_manifest_hash
            ),
            CONSTRAINT fk_pipeline_initializations_ownership FOREIGN KEY (
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
            CONSTRAINT fk_pipeline_initializations_capability FOREIGN KEY (
                capability_hash,
                capability_stage_ordinal,
                policy_manifest_hash
            ) REFERENCES pipeline_runtime_capabilities (
                capability_hash,
                stage_ordinal,
                policy_manifest_hash
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT fk_pipeline_initializations_receipt FOREIGN KEY (
                command_receipt_id,
                account_id,
                receipt_command_name,
                authority_epoch
            ) REFERENCES pipeline_command_receipts (
                id,
                account_id,
                command_name,
                authority_epoch
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_pipeline_initializations_greenfield_identity CHECK (
                account_id > 0
                AND generation = 1
                AND fencing_token = 1
                AND pipeline_name = 'durable_v1'
                AND authority_epoch = 1
                AND authority_version = 1
                AND capability_stage_ordinal = 1
            ),
            CONSTRAINT ck_pipeline_initializations_hashes CHECK (
                capability_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
                AND policy_manifest_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
            ),
            CONSTRAINT ck_pipeline_initializations_transaction CHECK (
                transaction_id OPERATOR(pg_catalog.~) '^[1-9][0-9]{0,19}$'
            ),
            CONSTRAINT ck_pipeline_initializations_operator CHECK (
                pg_catalog.btrim(actor) <> ''
                AND pg_catalog.char_length(actor) <= 128
                AND pg_catalog.btrim(reason) <> ''
                AND pg_catalog.char_length(reason) <= 512
            )
        )
        """
    )

    op.execute(
        """
        CREATE TABLE pipeline_folder_scopes (
            initialization_id pg_catalog.uuid NOT NULL,
            account_id pg_catalog.int8 NOT NULL,
            canonical_key pg_catalog.text NOT NULL,
            webhook_ids pg_catalog.jsonb NOT NULL,
            sync_folder pg_catalog.text NOT NULL,
            event_policy_matrix pg_catalog.jsonb NOT NULL,
            scope_hash pg_catalog.bpchar(64) NOT NULL,
            policy_manifest_hash pg_catalog.bpchar(64) NOT NULL,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_folder_scopes PRIMARY KEY (
                initialization_id,
                canonical_key
            ),
            CONSTRAINT uq_pipeline_folder_scopes_sync_folder UNIQUE (
                initialization_id,
                sync_folder
            ),
            CONSTRAINT uq_pipeline_folder_scopes_hash UNIQUE (
                initialization_id,
                scope_hash
            ),
            CONSTRAINT fk_pipeline_folder_scopes_initialization FOREIGN KEY (
                initialization_id,
                account_id,
                policy_manifest_hash
            ) REFERENCES pipeline_initializations (
                initialization_id,
                account_id,
                policy_manifest_hash
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_pipeline_folder_scopes_identity CHECK (
                account_id > 0
                AND pg_catalog.btrim(canonical_key) = canonical_key
                AND pg_catalog.btrim(canonical_key) <> ''
                AND pg_catalog.char_length(canonical_key) <= 512
                AND (
                    canonical_key IN (
                        'ARCHIVE', 'TRASH', 'DRAFTS', 'INBOX',
                        'JUNK', 'OUTBOX', 'SENT'
                    ) OR (
                        pg_catalog.lower(canonical_key COLLATE "C") NOT IN (
                            'archive', 'deleted', 'deleted items',
                            'deleteditems', 'draft', 'drafts', 'inbox',
                            'junk', 'junk email', 'junkemail', 'outbox',
                            'sent', 'sent items', 'sentitems', 'spam', 'trash'
                        )
                        AND canonical_key NOT IN ('已发送', '已发送邮件', '草稿')
                    )
                )
            ),
            CONSTRAINT ck_pipeline_folder_scopes_webhook_ids CHECK (
                pg_catalog.jsonb_typeof(webhook_ids) = 'array'
                AND pg_catalog.jsonb_array_length(webhook_ids) BETWEEN 1 AND 64
                AND pg_catalog.octet_length(webhook_ids::pg_catalog.text) <= 32768
            ),
            CONSTRAINT ck_pipeline_folder_scopes_sync_folder CHECK (
                pg_catalog.btrim(sync_folder) = sync_folder
                AND pg_catalog.btrim(sync_folder) <> ''
                AND pg_catalog.char_length(sync_folder) <= 512
            ),
            CONSTRAINT ck_pipeline_folder_scopes_event_policy_matrix CHECK (
                pg_catalog.jsonb_typeof(event_policy_matrix) = 'object'
                AND (
                    event_policy_matrix
                        - 'webhook:NewMailEvent:create'
                        - 'webhook:CreatedEvent:create'
                        - 'webhook:ModifiedEvent:update'
                        - 'webhook:DeletedEvent:delete'
                        - 'sync:create:create'
                        - 'sync:update:update'
                        - 'sync:delete:delete'
                ) = '{}'::pg_catalog.jsonb
                AND event_policy_matrix ? 'webhook:NewMailEvent:create'
                AND event_policy_matrix ? 'webhook:CreatedEvent:create'
                AND event_policy_matrix ? 'webhook:ModifiedEvent:update'
                AND event_policy_matrix ? 'webhook:DeletedEvent:delete'
                AND event_policy_matrix ? 'sync:create:create'
                AND event_policy_matrix ? 'sync:update:update'
                AND event_policy_matrix ? 'sync:delete:delete'
                AND event_policy_matrix ->> 'webhook:NewMailEvent:create'
                    IN ('full', 'archive', 'ignored')
                AND event_policy_matrix ->> 'webhook:CreatedEvent:create'
                    IN ('full', 'archive', 'ignored')
                AND event_policy_matrix ->> 'sync:create:create'
                    IN ('full', 'archive', 'ignored')
                AND event_policy_matrix ->> 'webhook:ModifiedEvent:update'
                    = 'metadata_only'
                AND event_policy_matrix ->> 'webhook:DeletedEvent:delete'
                    = 'metadata_only'
                AND event_policy_matrix ->> 'sync:update:update'
                    = 'metadata_only'
                AND event_policy_matrix ->> 'sync:delete:delete'
                    = 'metadata_only'
                AND event_policy_matrix ->> 'webhook:NewMailEvent:create'
                    = event_policy_matrix ->> 'sync:create:create'
                AND (
                    (
                        canonical_key = 'SENT'
                        AND event_policy_matrix
                            ->> 'webhook:NewMailEvent:create' = 'archive'
                        AND event_policy_matrix
                            ->> 'webhook:CreatedEvent:create' = 'archive'
                    ) OR (
                        canonical_key = 'DRAFTS'
                        AND event_policy_matrix
                            ->> 'webhook:NewMailEvent:create' = 'ignored'
                        AND event_policy_matrix
                            ->> 'webhook:CreatedEvent:create' = 'ignored'
                    ) OR (
                        canonical_key = 'ARCHIVE'
                        AND event_policy_matrix
                            ->> 'webhook:NewMailEvent:create' = 'archive'
                        AND event_policy_matrix
                            ->> 'webhook:CreatedEvent:create' = 'ignored'
                    ) OR (
                        canonical_key NOT IN ('SENT', 'DRAFTS', 'ARCHIVE')
                        AND event_policy_matrix
                            ->> 'webhook:CreatedEvent:create' = 'ignored'
                    )
                )
            ),
            CONSTRAINT ck_pipeline_folder_scopes_hashes CHECK (
                scope_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
                AND policy_manifest_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_pipeline_folder_scopes_account
        ON pipeline_folder_scopes(account_id, canonical_key)
        """
    )

    op.execute(
        """
        CREATE TABLE pipeline_runtime_authority (
            account_id pg_catalog.int8 NOT NULL,
            state pg_catalog.text NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            pipeline_name pg_catalog.text NOT NULL,
            authority_epoch pg_catalog.int8 NOT NULL,
            version pg_catalog.int8 NOT NULL,
            schema_revision pg_catalog.text NOT NULL,
            protocol_version pg_catalog.int8 NOT NULL,
            build_id pg_catalog.text NOT NULL,
            config_hash pg_catalog.bpchar(64) NOT NULL,
            capability_hash pg_catalog.bpchar(64) NOT NULL,
            capability_stage_ordinal pg_catalog.int2 NOT NULL DEFAULT 1,
            policy_manifest_hash pg_catalog.bpchar(64) NOT NULL,
            initialization_id pg_catalog.uuid NOT NULL,
            created_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_runtime_authority PRIMARY KEY (account_id),
            CONSTRAINT uq_pipeline_runtime_authority_stamp UNIQUE (
                account_id,
                generation,
                fencing_token,
                pipeline_name,
                authority_epoch,
                capability_hash
            ),
            CONSTRAINT fk_pipeline_runtime_authority_ownership FOREIGN KEY (
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
            CONSTRAINT fk_pipeline_runtime_authority_initialization FOREIGN KEY (
                initialization_id,
                account_id,
                generation,
                fencing_token,
                pipeline_name,
                policy_manifest_hash
            ) REFERENCES pipeline_initializations (
                initialization_id,
                account_id,
                generation,
                fencing_token,
                pipeline_name,
                policy_manifest_hash
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT fk_pipeline_runtime_authority_capability FOREIGN KEY (
                capability_hash,
                capability_stage_ordinal,
                schema_revision,
                protocol_version,
                build_id,
                config_hash,
                policy_manifest_hash
            ) REFERENCES pipeline_runtime_capabilities (
                capability_hash,
                stage_ordinal,
                schema_revision,
                protocol_version,
                minimum_build_id,
                config_hash,
                policy_manifest_hash
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_pipeline_runtime_authority_identity CHECK (
                account_id > 0
                AND generation = 1
                AND fencing_token = 1
                AND pipeline_name = 'durable_v1'
            ),
            CONSTRAINT ck_pipeline_runtime_authority_state CHECK (
                state IN ('ingest_only', 'paused', 'active')
                AND (state <> 'active' OR capability_stage_ordinal = 3)
            ),
            CONSTRAINT ck_pipeline_runtime_authority_versions CHECK (
                authority_epoch > 0
                AND authority_epoch < 9223372036854775807
                AND version > 0
                AND version < 9223372036854775807
            ),
            CONSTRAINT ck_pipeline_runtime_authority_contract CHECK (
                schema_revision OPERATOR(pg_catalog.~)
                    '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'
                AND protocol_version > 0
                AND build_id OPERATOR(pg_catalog.~)
                    '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'
                AND config_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
                AND capability_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
                AND policy_manifest_hash::pg_catalog.text
                    OPERATOR(pg_catalog.~) '^[0-9a-f]{64}$'
            )
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_pipeline_runtime_authority_state
        ON pipeline_runtime_authority(state, account_id)
        """
    )

    op.execute(
        """
        CREATE TABLE pipeline_runtime_instances (
            account_id pg_catalog.int8 NOT NULL,
            workload pg_catalog.text NOT NULL,
            instance_id pg_catalog.text NOT NULL,
            session_id pg_catalog.uuid NOT NULL,
            generation pg_catalog.int8 NOT NULL,
            fencing_token pg_catalog.int8 NOT NULL,
            authority_epoch pg_catalog.int8 NOT NULL,
            capability_hash pg_catalog.bpchar(64) NOT NULL,
            capability_stage_ordinal pg_catalog.int2 NOT NULL DEFAULT 1,
            schema_revision pg_catalog.text NOT NULL,
            protocol_version pg_catalog.int8 NOT NULL,
            build_id pg_catalog.text NOT NULL,
            config_hash pg_catalog.bpchar(64) NOT NULL,
            lifecycle pg_catalog.text NOT NULL,
            lease_version pg_catalog.int8 NOT NULL,
            accepted_count pg_catalog.int8 NOT NULL DEFAULT 0,
            rejected_count pg_catalog.int8 NOT NULL DEFAULT 0,
            registered_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            heartbeat_at pg_catalog.timestamptz NOT NULL,
            lease_until pg_catalog.timestamptz NOT NULL,
            updated_at pg_catalog.timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_pipeline_runtime_instances PRIMARY KEY (session_id),
            CONSTRAINT uq_pipeline_runtime_instances_session UNIQUE (
                account_id,
                session_id
            ),
            CONSTRAINT uq_pipeline_runtime_instances_lease_identity UNIQUE (
                session_id,
                account_id,
                generation,
                fencing_token,
                authority_epoch,
                capability_hash
            ),
            CONSTRAINT fk_pipeline_runtime_instances_capability FOREIGN KEY (
                capability_hash,
                capability_stage_ordinal,
                schema_revision,
                protocol_version,
                build_id,
                config_hash
            ) REFERENCES pipeline_runtime_capabilities (
                capability_hash,
                stage_ordinal,
                schema_revision,
                protocol_version,
                minimum_build_id,
                config_hash
            ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
            CONSTRAINT ck_pipeline_runtime_instances_identity CHECK (
                account_id > 0
                AND generation = 1
                AND fencing_token = 1
                AND authority_epoch > 0
                AND authority_epoch < 9223372036854775807
                AND pg_catalog.btrim(instance_id) = instance_id
                AND pg_catalog.btrim(instance_id) <> ''
                AND pg_catalog.char_length(instance_id) <= 128
            ),
            CONSTRAINT ck_pipeline_runtime_instances_workload CHECK (
                workload IN ('web', 'worker', 'scheduler', 'reaper')
            ),
            CONSTRAINT ck_pipeline_runtime_instances_contract CHECK (
                schema_revision OPERATOR(pg_catalog.~)
                    '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'
                AND protocol_version > 0
                AND build_id OPERATOR(pg_catalog.~)
                    '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'
                AND config_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
                AND capability_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                    '^[0-9a-f]{64}$'
            ),
            CONSTRAINT ck_pipeline_runtime_instances_lifecycle CHECK (
                lifecycle IN ('standby', 'active', 'draining')
                AND (
                    lifecycle <> 'active'
                    OR workload = 'web'
                    OR capability_stage_ordinal = 3
                )
            ),
            CONSTRAINT ck_pipeline_runtime_instances_counters CHECK (
                lease_version > 0
                AND lease_version < 9223372036854775807
                AND accepted_count >= 0
                AND accepted_count < 9223372036854775807
                AND rejected_count >= 0
                AND rejected_count < 9223372036854775807
            ),
            CONSTRAINT ck_pipeline_runtime_instances_lease CHECK (
                heartbeat_at >= registered_at
                AND lease_until > heartbeat_at
                AND updated_at >= registered_at
            )
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_pipeline_runtime_instances_live_identity
        ON pipeline_runtime_instances(account_id, workload, instance_id)
        WHERE lifecycle <> 'draining'
        """
    )
    op.execute(
        """
        CREATE INDEX ix_pipeline_runtime_instances_lease
        ON pipeline_runtime_instances(lease_until, session_id)
        WHERE lifecycle <> 'draining'
        """
    )
    op.execute(
        """
        CREATE INDEX ix_pipeline_runtime_instances_authority
        ON pipeline_runtime_instances(
            account_id,
            generation,
            fencing_token,
            authority_epoch,
            capability_hash,
            lifecycle
        )
        """
    )

    op.execute("ALTER TABLE emails DROP CONSTRAINT fk_emails_processing_inbox")
    op.execute(
        "ALTER TABLE event_inbox "
        "DROP CONSTRAINT uq_event_inbox_processing_identity, "
        "DROP CONSTRAINT ck_event_inbox_lease"
    )
    op.execute(
        """
        ALTER TABLE event_inbox
        ADD COLUMN execution_epoch pg_catalog.int8 NOT NULL DEFAULT 0,
        ADD COLUMN authority_epoch pg_catalog.int8 NOT NULL,
        ADD COLUMN capability_hash pg_catalog.bpchar(64) NOT NULL,
        ADD COLUMN lease_session_id pg_catalog.uuid,
        ADD CONSTRAINT ck_event_inbox_execution_epoch CHECK (
            execution_epoch >= 0
            AND execution_epoch < 9223372036854775807
        ),
        ADD CONSTRAINT ck_event_inbox_runtime_authority CHECK (
            authority_epoch > 0
            AND authority_epoch < 9223372036854775807
            AND capability_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                '^[0-9a-f]{64}$'
        ),
        ADD CONSTRAINT uq_event_inbox_processing_identity UNIQUE (
            id,
            account_id,
            external_email_id,
            generation,
            fencing_token,
            execution_epoch,
            authority_epoch,
            capability_hash
        ),
        ADD CONSTRAINT fk_event_inbox_runtime_capability FOREIGN KEY (
            capability_hash
        ) REFERENCES pipeline_runtime_capabilities (
            capability_hash
        ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
        ADD CONSTRAINT fk_event_inbox_lease_session FOREIGN KEY (
            lease_session_id,
            account_id,
            generation,
            fencing_token,
            authority_epoch,
            capability_hash
        ) REFERENCES pipeline_runtime_instances (
            session_id,
            account_id,
            generation,
            fencing_token,
            authority_epoch,
            capability_hash
        ) MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
        ADD CONSTRAINT ck_event_inbox_lease CHECK (
            (
                status = 'leased'
                AND lease_owner IS NOT NULL
                AND lease_until IS NOT NULL
                AND lease_session_id IS NOT NULL
                AND pg_catalog.btrim(lease_owner) <> ''
                AND pg_catalog.char_length(lease_owner) <= 128
            ) OR (
                status <> 'leased'
                AND lease_owner IS NULL
                AND lease_until IS NULL
                AND lease_session_id IS NULL
            )
        )
        """
    )
    op.execute("DROP INDEX ix_event_inbox_expired_lease")
    op.execute(
        """
        CREATE INDEX ix_event_inbox_expired_lease
        ON event_inbox(
            lease_until,
            execution_epoch,
            authority_epoch,
            capability_hash,
            lease_session_id,
            id
        )
        WHERE status = 'leased'
        """
    )

    op.execute("ALTER TABLE emails DROP CONSTRAINT uq_emails_outbox_identity")
    op.execute("DROP INDEX ix_emails_owner_status")
    op.execute(
        """
        ALTER TABLE emails
        ADD COLUMN owner_authority_epoch pg_catalog.int8 NOT NULL,
        ADD COLUMN owner_capability_hash pg_catalog.bpchar(64) NOT NULL,
        ADD COLUMN processing_execution_epoch pg_catalog.int8,
        ADD CONSTRAINT uq_emails_outbox_identity UNIQUE (
            id,
            account_id,
            owner_generation,
            owner_fencing_token,
            owner_authority_epoch,
            owner_capability_hash
        ),
        ADD CONSTRAINT ck_emails_runtime_ownership CHECK (
            owner_authority_epoch > 0
            AND owner_authority_epoch < 9223372036854775807
            AND owner_capability_hash::pg_catalog.text OPERATOR(pg_catalog.~)
                '^[0-9a-f]{64}$'
        ),
        ADD CONSTRAINT ck_emails_processing_runtime_identity CHECK (
            (
                processing_inbox_id IS NULL
                AND processing_execution_epoch IS NULL
            ) OR (
                processing_inbox_id IS NOT NULL
                AND processing_execution_epoch IS NOT NULL
                AND processing_execution_epoch >= 0
                AND processing_execution_epoch < 9223372036854775807
            )
        ),
        ADD CONSTRAINT fk_emails_runtime_capability FOREIGN KEY (
            owner_capability_hash
        ) REFERENCES pipeline_runtime_capabilities (
            capability_hash
        ) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT NOT DEFERRABLE,
        ADD CONSTRAINT fk_emails_processing_inbox FOREIGN KEY (
            processing_inbox_id,
            account_id,
            external_email_id,
            owner_generation,
            owner_fencing_token,
            processing_execution_epoch,
            owner_authority_epoch,
            owner_capability_hash
        ) REFERENCES event_inbox (
            id,
            account_id,
            external_email_id,
            generation,
            fencing_token,
            execution_epoch,
            authority_epoch,
            capability_hash
        ) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE RESTRICT
            DEFERRABLE INITIALLY DEFERRED
        """
    )
    op.execute(
        """
        CREATE INDEX ix_emails_owner_status
        ON emails(
            account_id,
            owner_generation,
            owner_fencing_token,
            owner_authority_epoch,
            owner_capability_hash,
            status
        )
        """
    )

    op.execute("DROP TRIGGER trg_event_inbox_guard_update ON event_inbox")
    op.execute("DROP FUNCTION guard_event_inbox_update()")
    op.execute("DROP TRIGGER trg_emails_processing_owner ON emails")
    op.execute("DROP FUNCTION enforce_email_processing_owner()")

    _create_guard_functions()
    _create_guard_triggers()
    _revoke_guard_execution()
    _create_greenfield_security_definer_functions()
    _revoke_greenfield_function_execution()


def _create_guard_functions() -> None:
    op.execute(
        """
        CREATE FUNCTION reject_pipeline_runtime_capabilities_mutation()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
            RAISE EXCEPTION 'pipeline runtime capabilities are append-only';
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_pipeline_initializations_mutation()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
            receipt_is_exact pg_catalog.bool;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                EXECUTE pg_catalog.format(
                    'SELECT EXISTS ('
                    'SELECT 1 FROM %I.pipeline_command_receipts '
                    'WHERE id = $1 '
                    'AND account_id = $2 '
                    'AND command_name = ''runtime.initialize'' '
                    'AND authority_epoch = $3 '
                    'AND result_type = ''runtime_initialization'' '
                    'AND result_id = $4)',
                    TG_TABLE_SCHEMA
                )
                INTO receipt_is_exact
                USING
                    NEW.command_receipt_id,
                    NEW.account_id,
                    NEW.authority_epoch,
                    NEW.initialization_id::pg_catalog.text;
                IF NOT receipt_is_exact THEN
                    RAISE EXCEPTION 'initialization receipt identity is not exact';
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'pipeline initializations are append-only';
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_pipeline_folder_scopes_mutation()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
            element_count pg_catalog.int8;
            distinct_count pg_catalog.int8;
            valid_count pg_catalog.int8;
            stored_ids pg_catalog.text[];
            canonical_ids pg_catalog.text[];
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'pipeline folder scopes are append-only';
            END IF;

            SELECT
                pg_catalog.count(*),
                pg_catalog.count(DISTINCT element #>> '{}'),
                pg_catalog.count(*) FILTER (
                    WHERE pg_catalog.jsonb_typeof(element) = 'string'
                      AND pg_catalog.btrim(element #>> '{}') = element #>> '{}'
                      AND pg_catalog.btrim(element #>> '{}') <> ''
                      AND pg_catalog.char_length(element #>> '{}') <= 512
                ),
                pg_catalog.array_agg(
                    element #>> '{}' ORDER BY position
                ),
                pg_catalog.array_agg(
                    element #>> '{}' ORDER BY (element #>> '{}') COLLATE "C"
                )
            INTO
                element_count,
                distinct_count,
                valid_count,
                stored_ids,
                canonical_ids
            FROM pg_catalog.jsonb_array_elements(NEW.webhook_ids)
                WITH ORDINALITY AS item(element, position);

            IF element_count = 0
               OR element_count <> distinct_count
               OR element_count <> valid_count
               OR stored_ids IS DISTINCT FROM canonical_ids THEN
                RAISE EXCEPTION 'pipeline folder scope webhook_ids are invalid';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_pipeline_runtime_authority()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
            exact_successor pg_catalog.bool;
        BEGIN
            IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION 'pipeline runtime authority cannot be removed';
            END IF;
            IF TG_OP = 'INSERT' THEN
                RETURN NEW;
            END IF;

            IF NEW.account_id IS DISTINCT FROM OLD.account_id
               OR NEW.generation IS DISTINCT FROM OLD.generation
               OR NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
               OR NEW.pipeline_name IS DISTINCT FROM OLD.pipeline_name
               OR NEW.initialization_id IS DISTINCT FROM OLD.initialization_id
               OR NEW.policy_manifest_hash IS DISTINCT FROM OLD.policy_manifest_hash
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'pipeline runtime authority identity is immutable';
            END IF;
            IF NEW.authority_epoch <> OLD.authority_epoch + 1
               OR NEW.version <> OLD.version + 1
               OR NEW.updated_at < OLD.updated_at THEN
                RAISE EXCEPTION 'pipeline runtime authority must advance monotonically';
            END IF;

            IF NEW.capability_hash IS NOT DISTINCT FROM OLD.capability_hash THEN
                IF NEW.capability_stage_ordinal
                        IS DISTINCT FROM OLD.capability_stage_ordinal
                   OR NEW.schema_revision IS DISTINCT FROM OLD.schema_revision
                   OR NEW.protocol_version IS DISTINCT FROM OLD.protocol_version
                   OR NEW.build_id IS DISTINCT FROM OLD.build_id
                   OR NEW.config_hash IS DISTINCT FROM OLD.config_hash THEN
                    RAISE EXCEPTION 'runtime contract cannot drift within a capability';
                END IF;
            ELSE
                IF NEW.authority_epoch <> OLD.authority_epoch + 1 THEN
                    RAISE EXCEPTION 'a capability successor must advance authority';
                END IF;
                EXECUTE pg_catalog.format(
                    'SELECT EXISTS ('
                    'SELECT 1 FROM %I.pipeline_runtime_capabilities '
                    'WHERE capability_hash = $1 '
                    'AND predecessor_hash = $2 '
                    'AND stage_ordinal = $3) ',
                    TG_TABLE_SCHEMA
                )
                INTO exact_successor
                USING
                    NEW.capability_hash,
                    OLD.capability_hash,
                    OLD.capability_stage_ordinal + 1;
                IF NOT exact_successor THEN
                    RAISE EXCEPTION 'runtime capability successor is not exact';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_pipeline_runtime_instances()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
            IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION 'pipeline runtime instance history cannot be removed';
            END IF;
            IF TG_OP = 'INSERT' THEN
                RETURN NEW;
            END IF;

            IF NEW.account_id IS DISTINCT FROM OLD.account_id
               OR NEW.workload IS DISTINCT FROM OLD.workload
               OR NEW.instance_id IS DISTINCT FROM OLD.instance_id
               OR NEW.session_id IS DISTINCT FROM OLD.session_id
               OR NEW.generation IS DISTINCT FROM OLD.generation
               OR NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
               OR NEW.authority_epoch IS DISTINCT FROM OLD.authority_epoch
               OR NEW.capability_hash IS DISTINCT FROM OLD.capability_hash
               OR NEW.capability_stage_ordinal
                    IS DISTINCT FROM OLD.capability_stage_ordinal
               OR NEW.schema_revision IS DISTINCT FROM OLD.schema_revision
               OR NEW.protocol_version IS DISTINCT FROM OLD.protocol_version
               OR NEW.build_id IS DISTINCT FROM OLD.build_id
               OR NEW.config_hash IS DISTINCT FROM OLD.config_hash
               OR NEW.registered_at IS DISTINCT FROM OLD.registered_at THEN
                RAISE EXCEPTION 'pipeline runtime instance identity is immutable';
            END IF;
            IF NEW.lease_version <> OLD.lease_version + 1
               OR NEW.accepted_count < OLD.accepted_count
               OR NEW.rejected_count < OLD.rejected_count
               OR NEW.heartbeat_at < OLD.heartbeat_at
               OR NEW.lease_until <= NEW.heartbeat_at
               OR NEW.updated_at < OLD.updated_at THEN
                RAISE EXCEPTION 'pipeline runtime instance must advance monotonically';
            END IF;
            IF NOT (
                (OLD.lifecycle = 'standby'
                    AND NEW.lifecycle IN ('standby', 'active', 'draining'))
                OR (OLD.lifecycle = 'active'
                    AND NEW.lifecycle IN ('active', 'draining'))
                OR (OLD.lifecycle = 'draining'
                    AND NEW.lifecycle = 'draining')
            ) THEN
                RAISE EXCEPTION 'pipeline runtime instance lifecycle regressed';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_event_inbox_runtime_identity()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.execution_epoch <> 0 THEN
                    RAISE EXCEPTION 'new Inbox work must start at execution epoch zero';
                END IF;
                RETURN NEW;
            END IF;

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
               OR NEW.authority_epoch IS DISTINCT FROM OLD.authority_epoch
               OR NEW.capability_hash IS DISTINCT FROM OLD.capability_hash
               OR NEW.received_at IS DISTINCT FROM OLD.received_at THEN
                RAISE EXCEPTION 'event inbox runtime identity is immutable';
            END IF;

            IF NEW.execution_epoch = OLD.execution_epoch + 1 THEN
                IF OLD.status NOT IN ('manual_review', 'dead_letter')
                   OR OLD.effect_started_at IS NOT NULL
                   OR NEW.status <> 'pending'
                   OR NEW.attempts <> 0
                   OR NEW.lease_owner IS NOT NULL
                   OR NEW.lease_until IS NOT NULL
                   OR NEW.lease_session_id IS NOT NULL
                   OR NEW.processing_started_at IS NOT NULL
                   OR NEW.effect_started_at IS NOT NULL
                   OR NEW.safe_error_code IS NOT NULL
                   OR NEW.safe_error_summary IS NOT NULL THEN
                    RAISE EXCEPTION 'event inbox requeue epoch transition rejected';
                END IF;
                RETURN NEW;
            END IF;

            IF NEW.execution_epoch <> OLD.execution_epoch THEN
                RAISE EXCEPTION 'event inbox execution epoch must advance by one';
            END IF;
            IF OLD.processing_started_at IS NOT NULL
               AND NEW.processing_started_at
                    IS DISTINCT FROM OLD.processing_started_at THEN
                RAISE EXCEPTION 'processing marker is immutable within an epoch';
            END IF;
            IF OLD.effect_started_at IS NOT NULL
               AND NEW.effect_started_at IS DISTINCT FROM OLD.effect_started_at THEN
                RAISE EXCEPTION 'effect marker is immutable within an epoch';
            END IF;
            IF NEW.attempts < OLD.attempts THEN
                RAISE EXCEPTION 'event attempts cannot decrease within an epoch';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION guard_emails_runtime_identity()
        RETURNS pg_catalog.trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
            owner_is_create pg_catalog.bool;
            epoch_advanced pg_catalog.bool;
            owner_cleared pg_catalog.bool;
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF NEW.id IS DISTINCT FROM OLD.id
                   OR NEW.account_id IS DISTINCT FROM OLD.account_id
                   OR NEW.external_email_id IS DISTINCT FROM OLD.external_email_id
                   OR NEW.owner_generation IS DISTINCT FROM OLD.owner_generation
                   OR NEW.owner_fencing_token
                        IS DISTINCT FROM OLD.owner_fencing_token
                   OR NEW.owner_authority_epoch
                        IS DISTINCT FROM OLD.owner_authority_epoch
                   OR NEW.owner_capability_hash
                        IS DISTINCT FROM OLD.owner_capability_hash
                   OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION 'email runtime ownership is immutable';
                END IF;

                epoch_advanced :=
                    OLD.processing_execution_epoch IS NOT NULL
                    AND NEW.processing_execution_epoch =
                        OLD.processing_execution_epoch + 1
                    AND NEW.processing_inbox_id
                        IS NOT DISTINCT FROM OLD.processing_inbox_id;
                owner_cleared :=
                    OLD.processing_inbox_id IS NOT NULL
                    AND NEW.processing_inbox_id IS NULL
                    AND NEW.processing_execution_epoch IS NULL;
                IF NEW.processing_execution_epoch
                        IS DISTINCT FROM OLD.processing_execution_epoch
                   AND NOT (
                        epoch_advanced
                        OR owner_cleared
                        OR (
                            OLD.processing_execution_epoch IS NULL
                            AND NEW.processing_execution_epoch IS NOT NULL
                            AND NEW.processing_inbox_id IS NOT NULL
                        )
                   ) THEN
                    RAISE EXCEPTION 'email processing epoch transition rejected';
                END IF;
                IF OLD.status <> 'ingested' AND NEW.status = 'ingested'
                   AND NOT epoch_advanced THEN
                    RAISE EXCEPTION 'email cannot return to ingested state';
                END IF;
                IF (OLD.create_seen_at IS NOT NULL
                       AND NEW.create_seen_at IS DISTINCT FROM OLD.create_seen_at)
                   OR (
                       OLD.processing_started_at IS NOT NULL
                       AND NEW.processing_started_at
                            IS DISTINCT FROM OLD.processing_started_at
                       AND NOT epoch_advanced
                   )
                   OR (OLD.source_deleted_at IS NOT NULL
                       AND NEW.source_deleted_at
                            IS DISTINCT FROM OLD.source_deleted_at)
                   OR (
                       OLD.external_effects_started_at IS NOT NULL
                       AND NEW.external_effects_started_at
                            IS DISTINCT FROM OLD.external_effects_started_at
                   ) THEN
                    RAISE EXCEPTION 'email processing facts are immutable';
                END IF;
                IF OLD.processing_inbox_id IS NOT NULL
                   AND NEW.processing_inbox_id
                        IS DISTINCT FROM OLD.processing_inbox_id
                   AND NOT owner_cleared THEN
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

            IF NEW.processing_inbox_id IS NOT NULL THEN
                EXECUTE pg_catalog.format(
                    'SELECT EXISTS ('
                    'SELECT 1 FROM %I.event_inbox AS inbox '
                    'WHERE inbox.id = $1 '
                    'AND inbox.account_id = $2 '
                    'AND inbox.external_email_id = $3 '
                    'AND inbox.generation = $4 '
                    'AND inbox.fencing_token = $5 '
                    'AND inbox.execution_epoch = $6 '
                    'AND inbox.authority_epoch = $7 '
                    'AND inbox.capability_hash = $8 '
                    'AND inbox.change_kind = ''create'')',
                    TG_TABLE_SCHEMA
                )
                INTO owner_is_create
                USING
                    NEW.processing_inbox_id,
                    NEW.account_id,
                    NEW.external_email_id,
                    NEW.owner_generation,
                    NEW.owner_fencing_token,
                    NEW.processing_execution_epoch,
                    NEW.owner_authority_epoch,
                    NEW.owner_capability_hash;
                IF NOT owner_is_create THEN
                    RAISE EXCEPTION 'processing owner must be an exact create epoch';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
        """
    )


def _create_guard_triggers() -> None:
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_runtime_capabilities_guard_row
        BEFORE UPDATE OR DELETE ON pipeline_runtime_capabilities
        FOR EACH ROW
        EXECUTE FUNCTION reject_pipeline_runtime_capabilities_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_runtime_capabilities_guard_truncate
        BEFORE TRUNCATE ON pipeline_runtime_capabilities
        FOR EACH STATEMENT
        EXECUTE FUNCTION reject_pipeline_runtime_capabilities_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_initializations_guard_row
        BEFORE INSERT OR UPDATE OR DELETE ON pipeline_initializations
        FOR EACH ROW EXECUTE FUNCTION reject_pipeline_initializations_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_initializations_guard_truncate
        BEFORE TRUNCATE ON pipeline_initializations
        FOR EACH STATEMENT
        EXECUTE FUNCTION reject_pipeline_initializations_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_folder_scopes_guard_row
        BEFORE INSERT OR UPDATE OR DELETE ON pipeline_folder_scopes
        FOR EACH ROW EXECUTE FUNCTION reject_pipeline_folder_scopes_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_folder_scopes_guard_truncate
        BEFORE TRUNCATE ON pipeline_folder_scopes
        FOR EACH STATEMENT
        EXECUTE FUNCTION reject_pipeline_folder_scopes_mutation()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_runtime_authority_guard_row
        BEFORE INSERT OR UPDATE OR DELETE ON pipeline_runtime_authority
        FOR EACH ROW EXECUTE FUNCTION guard_pipeline_runtime_authority()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_runtime_authority_guard_truncate
        BEFORE TRUNCATE ON pipeline_runtime_authority
        FOR EACH STATEMENT EXECUTE FUNCTION guard_pipeline_runtime_authority()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_runtime_instances_guard_row
        BEFORE INSERT OR UPDATE OR DELETE ON pipeline_runtime_instances
        FOR EACH ROW EXECUTE FUNCTION guard_pipeline_runtime_instances()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_pipeline_runtime_instances_guard_truncate
        BEFORE TRUNCATE ON pipeline_runtime_instances
        FOR EACH STATEMENT EXECUTE FUNCTION guard_pipeline_runtime_instances()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_event_inbox_runtime_identity
        BEFORE INSERT OR UPDATE ON event_inbox
        FOR EACH ROW EXECUTE FUNCTION guard_event_inbox_runtime_identity()
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER trg_emails_runtime_identity
        AFTER INSERT OR UPDATE ON emails
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION guard_emails_runtime_identity()
        """
    )


def _revoke_guard_execution() -> None:
    op.execute(
        """
        REVOKE ALL ON FUNCTION
            reject_pipeline_runtime_capabilities_mutation()
        FROM PUBLIC;
        REVOKE ALL ON FUNCTION reject_pipeline_initializations_mutation()
        FROM PUBLIC;
        REVOKE ALL ON FUNCTION reject_pipeline_folder_scopes_mutation()
        FROM PUBLIC;
        REVOKE ALL ON FUNCTION guard_pipeline_runtime_authority()
        FROM PUBLIC;
        REVOKE ALL ON FUNCTION guard_pipeline_runtime_instances()
        FROM PUBLIC;
        REVOKE ALL ON FUNCTION guard_event_inbox_runtime_identity()
        FROM PUBLIC;
        REVOKE ALL ON FUNCTION guard_emails_runtime_identity()
        FROM PUBLIC
        """
    )


def _create_greenfield_security_definer_functions() -> None:
    _create_greenfield_authority_functions()
    _create_greenfield_instance_functions()
    _create_greenfield_webhook_function()
    _create_greenfield_recovery_function()
    _create_phase2_worker_function_stubs()


def _create_greenfield_authority_functions() -> None:
    _create_greenfield_get_authority_function()
    _create_greenfield_initialize_function()
    _create_greenfield_transition_functions()


def _create_greenfield_instance_functions() -> None:
    _create_greenfield_register_instance_function()
    _create_greenfield_heartbeat_instance_function()
    _create_greenfield_drain_instance_function()


def _create_greenfield_get_authority_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.greenfield_get_runtime_authority(
            p_account_id pg_catalog.int8
        )
        RETURNS TABLE (
            account_id pg_catalog.int8,
            state pg_catalog.text,
            generation pg_catalog.int8,
            fencing_token pg_catalog.int8,
            pipeline_name pg_catalog.text,
            authority_epoch pg_catalog.int8,
            version pg_catalog.int8,
            schema_revision pg_catalog.text,
            protocol_version pg_catalog.int8,
            build_id pg_catalog.text,
            config_hash pg_catalog.text,
            capability_hash pg_catalog.text,
            policy_manifest_hash pg_catalog.text,
            initialization_id pg_catalog.uuid,
            updated_at pg_catalog.timestamptz
        )
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $greenfield_get_runtime_authority$
            SELECT
                authority.account_id,
                authority.state,
                authority.generation,
                authority.fencing_token,
                authority.pipeline_name,
                authority.authority_epoch,
                authority.version,
                authority.schema_revision,
                authority.protocol_version,
                authority.build_id,
                authority.config_hash::pg_catalog.text,
                authority.capability_hash::pg_catalog.text,
                authority.policy_manifest_hash::pg_catalog.text,
                authority.initialization_id,
                authority.updated_at
            FROM public.pipeline_runtime_authority AS authority
            WHERE authority.account_id = p_account_id
              AND p_account_id > 0
        $greenfield_get_runtime_authority$
        """
    )


def _create_greenfield_initialize_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.greenfield_initialize_runtime(
            p_account_id pg_catalog.int8,
            p_capability_hash pg_catalog.text,
            p_predecessor_hash pg_catalog.text,
            p_capability_stage pg_catalog.text,
            p_schema_revision pg_catalog.text,
            p_schema_digest pg_catalog.text,
            p_protocol_version pg_catalog.int8,
            p_minimum_build_id pg_catalog.text,
            p_config_hash pg_catalog.text,
            p_adapter_hash pg_catalog.text,
            p_policy_manifest_hash pg_catalog.text,
            p_evidence_manifest_hash pg_catalog.text,
            p_policy_manifest_json pg_catalog.text,
            p_policy_scope_count pg_catalog.int8,
            p_actor pg_catalog.text,
            p_reason pg_catalog.text,
            p_idempotency_key pg_catalog.text,
            p_canonical_payload_hash pg_catalog.text
        )
        RETURNS TABLE (
            initialization_id pg_catalog.uuid,
            command_receipt_id pg_catalog.uuid,
            account_id pg_catalog.int8,
            generation pg_catalog.int8,
            fencing_token pg_catalog.int8,
            pipeline_name pg_catalog.text,
            authority_epoch pg_catalog.int8,
            authority_version pg_catalog.int8,
            capability_hash pg_catalog.text,
            policy_manifest_hash pg_catalog.text,
            transaction_id pg_catalog.text,
            replayed pg_catalog.bool,
            created_at pg_catalog.timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $greenfield_initialize_runtime$
        DECLARE
            v_zero pg_catalog.bytea := pg_catalog.decode('00', 'hex');
            v_unicode_edge_spaces pg_catalog.text :=
                pg_catalog.chr(133) || pg_catalog.chr(160) ||
                pg_catalog.chr(5760) || pg_catalog.chr(8192) ||
                pg_catalog.chr(8193) || pg_catalog.chr(8194) ||
                pg_catalog.chr(8195) || pg_catalog.chr(8196) ||
                pg_catalog.chr(8197) || pg_catalog.chr(8198) ||
                pg_catalog.chr(8199) || pg_catalog.chr(8200) ||
                pg_catalog.chr(8201) || pg_catalog.chr(8202) ||
                pg_catalog.chr(8232) || pg_catalog.chr(8233) ||
                pg_catalog.chr(8239) || pg_catalog.chr(8287) ||
                pg_catalog.chr(12288);
            v_policy pg_catalog.jsonb;
            v_scope pg_catalog.jsonb;
            v_event_policy pg_catalog.jsonb;
            v_scope_key pg_catalog.text;
            v_sync_folder pg_catalog.text;
            v_matrix_canonical pg_catalog.text;
            v_webhook_canonical pg_catalog.text;
            v_scope_config_canonical pg_catalog.text;
            v_scope_manifest_canonical pg_catalog.text;
            v_rebuilt_policy pg_catalog.text;
            v_rebuilt_policy_ascii pg_catalog.text := '';
            v_character pg_catalog.text;
            v_codepoint pg_catalog.int8;
            v_surrogate_value pg_catalog.int8;
            v_character_position pg_catalog.int8;
            v_expected_scope_hash pg_catalog.text;
            v_webhook_values pg_catalog.text[];
            v_webhook_sorted pg_catalog.text[];
            v_seen_webhook_ids pg_catalog.text[] := ARRAY[]::pg_catalog.text[];
            v_seen_scope_keys pg_catalog.text[] := ARRAY[]::pg_catalog.text[];
            v_seen_sync_folders pg_catalog.text[] := ARRAY[]::pg_catalog.text[];
            v_webhook_count pg_catalog.int8;
            v_webhook_distinct_count pg_catalog.int8;
            v_webhook_valid_count pg_catalog.int8;
            v_matrix_count pg_catalog.int8;
            v_matrix_distinct_count pg_catalog.int8;
            v_matrix_valid_count pg_catalog.int8;
            v_policy_scope_rows pg_catalog.int8 := 0;
            v_capability_canonical pg_catalog.text;
            v_initialization_canonical pg_catalog.text;
            v_expected_capability_hash pg_catalog.text;
            v_expected_policy_hash pg_catalog.text;
            v_expected_payload_hash pg_catalog.text;
            v_idempotency_hash pg_catalog.text;
            v_initialization_id pg_catalog.uuid;
            v_receipt_id pg_catalog.uuid;
            v_transaction_id pg_catalog.text;
            v_result_hash pg_catalog.text;
            v_created_at pg_catalog.timestamptz;
            v_scope_rows pg_catalog.int8 := 0;
            v_checkpoint_nonempty pg_catalog.bool := false;
            v_existing_receipt public.pipeline_command_receipts%ROWTYPE;
            v_existing_initialization public.pipeline_initializations%ROWTYPE;
            v_existing_capability public.pipeline_runtime_capabilities%ROWTYPE;
        BEGIN
            IF p_account_id IS NULL OR p_account_id <= 0
               OR p_capability_hash IS NULL
               OR p_predecessor_hash IS NULL
               OR p_capability_stage <> 'phase2_ingestion'
               OR p_schema_revision IS NULL
               OR p_schema_digest IS NULL
               OR p_schema_revision <> '20260716_0006'
               OR p_protocol_version IS NULL OR p_protocol_version <= 0
               OR p_minimum_build_id IS NULL
               OR p_config_hash IS NULL
               OR p_adapter_hash IS NULL
               OR p_policy_manifest_hash IS NULL
               OR p_evidence_manifest_hash IS NULL
               OR p_policy_manifest_json IS NULL
               OR p_policy_scope_count IS NULL OR p_policy_scope_count <= 0
               OR p_canonical_payload_hash IS NULL
               OR p_capability_hash !~ '^[0-9a-f]{64}$'
               OR p_predecessor_hash <>
                    '95771c6d473119376654d5530f7fe189c77d83e56fe08e91179f48b1040df86f'
               OR p_schema_digest !~ '^[0-9a-f]{64}$'
               OR p_config_hash !~ '^[0-9a-f]{64}$'
               OR p_adapter_hash !~ '^[0-9a-f]{64}$'
               OR p_policy_manifest_hash !~ '^[0-9a-f]{64}$'
               OR p_evidence_manifest_hash !~ '^[0-9a-f]{64}$'
               OR p_canonical_payload_hash !~ '^[0-9a-f]{64}$'
               OR p_schema_revision !~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'
               OR p_minimum_build_id !~
                    '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'
               OR p_actor IS NULL OR pg_catalog.btrim(p_actor) <> p_actor
               OR pg_catalog.btrim(p_actor, v_unicode_edge_spaces) <> p_actor
               OR p_actor ~ '^[[:space:]]|[[:space:]]$'
               OR pg_catalog.octet_length(p_actor) NOT BETWEEN 1 AND 128
               OR p_actor ~ '[[:cntrl:]]'
               OR p_reason IS NULL OR pg_catalog.btrim(p_reason) <> p_reason
               OR pg_catalog.btrim(p_reason, v_unicode_edge_spaces) <> p_reason
               OR p_reason ~ '^[[:space:]]|[[:space:]]$'
               OR pg_catalog.octet_length(p_reason) NOT BETWEEN 1 AND 512
               OR p_reason ~ '[[:cntrl:]]'
               OR p_idempotency_key IS NULL
               OR pg_catalog.btrim(p_idempotency_key) <> p_idempotency_key
               OR pg_catalog.btrim(
                    p_idempotency_key, v_unicode_edge_spaces
               ) <> p_idempotency_key
               OR p_idempotency_key ~ '^[[:space:]]|[[:space:]]$'
               OR p_idempotency_key ~ '[[:cntrl:]]'
               OR pg_catalog.octet_length(p_idempotency_key) NOT BETWEEN 1 AND 4096
            THEN
                RAISE EXCEPTION 'greenfield_initialization_input_invalid'
                    USING ERRCODE = 'P0001';
            END IF;

            BEGIN
                v_policy := p_policy_manifest_json::pg_catalog.jsonb;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'greenfield_initialization_input_invalid'
                    USING ERRCODE = 'P0001';
            END;
            IF pg_catalog.jsonb_typeof(v_policy) <> 'object'
               OR (v_policy - 'schema_version' - 'scopes') <>
                    '{}'::pg_catalog.jsonb
               OR v_policy ->> 'schema_version' <> '1'
               OR pg_catalog.jsonb_typeof(v_policy -> 'scopes') <> 'array'
               OR pg_catalog.jsonb_array_length(v_policy -> 'scopes') <>
                    p_policy_scope_count
            THEN
                RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                    USING ERRCODE = 'P0001';
            END IF;

            v_rebuilt_policy :=
                '{"schema_version":' || '1' || ',"scopes":[';
            FOR v_scope IN
                SELECT scope.value
                FROM pg_catalog.jsonb_array_elements(v_policy -> 'scopes')
                    AS scope(value)
                ORDER BY (scope.value ->> 'canonical_key') COLLATE "C"
            LOOP
                IF pg_catalog.jsonb_typeof(v_scope) <> 'object'
                   OR (v_scope - 'canonical_key' - 'event_policy_matrix' -
                        'scope_hash' - 'sync_folder' - 'webhook_ids') <>
                        '{}'::pg_catalog.jsonb
                   OR pg_catalog.jsonb_typeof(v_scope -> 'canonical_key') <>
                        'string'
                   OR pg_catalog.jsonb_typeof(v_scope -> 'sync_folder') <>
                        'string'
                   OR pg_catalog.jsonb_typeof(v_scope -> 'scope_hash') <>
                        'string'
                   OR pg_catalog.jsonb_typeof(v_scope -> 'webhook_ids') <>
                        'array'
                   OR pg_catalog.jsonb_typeof(
                        v_scope -> 'event_policy_matrix'
                   ) <> 'array'
                THEN
                    RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                        USING ERRCODE = 'P0001';
                END IF;

                v_scope_key := v_scope ->> 'canonical_key';
                v_sync_folder := v_scope ->> 'sync_folder';
                IF v_scope_key IS NULL
                   OR pg_catalog.btrim(v_scope_key) <> v_scope_key
                   OR pg_catalog.btrim(
                        v_scope_key, v_unicode_edge_spaces
                   ) <> v_scope_key
                   OR v_scope_key ~ '^[[:space:]]|[[:space:]]$'
                   OR v_scope_key ~ '[[:cntrl:]]'
                   OR pg_catalog.char_length(v_scope_key) NOT BETWEEN 1 AND 512
                   OR v_sync_folder IS NULL
                   OR pg_catalog.btrim(v_sync_folder) <> v_sync_folder
                   OR pg_catalog.btrim(
                        v_sync_folder, v_unicode_edge_spaces
                   ) <> v_sync_folder
                   OR v_sync_folder ~ '^[[:space:]]|[[:space:]]$'
                   OR v_sync_folder ~ '[[:cntrl:]]'
                   OR pg_catalog.char_length(v_sync_folder) NOT BETWEEN 1 AND 512
                   OR (v_scope ->> 'scope_hash') !~ '^[0-9a-f]{64}$'
                   OR v_scope_key = ANY(v_seen_scope_keys)
                   OR v_sync_folder = ANY(v_seen_sync_folders)
                   OR (
                        v_scope_key NOT IN (
                            'ARCHIVE', 'TRASH', 'DRAFTS', 'INBOX',
                            'JUNK', 'OUTBOX', 'SENT'
                        )
                        AND (
                            pg_catalog.lower(v_scope_key COLLATE "C") IN (
                                'archive', 'deleted', 'deleted items',
                                'deleteditems', 'draft', 'drafts', 'inbox',
                                'junk', 'junk email', 'junkemail', 'outbox',
                                'sent', 'sent items', 'sentitems', 'spam', 'trash'
                            )
                            OR v_scope_key IN ('已发送', '已发送邮件', '草稿')
                        )
                   )
                THEN
                    RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                        USING ERRCODE = 'P0001';
                END IF;

                SELECT
                    pg_catalog.count(*),
                    pg_catalog.count(DISTINCT element #>> '{}'),
                    pg_catalog.count(*) FILTER (
                        WHERE pg_catalog.jsonb_typeof(element) = 'string'
                          AND pg_catalog.btrim(element #>> '{}') =
                                element #>> '{}'
                          AND pg_catalog.btrim(
                                element #>> '{}', v_unicode_edge_spaces
                              ) = element #>> '{}'
                          AND (element #>> '{}') !~
                                '^[[:space:]]|[[:space:]]$'
                          AND (element #>> '{}') !~ '[[:cntrl:]]'
                          AND pg_catalog.char_length(element #>> '{}')
                                BETWEEN 1 AND 512
                    ),
                    pg_catalog.array_agg(
                        element #>> '{}' ORDER BY position
                    ),
                    pg_catalog.array_agg(
                        element #>> '{}'
                        ORDER BY (element #>> '{}') COLLATE "C"
                    ),
                    pg_catalog.string_agg(
                        pg_catalog.to_json(element #>> '{}')::pg_catalog.text,
                        ',' ORDER BY position
                    )
                INTO
                    v_webhook_count,
                    v_webhook_distinct_count,
                    v_webhook_valid_count,
                    v_webhook_values,
                    v_webhook_sorted,
                    v_webhook_canonical
                FROM pg_catalog.jsonb_array_elements(v_scope -> 'webhook_ids')
                    WITH ORDINALITY AS webhook(element, position);
                IF v_webhook_count NOT BETWEEN 1 AND 64
                   OR v_webhook_count <> v_webhook_distinct_count
                   OR v_webhook_count <> v_webhook_valid_count
                   OR v_webhook_values IS DISTINCT FROM v_webhook_sorted
                   OR v_seen_webhook_ids && v_webhook_values
                   OR pg_catalog.octet_length(
                        (v_scope -> 'webhook_ids')::pg_catalog.text
                   ) > 32768
                THEN
                    RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                        USING ERRCODE = 'P0001';
                END IF;

                SELECT
                    pg_catalog.count(*),
                    pg_catalog.count(DISTINCT (
                        entry.value ->> 'source',
                        entry.value ->> 'raw_event_type',
                        entry.value ->> 'change_kind'
                    )),
                    pg_catalog.count(*) FILTER (
                        WHERE CASE
                            WHEN pg_catalog.jsonb_typeof(entry.value) = 'object'
                            THEN
                                (entry.value - 'source' - 'raw_event_type' -
                                    'change_kind' - 'processing_policy') =
                                    '{}'::pg_catalog.jsonb
                                AND pg_catalog.jsonb_typeof(
                                    entry.value -> 'source'
                                ) = 'string'
                                AND pg_catalog.jsonb_typeof(
                                    entry.value -> 'raw_event_type'
                                ) = 'string'
                                AND pg_catalog.jsonb_typeof(
                                    entry.value -> 'change_kind'
                                ) = 'string'
                                AND pg_catalog.jsonb_typeof(
                                    entry.value -> 'processing_policy'
                                ) = 'string'
                                AND (entry.value ->> 'raw_event_type') !~
                                    '[[:cntrl:]]'
                                AND (entry.value ->> 'raw_event_type') !~
                                    '^[[:space:]]|[[:space:]]$'
                                AND pg_catalog.char_length(
                                    entry.value ->> 'raw_event_type'
                                ) BETWEEN 1 AND 128
                            ELSE false
                        END
                    ),
                    pg_catalog.string_agg(
                        '{"change_kind":' || pg_catalog.to_json(
                            entry.value ->> 'change_kind'
                        )::pg_catalog.text || ',"processing_policy":' ||
                        pg_catalog.to_json(
                            entry.value ->> 'processing_policy'
                        )::pg_catalog.text || ',"raw_event_type":' ||
                        pg_catalog.to_json(
                            entry.value ->> 'raw_event_type'
                        )::pg_catalog.text || ',"source":' ||
                        pg_catalog.to_json(
                            entry.value ->> 'source'
                        )::pg_catalog.text || '}',
                        ',' ORDER BY
                            (entry.value ->> 'source') COLLATE "C",
                            (entry.value ->> 'raw_event_type') COLLATE "C",
                            (entry.value ->> 'change_kind') COLLATE "C"
                    )
                INTO
                    v_matrix_count,
                    v_matrix_distinct_count,
                    v_matrix_valid_count,
                    v_matrix_canonical
                FROM pg_catalog.jsonb_array_elements(
                    v_scope -> 'event_policy_matrix'
                ) AS entry(value);
                IF v_matrix_count <> 7
                   OR v_matrix_distinct_count <> 7
                   OR v_matrix_valid_count <> 7
                THEN
                    RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                        USING ERRCODE = 'P0001';
                END IF;

                SELECT pg_catalog.jsonb_object_agg(
                    (entry.value ->> 'source') || ':' ||
                    (entry.value ->> 'raw_event_type') || ':' ||
                    (entry.value ->> 'change_kind'),
                    entry.value ->> 'processing_policy'
                )
                INTO v_event_policy
                FROM pg_catalog.jsonb_array_elements(
                    v_scope -> 'event_policy_matrix'
                ) AS entry(value);
                IF (v_event_policy
                        - 'webhook:NewMailEvent:create'
                        - 'webhook:CreatedEvent:create'
                        - 'webhook:ModifiedEvent:update'
                        - 'webhook:DeletedEvent:delete'
                        - 'sync:create:create'
                        - 'sync:update:update'
                        - 'sync:delete:delete') <>
                        '{}'::pg_catalog.jsonb
                   OR NOT v_event_policy ?& ARRAY[
                        'webhook:NewMailEvent:create',
                        'webhook:CreatedEvent:create',
                        'webhook:ModifiedEvent:update',
                        'webhook:DeletedEvent:delete',
                        'sync:create:create',
                        'sync:update:update',
                        'sync:delete:delete'
                   ]
                   OR v_event_policy ->> 'webhook:NewMailEvent:create'
                        NOT IN ('full', 'archive', 'ignored')
                   OR v_event_policy ->> 'webhook:CreatedEvent:create'
                        NOT IN ('full', 'archive', 'ignored')
                   OR v_event_policy ->> 'sync:create:create'
                        NOT IN ('full', 'archive', 'ignored')
                   OR v_event_policy ->> 'webhook:ModifiedEvent:update' <>
                        'metadata_only'
                   OR v_event_policy ->> 'webhook:DeletedEvent:delete' <>
                        'metadata_only'
                   OR v_event_policy ->> 'sync:update:update' <> 'metadata_only'
                   OR v_event_policy ->> 'sync:delete:delete' <> 'metadata_only'
                   OR v_event_policy ->> 'webhook:NewMailEvent:create' <>
                        v_event_policy ->> 'sync:create:create'
                   OR (
                        v_scope_key = 'SENT' AND (
                            v_event_policy ->> 'webhook:NewMailEvent:create' <>
                                'archive'
                            OR v_event_policy ->> 'webhook:CreatedEvent:create' <>
                                'archive'
                        )
                   )
                   OR (
                        v_scope_key = 'DRAFTS' AND (
                            v_event_policy ->> 'webhook:NewMailEvent:create' <>
                                'ignored'
                            OR v_event_policy ->> 'webhook:CreatedEvent:create' <>
                                'ignored'
                        )
                   )
                   OR (
                        v_scope_key = 'ARCHIVE' AND (
                            v_event_policy ->> 'webhook:NewMailEvent:create' <>
                                'archive'
                            OR v_event_policy ->> 'webhook:CreatedEvent:create' <>
                                'ignored'
                        )
                   )
                   OR (
                        v_scope_key NOT IN ('SENT', 'DRAFTS', 'ARCHIVE')
                        AND v_event_policy ->> 'webhook:CreatedEvent:create' <>
                            'ignored'
                   )
                THEN
                    RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                        USING ERRCODE = 'P0001';
                END IF;

                v_scope_config_canonical :=
                    '{"canonical_key":' ||
                    pg_catalog.to_json(v_scope_key)::pg_catalog.text ||
                    ',"event_policy_matrix":[' || v_matrix_canonical ||
                    '],"schema_version":' || '1' || ',"sync_folder":' ||
                    pg_catalog.to_json(v_sync_folder)::pg_catalog.text ||
                    ',"webhook_ids":[' || v_webhook_canonical || ']}';
                v_expected_scope_hash := pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(v_scope_config_canonical, 'UTF8')
                    ),
                    'hex'
                );
                IF v_expected_scope_hash <> v_scope ->> 'scope_hash' THEN
                    RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                        USING ERRCODE = 'P0001';
                END IF;

                v_scope_manifest_canonical :=
                    '{"canonical_key":' ||
                    pg_catalog.to_json(v_scope_key)::pg_catalog.text ||
                    ',"event_policy_matrix":[' || v_matrix_canonical ||
                    '],"scope_hash":"' || v_expected_scope_hash ||
                    '","sync_folder":' ||
                    pg_catalog.to_json(v_sync_folder)::pg_catalog.text ||
                    ',"webhook_ids":[' || v_webhook_canonical || ']}';
                IF v_policy_scope_rows > 0 THEN
                    v_rebuilt_policy := v_rebuilt_policy || ',';
                END IF;
                v_rebuilt_policy :=
                    v_rebuilt_policy || v_scope_manifest_canonical;
                v_policy_scope_rows := v_policy_scope_rows + 1;
                v_seen_scope_keys := v_seen_scope_keys || v_scope_key;
                v_seen_sync_folders := v_seen_sync_folders || v_sync_folder;
                v_seen_webhook_ids :=
                    v_seen_webhook_ids || v_webhook_values;
            END LOOP;
            v_rebuilt_policy := v_rebuilt_policy || ']}';
            FOR v_character_position IN 1..pg_catalog.char_length(
                v_rebuilt_policy
            ) LOOP
                v_character := pg_catalog.substr(
                    v_rebuilt_policy,
                    v_character_position,
                    1
                );
                v_codepoint := pg_catalog.ascii(v_character);
                IF v_codepoint <= 127 THEN
                    v_rebuilt_policy_ascii :=
                        v_rebuilt_policy_ascii || v_character;
                ELSIF v_codepoint <= 65535 THEN
                    v_rebuilt_policy_ascii := v_rebuilt_policy_ascii ||
                        pg_catalog.chr(92) || 'u' || pg_catalog.lpad(
                            pg_catalog.to_hex(v_codepoint), 4, '0'
                        );
                ELSE
                    v_surrogate_value := v_codepoint - 65536;
                    v_rebuilt_policy_ascii := v_rebuilt_policy_ascii ||
                        pg_catalog.chr(92) || 'u' || pg_catalog.lpad(
                            pg_catalog.to_hex(55296 +
                                (v_surrogate_value / 1024)), 4, '0'
                        ) || pg_catalog.chr(92) || 'u' || pg_catalog.lpad(
                            pg_catalog.to_hex(56320 +
                                (v_surrogate_value % 1024)), 4, '0'
                        );
                END IF;
            END LOOP;
            IF v_policy_scope_rows <> p_policy_scope_count
               OR v_rebuilt_policy_ascii <> p_policy_manifest_json
            THEN
                RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                    USING ERRCODE = 'P0001';
            END IF;

            v_expected_policy_hash := pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        'ai-exchange-folder-policy-manifest-v1', 'UTF8'
                    ) || v_zero || pg_catalog.convert_to(
                        p_policy_manifest_json, 'UTF8'
                    )
                ),
                'hex'
            );
            IF v_expected_policy_hash <> p_policy_manifest_hash THEN
                RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                    USING ERRCODE = 'P0001';
            END IF;

            v_capability_canonical :=
                '{"adapter_hash":"' || p_adapter_hash ||
                '","config_hash":"' || p_config_hash ||
                '","evidence_manifest_hash":"' || p_evidence_manifest_hash ||
                '","minimum_build_id":"' || p_minimum_build_id ||
                '","policy_manifest_hash":"' || p_policy_manifest_hash ||
                '","predecessor_hash":"' || p_predecessor_hash ||
                '","protocol_version":' || p_protocol_version::pg_catalog.text ||
                ',"schema_digest":"' || p_schema_digest ||
                '","schema_revision":"' || p_schema_revision ||
                '","schema_version":' || '1' || ',"stage":"' ||
                p_capability_stage || '"}';
            v_expected_capability_hash := pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        'ai-exchange-runtime-capability-manifest-v1', 'UTF8'
                    ) || v_zero || pg_catalog.convert_to(
                        v_capability_canonical, 'UTF8'
                    )
                ),
                'hex'
            );
            IF v_expected_capability_hash <> p_capability_hash THEN
                RAISE EXCEPTION 'greenfield_initialization_capability_invalid'
                    USING ERRCODE = 'P0001';
            END IF;

            v_initialization_canonical :=
                '{"account_id":' || p_account_id::pg_catalog.text ||
                ',"actor":' || pg_catalog.to_json(p_actor)::pg_catalog.text ||
                ',"capability_hash":"' || p_capability_hash ||
                '","pipeline_name":"durable_v1","policy_manifest":' ||
                p_policy_manifest_json || ',"policy_manifest_hash":"' ||
                p_policy_manifest_hash || '","reason":' ||
                pg_catalog.to_json(p_reason)::pg_catalog.text ||
                ',"runtime_contract":{"build_id":"' || p_minimum_build_id ||
                '","config_hash":"' || p_config_hash ||
                '","protocol_version":' || p_protocol_version::pg_catalog.text ||
                ',"schema_digest":"' || p_schema_digest ||
                '","schema_revision":"' || p_schema_revision ||
                '"},"schema_version":' || '1' || '}';
            v_expected_payload_hash := pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        'ai-exchange-greenfield-initialize-v1', 'UTF8'
                    ) || v_zero || pg_catalog.convert_to(
                        v_initialization_canonical, 'UTF8'
                    )
                ),
                'hex'
            );
            IF v_expected_payload_hash <> p_canonical_payload_hash THEN
                RAISE EXCEPTION 'greenfield_initialization_payload_invalid'
                    USING ERRCODE = 'P0001';
            END IF;

            v_idempotency_hash := pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        'pipeline-command-idempotency-v1', 'UTF8'
                    ) || v_zero ||
                    pg_catalog.convert_to(p_account_id::pg_catalog.text, 'UTF8') ||
                    v_zero || pg_catalog.convert_to('runtime.initialize', 'UTF8') ||
                    v_zero || pg_catalog.convert_to(p_idempotency_key, 'UTF8')
                ),
                'hex'
            );
            PERFORM pg_catalog.pg_advisory_xact_lock(0, 0);
            PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);

            SELECT receipt.*
            INTO v_existing_receipt
            FROM public.pipeline_command_receipts AS receipt
            WHERE receipt.account_id = p_account_id
              AND receipt.command_name = 'runtime.initialize'
              AND receipt.idempotency_key_hash = v_idempotency_hash;
            IF FOUND THEN
                IF v_existing_receipt.canonical_payload_hash::pg_catalog.text <>
                        p_canonical_payload_hash THEN
                    RAISE EXCEPTION 'command_idempotency_conflict'
                        USING ERRCODE = 'P0001';
                END IF;
                SELECT initialized.*
                INTO STRICT v_existing_initialization
                FROM public.pipeline_initializations AS initialized
                WHERE initialized.command_receipt_id = v_existing_receipt.id
                  AND initialized.account_id = p_account_id;
                RETURN QUERY SELECT
                    v_existing_initialization.initialization_id,
                    v_existing_initialization.command_receipt_id,
                    v_existing_initialization.account_id,
                    v_existing_initialization.generation,
                    v_existing_initialization.fencing_token,
                    v_existing_initialization.pipeline_name,
                    v_existing_initialization.authority_epoch,
                    v_existing_initialization.authority_version,
                    v_existing_initialization.capability_hash::pg_catalog.text,
                    v_existing_initialization.policy_manifest_hash::pg_catalog.text,
                    v_existing_initialization.transaction_id,
                    true,
                    v_existing_initialization.created_at;
                RETURN;
            END IF;

            IF pg_catalog.to_regclass('public.checkpoints') IS NOT NULL THEN
                EXECUTE 'SELECT EXISTS ('
                    'SELECT 1 FROM public.checkpoints LIMIT 1)'
                INTO v_checkpoint_nonempty;
            END IF;
            IF NOT v_checkpoint_nonempty
               AND pg_catalog.to_regclass('public.checkpoint_blobs') IS NOT NULL
            THEN
                EXECUTE 'SELECT EXISTS ('
                    'SELECT 1 FROM public.checkpoint_blobs LIMIT 1)'
                INTO v_checkpoint_nonempty;
            END IF;
            IF NOT v_checkpoint_nonempty
               AND pg_catalog.to_regclass('public.checkpoint_writes') IS NOT NULL
            THEN
                EXECUTE 'SELECT EXISTS ('
                    'SELECT 1 FROM public.checkpoint_writes LIMIT 1)'
                INTO v_checkpoint_nonempty;
            END IF;

            IF EXISTS (SELECT 1 FROM public.emails_log LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.app_kv_store LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.pipeline_ownership LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.event_inbox LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.sync_cursors LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.emails LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.audit_events LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.sync_cold_start_plans LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.pipeline_command_receipts LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.pipeline_initializations LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.pipeline_folder_scopes LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.pipeline_runtime_authority LIMIT 1)
               OR EXISTS (SELECT 1 FROM public.pipeline_runtime_instances LIMIT 1)
               OR v_checkpoint_nonempty
            THEN
                RAISE EXCEPTION 'greenfield_reinitialize_required'
                    USING ERRCODE = 'P0001';
            END IF;

            SELECT capability.*
            INTO v_existing_capability
            FROM public.pipeline_runtime_capabilities AS capability
            WHERE capability.capability_hash = p_capability_hash;
            IF FOUND THEN
                IF v_existing_capability.predecessor_hash::pg_catalog.text <>
                        p_predecessor_hash
                   OR v_existing_capability.stage <> p_capability_stage
                   OR v_existing_capability.schema_revision <> p_schema_revision
                   OR v_existing_capability.schema_digest::pg_catalog.text <>
                        p_schema_digest
                   OR v_existing_capability.protocol_version <> p_protocol_version
                   OR v_existing_capability.minimum_build_id <>
                        p_minimum_build_id
                   OR v_existing_capability.config_hash::pg_catalog.text <>
                        p_config_hash
                   OR v_existing_capability.adapter_hash::pg_catalog.text <>
                        p_adapter_hash
                   OR v_existing_capability.policy_manifest_hash::pg_catalog.text <>
                        p_policy_manifest_hash
                   OR v_existing_capability.evidence_manifest_hash::pg_catalog.text <>
                        p_evidence_manifest_hash
                THEN
                    RAISE EXCEPTION 'greenfield_capability_conflict'
                        USING ERRCODE = 'P0001';
                END IF;
            ELSE
                INSERT INTO public.pipeline_runtime_capabilities (
                    capability_hash,
                    predecessor_hash,
                    stage,
                    schema_revision,
                    schema_digest,
                    protocol_version,
                    minimum_build_id,
                    config_hash,
                    adapter_hash,
                    policy_manifest_hash,
                    evidence_manifest_hash
                ) VALUES (
                    p_capability_hash,
                    p_predecessor_hash,
                    p_capability_stage,
                    p_schema_revision,
                    p_schema_digest,
                    p_protocol_version,
                    p_minimum_build_id,
                    p_config_hash,
                    p_adapter_hash,
                    p_policy_manifest_hash,
                    p_evidence_manifest_hash
                );
            END IF;

            v_initialization_id := pg_catalog.gen_random_uuid();
            v_receipt_id := pg_catalog.gen_random_uuid();
            v_transaction_id := pg_catalog.pg_current_xact_id()::pg_catalog.text;
            v_created_at := pg_catalog.clock_timestamp();
            v_result_hash := pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        'ai-exchange-runtime-initialization-result-v1', 'UTF8'
                    ) || v_zero ||
                    pg_catalog.convert_to(
                        v_initialization_id::pg_catalog.text, 'UTF8'
                    ) || v_zero ||
                    pg_catalog.convert_to(p_capability_hash, 'UTF8') || v_zero ||
                    pg_catalog.convert_to(p_policy_manifest_hash, 'UTF8')
                ),
                'hex'
            );

            INSERT INTO public.pipeline_ownership (
                account_id,
                generation,
                pipeline_name,
                state,
                fencing_token,
                created_by,
                reason,
                created_at,
                updated_at
            ) VALUES (
                p_account_id,
                1,
                'durable_v1',
                'current_ingress',
                1,
                p_actor,
                p_reason,
                v_created_at,
                v_created_at
            );
            INSERT INTO public.pipeline_command_receipts (
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
            ) VALUES (
                v_receipt_id,
                p_account_id,
                'runtime.initialize',
                v_idempotency_hash,
                p_canonical_payload_hash,
                'succeeded',
                'runtime_initialization',
                v_initialization_id::pg_catalog.text,
                v_result_hash,
                1,
                v_created_at
            );
            INSERT INTO public.pipeline_initializations (
                initialization_id,
                command_receipt_id,
                account_id,
                generation,
                fencing_token,
                pipeline_name,
                authority_epoch,
                authority_version,
                capability_hash,
                policy_manifest_hash,
                transaction_id,
                actor,
                reason,
                created_at
            ) VALUES (
                v_initialization_id,
                v_receipt_id,
                p_account_id,
                1,
                1,
                'durable_v1',
                1,
                1,
                p_capability_hash,
                p_policy_manifest_hash,
                v_transaction_id,
                p_actor,
                p_reason,
                v_created_at
            );

            FOR v_scope IN
                SELECT scope.value
                FROM pg_catalog.jsonb_array_elements(v_policy -> 'scopes')
                    AS scope(value)
                ORDER BY scope.value ->> 'canonical_key'
            LOOP
                SELECT pg_catalog.jsonb_object_agg(
                    (entry.value ->> 'source') || ':' ||
                    (entry.value ->> 'raw_event_type') || ':' ||
                    (entry.value ->> 'change_kind'),
                    entry.value ->> 'processing_policy'
                )
                INTO v_event_policy
                FROM pg_catalog.jsonb_array_elements(
                    v_scope -> 'event_policy_matrix'
                ) AS entry(value);
                INSERT INTO public.pipeline_folder_scopes (
                    initialization_id,
                    account_id,
                    canonical_key,
                    webhook_ids,
                    sync_folder,
                    event_policy_matrix,
                    scope_hash,
                    policy_manifest_hash,
                    created_at
                ) VALUES (
                    v_initialization_id,
                    p_account_id,
                    v_scope ->> 'canonical_key',
                    v_scope -> 'webhook_ids',
                    v_scope ->> 'sync_folder',
                    v_event_policy,
                    v_scope ->> 'scope_hash',
                    p_policy_manifest_hash,
                    v_created_at
                );
                v_scope_rows := v_scope_rows + 1;
            END LOOP;
            IF v_scope_rows <> p_policy_scope_count THEN
                RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                    USING ERRCODE = 'P0001';
            END IF;

            INSERT INTO public.pipeline_runtime_authority (
                account_id,
                state,
                generation,
                fencing_token,
                pipeline_name,
                authority_epoch,
                version,
                schema_revision,
                protocol_version,
                build_id,
                config_hash,
                capability_hash,
                policy_manifest_hash,
                initialization_id,
                created_at,
                updated_at
            ) VALUES (
                p_account_id,
                'ingest_only',
                1,
                1,
                'durable_v1',
                1,
                1,
                p_schema_revision,
                p_protocol_version,
                p_minimum_build_id,
                p_config_hash,
                p_capability_hash,
                p_policy_manifest_hash,
                v_initialization_id,
                v_created_at,
                v_created_at
            );
            INSERT INTO public.audit_events (
                id,
                event_key,
                account_id,
                email_id,
                object_type,
                object_fingerprint,
                action,
                result,
                actor,
                reason,
                safe_metadata,
                created_at
            ) VALUES (
                pg_catalog.gen_random_uuid(),
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(
                            'ai-exchange-runtime-initialize-audit-v1', 'UTF8'
                        ) || v_zero || pg_catalog.convert_to(
                            v_receipt_id::pg_catalog.text, 'UTF8'
                        )
                    ),
                    'hex'
                ),
                p_account_id,
                NULL,
                'runtime_authority',
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(
                            'ai-exchange-runtime-authority-v1', 'UTF8'
                        ) || v_zero || pg_catalog.convert_to(
                            p_account_id::pg_catalog.text, 'UTF8'
                        )
                    ),
                    'hex'
                ),
                'runtime.initialize',
                'succeeded',
                p_actor,
                p_reason,
                pg_catalog.jsonb_build_object(
                    'authority_epoch', 1,
                    'authority_version', 1,
                    'capability_hash', p_capability_hash,
                    'policy_manifest_hash', p_policy_manifest_hash,
                    'scope_count', p_policy_scope_count
                ),
                v_created_at
            );

            RETURN QUERY SELECT
                v_initialization_id,
                v_receipt_id,
                p_account_id,
                1::pg_catalog.int8,
                1::pg_catalog.int8,
                'durable_v1'::pg_catalog.text,
                1::pg_catalog.int8,
                1::pg_catalog.int8,
                p_capability_hash,
                p_policy_manifest_hash,
                v_transaction_id,
                false,
                v_created_at;
        END
        $greenfield_initialize_runtime$
        """
    )


_GREENFIELD_TRANSITION_TEMPLATE = """
CREATE FUNCTION public.__ROUTINE_NAME__(
    p_account_id pg_catalog.int8,
    p_expected_authority_epoch pg_catalog.int8,
    p_expected_version pg_catalog.int8,
    p_expected_capability_hash pg_catalog.text,
    p_actor pg_catalog.text,
    p_reason pg_catalog.text,
    p_idempotency_key pg_catalog.text,
    p_canonical_payload_hash pg_catalog.text
)
RETURNS TABLE (
    command_receipt_id pg_catalog.uuid,
    command_name pg_catalog.text,
    previous_state pg_catalog.text,
    previous_authority_epoch pg_catalog.int8,
    previous_version pg_catalog.int8,
    transaction_id pg_catalog.text,
    replayed pg_catalog.bool,
    receipt_created_at pg_catalog.timestamptz,
    account_id pg_catalog.int8,
    state pg_catalog.text,
    generation pg_catalog.int8,
    fencing_token pg_catalog.int8,
    pipeline_name pg_catalog.text,
    authority_epoch pg_catalog.int8,
    version pg_catalog.int8,
    schema_revision pg_catalog.text,
    protocol_version pg_catalog.int8,
    build_id pg_catalog.text,
    config_hash pg_catalog.text,
    capability_hash pg_catalog.text,
    policy_manifest_hash pg_catalog.text,
    initialization_id pg_catalog.uuid,
    updated_at pg_catalog.timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $greenfield_transition$
DECLARE
    v_zero pg_catalog.bytea := pg_catalog.decode('00', 'hex');
    v_unicode_edge_spaces pg_catalog.text :=
        pg_catalog.chr(133) || pg_catalog.chr(160) ||
        pg_catalog.chr(5760) || pg_catalog.chr(8192) ||
        pg_catalog.chr(8193) || pg_catalog.chr(8194) ||
        pg_catalog.chr(8195) || pg_catalog.chr(8196) ||
        pg_catalog.chr(8197) || pg_catalog.chr(8198) ||
        pg_catalog.chr(8199) || pg_catalog.chr(8200) ||
        pg_catalog.chr(8201) || pg_catalog.chr(8202) ||
        pg_catalog.chr(8232) || pg_catalog.chr(8233) ||
        pg_catalog.chr(8239) || pg_catalog.chr(8287) ||
        pg_catalog.chr(12288);
    v_authority public.pipeline_runtime_authority%ROWTYPE;
    v_existing_receipt public.pipeline_command_receipts%ROWTYPE;
    v_replay_metadata pg_catalog.jsonb;
    v_replay_actor pg_catalog.text;
    v_replay_reason pg_catalog.text;
    v_canonical pg_catalog.text;
    v_expected_payload_hash pg_catalog.text;
    v_idempotency_hash pg_catalog.text;
    v_receipt_id pg_catalog.uuid;
    v_transaction_id pg_catalog.text;
    v_result_hash pg_catalog.text;
    v_created_at pg_catalog.timestamptz;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_expected_authority_epoch IS NULL
       OR p_expected_authority_epoch <= 0
       OR p_expected_authority_epoch >= 9223372036854775806
       OR p_expected_version IS NULL OR p_expected_version <= 0
       OR p_expected_version >= 9223372036854775806
       OR p_expected_capability_hash IS NULL
       OR p_expected_capability_hash !~ '^[0-9a-f]{64}$'
       OR p_canonical_payload_hash IS NULL
       OR p_canonical_payload_hash !~ '^[0-9a-f]{64}$'
       OR p_actor IS NULL OR pg_catalog.btrim(p_actor) <> p_actor
       OR pg_catalog.btrim(p_actor, v_unicode_edge_spaces) <> p_actor
       OR pg_catalog.octet_length(p_actor) NOT BETWEEN 1 AND 128
       OR p_actor ~ '[[:cntrl:]]'
       OR p_reason IS NULL OR pg_catalog.btrim(p_reason) <> p_reason
       OR pg_catalog.btrim(p_reason, v_unicode_edge_spaces) <> p_reason
       OR pg_catalog.octet_length(p_reason) NOT BETWEEN 1 AND 512
       OR p_reason ~ '[[:cntrl:]]'
       OR p_idempotency_key IS NULL
       OR pg_catalog.btrim(p_idempotency_key) <> p_idempotency_key
       OR pg_catalog.btrim(
            p_idempotency_key, v_unicode_edge_spaces
       ) <> p_idempotency_key
       OR pg_catalog.octet_length(p_idempotency_key) NOT BETWEEN 1 AND 4096
       OR p_idempotency_key ~ '[[:cntrl:]]'
    THEN
        RAISE EXCEPTION 'runtime_authority_transition_input_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    v_idempotency_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to('pipeline-command-idempotency-v1', 'UTF8') ||
            v_zero ||
            pg_catalog.convert_to(p_account_id::pg_catalog.text, 'UTF8') ||
            v_zero || pg_catalog.convert_to('__COMMAND_NAME__', 'UTF8') ||
            v_zero || pg_catalog.convert_to(p_idempotency_key, 'UTF8')
        ),
        'hex'
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);

    SELECT receipt.*
    INTO v_existing_receipt
    FROM public.pipeline_command_receipts AS receipt
    WHERE receipt.account_id = p_account_id
      AND receipt.command_name = '__COMMAND_NAME__'
      AND receipt.idempotency_key_hash = v_idempotency_hash;
    IF FOUND THEN
        IF v_existing_receipt.canonical_payload_hash::pg_catalog.text <>
                p_canonical_payload_hash THEN
            RAISE EXCEPTION 'command_idempotency_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        SELECT audit.safe_metadata, audit.actor, audit.reason
        INTO STRICT v_replay_metadata, v_replay_actor, v_replay_reason
        FROM public.audit_events AS audit
        WHERE audit.account_id = p_account_id
          AND audit.action = '__COMMAND_NAME__'
          AND audit.safe_metadata ->> 'command_receipt_id' =
                v_existing_receipt.id::pg_catalog.text;
        IF p_expected_authority_epoch <>
                v_existing_receipt.authority_epoch - 1
           OR p_expected_version <>
                v_existing_receipt.result_id::pg_catalog.int8 - 1
           OR p_expected_capability_hash <>
                v_replay_metadata ->> 'capability_hash'
           OR p_actor <> v_replay_actor
           OR p_reason <> v_replay_reason
        THEN
            RAISE EXCEPTION 'command_idempotency_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        RETURN QUERY SELECT
            v_existing_receipt.id,
            '__COMMAND_NAME__'::pg_catalog.text,
            '__PREVIOUS_STATE__'::pg_catalog.text,
            v_existing_receipt.authority_epoch - 1,
            v_existing_receipt.result_id::pg_catalog.int8 - 1,
            v_replay_metadata ->> 'transaction_id',
            true,
            v_existing_receipt.created_at,
            v_existing_receipt.account_id,
            '__TARGET_STATE__'::pg_catalog.text,
            (v_replay_metadata ->> 'generation')::pg_catalog.int8,
            (v_replay_metadata ->> 'fencing_token')::pg_catalog.int8,
            v_replay_metadata ->> 'pipeline_name',
            v_existing_receipt.authority_epoch,
            v_existing_receipt.result_id::pg_catalog.int8,
            v_replay_metadata ->> 'schema_revision',
            (v_replay_metadata ->> 'protocol_version')::pg_catalog.int8,
            v_replay_metadata ->> 'build_id',
            v_replay_metadata ->> 'config_hash',
            v_replay_metadata ->> 'capability_hash',
            v_replay_metadata ->> 'policy_manifest_hash',
            (v_replay_metadata ->> 'initialization_id')::pg_catalog.uuid,
            v_existing_receipt.created_at;
        RETURN;
    END IF;

    SELECT authority.*
    INTO v_authority
    FROM public.pipeline_runtime_authority AS authority
    WHERE authority.account_id = p_account_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_authority.state <> '__PREVIOUS_STATE__'
       OR v_authority.authority_epoch <> p_expected_authority_epoch
       OR v_authority.version <> p_expected_version
       OR v_authority.capability_hash::pg_catalog.text <>
            p_expected_capability_hash
       OR v_authority.capability_stage_ordinal <> 1
       OR v_authority.schema_revision <> '20260716_0006'
    THEN
        RAISE EXCEPTION 'runtime_authority_cas_conflict'
            USING ERRCODE = 'P0001';
    END IF;

    v_canonical :=
        '{"account_id":' || p_account_id::pg_catalog.text ||
        ',"actor":' || pg_catalog.to_json(p_actor)::pg_catalog.text ||
        ',"build_id":"' || v_authority.build_id ||
        '","capability_hash":"' ||
        v_authority.capability_hash::pg_catalog.text ||
        '","command_name":"__COMMAND_NAME__"' ||
        ',"config_hash":"' || v_authority.config_hash::pg_catalog.text ||
        '","expected_authority_epoch":' ||
        p_expected_authority_epoch::pg_catalog.text ||
        ',"expected_version":' || p_expected_version::pg_catalog.text ||
        ',"fencing_token":' || v_authority.fencing_token::pg_catalog.text ||
        ',"generation":' || v_authority.generation::pg_catalog.text ||
        ',"initialization_id":"' ||
        v_authority.initialization_id::pg_catalog.text ||
        '","pipeline_name":"' || v_authority.pipeline_name ||
        '","policy_manifest_hash":"' ||
        v_authority.policy_manifest_hash::pg_catalog.text ||
        '","previous_state":"__PREVIOUS_STATE__"' ||
        ',"protocol_version":' ||
        v_authority.protocol_version::pg_catalog.text ||
        ',"reason":' || pg_catalog.to_json(p_reason)::pg_catalog.text ||
        ',"schema_revision":"' || v_authority.schema_revision ||
        '","schema_version":' || '1' ||
        ',"target_state":"__TARGET_STATE__"}';
    v_expected_payload_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(
                'ai-exchange-runtime-authority-transition-v1', 'UTF8'
            ) || v_zero || pg_catalog.convert_to(v_canonical, 'UTF8')
        ),
        'hex'
    );
    IF v_expected_payload_hash <> p_canonical_payload_hash THEN
        RAISE EXCEPTION 'runtime_authority_transition_payload_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    v_receipt_id := pg_catalog.gen_random_uuid();
    v_transaction_id := pg_catalog.pg_current_xact_id()::pg_catalog.text;
    v_created_at := pg_catalog.clock_timestamp();
    v_result_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(
                'ai-exchange-runtime-authority-transition-result-v1', 'UTF8'
            ) || v_zero ||
            pg_catalog.convert_to(v_receipt_id::pg_catalog.text, 'UTF8') ||
            v_zero || pg_catalog.convert_to(
                (v_authority.authority_epoch + 1)::pg_catalog.text, 'UTF8'
            )
        ),
        'hex'
    );

    UPDATE public.pipeline_runtime_authority AS authority
    SET state = '__TARGET_STATE__',
        authority_epoch = authority.authority_epoch + 1,
        version = authority.version + 1,
        updated_at = v_created_at
    WHERE authority.account_id = p_account_id
      AND authority.state = '__PREVIOUS_STATE__'
      AND authority.authority_epoch = p_expected_authority_epoch
      AND authority.version = p_expected_version
      AND authority.capability_hash =
            p_expected_capability_hash::pg_catalog.bpchar
    RETURNING authority.* INTO STRICT v_authority;

    INSERT INTO public.pipeline_command_receipts (
        id, account_id, command_name, idempotency_key_hash,
        canonical_payload_hash, outcome, result_type, result_id,
        result_hash, authority_epoch, created_at
    ) VALUES (
        v_receipt_id, p_account_id, '__COMMAND_NAME__', v_idempotency_hash,
        p_canonical_payload_hash, 'succeeded', 'runtime_authority',
        v_authority.version::pg_catalog.text, v_result_hash,
        v_authority.authority_epoch, v_created_at
    );
    INSERT INTO public.audit_events (
        id, event_key, account_id, email_id, object_type,
        object_fingerprint, action, result, actor, reason, safe_metadata,
        created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(),
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    'ai-exchange-runtime-transition-audit-v1', 'UTF8'
                ) || v_zero ||
                pg_catalog.convert_to(v_receipt_id::pg_catalog.text, 'UTF8')
            ),
            'hex'
        ),
        p_account_id,
        NULL,
        'runtime_authority',
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    'ai-exchange-runtime-authority-v1', 'UTF8'
                ) || v_zero ||
                pg_catalog.convert_to(p_account_id::pg_catalog.text, 'UTF8')
            ),
            'hex'
        ),
        '__COMMAND_NAME__',
        'succeeded',
        p_actor,
        p_reason,
        pg_catalog.jsonb_build_object(
            'build_id', v_authority.build_id,
            'capability_hash', v_authority.capability_hash::pg_catalog.text,
            'command_receipt_id', v_receipt_id::pg_catalog.text,
            'config_hash', v_authority.config_hash::pg_catalog.text,
            'fencing_token', v_authority.fencing_token,
            'generation', v_authority.generation,
            'initialization_id',
                v_authority.initialization_id::pg_catalog.text,
            'pipeline_name', v_authority.pipeline_name,
            'policy_manifest_hash',
                v_authority.policy_manifest_hash::pg_catalog.text,
            'protocol_version', v_authority.protocol_version,
            'schema_revision', v_authority.schema_revision,
            'transaction_id', v_transaction_id
        ),
        v_created_at
    );

    RETURN QUERY SELECT
        v_receipt_id,
        '__COMMAND_NAME__'::pg_catalog.text,
        '__PREVIOUS_STATE__'::pg_catalog.text,
        p_expected_authority_epoch,
        p_expected_version,
        v_transaction_id,
        false,
        v_created_at,
        v_authority.account_id,
        v_authority.state,
        v_authority.generation,
        v_authority.fencing_token,
        v_authority.pipeline_name,
        v_authority.authority_epoch,
        v_authority.version,
        v_authority.schema_revision,
        v_authority.protocol_version,
        v_authority.build_id,
        v_authority.config_hash::pg_catalog.text,
        v_authority.capability_hash::pg_catalog.text,
        v_authority.policy_manifest_hash::pg_catalog.text,
        v_authority.initialization_id,
        v_authority.updated_at;
END
$greenfield_transition$
"""

_GREENFIELD_TRANSITION_CONTRACTS = (
    ("greenfield_pause_runtime", "runtime.pause", "ingest_only", "paused"),
    (
        "greenfield_resume_ingress",
        "runtime.resume_ingress",
        "paused",
        "ingest_only",
    ),
)


def _create_greenfield_transition_functions() -> None:
    for (
        routine_name,
        command_name,
        previous_state,
        target_state,
    ) in _GREENFIELD_TRANSITION_CONTRACTS:
        _create_greenfield_transition_function(
            routine_name=routine_name,
            command_name=command_name,
            previous_state=previous_state,
            target_state=target_state,
        )


def _create_greenfield_transition_function(
    *,
    routine_name: str,
    command_name: str,
    previous_state: str,
    target_state: str,
) -> None:
    statement = _GREENFIELD_TRANSITION_TEMPLATE
    for marker, value in (
        ("__ROUTINE_NAME__", routine_name),
        ("__COMMAND_NAME__", command_name),
        ("__PREVIOUS_STATE__", previous_state),
        ("__TARGET_STATE__", target_state),
    ):
        statement = statement.replace(marker, value)
    op.execute(statement)


_GREENFIELD_REGISTER_INSTANCE_SQL = """
CREATE FUNCTION public.greenfield_register_web_instance(
    p_account_id pg_catalog.int8,
    p_instance_id pg_catalog.text,
    p_session_id pg_catalog.uuid,
    p_expected_authority_epoch pg_catalog.int8,
    p_expected_authority_version pg_catalog.int8,
    p_schema_revision pg_catalog.text,
    p_protocol_version pg_catalog.int8,
    p_build_id pg_catalog.text,
    p_config_hash pg_catalog.text,
    p_capability_hash pg_catalog.text,
    p_lease_seconds pg_catalog.int8
)
RETURNS TABLE (
    account_id pg_catalog.int8,
    workload pg_catalog.text,
    instance_id pg_catalog.text,
    session_id pg_catalog.uuid,
    generation pg_catalog.int8,
    fencing_token pg_catalog.int8,
    authority_epoch pg_catalog.int8,
    capability_hash pg_catalog.text,
    schema_revision pg_catalog.text,
    protocol_version pg_catalog.int8,
    build_id pg_catalog.text,
    config_hash pg_catalog.text,
    lifecycle pg_catalog.text,
    lease_version pg_catalog.int8,
    accepted_count pg_catalog.int8,
    rejected_count pg_catalog.int8,
    heartbeat_at pg_catalog.timestamptz,
    lease_until pg_catalog.timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $greenfield_register_web_instance$
DECLARE
    v_authority public.pipeline_runtime_authority%ROWTYPE;
    v_existing public.pipeline_runtime_instances%ROWTYPE;
    v_instance public.pipeline_runtime_instances%ROWTYPE;
    v_now pg_catalog.timestamptz;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_instance_id IS NULL
       OR p_instance_id !~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$'
       OR p_session_id IS NULL
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 15, 1) <> '4'
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 20, 1) !~ '^[89ab]$'
       OR p_expected_authority_epoch IS NULL
       OR p_expected_authority_epoch <= 0
       OR p_expected_authority_epoch >= 9223372036854775807
       OR p_expected_authority_version IS NULL
       OR p_expected_authority_version <= 0
       OR p_expected_authority_version >= 9223372036854775807
       OR p_schema_revision IS NULL OR p_schema_revision <> '20260716_0006'
       OR p_protocol_version IS NULL OR p_protocol_version <= 0
       OR p_build_id IS NULL
       OR p_build_id !~ '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'
       OR p_config_hash IS NULL OR p_config_hash !~ '^[0-9a-f]{64}$'
       OR p_capability_hash IS NULL
       OR p_capability_hash !~ '^[0-9a-f]{64}$'
       OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'runtime_instance_registration_input_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);
    v_now := pg_catalog.clock_timestamp();
    SELECT authority.*
    INTO v_authority
    FROM public.pipeline_runtime_authority AS authority
    WHERE authority.account_id = p_account_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_authority.state <> 'ingest_only'
       OR v_authority.authority_epoch <> p_expected_authority_epoch
       OR v_authority.version <> p_expected_authority_version
       OR v_authority.schema_revision <> p_schema_revision
       OR v_authority.protocol_version <> p_protocol_version
       OR v_authority.build_id <> p_build_id
       OR v_authority.config_hash::pg_catalog.text <> p_config_hash
       OR v_authority.capability_hash::pg_catalog.text <> p_capability_hash
       OR v_authority.capability_stage_ordinal <> 1
    THEN
        RAISE EXCEPTION 'runtime_instance_authority_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT instance.*
    INTO v_existing
    FROM public.pipeline_runtime_instances AS instance
    WHERE instance.session_id = p_session_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.account_id <> p_account_id
           OR v_existing.workload <> 'web'
           OR v_existing.instance_id <> p_instance_id
           OR v_existing.generation <> v_authority.generation
           OR v_existing.fencing_token <> v_authority.fencing_token
           OR v_existing.authority_epoch <> v_authority.authority_epoch
           OR v_existing.capability_hash::pg_catalog.text <> p_capability_hash
           OR v_existing.schema_revision <> p_schema_revision
           OR v_existing.protocol_version <> p_protocol_version
           OR v_existing.build_id <> p_build_id
           OR v_existing.config_hash::pg_catalog.text <> p_config_hash
           OR v_existing.lifecycle <> 'active'
           OR v_existing.lease_version <> 1
           OR v_existing.accepted_count <> 0
           OR v_existing.rejected_count <> 0
           OR v_existing.lease_until - v_existing.heartbeat_at <>
                p_lease_seconds * INTERVAL '1 second'
           OR v_existing.lease_until <= v_now
        THEN
            RAISE EXCEPTION 'runtime_instance_registration_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        RETURN QUERY SELECT
            v_existing.account_id,
            v_existing.workload,
            v_existing.instance_id,
            v_existing.session_id,
            v_existing.generation,
            v_existing.fencing_token,
            v_existing.authority_epoch,
            v_existing.capability_hash::pg_catalog.text,
            v_existing.schema_revision,
            v_existing.protocol_version,
            v_existing.build_id,
            v_existing.config_hash::pg_catalog.text,
            v_existing.lifecycle,
            v_existing.lease_version,
            v_existing.accepted_count,
            v_existing.rejected_count,
            v_existing.heartbeat_at,
            v_existing.lease_until;
        RETURN;
    END IF;

    SELECT instance.*
    INTO v_existing
    FROM public.pipeline_runtime_instances AS instance
    WHERE instance.account_id = p_account_id
      AND instance.workload = 'web'
      AND instance.instance_id = p_instance_id
      AND instance.lifecycle <> 'draining'
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.lease_until > v_now THEN
            RAISE EXCEPTION 'runtime_instance_registration_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        IF v_existing.lease_version >= 9223372036854775806 THEN
            RAISE EXCEPTION 'runtime_instance_lease_exhausted'
                USING ERRCODE = 'P0001';
        END IF;
        UPDATE public.pipeline_runtime_instances AS instance
        SET lifecycle = 'draining',
            lease_version = instance.lease_version + 1,
            heartbeat_at = v_now,
            lease_until = v_now + INTERVAL '1 microsecond',
            updated_at = v_now
        WHERE instance.session_id = v_existing.session_id;
    END IF;

    INSERT INTO public.pipeline_runtime_instances (
        account_id, workload, instance_id, session_id, generation,
        fencing_token, authority_epoch, capability_hash, schema_revision,
        protocol_version, build_id, config_hash, lifecycle, lease_version,
        accepted_count, rejected_count, registered_at, heartbeat_at,
        lease_until, updated_at
    ) VALUES (
        p_account_id, 'web', p_instance_id, p_session_id,
        v_authority.generation, v_authority.fencing_token,
        v_authority.authority_epoch, v_authority.capability_hash,
        p_schema_revision, p_protocol_version, p_build_id, p_config_hash,
        'active', 1, 0, 0, v_now, v_now,
        v_now + (p_lease_seconds * INTERVAL '1 second'), v_now
    )
    RETURNING * INTO STRICT v_instance;

    RETURN QUERY SELECT
        v_instance.account_id,
        v_instance.workload,
        v_instance.instance_id,
        v_instance.session_id,
        v_instance.generation,
        v_instance.fencing_token,
        v_instance.authority_epoch,
        v_instance.capability_hash::pg_catalog.text,
        v_instance.schema_revision,
        v_instance.protocol_version,
        v_instance.build_id,
        v_instance.config_hash::pg_catalog.text,
        v_instance.lifecycle,
        v_instance.lease_version,
        v_instance.accepted_count,
        v_instance.rejected_count,
        v_instance.heartbeat_at,
        v_instance.lease_until;
END
$greenfield_register_web_instance$
"""


def _create_greenfield_register_instance_function() -> None:
    op.execute(_GREENFIELD_REGISTER_INSTANCE_SQL)


_GREENFIELD_HEARTBEAT_INSTANCE_SQL = """
CREATE FUNCTION public.greenfield_heartbeat_web_instance(
    p_account_id pg_catalog.int8,
    p_session_id pg_catalog.uuid,
    p_expected_lease_version pg_catalog.int8,
    p_expected_authority_epoch pg_catalog.int8,
    p_expected_capability_hash pg_catalog.text,
    p_accepted_count pg_catalog.int8,
    p_rejected_count pg_catalog.int8,
    p_lease_seconds pg_catalog.int8
)
RETURNS TABLE (
    account_id pg_catalog.int8,
    workload pg_catalog.text,
    instance_id pg_catalog.text,
    session_id pg_catalog.uuid,
    generation pg_catalog.int8,
    fencing_token pg_catalog.int8,
    authority_epoch pg_catalog.int8,
    capability_hash pg_catalog.text,
    schema_revision pg_catalog.text,
    protocol_version pg_catalog.int8,
    build_id pg_catalog.text,
    config_hash pg_catalog.text,
    lifecycle pg_catalog.text,
    lease_version pg_catalog.int8,
    accepted_count pg_catalog.int8,
    rejected_count pg_catalog.int8,
    heartbeat_at pg_catalog.timestamptz,
    lease_until pg_catalog.timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $greenfield_heartbeat_web_instance$
DECLARE
    v_authority public.pipeline_runtime_authority%ROWTYPE;
    v_instance public.pipeline_runtime_instances%ROWTYPE;
    v_now pg_catalog.timestamptz;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_session_id IS NULL
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 15, 1) <> '4'
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 20, 1) !~ '^[89ab]$'
       OR p_expected_lease_version IS NULL
       OR p_expected_lease_version <= 0
       OR p_expected_lease_version >= 9223372036854775806
       OR p_expected_authority_epoch IS NULL
       OR p_expected_authority_epoch <= 0
       OR p_expected_authority_epoch >= 9223372036854775807
       OR p_expected_capability_hash IS NULL
       OR p_expected_capability_hash !~ '^[0-9a-f]{64}$'
       OR p_accepted_count IS NULL OR p_accepted_count < 0
       OR p_accepted_count >= 9223372036854775807
       OR p_rejected_count IS NULL OR p_rejected_count < 0
       OR p_rejected_count >= 9223372036854775807
       OR p_lease_seconds IS NULL OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'runtime_instance_heartbeat_input_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);
    v_now := pg_catalog.clock_timestamp();
    SELECT authority.*
    INTO v_authority
    FROM public.pipeline_runtime_authority AS authority
    WHERE authority.account_id = p_account_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_authority.state <> 'ingest_only'
       OR v_authority.authority_epoch <> p_expected_authority_epoch
       OR v_authority.capability_hash::pg_catalog.text <>
            p_expected_capability_hash
       OR v_authority.capability_stage_ordinal <> 1
       OR v_authority.schema_revision <> '20260716_0006'
    THEN
        RAISE EXCEPTION 'runtime_instance_authority_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT instance.*
    INTO v_instance
    FROM public.pipeline_runtime_instances AS instance
    WHERE instance.account_id = p_account_id
      AND instance.session_id = p_session_id
      AND instance.workload = 'web'
      AND instance.lifecycle = 'active'
      AND instance.lease_version = p_expected_lease_version
      AND instance.authority_epoch = p_expected_authority_epoch
      AND instance.capability_hash =
            p_expected_capability_hash::pg_catalog.bpchar
      AND instance.lease_until > v_now
    FOR UPDATE;
    IF NOT FOUND
       OR p_accepted_count < v_instance.accepted_count
       OR p_rejected_count < v_instance.rejected_count
    THEN
        RAISE EXCEPTION 'runtime_instance_lease_cas_conflict'
            USING ERRCODE = 'P0001';
    END IF;

    UPDATE public.pipeline_runtime_instances AS instance
    SET lease_version = instance.lease_version + 1,
        accepted_count = p_accepted_count,
        rejected_count = p_rejected_count,
        heartbeat_at = v_now,
        lease_until = v_now + (p_lease_seconds * INTERVAL '1 second'),
        updated_at = v_now
    WHERE instance.session_id = p_session_id
      AND instance.lease_version = p_expected_lease_version
    RETURNING instance.* INTO STRICT v_instance;

    RETURN QUERY SELECT
        v_instance.account_id,
        v_instance.workload,
        v_instance.instance_id,
        v_instance.session_id,
        v_instance.generation,
        v_instance.fencing_token,
        v_instance.authority_epoch,
        v_instance.capability_hash::pg_catalog.text,
        v_instance.schema_revision,
        v_instance.protocol_version,
        v_instance.build_id,
        v_instance.config_hash::pg_catalog.text,
        v_instance.lifecycle,
        v_instance.lease_version,
        v_instance.accepted_count,
        v_instance.rejected_count,
        v_instance.heartbeat_at,
        v_instance.lease_until;
END
$greenfield_heartbeat_web_instance$
"""


def _create_greenfield_heartbeat_instance_function() -> None:
    op.execute(_GREENFIELD_HEARTBEAT_INSTANCE_SQL)


_GREENFIELD_DRAIN_INSTANCE_SQL = """
CREATE FUNCTION public.greenfield_drain_web_instance(
    p_account_id pg_catalog.int8,
    p_session_id pg_catalog.uuid,
    p_expected_lease_version pg_catalog.int8,
    p_expected_authority_epoch pg_catalog.int8,
    p_expected_capability_hash pg_catalog.text
)
RETURNS TABLE (
    account_id pg_catalog.int8,
    workload pg_catalog.text,
    instance_id pg_catalog.text,
    session_id pg_catalog.uuid,
    generation pg_catalog.int8,
    fencing_token pg_catalog.int8,
    authority_epoch pg_catalog.int8,
    capability_hash pg_catalog.text,
    schema_revision pg_catalog.text,
    protocol_version pg_catalog.int8,
    build_id pg_catalog.text,
    config_hash pg_catalog.text,
    lifecycle pg_catalog.text,
    lease_version pg_catalog.int8,
    accepted_count pg_catalog.int8,
    rejected_count pg_catalog.int8,
    heartbeat_at pg_catalog.timestamptz,
    lease_until pg_catalog.timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $greenfield_drain_web_instance$
DECLARE
    v_instance public.pipeline_runtime_instances%ROWTYPE;
    v_now pg_catalog.timestamptz;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_session_id IS NULL
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 15, 1) <> '4'
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 20, 1) !~ '^[89ab]$'
       OR p_expected_lease_version IS NULL
       OR p_expected_lease_version <= 0
       OR p_expected_lease_version >= 9223372036854775806
       OR p_expected_authority_epoch IS NULL
       OR p_expected_authority_epoch <= 0
       OR p_expected_authority_epoch >= 9223372036854775807
       OR p_expected_capability_hash IS NULL
       OR p_expected_capability_hash !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION 'runtime_instance_drain_input_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);
    v_now := pg_catalog.clock_timestamp();
    SELECT instance.*
    INTO v_instance
    FROM public.pipeline_runtime_instances AS instance
    WHERE instance.account_id = p_account_id
      AND instance.session_id = p_session_id
      AND instance.workload = 'web'
      AND instance.lifecycle = 'active'
      AND instance.lease_version = p_expected_lease_version
      AND instance.authority_epoch = p_expected_authority_epoch
      AND instance.capability_hash =
            p_expected_capability_hash::pg_catalog.bpchar
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'runtime_instance_lease_cas_conflict'
            USING ERRCODE = 'P0001';
    END IF;

    UPDATE public.pipeline_runtime_instances AS instance
    SET lifecycle = 'draining',
        lease_version = instance.lease_version + 1,
        heartbeat_at = v_now,
        lease_until = v_now + INTERVAL '1 microsecond',
        updated_at = v_now
    WHERE instance.session_id = p_session_id
      AND instance.lease_version = p_expected_lease_version
    RETURNING instance.* INTO STRICT v_instance;

    RETURN QUERY SELECT
        v_instance.account_id,
        v_instance.workload,
        v_instance.instance_id,
        v_instance.session_id,
        v_instance.generation,
        v_instance.fencing_token,
        v_instance.authority_epoch,
        v_instance.capability_hash::pg_catalog.text,
        v_instance.schema_revision,
        v_instance.protocol_version,
        v_instance.build_id,
        v_instance.config_hash::pg_catalog.text,
        v_instance.lifecycle,
        v_instance.lease_version,
        v_instance.accepted_count,
        v_instance.rejected_count,
        v_instance.heartbeat_at,
        v_instance.lease_until;
END
$greenfield_drain_web_instance$
"""


def _create_greenfield_drain_instance_function() -> None:
    op.execute(_GREENFIELD_DRAIN_INSTANCE_SQL)


_GREENFIELD_WEBHOOK_SQL = """
CREATE FUNCTION public.greenfield_insert_webhook_event(
    p_account_id pg_catalog.int8,
    p_session_id pg_catalog.uuid,
    p_expected_lease_version pg_catalog.int8,
    p_external_email_id pg_catalog.text,
    p_folder_key pg_catalog.text,
    p_raw_event_type pg_catalog.text,
    p_change_kind pg_catalog.text,
    p_dedupe_key pg_catalog.text,
    p_source_version pg_catalog.text,
    p_source_event_at pg_catalog.timestamptz,
    p_payload pg_catalog.jsonb,
    p_processing_policy pg_catalog.text
)
RETURNS TABLE (
    inbox_id pg_catalog.uuid,
    duplicate pg_catalog.bool
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $greenfield_insert_webhook_event$
DECLARE
    v_zero pg_catalog.bytea := pg_catalog.decode('00', 'hex');
    v_unicode_edge_spaces pg_catalog.text :=
        pg_catalog.chr(133) || pg_catalog.chr(160) ||
        pg_catalog.chr(5760) || pg_catalog.chr(8192) ||
        pg_catalog.chr(8193) || pg_catalog.chr(8194) ||
        pg_catalog.chr(8195) || pg_catalog.chr(8196) ||
        pg_catalog.chr(8197) || pg_catalog.chr(8198) ||
        pg_catalog.chr(8199) || pg_catalog.chr(8200) ||
        pg_catalog.chr(8201) || pg_catalog.chr(8202) ||
        pg_catalog.chr(8232) || pg_catalog.chr(8233) ||
        pg_catalog.chr(8239) || pg_catalog.chr(8287) ||
        pg_catalog.chr(12288);
    v_authority public.pipeline_runtime_authority%ROWTYPE;
    v_instance public.pipeline_runtime_instances%ROWTYPE;
    v_existing public.event_inbox%ROWTYPE;
    v_expected_policy pg_catalog.text;
    v_scope_found pg_catalog.bool;
    v_inbox_id pg_catalog.uuid;
    v_status pg_catalog.text;
    v_now pg_catalog.timestamptz;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_session_id IS NULL
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 15, 1) <> '4'
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 20, 1) !~ '^[89ab]$'
       OR p_expected_lease_version IS NULL
       OR p_expected_lease_version <= 0
       OR p_expected_lease_version >= 9223372036854775807
       OR p_external_email_id IS NULL
       OR pg_catalog.btrim(p_external_email_id) <> p_external_email_id
       OR pg_catalog.btrim(
            p_external_email_id, v_unicode_edge_spaces
       ) <> p_external_email_id
       OR p_external_email_id ~ '^[[:space:]]|[[:space:]]$'
       OR p_external_email_id ~ '[[:cntrl:]]'
       OR pg_catalog.char_length(p_external_email_id) NOT BETWEEN 1 AND 1024
       OR p_folder_key IS NULL
       OR pg_catalog.btrim(p_folder_key) <> p_folder_key
       OR pg_catalog.btrim(p_folder_key, v_unicode_edge_spaces) <> p_folder_key
       OR p_folder_key ~ '^[[:space:]]|[[:space:]]$'
       OR p_folder_key ~ '[[:cntrl:]]'
       OR pg_catalog.char_length(p_folder_key) NOT BETWEEN 1 AND 512
       OR p_raw_event_type IS NULL
       OR p_raw_event_type ~ '[[:cntrl:]]'
       OR p_raw_event_type ~ '^[[:space:]]|[[:space:]]$'
       OR pg_catalog.char_length(p_raw_event_type) NOT BETWEEN 1 AND 128
       OR p_change_kind IS NULL
       OR NOT (
            (p_raw_event_type IN ('NewMailEvent', 'CreatedEvent')
                AND p_change_kind = 'create')
            OR (p_raw_event_type = 'ModifiedEvent'
                AND p_change_kind = 'update')
            OR (p_raw_event_type = 'DeletedEvent'
                AND p_change_kind = 'delete')
       )
       OR p_dedupe_key IS NULL OR p_dedupe_key !~ '^[0-9a-f]{64}$'
       OR (
            p_source_version IS NOT NULL AND (
                pg_catalog.btrim(p_source_version) <> p_source_version
                OR pg_catalog.btrim(
                    p_source_version, v_unicode_edge_spaces
                ) <> p_source_version
                OR p_source_version ~ '^[[:space:]]|[[:space:]]$'
                OR p_source_version ~ '[[:cntrl:]]'
                OR pg_catalog.char_length(p_source_version) NOT BETWEEN 1 AND 512
            )
       )
       OR (
            p_source_event_at IS NOT NULL AND (
                NOT pg_catalog.isfinite(p_source_event_at)
                OR p_source_event_at <
                    '0002-01-01 00:00:00+00'::pg_catalog.timestamptz
                OR p_source_event_at >=
                    '9999-01-01 00:00:00+00'::pg_catalog.timestamptz
            )
       )
       OR p_payload IS NULL
       OR pg_catalog.jsonb_typeof(p_payload) <> 'object'
       OR pg_catalog.octet_length(p_payload::pg_catalog.text) > 262144
       OR p_processing_policy IS NULL
       OR p_processing_policy NOT IN (
            'full', 'archive', 'metadata_only', 'ignored'
       )
    THEN
        RAISE EXCEPTION 'greenfield_webhook_input_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);
    v_now := pg_catalog.clock_timestamp();
    SELECT authority.*
    INTO v_authority
    FROM public.pipeline_runtime_authority AS authority
    WHERE authority.account_id = p_account_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_authority.state <> 'ingest_only'
       OR v_authority.capability_stage_ordinal <> 1
       OR v_authority.schema_revision <> '20260716_0006'
    THEN
        RAISE EXCEPTION 'greenfield_webhook_authority_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT instance.*
    INTO v_instance
    FROM public.pipeline_runtime_instances AS instance
    WHERE instance.account_id = p_account_id
      AND instance.session_id = p_session_id
      AND instance.workload = 'web'
      AND instance.lifecycle = 'active'
      AND instance.lease_version = p_expected_lease_version
      AND instance.generation = v_authority.generation
      AND instance.fencing_token = v_authority.fencing_token
      AND instance.authority_epoch = v_authority.authority_epoch
      AND instance.capability_hash = v_authority.capability_hash
      AND instance.lease_until > v_now
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'greenfield_webhook_authority_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT scope.event_policy_matrix ->> (
        'webhook:' || p_raw_event_type || ':' || p_change_kind
    )
    INTO v_expected_policy
    FROM public.pipeline_folder_scopes AS scope
    WHERE scope.account_id = p_account_id
      AND scope.webhook_ids ? p_folder_key;
    v_scope_found := FOUND;
    IF (v_scope_found AND v_expected_policy IS DISTINCT FROM p_processing_policy)
       OR (NOT v_scope_found AND p_processing_policy <> 'ignored')
    THEN
        RAISE EXCEPTION 'greenfield_webhook_policy_mismatch'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT inbox.*
    INTO v_existing
    FROM public.event_inbox AS inbox
    WHERE inbox.dedupe_key = p_dedupe_key::pg_catalog.bpchar
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.account_id <> p_account_id
           OR v_existing.external_email_id <> p_external_email_id
           OR v_existing.folder_key <> p_folder_key
           OR v_existing.source <> 'webhook'
           OR v_existing.raw_event_type <> p_raw_event_type
           OR v_existing.change_kind <> p_change_kind
           OR v_existing.source_version IS DISTINCT FROM p_source_version
           OR v_existing.source_event_at IS DISTINCT FROM p_source_event_at
           OR v_existing.payload IS DISTINCT FROM p_payload
           OR v_existing.processing_policy <> p_processing_policy
           OR v_existing.pipeline_name <> v_authority.pipeline_name
           OR v_existing.generation <> v_authority.generation
           OR v_existing.fencing_token <> v_authority.fencing_token
           OR v_existing.authority_epoch <> v_authority.authority_epoch
           OR v_existing.capability_hash <> v_authority.capability_hash
        THEN
            RAISE EXCEPTION 'greenfield_webhook_dedupe_identity_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        RETURN QUERY SELECT v_existing.id, true;
        RETURN;
    END IF;

    v_inbox_id := pg_catalog.gen_random_uuid();
    v_status := CASE
        WHEN p_processing_policy = 'ignored' THEN 'completed'
        ELSE 'pending'
    END;
    INSERT INTO public.event_inbox (
        id, account_id, external_email_id, folder_key, source,
        raw_event_type, change_kind, dedupe_key, source_version,
        source_event_at, payload, processing_policy, pipeline_name,
        generation, fencing_token, execution_epoch, authority_epoch,
        capability_hash, status, attempts, available_at, received_at,
        updated_at
    ) VALUES (
        v_inbox_id, p_account_id, p_external_email_id, p_folder_key,
        'webhook', p_raw_event_type, p_change_kind, p_dedupe_key,
        p_source_version, p_source_event_at, p_payload, p_processing_policy,
        v_authority.pipeline_name, v_authority.generation,
        v_authority.fencing_token, 0, v_authority.authority_epoch,
        v_authority.capability_hash, v_status, 0, v_now, v_now, v_now
    );
    INSERT INTO public.audit_events (
        id, event_key, account_id, email_id, object_type,
        object_fingerprint, action, result, actor, reason, safe_metadata,
        created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(),
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    'ai-exchange-greenfield-webhook-audit-v1', 'UTF8'
                ) || v_zero ||
                pg_catalog.convert_to(v_inbox_id::pg_catalog.text, 'UTF8')
            ),
            'hex'
        ),
        p_account_id,
        NULL,
        'event_inbox',
        p_dedupe_key,
        'ingress.webhook_accepted',
        v_status,
        'greenfield_webhook',
        'verified_webhook_persisted',
        pg_catalog.jsonb_build_object(
            'authority_epoch', v_authority.authority_epoch,
            'capability_hash', v_authority.capability_hash::pg_catalog.text,
            'inbox_id', v_inbox_id::pg_catalog.text,
            'processing_policy', p_processing_policy,
            'status', v_status
        ),
        v_now
    );
    RETURN QUERY SELECT v_inbox_id, false;
END
$greenfield_insert_webhook_event$
"""


def _create_greenfield_webhook_function() -> None:
    op.execute(_GREENFIELD_WEBHOOK_SQL)


_GREENFIELD_REQUEUE_SQL = """
CREATE FUNCTION public.greenfield_requeue_inbox(
    p_account_id pg_catalog.int8,
    p_inbox_id pg_catalog.uuid,
    p_expected_execution_epoch pg_catalog.int8,
    p_expected_email_version pg_catalog.int8,
    p_actor pg_catalog.text,
    p_reason pg_catalog.text,
    p_idempotency_key pg_catalog.text,
    p_canonical_payload_hash pg_catalog.text
)
RETURNS TABLE (
    command_receipt_id pg_catalog.uuid,
    inbox_id pg_catalog.uuid,
    email_id pg_catalog.uuid,
    previous_execution_epoch pg_catalog.int8,
    execution_epoch pg_catalog.int8,
    email_version pg_catalog.int8,
    status pg_catalog.text,
    transaction_id pg_catalog.text,
    replayed pg_catalog.bool,
    created_at pg_catalog.timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $greenfield_requeue_inbox$
DECLARE
    v_zero pg_catalog.bytea := pg_catalog.decode('00', 'hex');
    v_unicode_edge_spaces pg_catalog.text :=
        pg_catalog.chr(133) || pg_catalog.chr(160) ||
        pg_catalog.chr(5760) || pg_catalog.chr(8192) ||
        pg_catalog.chr(8193) || pg_catalog.chr(8194) ||
        pg_catalog.chr(8195) || pg_catalog.chr(8196) ||
        pg_catalog.chr(8197) || pg_catalog.chr(8198) ||
        pg_catalog.chr(8199) || pg_catalog.chr(8200) ||
        pg_catalog.chr(8201) || pg_catalog.chr(8202) ||
        pg_catalog.chr(8232) || pg_catalog.chr(8233) ||
        pg_catalog.chr(8239) || pg_catalog.chr(8287) ||
        pg_catalog.chr(12288);
    v_authority public.pipeline_runtime_authority%ROWTYPE;
    v_inbox public.event_inbox%ROWTYPE;
    v_email public.emails%ROWTYPE;
    v_existing_receipt public.pipeline_command_receipts%ROWTYPE;
    v_replay_metadata pg_catalog.jsonb;
    v_replay_actor pg_catalog.text;
    v_replay_reason pg_catalog.text;
    v_canonical pg_catalog.text;
    v_expected_payload_hash pg_catalog.text;
    v_idempotency_hash pg_catalog.text;
    v_receipt_id pg_catalog.uuid;
    v_transaction_id pg_catalog.text;
    v_result_hash pg_catalog.text;
    v_now pg_catalog.timestamptz;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_inbox_id IS NULL
       OR pg_catalog.substr(p_inbox_id::pg_catalog.text, 15, 1) <> '4'
       OR pg_catalog.substr(p_inbox_id::pg_catalog.text, 20, 1) !~ '^[89ab]$'
       OR p_expected_execution_epoch IS NULL
       OR p_expected_execution_epoch < 0
       OR p_expected_execution_epoch >= 9223372036854775806
       OR p_expected_email_version IS NULL OR p_expected_email_version < 0
       OR p_expected_email_version >= 9223372036854775805
       OR p_actor IS NULL OR pg_catalog.btrim(p_actor) <> p_actor
       OR pg_catalog.btrim(p_actor, v_unicode_edge_spaces) <> p_actor
       OR p_actor ~ '^[[:space:]]|[[:space:]]$'
       OR p_actor ~ '[[:cntrl:]]'
       OR pg_catalog.octet_length(p_actor) NOT BETWEEN 1 AND 128
       OR p_reason IS NULL OR pg_catalog.btrim(p_reason) <> p_reason
       OR pg_catalog.btrim(p_reason, v_unicode_edge_spaces) <> p_reason
       OR p_reason ~ '^[[:space:]]|[[:space:]]$'
       OR p_reason ~ '[[:cntrl:]]'
       OR pg_catalog.octet_length(p_reason) NOT BETWEEN 1 AND 512
       OR p_idempotency_key IS NULL
       OR pg_catalog.btrim(p_idempotency_key) <> p_idempotency_key
       OR pg_catalog.btrim(
            p_idempotency_key, v_unicode_edge_spaces
       ) <> p_idempotency_key
       OR p_idempotency_key ~ '^[[:space:]]|[[:space:]]$'
       OR p_idempotency_key ~ '[[:cntrl:]]'
       OR pg_catalog.octet_length(p_idempotency_key) NOT BETWEEN 1 AND 4096
       OR p_canonical_payload_hash IS NULL
       OR p_canonical_payload_hash !~ '^[0-9a-f]{64}$'
    THEN
        RAISE EXCEPTION 'greenfield_requeue_input_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    v_canonical :=
        '{"account_id":' || p_account_id::pg_catalog.text ||
        ',"actor":' || pg_catalog.to_json(p_actor)::pg_catalog.text ||
        ',"command_name":"inbox.requeue"' ||
        ',"expected_email_version":' ||
        p_expected_email_version::pg_catalog.text ||
        ',"expected_execution_epoch":' ||
        p_expected_execution_epoch::pg_catalog.text ||
        ',"inbox_id":"' || p_inbox_id::pg_catalog.text ||
        '","reason":' || pg_catalog.to_json(p_reason)::pg_catalog.text ||
        ',"schema_version":' || '1' || '}';
    v_expected_payload_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(
                'ai-exchange-inbox-requeue-command-v1', 'UTF8'
            ) || v_zero || pg_catalog.convert_to(v_canonical, 'UTF8')
        ),
        'hex'
    );
    IF v_expected_payload_hash <> p_canonical_payload_hash THEN
        RAISE EXCEPTION 'greenfield_requeue_payload_invalid'
            USING ERRCODE = 'P0001';
    END IF;

    v_idempotency_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to('pipeline-command-idempotency-v1', 'UTF8') ||
            v_zero ||
            pg_catalog.convert_to(p_account_id::pg_catalog.text, 'UTF8') ||
            v_zero || pg_catalog.convert_to('inbox.requeue', 'UTF8') ||
            v_zero || pg_catalog.convert_to(p_idempotency_key, 'UTF8')
        ),
        'hex'
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);

    SELECT receipt.*
    INTO v_existing_receipt
    FROM public.pipeline_command_receipts AS receipt
    WHERE receipt.account_id = p_account_id
      AND receipt.command_name = 'inbox.requeue'
      AND receipt.idempotency_key_hash = v_idempotency_hash;
    IF FOUND THEN
        IF v_existing_receipt.canonical_payload_hash::pg_catalog.text <>
                p_canonical_payload_hash THEN
            RAISE EXCEPTION 'command_idempotency_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        SELECT audit.safe_metadata, audit.actor, audit.reason
        INTO STRICT v_replay_metadata, v_replay_actor, v_replay_reason
        FROM public.audit_events AS audit
        WHERE audit.account_id = p_account_id
          AND audit.action = 'inbox.requeue'
          AND audit.safe_metadata ->> 'command_receipt_id' =
                v_existing_receipt.id::pg_catalog.text;
        IF p_inbox_id::pg_catalog.text <> v_existing_receipt.result_id
           OR p_expected_execution_epoch <>
                (v_replay_metadata ->> 'previous_execution_epoch')::pg_catalog.int8
           OR p_expected_email_version <>
                (v_replay_metadata ->> 'email_version')::pg_catalog.int8 - 1
           OR p_actor <> v_replay_actor
           OR p_reason <> v_replay_reason
        THEN
            RAISE EXCEPTION 'command_idempotency_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        RETURN QUERY SELECT
            v_existing_receipt.id,
            v_existing_receipt.result_id::pg_catalog.uuid,
            (v_replay_metadata ->> 'email_id')::pg_catalog.uuid,
            (v_replay_metadata ->> 'previous_execution_epoch')::pg_catalog.int8,
            (v_replay_metadata ->> 'execution_epoch')::pg_catalog.int8,
            (v_replay_metadata ->> 'email_version')::pg_catalog.int8,
            'retry_wait'::pg_catalog.text,
            v_replay_metadata ->> 'transaction_id',
            true,
            v_existing_receipt.created_at;
        RETURN;
    END IF;

    SELECT authority.*
    INTO v_authority
    FROM public.pipeline_runtime_authority AS authority
    WHERE authority.account_id = p_account_id
    FOR UPDATE;
    IF NOT FOUND
       OR v_authority.state NOT IN ('ingest_only', 'paused')
       OR v_authority.capability_stage_ordinal <> 1
       OR v_authority.schema_revision <> '20260716_0006'
    THEN
        RAISE EXCEPTION 'greenfield_requeue_authority_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT inbox.*
    INTO v_inbox
    FROM public.event_inbox AS inbox
    WHERE inbox.account_id = p_account_id
      AND inbox.id = p_inbox_id
      AND inbox.pipeline_name = v_authority.pipeline_name
      AND inbox.generation = v_authority.generation
      AND inbox.fencing_token = v_authority.fencing_token
      AND inbox.authority_epoch = v_authority.authority_epoch
      AND inbox.capability_hash = v_authority.capability_hash
      AND inbox.execution_epoch = p_expected_execution_epoch
      AND inbox.status IN ('manual_review', 'dead_letter')
      AND inbox.processing_started_at IS NOT NULL
      AND inbox.effect_started_at IS NULL
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'greenfield_requeue_not_safe'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT email.*
    INTO v_email
    FROM public.emails AS email
    WHERE email.account_id = p_account_id
      AND email.processing_inbox_id = p_inbox_id
      AND email.processing_execution_epoch = p_expected_execution_epoch
      AND email.version = p_expected_email_version
      AND email.external_email_id = v_inbox.external_email_id
      AND email.owner_generation = v_inbox.generation
      AND email.owner_fencing_token = v_inbox.fencing_token
      AND email.owner_authority_epoch = v_inbox.authority_epoch
      AND email.owner_capability_hash = v_inbox.capability_hash
      AND email.status IN ('manual_review', 'dead_letter')
      AND email.status::pg_catalog.text = v_inbox.status::pg_catalog.text
      AND email.create_seen_at IS NOT NULL
      AND email.processing_started_at IS NOT NULL
      AND email.source_deleted_at IS NULL
      AND email.external_effects_started_at IS NULL
      AND (
            email.safe_error_code IS NOT DISTINCT FROM v_inbox.safe_error_code
      )
      AND (
            email.safe_error_summary
                IS NOT DISTINCT FROM v_inbox.safe_error_summary
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'greenfield_requeue_not_safe'
            USING ERRCODE = 'P0001';
    END IF;

    v_receipt_id := pg_catalog.gen_random_uuid();
    v_transaction_id := pg_catalog.pg_current_xact_id()::pg_catalog.text;
    v_now := pg_catalog.clock_timestamp();
    UPDATE public.event_inbox AS inbox
    SET execution_epoch = inbox.execution_epoch + 1,
        status = 'pending',
        lease_owner = NULL,
        lease_until = NULL,
        lease_session_id = NULL,
        attempts = 0,
        available_at = v_now,
        processing_started_at = NULL,
        effect_started_at = NULL,
        safe_error_code = NULL,
        safe_error_summary = NULL,
        updated_at = v_now
    WHERE inbox.id = p_inbox_id
      AND inbox.execution_epoch = p_expected_execution_epoch
    RETURNING inbox.* INTO STRICT v_inbox;
    UPDATE public.emails AS email
    SET processing_execution_epoch = email.processing_execution_epoch + 1,
        status = 'retry_wait',
        version = email.version + 1,
        processing_started_at = NULL,
        safe_error_code = 'inbox.requeued',
        safe_error_summary = NULL,
        updated_at = v_now
    WHERE email.id = v_email.id
      AND email.version = p_expected_email_version
    RETURNING email.* INTO STRICT v_email;

    v_result_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to(
                'ai-exchange-inbox-requeue-result-v1', 'UTF8'
            ) || v_zero ||
            pg_catalog.convert_to(v_receipt_id::pg_catalog.text, 'UTF8') ||
            v_zero || pg_catalog.convert_to(p_inbox_id::pg_catalog.text, 'UTF8') ||
            v_zero || pg_catalog.convert_to(v_email.id::pg_catalog.text, 'UTF8')
        ),
        'hex'
    );
    INSERT INTO public.pipeline_command_receipts (
        id, account_id, command_name, idempotency_key_hash,
        canonical_payload_hash, outcome, result_type, result_id,
        result_hash, authority_epoch, created_at
    ) VALUES (
        v_receipt_id, p_account_id, 'inbox.requeue', v_idempotency_hash,
        p_canonical_payload_hash, 'succeeded', 'event_inbox',
        p_inbox_id::pg_catalog.text, v_result_hash,
        v_authority.authority_epoch, v_now
    );
    INSERT INTO public.audit_events (
        id, event_key, account_id, email_id, object_type,
        object_fingerprint, action, result, actor, reason, safe_metadata,
        created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(),
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    'ai-exchange-inbox-requeue-audit-v1', 'UTF8'
                ) || v_zero ||
                pg_catalog.convert_to(v_receipt_id::pg_catalog.text, 'UTF8')
            ),
            'hex'
        ),
        p_account_id,
        v_email.id,
        'event_inbox',
        v_inbox.dedupe_key,
        'inbox.requeue',
        'succeeded',
        p_actor,
        p_reason,
        pg_catalog.jsonb_build_object(
            'command_receipt_id', v_receipt_id::pg_catalog.text,
            'email_id', v_email.id::pg_catalog.text,
            'email_version', v_email.version,
            'execution_epoch', v_inbox.execution_epoch,
            'inbox_id', v_inbox.id::pg_catalog.text,
            'previous_execution_epoch', p_expected_execution_epoch,
            'transaction_id', v_transaction_id
        ),
        v_now
    );

    RETURN QUERY SELECT
        v_receipt_id,
        v_inbox.id,
        v_email.id,
        p_expected_execution_epoch,
        v_inbox.execution_epoch,
        v_email.version,
        'retry_wait'::pg_catalog.text,
        v_transaction_id,
        false,
        v_now;
END
$greenfield_requeue_inbox$
"""


def _create_greenfield_recovery_function() -> None:
    op.execute(_GREENFIELD_REQUEUE_SQL)


def _create_phase2_worker_function_stubs() -> None:
    signatures = (
        (
            "greenfield_claim_inbox",
            """
                p_account_id pg_catalog.int8,
                p_session_id pg_catalog.uuid,
                p_expected_lease_version pg_catalog.int8,
                p_lease_owner pg_catalog.text,
                p_limit pg_catalog.int8,
                p_lease_seconds pg_catalog.int8
            """,
        ),
        (
            "greenfield_renew_inbox",
            """
                p_account_id pg_catalog.int8,
                p_session_id pg_catalog.uuid,
                p_expected_lease_version pg_catalog.int8,
                p_inbox_id pg_catalog.uuid,
                p_execution_epoch pg_catalog.int8,
                p_lease_owner pg_catalog.text,
                p_attempts pg_catalog.int8,
                p_lease_seconds pg_catalog.int8
            """,
        ),
        (
            "greenfield_apply_email_event",
            """
                p_account_id pg_catalog.int8,
                p_session_id pg_catalog.uuid,
                p_expected_lease_version pg_catalog.int8,
                p_inbox_id pg_catalog.uuid,
                p_execution_epoch pg_catalog.int8,
                p_expected_email_version pg_catalog.int8
            """,
        ),
        (
            "greenfield_begin_inbox_effect",
            """
                p_account_id pg_catalog.int8,
                p_session_id pg_catalog.uuid,
                p_expected_lease_version pg_catalog.int8,
                p_inbox_id pg_catalog.uuid,
                p_execution_epoch pg_catalog.int8,
                p_attempts pg_catalog.int8
            """,
        ),
        (
            "greenfield_finish_inbox",
            """
                p_account_id pg_catalog.int8,
                p_session_id pg_catalog.uuid,
                p_expected_lease_version pg_catalog.int8,
                p_inbox_id pg_catalog.uuid,
                p_execution_epoch pg_catalog.int8,
                p_attempts pg_catalog.int8,
                p_completion pg_catalog.jsonb
            """,
        ),
        (
            "greenfield_fail_inbox",
            """
                p_account_id pg_catalog.int8,
                p_session_id pg_catalog.uuid,
                p_expected_lease_version pg_catalog.int8,
                p_inbox_id pg_catalog.uuid,
                p_execution_epoch pg_catalog.int8,
                p_attempts pg_catalog.int8,
                p_safe_error_code pg_catalog.text,
                p_safe_error_summary pg_catalog.text
            """,
        ),
        (
            "greenfield_reap_inbox",
            """
                p_account_id pg_catalog.int8,
                p_session_id pg_catalog.uuid,
                p_expected_lease_version pg_catalog.int8,
                p_limit pg_catalog.int8
            """,
        ),
    )
    for routine_name, arguments in signatures:
        op.execute(
            f"""
            CREATE FUNCTION public.{routine_name}(
                {arguments}
            )
            RETURNS pg_catalog.jsonb
            LANGUAGE plpgsql
            SECURITY DEFINER
            SET search_path = pg_catalog
            AS $phase2_worker_authority_unavailable$
            BEGIN
                RAISE EXCEPTION 'phase2_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $phase2_worker_authority_unavailable$
            """
        )


def _revoke_greenfield_function_execution() -> None:
    op.execute(
        """
        REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC
        """
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
