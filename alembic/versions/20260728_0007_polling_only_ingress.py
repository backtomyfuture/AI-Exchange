"""Add the session-fenced polling-only Inbox ingress boundary."""

from __future__ import annotations

from alembic import op


revision = "20260728_0007"
down_revision = "20260716_0006"
branch_labels = None
depends_on = None


_GREENFIELD_SYNC_PAGE_SQL = """
CREATE FUNCTION public.greenfield_commit_sync_page(
    p_account_id pg_catalog.int8,
    p_session_id pg_catalog.uuid,
    p_expected_lease_version pg_catalog.int8,
    p_folder_key pg_catalog.text,
    p_expected_cursor pg_catalog.text,
    p_expected_cursor_version pg_catalog.int8,
    p_next_cursor pg_catalog.text,
    p_events pg_catalog.jsonb,
    p_activation pg_catalog.bool
)
RETURNS TABLE (
    committed_cursor pg_catalog.text,
    committed_version pg_catalog.int8,
    inserted_count pg_catalog.int8,
    duplicate_count pg_catalog.int8
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $greenfield_commit_sync_page$
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
    v_scope public.pipeline_folder_scopes%ROWTYPE;
    v_cursor public.sync_cursors%ROWTYPE;
    v_existing public.event_inbox%ROWTYPE;
    v_event pg_catalog.jsonb;
    v_external_email_id pg_catalog.text;
    v_change_kind pg_catalog.text;
    v_dedupe_key pg_catalog.text;
    v_source_version pg_catalog.text;
    v_payload pg_catalog.jsonb;
    v_processing_policy pg_catalog.text;
    v_status pg_catalog.text;
    v_inbox_id pg_catalog.uuid;
    v_now pg_catalog.timestamptz;
    v_seen_dedupe_keys pg_catalog.text[] := ARRAY[]::pg_catalog.text[];
    v_seen_identities pg_catalog.text[] := ARRAY[]::pg_catalog.text[];
    v_inserted_count pg_catalog.int8 := 0;
    v_duplicate_count pg_catalog.int8 := 0;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_session_id IS NULL
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 15, 1) <> '4'
       OR pg_catalog.substr(p_session_id::pg_catalog.text, 20, 1) !~ '^[89ab]$'
       OR p_expected_lease_version IS NULL
       OR p_expected_lease_version <= 0
       OR p_expected_lease_version >= 9223372036854775807
       OR p_folder_key IS DISTINCT FROM 'INBOX'
       OR p_expected_cursor_version IS NULL
       OR p_expected_cursor_version < 0
       OR p_next_cursor IS NULL
       OR pg_catalog.btrim(p_next_cursor) <> p_next_cursor
       OR pg_catalog.btrim(p_next_cursor, v_unicode_edge_spaces) <> p_next_cursor
       OR p_next_cursor ~ '^[[:space:]]|[[:space:]]$'
       OR p_next_cursor ~ '[[:cntrl:]]'
       OR pg_catalog.char_length(p_next_cursor) NOT BETWEEN 1 AND 8192
       OR p_events IS NULL
       OR pg_catalog.jsonb_typeof(p_events) <> 'array'
       OR pg_catalog.jsonb_array_length(p_events) > 500
       OR pg_catalog.octet_length(p_events::pg_catalog.text) > 1048576
       OR p_activation IS NULL
       OR (
            p_activation
            AND (
            p_expected_cursor IS NOT NULL
                OR pg_catalog.jsonb_array_length(p_events) <> 0
            )
       )
       OR (
            NOT p_activation
            AND (
                p_expected_cursor IS NULL
                OR pg_catalog.btrim(p_expected_cursor) <> p_expected_cursor
                OR pg_catalog.btrim(
                    p_expected_cursor, v_unicode_edge_spaces
                ) <> p_expected_cursor
                OR p_expected_cursor ~ '^[[:space:]]|[[:space:]]$'
                OR p_expected_cursor ~ '[[:cntrl:]]'
                OR pg_catalog.char_length(p_expected_cursor) NOT BETWEEN 1 AND 8192
                OR p_expected_cursor_version <= 0
            )
       )
    THEN
        RAISE EXCEPTION 'greenfield_sync_input_invalid'
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
        RAISE EXCEPTION 'greenfield_sync_authority_unavailable'
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
        RAISE EXCEPTION 'greenfield_sync_lease_conflict'
            USING ERRCODE = 'P0001';
    END IF;

    SELECT scope.*
    INTO v_scope
    FROM public.pipeline_folder_scopes AS scope
    WHERE scope.account_id = p_account_id
      AND scope.initialization_id = v_authority.initialization_id
      AND scope.canonical_key = p_folder_key
      AND scope.sync_folder = p_folder_key
    FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'greenfield_sync_scope_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    IF p_activation THEN
        INSERT INTO public.sync_cursors (
            account_id, folder_key, cursor, status, blocked_reason_code
        ) VALUES (
            p_account_id, p_folder_key, NULL,
            'cold_start_pending', 'sync.cold_start_required'
        ) ON CONFLICT (account_id, folder_key) DO NOTHING;
    END IF;

    SELECT sync_cursor.*
    INTO v_cursor
    FROM public.sync_cursors AS sync_cursor
    WHERE sync_cursor.account_id = p_account_id
      AND sync_cursor.folder_key = p_folder_key
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'greenfield_sync_cursor_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    IF p_activation THEN
        IF v_cursor.status <> 'cold_start_pending'
           OR v_cursor.cursor IS NOT NULL
           OR v_cursor.version <> p_expected_cursor_version
        THEN
            RAISE EXCEPTION 'greenfield_sync_cursor_conflict'
                USING ERRCODE = 'P0001';
        END IF;
    ELSIF v_cursor.status <> 'active'
       OR v_cursor.cursor IS DISTINCT FROM p_expected_cursor
       OR v_cursor.version <> p_expected_cursor_version
    THEN
        IF v_cursor.status = 'active' THEN
            RAISE EXCEPTION 'greenfield_sync_cursor_conflict'
                USING ERRCODE = 'P0001';
        END IF;
        RAISE EXCEPTION 'greenfield_sync_cursor_unavailable'
            USING ERRCODE = 'P0001';
    END IF;

    FOR v_event IN
        SELECT page_event.value
        FROM pg_catalog.jsonb_array_elements(p_events) AS page_event(value)
        ORDER BY (page_event.value ->> 'dedupe_key') COLLATE "C"
    LOOP
        IF pg_catalog.jsonb_typeof(v_event) <> 'object'
           OR NOT (
               v_event ?& ARRAY[
                   'external_email_id', 'change_kind', 'dedupe_key',
                   'source_version'
               ]::pg_catalog.text[]
           )
           OR (
               v_event - 'external_email_id' - 'change_kind' - 'dedupe_key' -
               'source_version'
           ) <> '{}'::pg_catalog.jsonb
           OR pg_catalog.jsonb_typeof(v_event -> 'external_email_id') <> 'string'
           OR pg_catalog.jsonb_typeof(v_event -> 'change_kind') <> 'string'
           OR pg_catalog.jsonb_typeof(v_event -> 'dedupe_key') <> 'string'
           OR pg_catalog.jsonb_typeof(v_event -> 'source_version')
                NOT IN ('string', 'null')
        THEN
            RAISE EXCEPTION 'greenfield_sync_input_invalid'
                USING ERRCODE = 'P0001';
        END IF;

        v_external_email_id := v_event ->> 'external_email_id';
        v_change_kind := v_event ->> 'change_kind';
        v_dedupe_key := v_event ->> 'dedupe_key';
        v_source_version := v_event ->> 'source_version';
        IF pg_catalog.btrim(v_external_email_id) <> v_external_email_id
           OR pg_catalog.btrim(
                v_external_email_id, v_unicode_edge_spaces
           ) <> v_external_email_id
           OR v_external_email_id ~ '^[[:space:]]|[[:space:]]$'
           OR v_external_email_id ~ '[[:cntrl:]]'
           OR pg_catalog.char_length(v_external_email_id) NOT BETWEEN 1 AND 1024
           OR v_change_kind NOT IN ('create', 'update', 'delete')
           OR v_dedupe_key !~ '^[0-9a-f]{64}$'
           OR (
                v_source_version IS NOT NULL AND (
                    pg_catalog.btrim(v_source_version) <> v_source_version
                    OR pg_catalog.btrim(
                        v_source_version, v_unicode_edge_spaces
                    ) <> v_source_version
                    OR v_source_version ~ '^[[:space:]]|[[:space:]]$'
                    OR v_source_version ~ '[[:cntrl:]]'
                    OR pg_catalog.char_length(v_source_version) NOT BETWEEN 1 AND 512
                )
           )
           OR v_dedupe_key = ANY(v_seen_dedupe_keys)
           OR (v_change_kind || ':' || v_external_email_id) = ANY(v_seen_identities)
        THEN
            RAISE EXCEPTION 'greenfield_sync_input_invalid'
                USING ERRCODE = 'P0001';
        END IF;
        v_seen_dedupe_keys := pg_catalog.array_append(
            v_seen_dedupe_keys, v_dedupe_key
        );
        v_seen_identities := pg_catalog.array_append(
            v_seen_identities, v_change_kind || ':' || v_external_email_id
        );

        v_processing_policy := v_scope.event_policy_matrix ->> (
            'sync:' || v_change_kind || ':' || v_change_kind
        );
        IF v_processing_policy NOT IN (
            'full', 'archive', 'metadata_only', 'ignored'
        ) THEN
            RAISE EXCEPTION 'greenfield_sync_policy_unavailable'
                USING ERRCODE = 'P0001';
        END IF;

        v_payload := pg_catalog.jsonb_build_object(
            'cursor', p_next_cursor,
            'change_type', v_change_kind,
            'id', v_external_email_id,
            'item', CASE
                WHEN v_change_kind IN ('create', 'update')
                    THEN '{}'::pg_catalog.jsonb
                ELSE 'null'::pg_catalog.jsonb
            END,
            'source_version', v_source_version
        );

        SELECT inbox.*
        INTO v_existing
        FROM public.event_inbox AS inbox
        WHERE inbox.dedupe_key = v_dedupe_key::pg_catalog.bpchar
        FOR UPDATE;
        IF FOUND THEN
            IF v_existing.account_id <> p_account_id
               OR v_existing.external_email_id <> v_external_email_id
               OR v_existing.folder_key <> p_folder_key
               OR v_existing.source <> 'sync'
               OR v_existing.raw_event_type <> v_change_kind
               OR v_existing.change_kind <> v_change_kind
               OR v_existing.source_version IS DISTINCT FROM v_source_version
               OR v_existing.source_event_at IS NOT NULL
               OR v_existing.payload IS DISTINCT FROM v_payload
               OR v_existing.processing_policy <> v_processing_policy
               OR v_existing.pipeline_name <> v_authority.pipeline_name
               OR v_existing.generation <> v_authority.generation
               OR v_existing.fencing_token <> v_authority.fencing_token
               OR v_existing.authority_epoch <> v_authority.authority_epoch
               OR v_existing.capability_hash <> v_authority.capability_hash
            THEN
                RAISE EXCEPTION 'greenfield_sync_dedupe_identity_conflict'
                    USING ERRCODE = 'P0001';
            END IF;
            v_duplicate_count := v_duplicate_count + 1;
            CONTINUE;
        END IF;

        v_inbox_id := pg_catalog.gen_random_uuid();
        v_status := CASE
            WHEN v_processing_policy = 'ignored' THEN 'completed'
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
            v_inbox_id, p_account_id, v_external_email_id, p_folder_key,
            'sync', v_change_kind, v_change_kind, v_dedupe_key,
            v_source_version, NULL, v_payload, v_processing_policy,
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
                        'ai-exchange-greenfield-sync-audit-v1', 'UTF8'
                    ) || v_zero ||
                    pg_catalog.convert_to(v_inbox_id::pg_catalog.text, 'UTF8')
                ),
                'hex'
            ),
            p_account_id, NULL, 'event_inbox', v_dedupe_key,
            'ingress.sync_accepted', v_status, 'greenfield_sync',
            'sync_state_page_persisted',
            pg_catalog.jsonb_build_object(
                'authority_epoch', v_authority.authority_epoch,
                'capability_hash', v_authority.capability_hash::pg_catalog.text,
                'inbox_id', v_inbox_id::pg_catalog.text,
                'processing_policy', v_processing_policy,
                'status', v_status
            ),
            v_now
        );
        v_inserted_count := v_inserted_count + 1;
    END LOOP;

    UPDATE public.sync_cursors AS sync_cursor
    SET cursor = p_next_cursor,
        status = 'active',
        blocked_reason_code = NULL,
        contract_fingerprint = NULL,
        blocked_at = NULL,
        transient_failures = 0,
        retry_after_at = NULL,
        cold_start_plan_id = NULL,
        cold_start_plan_state = NULL,
        version = sync_cursor.version + 1,
        last_success_at = v_now,
        last_attempt_at = v_now,
        updated_at = v_now
    WHERE sync_cursor.account_id = p_account_id
      AND sync_cursor.folder_key = p_folder_key
      AND sync_cursor.status = CASE
            WHEN p_activation THEN 'cold_start_pending' ELSE 'active'
          END
      AND sync_cursor.cursor IS NOT DISTINCT FROM p_expected_cursor
      AND sync_cursor.version = p_expected_cursor_version
    RETURNING sync_cursor.cursor, sync_cursor.version
    INTO committed_cursor, committed_version;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'greenfield_sync_cursor_conflict'
            USING ERRCODE = 'P0001';
    END IF;

    inserted_count := v_inserted_count;
    duplicate_count := v_duplicate_count;
    RETURN NEXT;
END
$greenfield_commit_sync_page$
"""


def upgrade() -> None:
    op.execute(_GREENFIELD_SYNC_PAGE_SQL)
    op.execute(
        """
        REVOKE ALL ON FUNCTION public.greenfield_commit_sync_page(
            pg_catalog.int8,
            pg_catalog.uuid,
            pg_catalog.int8,
            pg_catalog.text,
            pg_catalog.text,
            pg_catalog.int8,
            pg_catalog.text,
            pg_catalog.jsonb,
            pg_catalog.bool
        ) FROM PUBLIC
        """
    )


def downgrade() -> None:
    raise RuntimeError("Forward-only production migration")
