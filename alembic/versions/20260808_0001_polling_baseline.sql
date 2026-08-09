SET LOCAL check_function_bodies = false;
CREATE FUNCTION public.greenfield_apply_email_event(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_inbox_id uuid, p_execution_epoch bigint, p_expected_email_version bigint) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
            BEGIN
                RAISE EXCEPTION 'polling_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $$;
CREATE FUNCTION public.greenfield_begin_inbox_effect(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
            BEGIN
                RAISE EXCEPTION 'polling_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $$;
CREATE FUNCTION public.greenfield_claim_inbox(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_lease_owner text, p_limit bigint, p_lease_seconds bigint) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
            BEGIN
                RAISE EXCEPTION 'polling_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $$;
CREATE FUNCTION public.greenfield_commit_sync_page(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_folder_key text, p_expected_cursor text, p_expected_cursor_version bigint, p_next_cursor text, p_events jsonb, p_activation boolean) RETURNS TABLE(committed_cursor text, committed_version bigint, inserted_count bigint, duplicate_count bigint)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
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
    v_authority public.pipeline_runtime_authority%%ROWTYPE;
    v_instance public.pipeline_runtime_instances%%ROWTYPE;
    v_scope public.pipeline_folder_scopes%%ROWTYPE;
    v_cursor public.sync_cursors%%ROWTYPE;
    v_existing public.event_inbox%%ROWTYPE;
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
       OR v_authority.schema_revision <> 'polling-v1'
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
            'baselining', 'sync.baseline_required'
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
        IF v_cursor.status <> 'baselining'
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
        version = sync_cursor.version + 1,
        last_success_at = v_now,
        last_attempt_at = v_now,
        updated_at = v_now
    WHERE sync_cursor.account_id = p_account_id
      AND sync_cursor.folder_key = p_folder_key
      AND sync_cursor.status = CASE
            WHEN p_activation THEN 'baselining' ELSE 'active'
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
$_$;
CREATE FUNCTION public.greenfield_drain_web_instance(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_expected_authority_epoch bigint, p_expected_capability_hash text) RETURNS TABLE(account_id bigint, workload text, instance_id text, session_id uuid, generation bigint, fencing_token bigint, authority_epoch bigint, capability_hash text, schema_revision text, protocol_version bigint, build_id text, config_hash text, lifecycle text, lease_version bigint, accepted_count bigint, rejected_count bigint, heartbeat_at timestamp with time zone, lease_until timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE
    v_instance public.pipeline_runtime_instances%%ROWTYPE;
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
$_$;
CREATE FUNCTION public.greenfield_fail_inbox(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint, p_safe_error_code text, p_safe_error_summary text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
            BEGIN
                RAISE EXCEPTION 'polling_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $$;
CREATE FUNCTION public.greenfield_finish_inbox(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_inbox_id uuid, p_execution_epoch bigint, p_attempts bigint, p_completion jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
            BEGIN
                RAISE EXCEPTION 'polling_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $$;
CREATE FUNCTION public.greenfield_get_runtime_authority(p_account_id bigint) RETURNS TABLE(account_id bigint, state text, generation bigint, fencing_token bigint, pipeline_name text, authority_epoch bigint, version bigint, schema_revision text, protocol_version bigint, build_id text, config_hash text, capability_hash text, policy_manifest_hash text, initialization_id uuid, updated_at timestamp with time zone)
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
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
        $$;
CREATE FUNCTION public.greenfield_heartbeat_web_instance(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_expected_authority_epoch bigint, p_expected_capability_hash text, p_accepted_count bigint, p_rejected_count bigint, p_lease_seconds bigint) RETURNS TABLE(account_id bigint, workload text, instance_id text, session_id uuid, generation bigint, fencing_token bigint, authority_epoch bigint, capability_hash text, schema_revision text, protocol_version bigint, build_id text, config_hash text, lifecycle text, lease_version bigint, accepted_count bigint, rejected_count bigint, heartbeat_at timestamp with time zone, lease_until timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE
    v_authority public.pipeline_runtime_authority%%ROWTYPE;
    v_instance public.pipeline_runtime_instances%%ROWTYPE;
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
       OR v_authority.schema_revision <> 'polling-v1'
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
$_$;
CREATE FUNCTION public.greenfield_initialize_runtime(p_account_id bigint, p_capability_hash text, p_predecessor_hash text, p_capability_stage text, p_schema_revision text, p_schema_digest text, p_protocol_version bigint, p_minimum_build_id text, p_config_hash text, p_adapter_hash text, p_policy_manifest_hash text, p_evidence_manifest_hash text, p_policy_manifest_json text, p_policy_scope_count bigint, p_actor text, p_reason text, p_idempotency_key text, p_canonical_payload_hash text) RETURNS TABLE(initialization_id uuid, command_receipt_id uuid, account_id bigint, generation bigint, fencing_token bigint, pipeline_name text, authority_epoch bigint, authority_version bigint, capability_hash text, policy_manifest_hash text, transaction_id text, replayed boolean, created_at timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE
    v_zero pg_catalog.bytea := pg_catalog.decode('00', 'hex');
    v_policy pg_catalog.jsonb;
    v_scope pg_catalog.jsonb;
    v_event_policy pg_catalog.jsonb;
    v_scope_rows pg_catalog.int8 := 0;
    v_event_rows pg_catalog.int8;
    v_expected_policy_hash pg_catalog.text;
    v_expected_capability_hash pg_catalog.text;
    v_expected_payload_hash pg_catalog.text;
    v_capability_canonical pg_catalog.text;
    v_payload_canonical pg_catalog.text;
    v_idempotency_hash pg_catalog.text;
    v_receipt_id pg_catalog.uuid;
    v_initialization_id pg_catalog.uuid;
    v_transaction_id pg_catalog.text;
    v_created_at pg_catalog.timestamptz;
    v_existing_receipt public.pipeline_command_receipts%%ROWTYPE;
    v_existing_initialization public.pipeline_initializations%%ROWTYPE;
    v_existing_capability public.pipeline_runtime_capabilities%%ROWTYPE;
BEGIN
    IF p_account_id IS NULL OR p_account_id <= 0
       OR p_capability_stage <> 'polling_ingestion'
       OR p_schema_revision <> 'polling-v1'
       OR p_protocol_version IS NULL OR p_protocol_version <= 0
       OR p_policy_scope_count IS NULL OR p_policy_scope_count NOT BETWEEN 1 AND 64
       OR p_actor IS NULL OR pg_catalog.btrim(p_actor) <> p_actor
       OR pg_catalog.char_length(p_actor) NOT BETWEEN 1 AND 128
       OR p_reason IS NULL OR pg_catalog.btrim(p_reason) <> p_reason
       OR pg_catalog.char_length(p_reason) NOT BETWEEN 1 AND 512
       OR p_idempotency_key IS NULL OR pg_catalog.btrim(p_idempotency_key) = ''
       OR pg_catalog.char_length(p_idempotency_key) > 4096
       OR p_predecessor_hash <> '95771c6d473119376654d5530f7fe189c77d83e56fe08e91179f48b1040df86f'
       OR p_capability_hash !~ '^[0-9a-f]{64}$'
       OR p_predecessor_hash !~ '^[0-9a-f]{64}$'
       OR p_schema_digest !~ '^[0-9a-f]{64}$'
       OR p_config_hash !~ '^[0-9a-f]{64}$'
       OR p_adapter_hash !~ '^[0-9a-f]{64}$'
       OR p_policy_manifest_hash !~ '^[0-9a-f]{64}$'
       OR p_evidence_manifest_hash !~ '^[0-9a-f]{64}$'
       OR p_canonical_payload_hash !~ '^[0-9a-f]{64}$'
       OR p_minimum_build_id !~ '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'
    THEN
        RAISE EXCEPTION 'greenfield_initialization_input_invalid'
            USING ERRCODE = 'P0001';
    END IF;
    BEGIN
        v_policy := p_policy_manifest_json::pg_catalog.jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
            USING ERRCODE = 'P0001';
    END;
    IF pg_catalog.jsonb_typeof(v_policy) <> 'object'
       OR (v_policy - 'schema_version' - 'scopes') <> '{}'::pg_catalog.jsonb
       OR v_policy -> 'schema_version' <> '1'::pg_catalog.jsonb
       OR pg_catalog.jsonb_typeof(v_policy -> 'scopes') <> 'array'
       OR pg_catalog.jsonb_array_length(v_policy -> 'scopes') <> p_policy_scope_count
    THEN
        RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
            USING ERRCODE = 'P0001';
    END IF;
    v_expected_policy_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to('ai-exchange-folder-policy-manifest-v1', 'UTF8')
            || v_zero || pg_catalog.convert_to(p_policy_manifest_json, 'UTF8')
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
        '","schema_version":' || '1' || ',"stage":"' || p_capability_stage || '"}';
    v_expected_capability_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to('ai-exchange-runtime-capability-manifest-v1', 'UTF8')
            || v_zero || pg_catalog.convert_to(v_capability_canonical, 'UTF8')
        ),
        'hex'
    );
    IF v_expected_capability_hash <> p_capability_hash THEN
        RAISE EXCEPTION 'greenfield_initialization_capability_invalid'
            USING ERRCODE = 'P0001';
    END IF;
    v_payload_canonical :=
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
            pg_catalog.convert_to('ai-exchange-greenfield-initialize-v1', 'UTF8')
            || v_zero || pg_catalog.convert_to(v_payload_canonical, 'UTF8')
        ),
        'hex'
    );
    IF v_expected_payload_hash <> p_canonical_payload_hash THEN
        RAISE EXCEPTION 'greenfield_initialization_payload_invalid'
            USING ERRCODE = 'P0001';
    END IF;
    v_idempotency_hash := pg_catalog.encode(
        pg_catalog.sha256(
            pg_catalog.convert_to('pipeline-command-idempotency-v1', 'UTF8')
            || v_zero || pg_catalog.convert_to(p_account_id::pg_catalog.text, 'UTF8')
            || v_zero || pg_catalog.convert_to('runtime.initialize', 'UTF8')
            || v_zero || pg_catalog.convert_to(p_idempotency_key, 'UTF8')
        ),
        'hex'
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(0, 0);
    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);
    SELECT receipt.* INTO v_existing_receipt
    FROM public.pipeline_command_receipts AS receipt
    WHERE receipt.account_id = p_account_id
      AND receipt.command_name = 'runtime.initialize'
      AND receipt.idempotency_key_hash = v_idempotency_hash;
    IF FOUND THEN
        IF v_existing_receipt.canonical_payload_hash::pg_catalog.text
                <> p_canonical_payload_hash THEN
            RAISE EXCEPTION 'command_idempotency_conflict' USING ERRCODE = 'P0001';
        END IF;
        SELECT initialized.* INTO STRICT v_existing_initialization
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
    IF EXISTS (SELECT 1 FROM public.emails_log LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.app_kv_store LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.pipeline_ownership LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.event_inbox LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.sync_cursors LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.emails LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.audit_events LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.pipeline_command_receipts LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.pipeline_initializations LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.pipeline_folder_scopes LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.pipeline_runtime_authority LIMIT 1)
       OR EXISTS (SELECT 1 FROM public.pipeline_runtime_instances LIMIT 1)
    THEN
        RAISE EXCEPTION 'greenfield_reinitialize_required' USING ERRCODE = 'P0001';
    END IF;
    SELECT capability.* INTO v_existing_capability
    FROM public.pipeline_runtime_capabilities AS capability
    WHERE capability.capability_hash = p_capability_hash;
    IF FOUND THEN
        IF v_existing_capability.predecessor_hash::pg_catalog.text <> p_predecessor_hash
           OR v_existing_capability.stage <> p_capability_stage
           OR v_existing_capability.schema_revision <> p_schema_revision
           OR v_existing_capability.schema_digest::pg_catalog.text <> p_schema_digest
           OR v_existing_capability.protocol_version <> p_protocol_version
           OR v_existing_capability.minimum_build_id <> p_minimum_build_id
           OR v_existing_capability.config_hash::pg_catalog.text <> p_config_hash
           OR v_existing_capability.adapter_hash::pg_catalog.text <> p_adapter_hash
           OR v_existing_capability.policy_manifest_hash::pg_catalog.text
                <> p_policy_manifest_hash
           OR v_existing_capability.evidence_manifest_hash::pg_catalog.text
                <> p_evidence_manifest_hash
        THEN
            RAISE EXCEPTION 'greenfield_capability_conflict' USING ERRCODE = 'P0001';
        END IF;
    ELSE
        INSERT INTO public.pipeline_runtime_capabilities (
            capability_hash, predecessor_hash, stage, schema_revision,
            schema_digest, protocol_version, minimum_build_id, config_hash,
            adapter_hash, policy_manifest_hash, evidence_manifest_hash
        ) VALUES (
            p_capability_hash, p_predecessor_hash, p_capability_stage,
            p_schema_revision, p_schema_digest, p_protocol_version,
            p_minimum_build_id, p_config_hash, p_adapter_hash,
            p_policy_manifest_hash, p_evidence_manifest_hash
        );
    END IF;
    v_initialization_id := pg_catalog.gen_random_uuid();
    v_receipt_id := pg_catalog.gen_random_uuid();
    v_transaction_id := pg_catalog.pg_current_xact_id()::pg_catalog.text;
    v_created_at := pg_catalog.clock_timestamp();
    INSERT INTO public.pipeline_ownership (
        account_id, generation, pipeline_name, state, fencing_token,
        created_by, reason, created_at, updated_at
    ) VALUES (
        p_account_id, 1, 'durable_v1', 'current_ingress', 1,
        p_actor, p_reason, v_created_at, v_created_at
    );
    INSERT INTO public.pipeline_command_receipts (
        id, account_id, command_name, idempotency_key_hash,
        canonical_payload_hash, outcome, result_type, result_id, result_hash,
        authority_epoch, created_at
    ) VALUES (
        v_receipt_id, p_account_id, 'runtime.initialize', v_idempotency_hash,
        p_canonical_payload_hash, 'succeeded', 'runtime_initialization',
        v_initialization_id::pg_catalog.text,
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to('ai-exchange-runtime-initialization-result-v1', 'UTF8')
                || v_zero || pg_catalog.convert_to(v_initialization_id::pg_catalog.text, 'UTF8')
                || v_zero || pg_catalog.convert_to(p_capability_hash, 'UTF8')
                || v_zero || pg_catalog.convert_to(p_policy_manifest_hash, 'UTF8')
            ),
            'hex'
        ),
        1, v_created_at
    );
    INSERT INTO public.pipeline_initializations (
        initialization_id, command_receipt_id, account_id, generation,
        fencing_token, pipeline_name, authority_epoch, authority_version,
        capability_hash, policy_manifest_hash, transaction_id, actor, reason,
        created_at
    ) VALUES (
        v_initialization_id, v_receipt_id, p_account_id, 1, 1, 'durable_v1',
        1, 1, p_capability_hash, p_policy_manifest_hash, v_transaction_id,
        p_actor, p_reason, v_created_at
    );
    FOR v_scope IN
        SELECT scope.value
        FROM pg_catalog.jsonb_array_elements(v_policy -> 'scopes') AS scope(value)
        ORDER BY scope.value ->> 'canonical_key'
    LOOP
        IF pg_catalog.jsonb_typeof(v_scope) <> 'object'
           OR (v_scope - 'canonical_key' - 'event_policy_matrix' - 'scope_hash' - 'sync_folder')
                <> '{}'::pg_catalog.jsonb
           OR pg_catalog.jsonb_typeof(v_scope -> 'canonical_key') <> 'string'
           OR pg_catalog.jsonb_typeof(v_scope -> 'sync_folder') <> 'string'
           OR pg_catalog.jsonb_typeof(v_scope -> 'scope_hash') <> 'string'
           OR pg_catalog.jsonb_typeof(v_scope -> 'event_policy_matrix') <> 'array'
           OR pg_catalog.btrim(v_scope ->> 'canonical_key') <> v_scope ->> 'canonical_key'
           OR pg_catalog.char_length(v_scope ->> 'canonical_key') NOT BETWEEN 1 AND 512
           OR pg_catalog.btrim(v_scope ->> 'sync_folder') <> v_scope ->> 'sync_folder'
           OR pg_catalog.char_length(v_scope ->> 'sync_folder') NOT BETWEEN 1 AND 512
           OR v_scope ->> 'scope_hash' !~ '^[0-9a-f]{64}$'
           OR EXISTS (
                SELECT 1
                FROM pg_catalog.jsonb_array_elements(v_scope -> 'event_policy_matrix')
                    AS entry(value)
                WHERE pg_catalog.jsonb_typeof(entry.value) <> 'object'
                   OR (entry.value - 'source' - 'raw_event_type' - 'change_kind' - 'processing_policy')
                        <> '{}'::pg_catalog.jsonb
                   OR pg_catalog.jsonb_typeof(entry.value -> 'source') <> 'string'
                   OR pg_catalog.jsonb_typeof(entry.value -> 'raw_event_type') <> 'string'
                   OR pg_catalog.jsonb_typeof(entry.value -> 'change_kind') <> 'string'
                   OR pg_catalog.jsonb_typeof(entry.value -> 'processing_policy') <> 'string'
            )
        THEN
            RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                USING ERRCODE = 'P0001';
        END IF;
        SELECT pg_catalog.count(*) INTO v_event_rows
        FROM pg_catalog.jsonb_array_elements(v_scope -> 'event_policy_matrix');
        SELECT pg_catalog.jsonb_object_agg(
            (entry.value ->> 'source') || ':' ||
            (entry.value ->> 'raw_event_type') || ':' ||
            (entry.value ->> 'change_kind'),
            entry.value ->> 'processing_policy'
        ) INTO v_event_policy
        FROM pg_catalog.jsonb_array_elements(v_scope -> 'event_policy_matrix')
            AS entry(value);
        IF v_event_rows <> 3
           OR v_event_policy IS NULL
           OR (v_event_policy - 'sync:create:create' - 'sync:update:update' - 'sync:delete:delete')
                <> '{}'::pg_catalog.jsonb
           OR v_event_policy ->> 'sync:create:create' NOT IN ('full', 'archive', 'ignored')
           OR v_event_policy ->> 'sync:update:update' <> 'metadata_only'
           OR v_event_policy ->> 'sync:delete:delete' <> 'metadata_only'
        THEN
            RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
                USING ERRCODE = 'P0001';
        END IF;
        INSERT INTO public.pipeline_folder_scopes (
            initialization_id, account_id, canonical_key, sync_folder,
            event_policy_matrix, scope_hash, policy_manifest_hash, created_at
        ) VALUES (
            v_initialization_id, p_account_id, v_scope ->> 'canonical_key',
            v_scope ->> 'sync_folder', v_event_policy, v_scope ->> 'scope_hash',
            p_policy_manifest_hash, v_created_at
        );
        v_scope_rows := v_scope_rows + 1;
    END LOOP;
    IF v_scope_rows <> p_policy_scope_count THEN
        RAISE EXCEPTION 'greenfield_initialization_policy_invalid'
            USING ERRCODE = 'P0001';
    END IF;
    INSERT INTO public.pipeline_runtime_authority (
        account_id, state, generation, fencing_token, pipeline_name,
        authority_epoch, version, schema_revision, protocol_version, build_id,
        config_hash, capability_hash, policy_manifest_hash, initialization_id,
        created_at, updated_at
    ) VALUES (
        p_account_id, 'ingest_only', 1, 1, 'durable_v1', 1, 1,
        p_schema_revision, p_protocol_version, p_minimum_build_id,
        p_config_hash, p_capability_hash, p_policy_manifest_hash,
        v_initialization_id, v_created_at, v_created_at
    );
    INSERT INTO public.audit_events (
        id, event_key, account_id, email_id, object_type, object_fingerprint,
        action, result, actor, reason, safe_metadata, created_at
    ) VALUES (
        pg_catalog.gen_random_uuid(),
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to('ai-exchange-runtime-initialize-audit-v1', 'UTF8')
                || v_zero || pg_catalog.convert_to(v_receipt_id::pg_catalog.text, 'UTF8')
            ),
            'hex'
        ),
        p_account_id, NULL, 'runtime_authority',
        pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to('ai-exchange-runtime-authority-v1', 'UTF8')
                || v_zero || pg_catalog.convert_to(p_account_id::pg_catalog.text, 'UTF8')
            ),
            'hex'
        ),
        'runtime.initialize', 'succeeded', p_actor, p_reason,
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
        v_initialization_id, v_receipt_id, p_account_id, 1::pg_catalog.int8,
        1::pg_catalog.int8, 'durable_v1'::pg_catalog.text, 1::pg_catalog.int8,
        1::pg_catalog.int8, p_capability_hash, p_policy_manifest_hash,
        v_transaction_id, false, v_created_at;
END
$_$;
CREATE FUNCTION public.greenfield_pause_runtime(p_account_id bigint, p_expected_authority_epoch bigint, p_expected_version bigint, p_expected_capability_hash text, p_actor text, p_reason text, p_idempotency_key text, p_canonical_payload_hash text) RETURNS TABLE(command_receipt_id uuid, command_name text, previous_state text, previous_authority_epoch bigint, previous_version bigint, transaction_id text, replayed boolean, receipt_created_at timestamp with time zone, account_id bigint, state text, generation bigint, fencing_token bigint, pipeline_name text, authority_epoch bigint, version bigint, schema_revision text, protocol_version bigint, build_id text, config_hash text, capability_hash text, policy_manifest_hash text, initialization_id uuid, updated_at timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
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
    v_authority public.pipeline_runtime_authority%%ROWTYPE;
    v_existing_receipt public.pipeline_command_receipts%%ROWTYPE;
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
            v_zero || pg_catalog.convert_to('runtime.pause', 'UTF8') ||
            v_zero || pg_catalog.convert_to(p_idempotency_key, 'UTF8')
        ),
        'hex'
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);
    SELECT receipt.*
    INTO v_existing_receipt
    FROM public.pipeline_command_receipts AS receipt
    WHERE receipt.account_id = p_account_id
      AND receipt.command_name = 'runtime.pause'
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
          AND audit.action = 'runtime.pause'
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
            'runtime.pause'::pg_catalog.text,
            'ingest_only'::pg_catalog.text,
            v_existing_receipt.authority_epoch - 1,
            v_existing_receipt.result_id::pg_catalog.int8 - 1,
            v_replay_metadata ->> 'transaction_id',
            true,
            v_existing_receipt.created_at,
            v_existing_receipt.account_id,
            'paused'::pg_catalog.text,
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
       OR v_authority.state <> 'ingest_only'
       OR v_authority.authority_epoch <> p_expected_authority_epoch
       OR v_authority.version <> p_expected_version
       OR v_authority.capability_hash::pg_catalog.text <>
            p_expected_capability_hash
       OR v_authority.capability_stage_ordinal <> 1
       OR v_authority.schema_revision <> 'polling-v1'
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
        '","command_name":"runtime.pause"' ||
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
        '","previous_state":"ingest_only"' ||
        ',"protocol_version":' ||
        v_authority.protocol_version::pg_catalog.text ||
        ',"reason":' || pg_catalog.to_json(p_reason)::pg_catalog.text ||
        ',"schema_revision":"' || v_authority.schema_revision ||
        '","schema_version":' || '1' ||
        ',"target_state":"paused"}';
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
    SET state = 'paused',
        authority_epoch = authority.authority_epoch + 1,
        version = authority.version + 1,
        updated_at = v_created_at
    WHERE authority.account_id = p_account_id
      AND authority.state = 'ingest_only'
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
        v_receipt_id, p_account_id, 'runtime.pause', v_idempotency_hash,
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
        'runtime.pause',
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
        'runtime.pause'::pg_catalog.text,
        'ingest_only'::pg_catalog.text,
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
$_$;
CREATE FUNCTION public.greenfield_reap_inbox(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_limit bigint) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
            BEGIN
                RAISE EXCEPTION 'polling_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $$;
CREATE FUNCTION public.greenfield_register_web_instance(p_account_id bigint, p_instance_id text, p_session_id uuid, p_expected_authority_epoch bigint, p_expected_authority_version bigint, p_schema_revision text, p_protocol_version bigint, p_build_id text, p_config_hash text, p_capability_hash text, p_lease_seconds bigint) RETURNS TABLE(account_id bigint, workload text, instance_id text, session_id uuid, generation bigint, fencing_token bigint, authority_epoch bigint, capability_hash text, schema_revision text, protocol_version bigint, build_id text, config_hash text, lifecycle text, lease_version bigint, accepted_count bigint, rejected_count bigint, heartbeat_at timestamp with time zone, lease_until timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
DECLARE
    v_authority public.pipeline_runtime_authority%%ROWTYPE;
    v_existing public.pipeline_runtime_instances%%ROWTYPE;
    v_instance public.pipeline_runtime_instances%%ROWTYPE;
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
       OR p_schema_revision IS NULL OR p_schema_revision <> 'polling-v1'
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
$_$;
CREATE FUNCTION public.greenfield_renew_inbox(p_account_id bigint, p_session_id uuid, p_expected_lease_version bigint, p_inbox_id uuid, p_execution_epoch bigint, p_lease_owner text, p_attempts bigint, p_lease_seconds bigint) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $$
            BEGIN
                RAISE EXCEPTION 'polling_worker_authority_unavailable'
                    USING ERRCODE = 'P0001';
            END
            $$;
CREATE FUNCTION public.greenfield_requeue_inbox(p_account_id bigint, p_inbox_id uuid, p_expected_execution_epoch bigint, p_expected_email_version bigint, p_actor text, p_reason text, p_idempotency_key text, p_canonical_payload_hash text) RETURNS TABLE(command_receipt_id uuid, inbox_id uuid, email_id uuid, previous_execution_epoch bigint, execution_epoch bigint, email_version bigint, status text, transaction_id text, replayed boolean, created_at timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
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
    v_authority public.pipeline_runtime_authority%%ROWTYPE;
    v_inbox public.event_inbox%%ROWTYPE;
    v_email public.emails%%ROWTYPE;
    v_existing_receipt public.pipeline_command_receipts%%ROWTYPE;
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
       OR v_authority.schema_revision <> 'polling-v1'
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
$_$;
CREATE FUNCTION public.greenfield_resume_ingress(p_account_id bigint, p_expected_authority_epoch bigint, p_expected_version bigint, p_expected_capability_hash text, p_actor text, p_reason text, p_idempotency_key text, p_canonical_payload_hash text) RETURNS TABLE(command_receipt_id uuid, command_name text, previous_state text, previous_authority_epoch bigint, previous_version bigint, transaction_id text, replayed boolean, receipt_created_at timestamp with time zone, account_id bigint, state text, generation bigint, fencing_token bigint, pipeline_name text, authority_epoch bigint, version bigint, schema_revision text, protocol_version bigint, build_id text, config_hash text, capability_hash text, policy_manifest_hash text, initialization_id uuid, updated_at timestamp with time zone)
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog'
    AS $_$
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
    v_authority public.pipeline_runtime_authority%%ROWTYPE;
    v_existing_receipt public.pipeline_command_receipts%%ROWTYPE;
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
            v_zero || pg_catalog.convert_to('runtime.resume_ingress', 'UTF8') ||
            v_zero || pg_catalog.convert_to(p_idempotency_key, 'UTF8')
        ),
        'hex'
    );
    PERFORM pg_catalog.pg_advisory_xact_lock(p_account_id);
    SELECT receipt.*
    INTO v_existing_receipt
    FROM public.pipeline_command_receipts AS receipt
    WHERE receipt.account_id = p_account_id
      AND receipt.command_name = 'runtime.resume_ingress'
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
          AND audit.action = 'runtime.resume_ingress'
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
            'runtime.resume_ingress'::pg_catalog.text,
            'paused'::pg_catalog.text,
            v_existing_receipt.authority_epoch - 1,
            v_existing_receipt.result_id::pg_catalog.int8 - 1,
            v_replay_metadata ->> 'transaction_id',
            true,
            v_existing_receipt.created_at,
            v_existing_receipt.account_id,
            'ingest_only'::pg_catalog.text,
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
       OR v_authority.state <> 'paused'
       OR v_authority.authority_epoch <> p_expected_authority_epoch
       OR v_authority.version <> p_expected_version
       OR v_authority.capability_hash::pg_catalog.text <>
            p_expected_capability_hash
       OR v_authority.capability_stage_ordinal <> 1
       OR v_authority.schema_revision <> 'polling-v1'
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
        '","command_name":"runtime.resume_ingress"' ||
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
        '","previous_state":"paused"' ||
        ',"protocol_version":' ||
        v_authority.protocol_version::pg_catalog.text ||
        ',"reason":' || pg_catalog.to_json(p_reason)::pg_catalog.text ||
        ',"schema_revision":"' || v_authority.schema_revision ||
        '","schema_version":' || '1' ||
        ',"target_state":"ingest_only"}';
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
    SET state = 'ingest_only',
        authority_epoch = authority.authority_epoch + 1,
        version = authority.version + 1,
        updated_at = v_created_at
    WHERE authority.account_id = p_account_id
      AND authority.state = 'paused'
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
        v_receipt_id, p_account_id, 'runtime.resume_ingress', v_idempotency_hash,
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
        'runtime.resume_ingress',
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
        'runtime.resume_ingress'::pg_catalog.text,
        'paused'::pg_catalog.text,
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
$_$;
CREATE FUNCTION public.guard_emails_runtime_identity() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $_$
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
                    'SELECT 1 FROM %%I.event_inbox AS inbox '
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
        $_$;
CREATE FUNCTION public.guard_event_inbox_runtime_identity() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
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
        $$;
CREATE FUNCTION public.guard_pipeline_ownership() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public'
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
        $$;
CREATE FUNCTION public.guard_pipeline_runtime_authority() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $_$
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
                    'SELECT 1 FROM %%I.pipeline_runtime_capabilities '
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
        $_$;
CREATE FUNCTION public.guard_pipeline_runtime_instances() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
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
        $$;
CREATE FUNCTION public.reject_audit_events_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public'
    AS $$
        BEGIN
            RAISE EXCEPTION 'audit history is append only';
        END
        $$;
CREATE FUNCTION public.reject_pipeline_command_receipts_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public'
    AS $$
        BEGIN
            RAISE EXCEPTION 'pipeline command receipts are append-only';
        END
        $$;
CREATE FUNCTION public.reject_tier1_decisions_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public'
    AS $$
        BEGIN
            RAISE EXCEPTION 'canonical route decisions are append-only';
        END
        $$;
CREATE FUNCTION public.reject_durable_artifact_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public'
    AS $$
        BEGIN
            RAISE EXCEPTION 'durable artifact history is append-only';
        END
        $$;
CREATE FUNCTION public.reject_pipeline_folder_scopes_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'pipeline folder scopes are append-only';
            END IF;
            RETURN NEW;
        END
        $$;
CREATE FUNCTION public.reject_pipeline_initializations_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $_$
        DECLARE
            receipt_is_exact pg_catalog.bool;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                EXECUTE pg_catalog.format(
                    'SELECT EXISTS ('
                    'SELECT 1 FROM %%I.pipeline_command_receipts '
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
        $_$;
CREATE FUNCTION public.reject_pipeline_runtime_capabilities_mutation() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
        BEGIN
            RAISE EXCEPTION 'pipeline runtime capabilities are append-only';
        END
        $$;
SET default_tablespace = '';
SET default_table_access_method = heap;
CREATE TABLE public.app_kv_store (
    key text NOT NULL,
    value text,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE public.audit_events (
    id uuid NOT NULL,
    event_key character(64) NOT NULL,
    account_id bigint NOT NULL,
    email_id uuid,
    object_type text NOT NULL,
    object_fingerprint character(64) NOT NULL,
    action text NOT NULL,
    result text NOT NULL,
    actor text NOT NULL,
    reason text,
    safe_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_audit_events_account CHECK ((account_id > 0)),
    CONSTRAINT ck_audit_events_action CHECK (((btrim(action) <> ''::text) AND (char_length(action) <= 64))),
    CONSTRAINT ck_audit_events_actor CHECK (((btrim(actor) <> ''::text) AND (char_length(actor) <= 128))),
    CONSTRAINT ck_audit_events_event_key CHECK (((event_key)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT ck_audit_events_object_fingerprint CHECK (((object_fingerprint)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT ck_audit_events_object_type CHECK (((btrim(object_type) <> ''::text) AND (char_length(object_type) <= 64))),
    CONSTRAINT ck_audit_events_reason CHECK (((reason IS NULL) OR ((btrim(reason) <> ''::text) AND (char_length(reason) <= 512)))),
    CONSTRAINT ck_audit_events_result CHECK (((btrim(result) <> ''::text) AND (char_length(result) <= 64))),
    CONSTRAINT ck_audit_events_safe_metadata CHECK (((jsonb_typeof(safe_metadata) = 'object'::text) AND (octet_length((safe_metadata)::text) <= 16384)))
);
CREATE TABLE public.daily_digest_executions (
    account_id bigint NOT NULL,
    delivery_scope_hash character(64) NOT NULL,
    window_start timestamp with time zone NOT NULL,
    window_end timestamp with time zone NOT NULL,
    state text NOT NULL,
    is_backfill boolean DEFAULT false NOT NULL,
    delivery_parts jsonb NOT NULL,
    attempt_count bigint DEFAULT 0 NOT NULL,
    last_attempt_at timestamp with time zone,
    last_error_code text,
    confirmed_at timestamp with time zone,
    missed_at timestamp with time zone,
    missed_reported_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE TABLE public.emails (
    id uuid NOT NULL,
    account_id bigint NOT NULL,
    external_email_id text NOT NULL,
    source_folder_key text NOT NULL,
    status text NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    owner_generation bigint NOT NULL,
    owner_fencing_token bigint NOT NULL,
    processing_inbox_id uuid,
    create_seen_at timestamp with time zone,
    processing_started_at timestamp with time zone,
    source_deleted_at timestamp with time zone,
    external_effects_started_at timestamp with time zone,
    safe_error_code text,
    safe_error_summary text,
    content_ref jsonb,
    is_read boolean,
    is_read_refresh_required boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    owner_authority_epoch bigint NOT NULL,
    owner_capability_hash character(64) NOT NULL,
    processing_execution_epoch bigint,
    CONSTRAINT ck_emails_content_ref CHECK (((content_ref IS NULL) OR (jsonb_typeof(content_ref) = 'object'::text))),
    CONSTRAINT ck_emails_error CHECK (((safe_error_summary IS NULL) OR (safe_error_code IS NOT NULL))),
    CONSTRAINT ck_emails_external_email_id CHECK (((btrim(external_email_id) <> ''::text) AND (char_length(external_email_id) <= 1024))),
    CONSTRAINT ck_emails_positive_identity CHECK (((account_id > 0) AND (owner_generation > 0) AND (owner_fencing_token > 0))),
    CONSTRAINT ck_emails_processing_runtime_identity CHECK ((((processing_inbox_id IS NULL) AND (processing_execution_epoch IS NULL)) OR ((processing_inbox_id IS NOT NULL) AND (processing_execution_epoch IS NOT NULL) AND (processing_execution_epoch >= 0) AND (processing_execution_epoch < '9223372036854775807'::bigint)))),
    CONSTRAINT ck_emails_processing_state CHECK ((((status = 'processing'::text) AND (processing_inbox_id IS NOT NULL) AND (safe_error_code IS NULL) AND (safe_error_summary IS NULL)) OR ((status = ANY (ARRAY['retry_wait'::text, 'manual_review'::text, 'dead_letter'::text])) AND (processing_inbox_id IS NOT NULL) AND (safe_error_code IS NOT NULL) AND (btrim(safe_error_code) <> ''::text) AND (char_length(safe_error_code) <= 64) AND ((safe_error_summary IS NULL) OR ((btrim(safe_error_summary) <> ''::text) AND (char_length(safe_error_summary) <= 256)))) OR ((status <> ALL (ARRAY['processing'::text, 'retry_wait'::text, 'manual_review'::text, 'dead_letter'::text])) AND (processing_inbox_id IS NULL) AND (safe_error_code IS NULL) AND (safe_error_summary IS NULL)))),
    CONSTRAINT ck_emails_read_projection CHECK (((is_read IS NOT NULL) OR is_read_refresh_required)),
    CONSTRAINT ck_emails_runtime_ownership CHECK (((owner_authority_epoch > 0) AND (owner_authority_epoch < '9223372036854775807'::bigint) AND ((owner_capability_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_emails_source_folder_key CHECK (((btrim(source_folder_key) <> ''::text) AND (char_length(source_folder_key) <= 512))),
    CONSTRAINT ck_emails_status CHECK ((status = ANY (ARRAY['ingested'::text, 'processing'::text, 'retry_wait'::text, 'manual_review'::text, 'waiting_approval'::text, 'notified_readonly'::text, 'send_queued'::text, 'sending'::text, 'accepted'::text, 'sent'::text, 'send_failed'::text, 'delivery_failed'::text, 'send_unknown'::text, 'no_action'::text, 'archived'::text, 'rejected'::text, 'draft_saved'::text, 'expired'::text, 'cancelled'::text, 'dead_letter'::text]))),
    CONSTRAINT ck_emails_version CHECK ((version >= 0))
);
CREATE TABLE public.emails_log (
    id text NOT NULL,
    subject text,
    sender text,
    received_at timestamp without time zone,
    status text DEFAULT 'pending'::text,
    classification jsonb,
    draft_content text,
    processed_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    routing_log jsonb,
    original_draft text,
    final_draft text,
    draft_diff text,
    approver_user_id text,
    approval_at timestamp with time zone,
    rejection_reason text,
    error_message text,
    content_ref jsonb,
    version bigint DEFAULT 0 NOT NULL
);
CREATE TABLE public.event_inbox (
    id uuid NOT NULL,
    account_id bigint NOT NULL,
    external_email_id text NOT NULL,
    folder_key text NOT NULL,
    source text NOT NULL,
    raw_event_type text NOT NULL,
    change_kind text NOT NULL,
    dedupe_key character(64) NOT NULL,
    source_version text,
    source_event_at timestamp with time zone,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    processing_policy text NOT NULL,
    pipeline_name text NOT NULL,
    generation bigint NOT NULL,
    fencing_token bigint NOT NULL,
    status text NOT NULL,
    lease_owner text,
    lease_until timestamp with time zone,
    attempts bigint DEFAULT 0 NOT NULL,
    available_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    processing_started_at timestamp with time zone,
    effect_started_at timestamp with time zone,
    safe_error_code text,
    safe_error_summary text,
    received_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    execution_epoch bigint DEFAULT 0 NOT NULL,
    authority_epoch bigint NOT NULL,
    capability_hash character(64) NOT NULL,
    lease_session_id uuid,
    CONSTRAINT ck_event_inbox_attempts CHECK ((attempts >= 0)),
    CONSTRAINT ck_event_inbox_change_kind CHECK ((change_kind = ANY (ARRAY['create'::text, 'update'::text, 'delete'::text]))),
    CONSTRAINT ck_event_inbox_dedupe_key CHECK (((dedupe_key)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT ck_event_inbox_effect_order CHECK (((effect_started_at IS NULL) OR ((processing_started_at IS NOT NULL) AND (processing_started_at <= effect_started_at) AND (status = ANY (ARRAY['leased'::text, 'completed'::text, 'manual_review'::text]))))),
    CONSTRAINT ck_event_inbox_error CHECK (((safe_error_summary IS NULL) OR (safe_error_code IS NOT NULL))),
    CONSTRAINT ck_event_inbox_error_state CHECK ((((status = ANY (ARRAY['retry_wait'::text, 'dead_letter'::text, 'manual_review'::text])) AND (safe_error_code IS NOT NULL) AND (btrim(safe_error_code) <> ''::text) AND (char_length(safe_error_code) <= 64) AND ((safe_error_summary IS NULL) OR ((btrim(safe_error_summary) <> ''::text) AND (char_length(safe_error_summary) <= 256)))) OR ((status = ANY (ARRAY['pending'::text, 'leased'::text, 'completed'::text])) AND (safe_error_code IS NULL) AND (safe_error_summary IS NULL)))),
    CONSTRAINT ck_event_inbox_execution_epoch CHECK (((execution_epoch >= 0) AND (execution_epoch < '9223372036854775807'::bigint))),
    CONSTRAINT ck_event_inbox_external_email_id CHECK (((btrim(external_email_id) <> ''::text) AND (char_length(external_email_id) <= 1024))),
    CONSTRAINT ck_event_inbox_folder_key CHECK (((btrim(folder_key) <> ''::text) AND (char_length(folder_key) <= 512))),
    CONSTRAINT ck_event_inbox_lease CHECK ((((status = 'leased'::text) AND (lease_owner IS NOT NULL) AND (lease_until IS NOT NULL) AND (lease_session_id IS NOT NULL) AND (btrim(lease_owner) <> ''::text) AND (char_length(lease_owner) <= 128)) OR ((status <> 'leased'::text) AND (lease_owner IS NULL) AND (lease_until IS NULL) AND (lease_session_id IS NULL)))),
    CONSTRAINT ck_event_inbox_payload CHECK (((jsonb_typeof(payload) = 'object'::text) AND (octet_length((payload)::text) <= 262144))),
    CONSTRAINT ck_event_inbox_pipeline_name CHECK (((btrim(pipeline_name) <> ''::text) AND (char_length(pipeline_name) <= 64))),
    CONSTRAINT ck_event_inbox_positive_identity CHECK (((account_id > 0) AND (generation > 0) AND (fencing_token > 0))),
    CONSTRAINT ck_event_inbox_processing_policy CHECK ((processing_policy = ANY (ARRAY['full'::text, 'archive'::text, 'metadata_only'::text, 'ignored'::text]))),
    CONSTRAINT ck_event_inbox_raw_event_type CHECK (((btrim(raw_event_type) <> ''::text) AND (char_length(raw_event_type) <= 128))),
    CONSTRAINT ck_event_inbox_runtime_authority CHECK (((authority_epoch > 0) AND (authority_epoch < '9223372036854775807'::bigint) AND ((capability_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_event_inbox_source CHECK ((source = 'sync'::text)),
    CONSTRAINT ck_event_inbox_source_version CHECK (((source_version IS NULL) OR ((btrim(source_version) <> ''::text) AND (char_length(source_version) <= 512)))),
    CONSTRAINT ck_event_inbox_status CHECK ((status = ANY (ARRAY['pending'::text, 'retry_wait'::text, 'leased'::text, 'completed'::text, 'dead_letter'::text, 'manual_review'::text])))
);
CREATE TABLE public.tier1_decisions (
    inbox_id uuid NOT NULL,
    account_id bigint NOT NULL,
    external_email_id text NOT NULL,
    decision_digest character(64) NOT NULL,
    decision_json jsonb NOT NULL,
    outcome text NOT NULL,
    route text NOT NULL,
    tier text NOT NULL,
    artifact_digest character(64),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_tier1_decisions_account CHECK (account_id > 0),
    CONSTRAINT ck_tier1_decisions_artifact CHECK ((artifact_digest IS NULL) OR ((artifact_digest)::text ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT ck_tier1_decisions_digest CHECK ((decision_digest)::text ~ '^[0-9a-f]{64}$'::text),
    CONSTRAINT ck_tier1_decisions_external_email CHECK ((btrim(external_email_id) <> ''::text) AND (char_length(external_email_id) <= 1024)),
    CONSTRAINT ck_tier1_decisions_json CHECK ((jsonb_typeof(decision_json) = 'object'::text) AND (octet_length((decision_json)::text) <= 16384)),
    CONSTRAINT ck_tier1_decisions_outcome CHECK (outcome = ANY (ARRAY['matched'::text, 'conflict'::text, 'error'::text])),
    CONSTRAINT ck_tier1_decisions_route CHECK (route = ANY (ARRAY['reply'::text, 'forward'::text, 'read_only'::text, 'no_action'::text, 'manual_review'::text])),
    CONSTRAINT ck_tier1_decisions_tier CHECK (tier = ANY (ARRAY['tier1'::text, 'tier2'::text, 'tier3'::text, 'system'::text]))
);
CREATE TABLE public.intake_decisions (
    inbox_id uuid NOT NULL, execution_epoch bigint NOT NULL, external_email_id text NOT NULL,
    decision_json jsonb NOT NULL, decision_digest character(64) NOT NULL,
    disposition text NOT NULL, reason_code text NOT NULL, policy_version text NOT NULL,
    snapshot_digest character(64) NOT NULL, created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_intake_decisions PRIMARY KEY (inbox_id, execution_epoch),
    CONSTRAINT ck_intake_execution_epoch CHECK (execution_epoch >= 0),
    CONSTRAINT ck_intake_disposition CHECK (disposition IN ('pass','suppress','quarantine')),
    CONSTRAINT ck_intake_digests CHECK (decision_digest::text ~ '^[0-9a-f]{64}$' AND snapshot_digest::text ~ '^[0-9a-f]{64}$')
);
CREATE TABLE public.intake_releases (
    id uuid NOT NULL, inbox_id uuid NOT NULL, prior_execution_epoch bigint NOT NULL,
    new_execution_epoch bigint NOT NULL, actor text NOT NULL, reason text NOT NULL,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_intake_releases PRIMARY KEY (id),
    CONSTRAINT ck_intake_release_epoch CHECK (new_execution_epoch > prior_execution_epoch)
);
CREATE TABLE public.handoff_runs (
    inbox_id uuid NOT NULL, decision_digest character(64) NOT NULL,
    plan_json jsonb NOT NULL, plan_digest character(64) NOT NULL,
    evidence_json jsonb, evidence_digest character(64), state text NOT NULL DEFAULT 'planned',
    payload_revision bigint, version bigint NOT NULL DEFAULT 0,
    execution_claimed_at timestamptz, execution_claim_id uuid,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_handoff_runs PRIMARY KEY (inbox_id),
    CONSTRAINT ck_handoff_run_state CHECK (state IN ('planned','evidence_ready','manual_review','approval_pending','approved','rejected','draft_saving','draft_saved','executing','completed','failed')),
    CONSTRAINT ck_handoff_run_version CHECK (version >= 0),
    CONSTRAINT ck_handoff_run_digests CHECK (decision_digest::text ~ '^[0-9a-f]{64}$' AND plan_digest::text ~ '^[0-9a-f]{64}$' AND (evidence_digest IS NULL OR evidence_digest::text ~ '^[0-9a-f]{64}$'))
);
CREATE TABLE public.execution_payload_revisions (
    inbox_id uuid NOT NULL, revision bigint NOT NULL, decision_digest character(64) NOT NULL,
    payload_digest character(64) NOT NULL,
    plan_digest character(64) NOT NULL, evidence_digest character(64) NOT NULL,
    draft_digest character(64) NOT NULL, draft_content text, draft_ref jsonb,
    to_recipients jsonb NOT NULL, cc_recipients jsonb NOT NULL,
    attachment_refs jsonb NOT NULL, attachment_digests jsonb NOT NULL,
    external_recipient_acknowledged boolean NOT NULL DEFAULT false,
    editor text NOT NULL, edited_at timestamptz NOT NULL,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_execution_payload_revisions PRIMARY KEY (inbox_id, revision),
    CONSTRAINT ck_payload_revision CHECK (revision > 0),
    CONSTRAINT ck_payload_digest CHECK (payload_digest::text ~ '^[0-9a-f]{64}$')
);
CREATE TABLE public.approved_execution_envelopes (
    inbox_id uuid NOT NULL, payload_revision bigint NOT NULL,
    payload_digest character(64) NOT NULL,
    envelope_json jsonb NOT NULL, envelope_digest character(64) NOT NULL,
    decision_digest character(64) NOT NULL, plan_digest character(64) NOT NULL,
    evidence_digest character(64) NOT NULL, draft_digest character(64) NOT NULL,
    approver text NOT NULL, approved_at timestamptz NOT NULL,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_approved_execution_envelopes PRIMARY KEY (inbox_id, payload_revision),
    CONSTRAINT ck_approved_envelope_payload_digest CHECK (payload_digest::text ~ '^[0-9a-f]{64}$')
);
CREATE TABLE public.handoff_executions (
    inbox_id uuid NOT NULL,
    decision_digest character(64) NOT NULL,
    state text DEFAULT 'planned'::text NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    safe_error_code text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_handoff_executions_digest CHECK ((decision_digest)::text ~ '^[0-9a-f]{64}$'::text),
    CONSTRAINT ck_handoff_executions_error CHECK ((safe_error_code IS NULL) OR ((btrim(safe_error_code) <> ''::text) AND (char_length(safe_error_code) <= 128))),
    CONSTRAINT ck_handoff_executions_state CHECK (state = ANY (ARRAY['planned'::text, 'effect_committed'::text, 'completed'::text, 'failed'::text])),
    CONSTRAINT ck_handoff_executions_version CHECK (version >= 0)
);
CREATE TABLE public.pipeline_command_receipts (
    id uuid NOT NULL,
    account_id bigint NOT NULL,
    command_name text NOT NULL,
    idempotency_key_hash character(64) NOT NULL,
    canonical_payload_hash character(64) NOT NULL,
    outcome text NOT NULL,
    result_type text NOT NULL,
    result_id text NOT NULL,
    result_hash character(64) NOT NULL,
    authority_epoch bigint NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pipeline_command_receipts_account CHECK ((account_id > 0)),
    CONSTRAINT ck_pipeline_command_receipts_authority_epoch CHECK ((authority_epoch > 0)),
    CONSTRAINT ck_pipeline_command_receipts_command_name CHECK ((command_name = ANY (ARRAY['runtime.initialize'::text, 'runtime.pause'::text, 'runtime.resume_ingress'::text, 'inbox.requeue'::text]))),
    CONSTRAINT ck_pipeline_command_receipts_hashes CHECK ((((idempotency_key_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((canonical_payload_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((result_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_pipeline_command_receipts_outcome CHECK ((outcome = 'succeeded'::text)),
    CONSTRAINT ck_pipeline_command_receipts_result CHECK ((((command_name = 'runtime.initialize'::text) AND (result_type = 'runtime_initialization'::text) AND (result_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'::text)) OR ((command_name = ANY (ARRAY['runtime.pause'::text, 'runtime.resume_ingress'::text])) AND (result_type = 'runtime_authority'::text) AND (result_id ~ '^[1-9][0-9]{0,18}$'::text)) OR ((command_name = 'inbox.requeue'::text) AND (result_type = 'event_inbox'::text) AND (result_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'::text))))
);
CREATE TABLE public.pipeline_folder_scopes (
    initialization_id uuid NOT NULL,
    account_id bigint NOT NULL,
    canonical_key text NOT NULL,
    sync_folder text NOT NULL,
    event_policy_matrix jsonb NOT NULL,
    scope_hash character(64) NOT NULL,
    policy_manifest_hash character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pipeline_folder_scopes_event_policy_matrix CHECK (((jsonb_typeof(event_policy_matrix) = 'object'::text) AND ((((event_policy_matrix - 'sync:create:create'::text) - 'sync:update:update'::text) - 'sync:delete:delete'::text) = '{}'::jsonb) AND (event_policy_matrix ? 'sync:create:create'::text) AND (event_policy_matrix ? 'sync:update:update'::text) AND (event_policy_matrix ? 'sync:delete:delete'::text) AND ((event_policy_matrix ->> 'sync:create:create'::text) = ANY (ARRAY['full'::text, 'archive'::text, 'ignored'::text])) AND ((event_policy_matrix ->> 'sync:update:update'::text) = 'metadata_only'::text) AND ((event_policy_matrix ->> 'sync:delete:delete'::text) = 'metadata_only'::text))),
    CONSTRAINT ck_pipeline_folder_scopes_hashes CHECK ((((scope_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((policy_manifest_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_pipeline_folder_scopes_identity CHECK (((account_id > 0) AND (btrim(canonical_key) = canonical_key) AND (btrim(canonical_key) <> ''::text) AND (char_length(canonical_key) <= 512) AND ((canonical_key = ANY (ARRAY['ARCHIVE'::text, 'TRASH'::text, 'DRAFTS'::text, 'INBOX'::text, 'JUNK'::text, 'OUTBOX'::text, 'SENT'::text])) OR ((lower((canonical_key COLLATE "C")) <> ALL (ARRAY['archive'::text, 'deleted'::text, 'deleted items'::text, 'deleteditems'::text, 'draft'::text, 'drafts'::text, 'inbox'::text, 'junk'::text, 'junk email'::text, 'junkemail'::text, 'outbox'::text, 'sent'::text, 'sent items'::text, 'sentitems'::text, 'spam'::text, 'trash'::text])) AND (canonical_key <> ALL (ARRAY['已发送'::text, '已发送邮件'::text, '草稿'::text])))))),
    CONSTRAINT ck_pipeline_folder_scopes_sync_folder CHECK (((btrim(sync_folder) = sync_folder) AND (btrim(sync_folder) <> ''::text) AND (char_length(sync_folder) <= 512)))
);
CREATE TABLE public.pipeline_initializations (
    initialization_id uuid NOT NULL,
    command_receipt_id uuid NOT NULL,
    receipt_command_name text GENERATED ALWAYS AS ('runtime.initialize'::text) STORED,
    account_id bigint NOT NULL,
    generation bigint NOT NULL,
    fencing_token bigint NOT NULL,
    pipeline_name text NOT NULL,
    authority_epoch bigint NOT NULL,
    authority_version bigint NOT NULL,
    capability_hash character(64) NOT NULL,
    capability_stage_ordinal smallint DEFAULT 1 NOT NULL,
    policy_manifest_hash character(64) NOT NULL,
    transaction_id text NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pipeline_initializations_greenfield_identity CHECK (((account_id > 0) AND (generation = 1) AND (fencing_token = 1) AND (pipeline_name = 'durable_v1'::text) AND (authority_epoch = 1) AND (authority_version = 1) AND (capability_stage_ordinal = 1))),
    CONSTRAINT ck_pipeline_initializations_hashes CHECK ((((capability_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((policy_manifest_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_pipeline_initializations_operator CHECK (((btrim(actor) <> ''::text) AND (char_length(actor) <= 128) AND (btrim(reason) <> ''::text) AND (char_length(reason) <= 512))),
    CONSTRAINT ck_pipeline_initializations_transaction CHECK ((transaction_id ~ '^[1-9][0-9]{0,19}$'::text))
);
CREATE TABLE public.pipeline_ownership (
    account_id bigint NOT NULL,
    generation bigint NOT NULL,
    pipeline_name text NOT NULL,
    state text NOT NULL,
    fencing_token bigint NOT NULL,
    created_by text NOT NULL,
    reason text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pipeline_ownership_created_by CHECK (((btrim(created_by) <> ''::text) AND (char_length(created_by) <= 128))),
    CONSTRAINT ck_pipeline_ownership_pipeline_name CHECK (((btrim(pipeline_name) <> ''::text) AND (char_length(pipeline_name) <= 64))),
    CONSTRAINT ck_pipeline_ownership_positive_identity CHECK (((account_id > 0) AND (generation > 0) AND (fencing_token > 0))),
    CONSTRAINT ck_pipeline_ownership_reason CHECK (((reason IS NULL) OR ((btrim(reason) <> ''::text) AND (char_length(reason) <= 512)))),
    CONSTRAINT ck_pipeline_ownership_state CHECK ((state = ANY (ARRAY['current_ingress'::text, 'quiescing'::text, 'draining'::text, 'retired'::text])))
);
CREATE TABLE public.pipeline_runtime_authority (
    account_id bigint NOT NULL,
    state text NOT NULL,
    generation bigint NOT NULL,
    fencing_token bigint NOT NULL,
    pipeline_name text NOT NULL,
    authority_epoch bigint NOT NULL,
    version bigint NOT NULL,
    schema_revision text NOT NULL,
    protocol_version bigint NOT NULL,
    build_id text NOT NULL,
    config_hash character(64) NOT NULL,
    capability_hash character(64) NOT NULL,
    capability_stage_ordinal smallint DEFAULT 1 NOT NULL,
    policy_manifest_hash character(64) NOT NULL,
    initialization_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pipeline_runtime_authority_contract CHECK (((schema_revision ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'::text) AND (protocol_version > 0) AND (build_id ~ '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'::text) AND ((config_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((capability_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((policy_manifest_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_pipeline_runtime_authority_identity CHECK (((account_id > 0) AND (generation = 1) AND (fencing_token = 1) AND (pipeline_name = 'durable_v1'::text))),
    CONSTRAINT ck_pipeline_runtime_authority_state CHECK (((state = ANY (ARRAY['ingest_only'::text, 'paused'::text, 'active'::text])) AND ((state <> 'active'::text) OR (capability_stage_ordinal = 3)))),
    CONSTRAINT ck_pipeline_runtime_authority_versions CHECK (((authority_epoch > 0) AND (authority_epoch < '9223372036854775807'::bigint) AND (version > 0) AND (version < '9223372036854775807'::bigint)))
);
CREATE TABLE public.pipeline_runtime_capabilities (
    capability_hash character(64) NOT NULL,
    predecessor_hash character(64) NOT NULL,
    stage text NOT NULL,
    stage_ordinal smallint GENERATED ALWAYS AS (
CASE stage
    WHEN 'polling_ingestion'::text THEN (1)::smallint
    WHEN 'approval_send'::text THEN (2)::smallint
    WHEN 'graph_projection'::text THEN (3)::smallint
    ELSE NULL::smallint
END) STORED,
    predecessor_stage_ordinal smallint GENERATED ALWAYS AS (
CASE stage
    WHEN 'polling_ingestion'::text THEN NULL::smallint
    WHEN 'approval_send'::text THEN (1)::smallint
    WHEN 'graph_projection'::text THEN (2)::smallint
    ELSE NULL::smallint
END) STORED,
    schema_revision text NOT NULL,
    schema_digest character(64) NOT NULL,
    protocol_version bigint NOT NULL,
    minimum_build_id text NOT NULL,
    config_hash character(64) NOT NULL,
    adapter_hash character(64) NOT NULL,
    policy_manifest_hash character(64) NOT NULL,
    evidence_manifest_hash character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pipeline_runtime_capabilities_build CHECK ((minimum_build_id ~ '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'::text)),
    CONSTRAINT ck_pipeline_runtime_capabilities_hashes CHECK ((((capability_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((predecessor_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((schema_digest)::text ~ '^[0-9a-f]{64}$'::text) AND ((config_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((adapter_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((policy_manifest_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((evidence_manifest_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_pipeline_runtime_capabilities_predecessor CHECK ((((stage = 'polling_ingestion'::text) AND (predecessor_hash = '95771c6d473119376654d5530f7fe189c77d83e56fe08e91179f48b1040df86f'::bpchar) AND (predecessor_stage_ordinal IS NULL)) OR ((stage = ANY (ARRAY['approval_send'::text, 'graph_projection'::text])) AND (predecessor_hash <> '95771c6d473119376654d5530f7fe189c77d83e56fe08e91179f48b1040df86f'::bpchar) AND (predecessor_stage_ordinal = (stage_ordinal - 1))))),
    CONSTRAINT ck_pipeline_runtime_capabilities_protocol CHECK ((protocol_version > 0)),
    CONSTRAINT ck_pipeline_runtime_capabilities_schema CHECK (((schema_revision ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'::text) AND ((stage <> 'polling_ingestion'::text) OR (schema_revision = 'polling-v1'::text)))),
    CONSTRAINT ck_pipeline_runtime_capabilities_stage CHECK (((stage = ANY (ARRAY['polling_ingestion'::text, 'approval_send'::text, 'graph_projection'::text])) AND ((stage_ordinal >= 1) AND (stage_ordinal <= 3))))
);
CREATE TABLE public.pipeline_runtime_instances (
    account_id bigint NOT NULL,
    workload text NOT NULL,
    instance_id text NOT NULL,
    session_id uuid NOT NULL,
    generation bigint NOT NULL,
    fencing_token bigint NOT NULL,
    authority_epoch bigint NOT NULL,
    capability_hash character(64) NOT NULL,
    capability_stage_ordinal smallint DEFAULT 1 NOT NULL,
    schema_revision text NOT NULL,
    protocol_version bigint NOT NULL,
    build_id text NOT NULL,
    config_hash character(64) NOT NULL,
    lifecycle text NOT NULL,
    lease_version bigint NOT NULL,
    accepted_count bigint DEFAULT 0 NOT NULL,
    rejected_count bigint DEFAULT 0 NOT NULL,
    registered_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    heartbeat_at timestamp with time zone NOT NULL,
    lease_until timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_pipeline_runtime_instances_contract CHECK (((schema_revision ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'::text) AND (protocol_version > 0) AND (build_id ~ '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$'::text) AND ((config_hash)::text ~ '^[0-9a-f]{64}$'::text) AND ((capability_hash)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_pipeline_runtime_instances_counters CHECK (((lease_version > 0) AND (lease_version < '9223372036854775807'::bigint) AND (accepted_count >= 0) AND (accepted_count < '9223372036854775807'::bigint) AND (rejected_count >= 0) AND (rejected_count < '9223372036854775807'::bigint))),
    CONSTRAINT ck_pipeline_runtime_instances_identity CHECK (((account_id > 0) AND (generation = 1) AND (fencing_token = 1) AND (authority_epoch > 0) AND (authority_epoch < '9223372036854775807'::bigint) AND (btrim(instance_id) = instance_id) AND (btrim(instance_id) <> ''::text) AND (char_length(instance_id) <= 128))),
    CONSTRAINT ck_pipeline_runtime_instances_lease CHECK (((heartbeat_at >= registered_at) AND (lease_until > heartbeat_at) AND (updated_at >= registered_at))),
    CONSTRAINT ck_pipeline_runtime_instances_lifecycle CHECK (((lifecycle = ANY (ARRAY['standby'::text, 'active'::text, 'draining'::text])) AND ((lifecycle <> 'active'::text) OR (workload = 'web'::text) OR (capability_stage_ordinal = 3)))),
    CONSTRAINT ck_pipeline_runtime_instances_workload CHECK ((workload = ANY (ARRAY['web'::text, 'worker'::text, 'scheduler'::text, 'reaper'::text])))
);
CREATE VIEW public.processed_emails WITH (security_invoker='true') AS
 SELECT emails_log.id,
    emails_log.processed_at
   FROM public.emails_log;
CREATE TABLE public.sync_cursors (
    account_id bigint NOT NULL,
    folder_key text NOT NULL,
    cursor text,
    status text NOT NULL,
    blocked_reason_code text,
    contract_fingerprint character(64),
    blocked_at timestamp with time zone,
    version bigint DEFAULT 0 NOT NULL,
    last_success_at timestamp with time zone,
    last_attempt_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_sync_cursors_account CHECK ((account_id > 0)),
    CONSTRAINT ck_sync_cursors_cursor CHECK (((cursor IS NULL) OR ((btrim(cursor) <> ''::text) AND (char_length(cursor) <= 8192)))),
    CONSTRAINT ck_sync_cursors_fingerprint CHECK (((contract_fingerprint IS NULL) OR ((contract_fingerprint)::text ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_sync_cursors_folder_key CHECK (((btrim(folder_key) <> ''::text) AND (char_length(folder_key) <= 512))),
    CONSTRAINT ck_sync_cursors_reason CHECK (((blocked_reason_code IS NULL) OR ((btrim(blocked_reason_code) <> ''::text) AND (char_length(blocked_reason_code) <= 64)))),
    CONSTRAINT ck_sync_cursors_state_matrix CHECK ((((status = 'baselining'::text) AND (cursor IS NULL) AND (blocked_reason_code = 'sync.baseline_required'::text) AND (contract_fingerprint IS NULL) AND (blocked_at IS NULL)) OR ((status = 'active'::text) AND (cursor IS NOT NULL) AND (blocked_reason_code IS NULL) AND (contract_fingerprint IS NULL) AND (blocked_at IS NULL) AND (last_success_at IS NOT NULL)) OR ((status = 'blocked_contract'::text) AND (blocked_reason_code IS NOT NULL) AND (contract_fingerprint IS NOT NULL) AND (blocked_at IS NOT NULL)))),
    CONSTRAINT ck_sync_cursors_status CHECK ((status = ANY (ARRAY['baselining'::text, 'active'::text, 'blocked_contract'::text]))),
    CONSTRAINT ck_sync_cursors_version CHECK ((version >= 0))
);
ALTER TABLE ONLY public.app_kv_store
    ADD CONSTRAINT app_kv_store_pkey PRIMARY KEY (key);
ALTER TABLE ONLY public.emails_log
    ADD CONSTRAINT emails_log_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT pk_audit_events PRIMARY KEY (id);
ALTER TABLE ONLY public.daily_digest_executions
    ADD CONSTRAINT pk_daily_digest_executions PRIMARY KEY (account_id, delivery_scope_hash, window_start, window_end);
ALTER TABLE ONLY public.emails
    ADD CONSTRAINT pk_emails PRIMARY KEY (id);
ALTER TABLE ONLY public.event_inbox
    ADD CONSTRAINT pk_event_inbox PRIMARY KEY (id);
ALTER TABLE ONLY public.tier1_decisions
    ADD CONSTRAINT pk_tier1_decisions PRIMARY KEY (inbox_id);
ALTER TABLE ONLY public.handoff_executions
    ADD CONSTRAINT pk_handoff_executions PRIMARY KEY (inbox_id);
ALTER TABLE ONLY public.pipeline_command_receipts
    ADD CONSTRAINT pk_pipeline_command_receipts PRIMARY KEY (id);
ALTER TABLE ONLY public.pipeline_folder_scopes
    ADD CONSTRAINT pk_pipeline_folder_scopes PRIMARY KEY (initialization_id, canonical_key);
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT pk_pipeline_initializations PRIMARY KEY (initialization_id);
ALTER TABLE ONLY public.pipeline_ownership
    ADD CONSTRAINT pk_pipeline_ownership PRIMARY KEY (account_id, generation);
ALTER TABLE ONLY public.pipeline_runtime_authority
    ADD CONSTRAINT pk_pipeline_runtime_authority PRIMARY KEY (account_id);
ALTER TABLE ONLY public.pipeline_runtime_capabilities
    ADD CONSTRAINT pk_pipeline_runtime_capabilities PRIMARY KEY (capability_hash);
ALTER TABLE ONLY public.pipeline_runtime_instances
    ADD CONSTRAINT pk_pipeline_runtime_instances PRIMARY KEY (session_id);
ALTER TABLE ONLY public.sync_cursors
    ADD CONSTRAINT pk_sync_cursors PRIMARY KEY (account_id, folder_key);
ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT uq_audit_events_event_key UNIQUE (event_key);
ALTER TABLE ONLY public.emails
    ADD CONSTRAINT uq_email_external UNIQUE (account_id, external_email_id);
ALTER TABLE ONLY public.emails
    ADD CONSTRAINT uq_emails_account_id UNIQUE (account_id, id);
ALTER TABLE ONLY public.emails
    ADD CONSTRAINT uq_emails_outbox_identity UNIQUE (id, account_id, owner_generation, owner_fencing_token, owner_authority_epoch, owner_capability_hash);
ALTER TABLE ONLY public.event_inbox
    ADD CONSTRAINT uq_event_inbox_dedupe UNIQUE (dedupe_key);
ALTER TABLE ONLY public.event_inbox
    ADD CONSTRAINT uq_event_inbox_processing_identity UNIQUE (id, account_id, external_email_id, generation, fencing_token, execution_epoch, authority_epoch, capability_hash);
ALTER TABLE ONLY public.pipeline_command_receipts
    ADD CONSTRAINT uq_pipeline_command_receipts_identity UNIQUE (account_id, command_name, idempotency_key_hash);
ALTER TABLE ONLY public.pipeline_command_receipts
    ADD CONSTRAINT uq_pipeline_command_receipts_runtime_binding UNIQUE (id, account_id, command_name, authority_epoch);
ALTER TABLE ONLY public.pipeline_folder_scopes
    ADD CONSTRAINT uq_pipeline_folder_scopes_hash UNIQUE (initialization_id, scope_hash);
ALTER TABLE ONLY public.pipeline_folder_scopes
    ADD CONSTRAINT uq_pipeline_folder_scopes_sync_folder UNIQUE (initialization_id, sync_folder);
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT uq_pipeline_initializations_account UNIQUE (account_id);
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT uq_pipeline_initializations_authority_binding UNIQUE (initialization_id, account_id, generation, fencing_token, pipeline_name, policy_manifest_hash);
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT uq_pipeline_initializations_policy_binding UNIQUE (initialization_id, account_id, policy_manifest_hash);
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT uq_pipeline_initializations_receipt UNIQUE (command_receipt_id);
ALTER TABLE ONLY public.pipeline_ownership
    ADD CONSTRAINT uq_pipeline_ownership_event_identity UNIQUE (account_id, generation, fencing_token, pipeline_name);
ALTER TABLE ONLY public.pipeline_ownership
    ADD CONSTRAINT uq_pipeline_ownership_fence UNIQUE (account_id, fencing_token);
ALTER TABLE ONLY public.pipeline_ownership
    ADD CONSTRAINT uq_pipeline_ownership_generation_fence UNIQUE (account_id, generation, fencing_token);
ALTER TABLE ONLY public.pipeline_runtime_authority
    ADD CONSTRAINT uq_pipeline_runtime_authority_stamp UNIQUE (account_id, generation, fencing_token, pipeline_name, authority_epoch, capability_hash);
ALTER TABLE ONLY public.pipeline_runtime_capabilities
    ADD CONSTRAINT uq_pipeline_runtime_capabilities_instance_contract UNIQUE (capability_hash, stage_ordinal, schema_revision, protocol_version, minimum_build_id, config_hash);
ALTER TABLE ONLY public.pipeline_runtime_capabilities
    ADD CONSTRAINT uq_pipeline_runtime_capabilities_policy_identity UNIQUE (capability_hash, stage_ordinal, policy_manifest_hash);
ALTER TABLE ONLY public.pipeline_runtime_capabilities
    ADD CONSTRAINT uq_pipeline_runtime_capabilities_runtime_contract UNIQUE (capability_hash, stage_ordinal, schema_revision, protocol_version, minimum_build_id, config_hash, policy_manifest_hash);
ALTER TABLE ONLY public.pipeline_runtime_capabilities
    ADD CONSTRAINT uq_pipeline_runtime_capabilities_stage_identity UNIQUE (capability_hash, stage_ordinal);
ALTER TABLE ONLY public.pipeline_runtime_instances
    ADD CONSTRAINT uq_pipeline_runtime_instances_lease_identity UNIQUE (session_id, account_id, generation, fencing_token, authority_epoch, capability_hash);
ALTER TABLE ONLY public.pipeline_runtime_instances
    ADD CONSTRAINT uq_pipeline_runtime_instances_session UNIQUE (account_id, session_id);
CREATE INDEX idx_emails_log_status_processed ON public.emails_log USING btree (status, processed_at DESC);
CREATE INDEX ix_audit_events_account_time ON public.audit_events USING btree (account_id, created_at DESC, id);
CREATE INDEX ix_audit_events_email_time ON public.audit_events USING btree (email_id, created_at DESC, id) WHERE (email_id IS NOT NULL);
CREATE INDEX ix_emails_account_status ON public.emails USING btree (account_id, status, updated_at, id);
CREATE INDEX ix_emails_owner_status ON public.emails USING btree (account_id, owner_generation, owner_fencing_token, owner_authority_epoch, owner_capability_hash, status);
CREATE INDEX ix_event_inbox_claim ON public.event_inbox USING btree (pipeline_name, status, available_at, received_at, id) WHERE (status = ANY (ARRAY['pending'::text, 'retry_wait'::text]));
CREATE INDEX ix_event_inbox_expired_lease ON public.event_inbox USING btree (lease_until, execution_epoch, authority_epoch, capability_hash, lease_session_id, id) WHERE (status = 'leased'::text);
CREATE INDEX ix_tier1_decisions_route ON public.tier1_decisions USING btree (account_id, route, created_at DESC);
CREATE INDEX ix_handoff_executions_state ON public.handoff_executions USING btree (state, updated_at);
CREATE INDEX ix_pipeline_folder_scopes_account ON public.pipeline_folder_scopes USING btree (account_id, canonical_key);
CREATE INDEX ix_pipeline_runtime_authority_state ON public.pipeline_runtime_authority USING btree (state, account_id);
CREATE INDEX ix_pipeline_runtime_capabilities_stage ON public.pipeline_runtime_capabilities USING btree (stage_ordinal, created_at, capability_hash);
CREATE INDEX ix_pipeline_runtime_instances_authority ON public.pipeline_runtime_instances USING btree (account_id, generation, fencing_token, authority_epoch, capability_hash, lifecycle);
CREATE INDEX ix_pipeline_runtime_instances_lease ON public.pipeline_runtime_instances USING btree (lease_until, session_id) WHERE (lifecycle <> 'draining'::text);
CREATE INDEX ix_sync_cursors_status_attempt ON public.sync_cursors USING btree (status, last_attempt_at);
CREATE UNIQUE INDEX uq_pipeline_current_ingress ON public.pipeline_ownership USING btree (account_id) WHERE (state = 'current_ingress'::text);
CREATE UNIQUE INDEX uq_pipeline_runtime_instances_live_identity ON public.pipeline_runtime_instances USING btree (account_id, workload, instance_id) WHERE (lifecycle <> 'draining'::text);
CREATE UNIQUE INDEX uq_approved_execution_envelopes_inbox ON public.approved_execution_envelopes USING btree (inbox_id);
CREATE TRIGGER trg_audit_events_guard_row BEFORE DELETE OR UPDATE ON public.audit_events FOR EACH ROW EXECUTE FUNCTION public.reject_audit_events_mutation();
CREATE TRIGGER trg_audit_events_guard_truncate BEFORE TRUNCATE ON public.audit_events FOR EACH STATEMENT EXECUTE FUNCTION public.reject_audit_events_mutation();
CREATE CONSTRAINT TRIGGER trg_emails_runtime_identity AFTER INSERT OR UPDATE ON public.emails DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.guard_emails_runtime_identity();
CREATE TRIGGER trg_event_inbox_runtime_identity BEFORE INSERT OR UPDATE ON public.event_inbox FOR EACH ROW EXECUTE FUNCTION public.guard_event_inbox_runtime_identity();
CREATE TRIGGER trg_pipeline_command_receipts_guard_row BEFORE DELETE OR UPDATE ON public.pipeline_command_receipts FOR EACH ROW EXECUTE FUNCTION public.reject_pipeline_command_receipts_mutation();
CREATE TRIGGER trg_pipeline_command_receipts_guard_truncate BEFORE TRUNCATE ON public.pipeline_command_receipts FOR EACH STATEMENT EXECUTE FUNCTION public.reject_pipeline_command_receipts_mutation();
CREATE TRIGGER trg_pipeline_folder_scopes_guard_row BEFORE INSERT OR DELETE OR UPDATE ON public.pipeline_folder_scopes FOR EACH ROW EXECUTE FUNCTION public.reject_pipeline_folder_scopes_mutation();
CREATE TRIGGER trg_pipeline_folder_scopes_guard_truncate BEFORE TRUNCATE ON public.pipeline_folder_scopes FOR EACH STATEMENT EXECUTE FUNCTION public.reject_pipeline_folder_scopes_mutation();
CREATE TRIGGER trg_pipeline_initializations_guard_row BEFORE INSERT OR DELETE OR UPDATE ON public.pipeline_initializations FOR EACH ROW EXECUTE FUNCTION public.reject_pipeline_initializations_mutation();
CREATE TRIGGER trg_pipeline_initializations_guard_truncate BEFORE TRUNCATE ON public.pipeline_initializations FOR EACH STATEMENT EXECUTE FUNCTION public.reject_pipeline_initializations_mutation();
CREATE TRIGGER trg_pipeline_ownership_guard_row BEFORE INSERT OR DELETE OR UPDATE ON public.pipeline_ownership FOR EACH ROW EXECUTE FUNCTION public.guard_pipeline_ownership();
CREATE TRIGGER trg_pipeline_ownership_guard_truncate BEFORE TRUNCATE ON public.pipeline_ownership FOR EACH STATEMENT EXECUTE FUNCTION public.guard_pipeline_ownership();
CREATE TRIGGER trg_pipeline_runtime_authority_guard_row BEFORE INSERT OR DELETE OR UPDATE ON public.pipeline_runtime_authority FOR EACH ROW EXECUTE FUNCTION public.guard_pipeline_runtime_authority();
CREATE TRIGGER trg_pipeline_runtime_authority_guard_truncate BEFORE TRUNCATE ON public.pipeline_runtime_authority FOR EACH STATEMENT EXECUTE FUNCTION public.guard_pipeline_runtime_authority();
CREATE TRIGGER trg_pipeline_runtime_capabilities_guard_row BEFORE DELETE OR UPDATE ON public.pipeline_runtime_capabilities FOR EACH ROW EXECUTE FUNCTION public.reject_pipeline_runtime_capabilities_mutation();
CREATE TRIGGER trg_pipeline_runtime_capabilities_guard_truncate BEFORE TRUNCATE ON public.pipeline_runtime_capabilities FOR EACH STATEMENT EXECUTE FUNCTION public.reject_pipeline_runtime_capabilities_mutation();
CREATE TRIGGER trg_pipeline_runtime_instances_guard_row BEFORE INSERT OR DELETE OR UPDATE ON public.pipeline_runtime_instances FOR EACH ROW EXECUTE FUNCTION public.guard_pipeline_runtime_instances();
CREATE TRIGGER trg_pipeline_runtime_instances_guard_truncate BEFORE TRUNCATE ON public.pipeline_runtime_instances FOR EACH STATEMENT EXECUTE FUNCTION public.guard_pipeline_runtime_instances();
CREATE TRIGGER trg_tier1_decisions_guard_row BEFORE DELETE OR UPDATE ON public.tier1_decisions FOR EACH ROW EXECUTE FUNCTION public.reject_tier1_decisions_mutation();
CREATE TRIGGER trg_tier1_decisions_guard_truncate BEFORE TRUNCATE ON public.tier1_decisions FOR EACH STATEMENT EXECUTE FUNCTION public.reject_tier1_decisions_mutation();
CREATE TRIGGER trg_intake_decisions_guard_row BEFORE DELETE OR UPDATE ON public.intake_decisions FOR EACH ROW EXECUTE FUNCTION public.reject_durable_artifact_mutation();
CREATE TRIGGER trg_intake_decisions_guard_truncate BEFORE TRUNCATE ON public.intake_decisions FOR EACH STATEMENT EXECUTE FUNCTION public.reject_durable_artifact_mutation();
CREATE TRIGGER trg_intake_releases_guard_row BEFORE DELETE OR UPDATE ON public.intake_releases FOR EACH ROW EXECUTE FUNCTION public.reject_durable_artifact_mutation();
CREATE TRIGGER trg_intake_releases_guard_truncate BEFORE TRUNCATE ON public.intake_releases FOR EACH STATEMENT EXECUTE FUNCTION public.reject_durable_artifact_mutation();
CREATE TRIGGER trg_payload_revisions_guard_row BEFORE DELETE OR UPDATE ON public.execution_payload_revisions FOR EACH ROW EXECUTE FUNCTION public.reject_durable_artifact_mutation();
CREATE TRIGGER trg_payload_revisions_guard_truncate BEFORE TRUNCATE ON public.execution_payload_revisions FOR EACH STATEMENT EXECUTE FUNCTION public.reject_durable_artifact_mutation();
CREATE TRIGGER trg_approved_envelopes_guard_row BEFORE DELETE OR UPDATE ON public.approved_execution_envelopes FOR EACH ROW EXECUTE FUNCTION public.reject_durable_artifact_mutation();
CREATE TRIGGER trg_approved_envelopes_guard_truncate BEFORE TRUNCATE ON public.approved_execution_envelopes FOR EACH STATEMENT EXECUTE FUNCTION public.reject_durable_artifact_mutation();
ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT fk_audit_events_email FOREIGN KEY (account_id, email_id) REFERENCES public.emails(account_id, id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.emails
    ADD CONSTRAINT fk_emails_pipeline_ownership FOREIGN KEY (account_id, owner_generation, owner_fencing_token) REFERENCES public.pipeline_ownership(account_id, generation, fencing_token) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.emails
    ADD CONSTRAINT fk_emails_processing_inbox FOREIGN KEY (processing_inbox_id, account_id, external_email_id, owner_generation, owner_fencing_token, processing_execution_epoch, owner_authority_epoch, owner_capability_hash) REFERENCES public.event_inbox(id, account_id, external_email_id, generation, fencing_token, execution_epoch, authority_epoch, capability_hash) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE ONLY public.emails
    ADD CONSTRAINT fk_emails_runtime_capability FOREIGN KEY (owner_capability_hash) REFERENCES public.pipeline_runtime_capabilities(capability_hash) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.event_inbox
    ADD CONSTRAINT fk_event_inbox_lease_session FOREIGN KEY (lease_session_id, account_id, generation, fencing_token, authority_epoch, capability_hash) REFERENCES public.pipeline_runtime_instances(session_id, account_id, generation, fencing_token, authority_epoch, capability_hash) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.event_inbox
    ADD CONSTRAINT fk_event_inbox_pipeline_ownership FOREIGN KEY (account_id, generation, fencing_token, pipeline_name) REFERENCES public.pipeline_ownership(account_id, generation, fencing_token, pipeline_name) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.event_inbox
    ADD CONSTRAINT fk_event_inbox_runtime_capability FOREIGN KEY (capability_hash) REFERENCES public.pipeline_runtime_capabilities(capability_hash) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.tier1_decisions
    ADD CONSTRAINT fk_tier1_decisions_inbox FOREIGN KEY (inbox_id) REFERENCES public.event_inbox(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.intake_decisions ADD CONSTRAINT fk_intake_decisions_inbox FOREIGN KEY (inbox_id) REFERENCES public.event_inbox(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.intake_releases ADD CONSTRAINT fk_intake_releases_inbox FOREIGN KEY (inbox_id) REFERENCES public.event_inbox(id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.handoff_runs ADD CONSTRAINT fk_handoff_runs_decision FOREIGN KEY (inbox_id) REFERENCES public.tier1_decisions(inbox_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.execution_payload_revisions ADD CONSTRAINT fk_payload_revision_handoff FOREIGN KEY (inbox_id) REFERENCES public.handoff_runs(inbox_id) ON DELETE RESTRICT;
ALTER TABLE ONLY public.approved_execution_envelopes ADD CONSTRAINT fk_approved_envelope_payload FOREIGN KEY (inbox_id, payload_revision) REFERENCES public.execution_payload_revisions(inbox_id, revision) ON DELETE RESTRICT;
ALTER TABLE ONLY public.handoff_executions
    ADD CONSTRAINT fk_handoff_executions_decision FOREIGN KEY (inbox_id) REFERENCES public.tier1_decisions(inbox_id) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_folder_scopes
    ADD CONSTRAINT fk_pipeline_folder_scopes_initialization FOREIGN KEY (initialization_id, account_id, policy_manifest_hash) REFERENCES public.pipeline_initializations(initialization_id, account_id, policy_manifest_hash) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT fk_pipeline_initializations_capability FOREIGN KEY (capability_hash, capability_stage_ordinal, policy_manifest_hash) REFERENCES public.pipeline_runtime_capabilities(capability_hash, stage_ordinal, policy_manifest_hash) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT fk_pipeline_initializations_ownership FOREIGN KEY (account_id, generation, fencing_token, pipeline_name) REFERENCES public.pipeline_ownership(account_id, generation, fencing_token, pipeline_name) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_initializations
    ADD CONSTRAINT fk_pipeline_initializations_receipt FOREIGN KEY (command_receipt_id, account_id, receipt_command_name, authority_epoch) REFERENCES public.pipeline_command_receipts(id, account_id, command_name, authority_epoch) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_runtime_authority
    ADD CONSTRAINT fk_pipeline_runtime_authority_capability FOREIGN KEY (capability_hash, capability_stage_ordinal, schema_revision, protocol_version, build_id, config_hash, policy_manifest_hash) REFERENCES public.pipeline_runtime_capabilities(capability_hash, stage_ordinal, schema_revision, protocol_version, minimum_build_id, config_hash, policy_manifest_hash) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_runtime_authority
    ADD CONSTRAINT fk_pipeline_runtime_authority_initialization FOREIGN KEY (initialization_id, account_id, generation, fencing_token, pipeline_name, policy_manifest_hash) REFERENCES public.pipeline_initializations(initialization_id, account_id, generation, fencing_token, pipeline_name, policy_manifest_hash) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_runtime_authority
    ADD CONSTRAINT fk_pipeline_runtime_authority_ownership FOREIGN KEY (account_id, generation, fencing_token, pipeline_name) REFERENCES public.pipeline_ownership(account_id, generation, fencing_token, pipeline_name) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_runtime_capabilities
    ADD CONSTRAINT fk_pipeline_runtime_capabilities_predecessor FOREIGN KEY (predecessor_hash, predecessor_stage_ordinal) REFERENCES public.pipeline_runtime_capabilities(capability_hash, stage_ordinal) ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY public.pipeline_runtime_instances
    ADD CONSTRAINT fk_pipeline_runtime_instances_capability FOREIGN KEY (capability_hash, capability_stage_ordinal, schema_revision, protocol_version, build_id, config_hash) REFERENCES public.pipeline_runtime_capabilities(capability_hash, stage_ordinal, schema_revision, protocol_version, minimum_build_id, config_hash) MATCH FULL ON UPDATE RESTRICT ON DELETE RESTRICT;
