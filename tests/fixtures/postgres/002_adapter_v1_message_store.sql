-- Direct-Postgres Message Store schema the adapter expects (store schema major 1).
-- Change it only as a deliberate schema change in both the .NET and Camel Quarkus adapters.
--
-- The message, attempt, replay-audit and step-outcome tables the adapter writes
-- and reads for resend and audit.
--
-- The adapter never creates or migrates these tables. Provision them once per
-- integration database before adapter-enabled integrations are started.
-- Integration repositories and runtime pods must not own these migrations.

create table if not exists bip_message_log (
    message_id text primary key,
    correlation_id text,
    content_type text,
    source_system text,
    target_system text,
    payload_storage text not null default 'metadata_only'
        check (payload_storage in ('metadata_only', 'postgres_inline')),
    payload_size_bytes bigint,
    payload_bytes bytea,
    payload_expires_at timestamptz,
    payload_deleted_at timestamptz,
    replayable boolean not null default false,
    replay_disabled_reason text,
    integration text not null,
    schema_version integer not null,
    status text not null check (status in ('RECEIVED', 'COMPLETED', 'FAILED', 'DLQ')),
    last_attempt integer not null,
    auto_redelivered boolean not null default false,
    auto_redelivery_count integer,
    auto_redelivery_max integer,
    occurred_at timestamptz not null,
    error text,
    updated_at timestamptz not null default current_timestamp
);

create index if not exists bip_message_log_correlation_id_idx
    on bip_message_log (correlation_id);

create index if not exists bip_message_log_integration_status_idx
    on bip_message_log (integration, status);

create index if not exists bip_message_log_integration_occurred_at_idx
    on bip_message_log (integration, occurred_at desc);

create index if not exists bip_message_log_source_target_idx
    on bip_message_log (source_system, target_system);

create table if not exists bip_message_attempt (
    message_id text not null,
    attempt integer not null,
    schema_version integer not null,
    correlation_id text,
    content_type text,
    source_system text,
    target_system text,
    payload_storage text not null default 'metadata_only'
        check (payload_storage in ('metadata_only', 'postgres_inline')),
    payload_size_bytes bigint,
    payload_bytes bytea,
    payload_expires_at timestamptz,
    payload_deleted_at timestamptz,
    replayable boolean not null default false,
    replay_disabled_reason text,
    integration text not null,
    route_id text not null,
    status text not null check (status in ('RECEIVED', 'COMPLETED', 'FAILED', 'DLQ')),
    auto_redelivered boolean not null default false,
    auto_redelivery_count integer,
    auto_redelivery_max integer,
    occurred_at timestamptz not null,
    steps_json jsonb not null default '[]'::jsonb,
    error text,
    primary key (message_id, attempt)
);

create index if not exists bip_message_attempt_message_id_idx
    on bip_message_attempt (message_id);

create index if not exists bip_message_attempt_integration_status_idx
    on bip_message_attempt (integration, status);

create table if not exists bip_message_replay_audit (
    replay_id uuid primary key,
    message_id text not null,
    integration text not null,
    replay_attempt integer not null,
    requested_by text,
    reason text,
    redo_steps jsonb not null default '[]'::jsonb,
    target_topic text not null,
    status text not null check (status in ('REQUESTED', 'PUBLISHED', 'FAILED')),
    requested_at timestamptz not null,
    published_at timestamptz,
    error text
);

create index if not exists bip_message_replay_audit_message_id_idx
    on bip_message_replay_audit (message_id);

create index if not exists bip_message_replay_audit_integration_requested_at_idx
    on bip_message_replay_audit (integration, requested_at desc);

create table if not exists bip_step_outcome (
    idempotency_key text primary key,
    message_id text,
    step_id text not null,
    status text not null check (status in ('DELIVERED', 'FAILED', 'SKIPPED')),
    endpoint_uri text,
    target_ref text,
    error text,
    occurred_at timestamptz not null,
    updated_at timestamptz not null default current_timestamp
);

create index if not exists bip_step_outcome_message_id_idx
    on bip_step_outcome (message_id);

create index if not exists bip_step_outcome_status_idx
    on bip_step_outcome (status);
