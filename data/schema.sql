-- VoiceBot Post-Call Processing — Database Schema
-- This schema represents the CURRENT state of the system.
-- Candidates should propose schema changes as part of their solution.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    name VARCHAR(255),
    phone VARCHAR(50),
    email VARCHAR(255),
    stage VARCHAR(100) DEFAULT 'new',
    lead_data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_leads_campaign ON leads(campaign_id);
CREATE INDEX idx_leads_customer ON leads(customer_id);

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sessions_lead ON sessions(lead_id);
CREATE INDEX idx_sessions_campaign ON sessions(campaign_id);

CREATE TABLE interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,

    status VARCHAR(20) DEFAULT 'INITIATED',
    call_sid VARCHAR(255),
    call_provider VARCHAR(50) DEFAULT 'exotel',

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds INTEGER,

    -- Transcript stored here: conversation_data->'transcript' is a JSON array
    -- of {"role": "agent"|"customer", "content": "..."}
    conversation_data JSONB DEFAULT '{}',

    -- Hot cache for dashboard. Contains extracted entities, analysis status,
    -- call_stage, and other dashboard-facing fields.
    -- Structure: {"entities": {...}, "call_stage": "...", "analysis_status": "..."}
    interaction_metadata JSONB DEFAULT '{}',

    recording_url TEXT,
    recording_s3_key VARCHAR(512),

    -- Current Celery task tracking (no workflow visibility)
    postcall_celery_task_id VARCHAR(255),

    retry_count INTEGER DEFAULT 0,
    error_log JSONB DEFAULT '[]',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_interactions_session ON interactions(session_id);
CREATE INDEX idx_interactions_lead ON interactions(lead_id);
CREATE INDEX idx_interactions_campaign ON interactions(campaign_id);
CREATE INDEX idx_interactions_customer ON interactions(customer_id);
CREATE INDEX idx_interactions_call_sid ON interactions(call_sid);
CREATE INDEX idx_interactions_status ON interactions(status);

-- Seed data: sample interactions for testing
-- (Uses fixed UUIDs for reproducibility)

INSERT INTO leads (id, campaign_id, customer_id, name, phone, stage) VALUES
    ('a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Rahul Sharma', '+919876543210', 'contacted'),
    ('a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Priya Gupta', '+919876543211', 'new'),
    ('a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Amit Verma', '+919876543212', 'contacted'),
    ('a0000000-0000-0000-0000-000000000004', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Neha Patel', '+919876543213', 'new'),
    ('a0000000-0000-0000-0000-000000000005', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Rajesh Kumar', '+919876543214', 'contacted');

INSERT INTO sessions (id, lead_id, campaign_id, customer_id, agent_id, status) VALUES
    ('b0000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000002', 'a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000003', 'a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED');

INSERT INTO interactions (id, session_id, lead_id, campaign_id, customer_id, agent_id, status, call_sid, duration_seconds, started_at, ended_at, conversation_data, interaction_metadata) VALUES
    (
        'f0000000-0000-0000-0000-000000000001',
        'b0000000-0000-0000-0000-000000000001',
        'a0000000-0000-0000-0000-000000000001',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-001',
        180,
        NOW() - INTERVAL '10 minutes',
        NOW() - INTERVAL '7 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"}, {"role": "customer", "content": "Haan ji"}, {"role": "agent", "content": "I am calling from Cashify regarding your phone evaluation. Can we reschedule?"}, {"role": "customer", "content": "Tomorrow 3:30 PM works"}, {"role": "agent", "content": "Confirmed, our executive will visit tomorrow at 3:30 PM"}, {"role": "customer", "content": "Okay, confirmed. Bye."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000002',
        'b0000000-0000-0000-0000-000000000002',
        'a0000000-0000-0000-0000-000000000002',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-002',
        45,
        NOW() - INTERVAL '15 minutes',
        NOW() - INTERVAL '14 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Ms. Gupta?"}, {"role": "customer", "content": "Not interested, dont call again"}, {"role": "agent", "content": "Sorry for the inconvenience. Have a good day."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000003',
        'b0000000-0000-0000-0000-000000000003',
        'a0000000-0000-0000-0000-000000000003',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-003',
        15,
        NOW() - INTERVAL '20 minutes',
        NOW() - INTERVAL '19 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello—"}, {"role": "customer", "content": "Wrong number"}]}',
        '{"analysis_status": "pending"}'
    );


-- ─────────────────────────────────────────────────────────────────────────────
-- POST-CALL PIPELINE EXTENSIONS
--
-- Schema changes that support rate-limited LLM scheduling, per-customer
-- budgets, durable job tracking, structured audit, and a dead-letter
-- queue for permanently failed work.
-- ─────────────────────────────────────────────────────────────────────────────


-- Per-customer budget configuration. Loaded by RateLimiter at startup.
CREATE TABLE customer_budgets (
    customer_id UUID PRIMARY KEY,
    reserved_tpm INT NOT NULL CHECK (reserved_tpm >= 0),
    reserved_rpm INT NOT NULL CHECK (reserved_rpm >= 0),
    priority_tier VARCHAR(16) NOT NULL DEFAULT 'silver'
        CHECK (priority_tier IN ('gold', 'silver', 'bronze')),
    allow_overflow BOOLEAN NOT NULL DEFAULT TRUE,
    hot_quota_pct NUMERIC(3,2) NOT NULL DEFAULT 0.30
        CHECK (hot_quota_pct BETWEEN 0 AND 1),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- Durable job tracking for post-call processing. Replaces the Redis-only
-- retry queue for any work that must survive a Redis restart. Workers
-- claim jobs via SELECT ... FOR UPDATE SKIP LOCKED.
CREATE TABLE postcall_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL UNIQUE REFERENCES interactions(id),
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    correlation_id UUID NOT NULL,

    state VARCHAR(32) NOT NULL DEFAULT 'queued',
    -- queued | in_progress | recording | analyzing | signaling | done
    -- | rate_limited | recording_pending_reconcile | failed | dlq

    priority VARCHAR(8) NOT NULL DEFAULT 'cold'
        CHECK (priority IN ('hot', 'cold', 'skip')),

    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMPTZ,
    last_error TEXT,

    payload JSONB NOT NULL,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_jobs_state_priority_next
    ON postcall_jobs(priority, state, next_attempt_at)
    WHERE state IN ('queued', 'rate_limited');
CREATE INDEX idx_jobs_customer ON postcall_jobs(customer_id);
CREATE INDEX idx_jobs_correlation ON postcall_jobs(correlation_id);


-- Analysis results — one row per attempt, separate from the JSONB hot
-- cache on interactions.interaction_metadata. Keeps a full history if
-- a result is ever overwritten or replayed.
CREATE TABLE analysis_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    customer_id UUID NOT NULL,
    attempt INT NOT NULL,

    call_stage VARCHAR(64),
    entities JSONB,
    summary TEXT,

    tokens_used INT NOT NULL,
    latency_ms INT NOT NULL,
    provider VARCHAR(32) NOT NULL,
    model VARCHAR(64) NOT NULL,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (interaction_id, attempt)
);

CREATE INDEX idx_results_customer_created
    ON analysis_results(customer_id, created_at);


-- Audit trail — one row per stage transition. Mirrors the structured
-- log events from src/utils/audit.py for durable querying.
CREATE TABLE postcall_audit (
    id BIGSERIAL PRIMARY KEY,
    interaction_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    customer_id UUID NOT NULL,

    stage VARCHAR(64) NOT NULL,
    event VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('ok', 'retry', 'fail')),

    tokens_estimated INT,
    tokens_actual INT,
    latency_ms INT,
    attempt INT,
    error_message TEXT,
    metadata JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_interaction ON postcall_audit(interaction_id);
CREATE INDEX idx_audit_correlation ON postcall_audit(correlation_id);
CREATE INDEX idx_audit_customer_created ON postcall_audit(customer_id, created_at);


-- Dead-letter queue. Permanently failed jobs land here with full payload
-- and last error so they can be inspected and replayed manually.
CREATE TABLE postcall_dlq (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL,
    interaction_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    correlation_id UUID NOT NULL,

    final_state VARCHAR(32) NOT NULL,
    last_error TEXT,
    payload JSONB NOT NULL,
    attempts INT NOT NULL,

    moved_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    resolution_note TEXT
);

CREATE INDEX idx_dlq_unresolved
    ON postcall_dlq(moved_at) WHERE resolved_at IS NULL;


-- New columns on interactions to support the pipeline.
ALTER TABLE interactions
    ADD COLUMN correlation_id UUID,
    ADD COLUMN processing_priority VARCHAR(8),
    ADD COLUMN recording_state VARCHAR(32) DEFAULT 'pending',
    ADD COLUMN recording_attempts INT DEFAULT 0;

CREATE INDEX idx_interactions_correlation ON interactions(correlation_id);
CREATE INDEX idx_interactions_recording_state
    ON interactions(recording_state)
    WHERE recording_state IN ('pending', 'failed_pending_reconcile');


-- Seed budgets matching the existing seed customers above.
INSERT INTO customer_budgets (customer_id, reserved_tpm, reserved_rpm, priority_tier) VALUES
    ('d0000000-0000-0000-0000-000000000001', 30000, 150, 'gold'),
    ('d0000000-0000-0000-0000-000000000002', 15000, 75, 'silver');
