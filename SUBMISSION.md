# Post-Call Processing Pipeline — Design Document

**Author:** Nitin Tummaganti
**Date:** 2026-05-08

---

## 1. Assumptions

I tried to make every load-bearing assumption explicit. If any of these is wrong, parts of the design break.

Business

1. Calls aren't all worth the same. A confirmed rebook or escalation has to reach sales fast; a "not interested" call doesn't. The fixtures encode this with `expected_lane: hot|cold|skip`, and I took that as ground truth.
2. Hot calls: under 60 seconds end-to-end. Cold calls: under 30 minutes, batching is fine.
3. Customers run campaigns at the same time and pay differently. Gold tier gets reserved capacity. Bronze can't drown them.
4. The dialler can pass `additional_data.priority` from campaign config. I trust it but cap it via `hot_quota_pct`.

Capacity

5. Platform LLM budget: 90,000 TPM, 500 RPM, ~1,500 tokens/call (`src/config.py`).
6. **A 100K-call campaign can't be processed in real time.** 100,000 × 1,500 = 150M tokens. At 90K TPM that's about 28 hours. The bottleneck is the provider rate limit, not workers, so adding workers doesn't help. Some calls have to be deferred. The design question is *which*.
7. Campaign window: typically 8 hours.
8. Short transcripts (<4 turns) skip the LLM. Existing behaviour, kept.
9. Recording delivery from Exotel is 10–120s with no SLA. Their status endpoint is poll-friendly.
10. Postgres durable, Redis volatile. Anything that has to survive restart goes to Postgres.

Provider

11. The provider returns `usage.total_tokens` per response, which is what billing trusts.
12. The provider sends `Retry-After` on 429. I respect it instead of using a fixed delay.
13. One provider, one model. No failover.

Operational

14. Postgres + Redis + S3 are running (`docker-compose.yml`).
15. The dialler is a separate system. It can read a `current_pressure()` value (float in [0,1]) to throttle proportionally.

---

## 2. Problem Diagnosis

The current code has five problems that compound.

1. Nothing reads the rate limits before firing. `LLM_TOKENS_PER_MINUTE` sits in config as documentation. Burst load → 429s → Celery retries → Redis fills.
2. Every call sits in one queue at one priority. A confirmed rebook waits behind tens of thousands of "not interested" hangups during a campaign.
3. The 45s sleep in `recording.py` blocks LLM analysis even though the LLM doesn't need the audio.
4. Both retries (Celery's and `retry_queue.py`) live in the same Redis. A Redis restart loses both. They also don't talk to each other, so a failed task can run twice.
5. The circuit breaker is binary, agent-scoped, and measures RPM while the provider rate-limits on TPM. A campaign of long transcripts will hit TPM long before RPM and the breaker won't see it coming.

The root cause behind all five: the system processes every call the same way, but the business doesn't treat them the same way.

---

## 3. Architecture Overview

```
Exotel ──POST /interaction/end──► FastAPI endpoint
                                  - generate correlation_id
                                  - mark ENDED
                                  - if <4 turns → skip lane
                                  - else → pre-classifier (regex)
                                  - INSERT postcall_jobs
                                  - audit event
                                  │
                              hot │ cold
                                  ▼
                       postcall_hot │ postcall_cold (Celery queues)
                                  ▼
                       Worker pool
                       ├─ rate_limiter.try_acquire(customer_id, est_tokens)
                       │  → ok | overflow | wait | reject_to_cold | defer
                       ├─ asyncio.gather(
                       │     recording poller (5/10/20/40/80s),
                       │     LLM analysis + tokens refund
                       │  )
                       └─ signal_jobs, lead_stage, analysis_results INSERT
                                  │
                                  ▼
              Postgres                 Redis (cache)
              - postcall_jobs          - rl:tpm:global
              - analysis_results       - rl:tpm:cust:{id}
              - postcall_audit         (sliding-window sorted sets)
              - postcall_dlq
```

### Key design decisions

1. Postgres holds the source of truth for jobs. Redis is cache only. A Redis restart no longer loses work.
2. Two real Celery queues, not one queue with a priority field. Priorities on a Redis broker are advisory under load and I don't trust them at this scale.
3. A regex pre-classifier at intake assigns hot or cold. It's cheap and wrong sometimes; both failure modes are bounded.
4. Recording and LLM run in parallel via `asyncio.gather`. They share no data, so coupling them was always an accident.
5. Token reservation with refund. Reserve the estimate, fire the LLM, write back the actual tokens once the response lands.
6. The endpoint no longer fires `signal_jobs` with an empty payload. That bug is gone.

---

## 4. Rate Limit Management

### How you track rate limit usage

Two Redis sorted sets per active customer plus one global. Each entry has `score = timestamp_ms` and `member = "{reservation_id}:{tokens}"`. Current TPM is the sum of token counts in the last 60s window. True sliding window, no fixed-bucket boundary, no 2× burst at the bucket edge.

### How you decide what to process now vs. defer

`try_acquire(customer_id, est_tokens, priority)` returns one of five outcomes:

| Result | When | Action |
|---|---|---|
| `OK` | within reserved AND global has room | fire |
| `OK_OVERFLOW` | over reserved but global has room | fire, audit-flag |
| `WAIT` | customer has budget, global jammed | re-queue |
| `REJECT_TO_COLD` | global saturated AND priority=hot | demote to cold |
| `REJECT_DEFER` | over budget AND saturated | push back |

The estimate is `1.2 × LLM_AVG_TOKENS_PER_CALL`. After the call, `usage.total_tokens` replaces the estimate via `finalize()` so the sliding window reflects real consumption, not the conservative estimate.

### What happens when the limit is hit (recovery, not crash)

In the happy path we don't hit it because we block the call before it fires. If a 429 sneaks through anyway (cross-tenant burst, miscalibrated estimate), the worker reads `Retry-After`, sets `next_attempt_at` on the job row, releases the slot, and moves on. No Celery retry storm.

---

## 5. Per-Customer Token Budgeting

Each customer has `reserved_tpm`. The sum of reservations is capped at 70% of platform TPM, leaving 30% as a shared overflow pool.

A customer with `reserved_tpm = X` is guaranteed at least X tokens/min, regardless of what other customers do.

If they go over and `allow_overflow=true`, they keep firing as long as the global pool has room. The call's audit row is tagged `budget_overflow=true` so billing and the customer dashboard can see it. If `allow_overflow=false` or the pool is full, the call drops to the cold lane.

Unallocated headroom (the 30%) is shared first-come. Tier (gold/silver/bronze) is only a tiebreaker when two simultaneous overflow attempts hit the same Redis decrement.

The API caller never sees a 429. Deferral happens behind the contract.

---

## 6. Differentiated Processing

Three lanes, picked at intake:

- **skip** — fewer than 4 turns, no LLM call.
- **hot** — keyword pre-classifier matches a positive-outcome pattern, OR `additional_data.priority == "hot"` is set by the dialler. Under-60s SLA.
- **cold** — everything else. Under-30-min SLA, batched.

The pre-classifier is plain regex on the first and last 4 turns. I considered an LLM-based classifier but a small LLM call to decide whether to make a big LLM call defeats the purpose.

The misclassification cost is asymmetric. Demotion is safe — the cold lane still meets its SLA — and promotion is capped by `hot_quota_pct` so a customer can't just label everything hot. Defaulting to cold makes errors cheap.

---

## 7. Recording Pipeline

The 45-second sleep is replaced with `[5, 10, 20, 40, 80]`-second polling: five attempts, 155 seconds total wall time. After all five fail, we emit `recording_permanently_unavailable` with `status=fail` so on-call can alert on the rate.

Recording and LLM analysis run in parallel via `asyncio.gather`. A late recording no longer blocks the dashboard update.

A separate reconciler runs every 5 minutes and scans `recording_state='failed_pending_reconcile'`. So even if Exotel takes 30 minutes for a particular recording, it eventually lands in S3 and the interaction row gets updated.

---

## 8. Reliability & Durability

Postgres outbox replaces the Redis-only retry. The endpoint inserts a `postcall_jobs` row in the same transaction as the interaction status update. Workers claim jobs with `SELECT ... FOR UPDATE SKIP LOCKED`.

State machine:

```
queued → in_progress → recording → analyzing → signaling → done
   │
   ├→ rate_limited (next_attempt_at set)
   ├→ recording_pending_reconcile
   └→ failed → dlq (after max_attempts)
```

Each transition is a Postgres transaction. `analysis_results` has `UNIQUE(interaction_id, attempt)`, so retries are idempotent — a re-run can't corrupt a previous successful analysis. After max attempts, the job moves to `postcall_dlq` with the full payload, replayable via a CLI.

What this fixes:

- Redis broker restart used to lose tasks. Now Postgres still has them and the worker resumes.
- Worker crash mid-task used to redeliver to the back of a 100K queue. Now the same row gets picked up via SKIP LOCKED, order preserved.
- Permanent failure used to be logged and dropped, payload lost. Now the row lands in DLQ with the payload.

---

## 9. Auditability & Observability

### What you log (and what fields every log event includes)

Every event carries: `correlation_id`, `interaction_id`, `customer_id`, `campaign_id`, `stage`, `event`, `status` (ok/retry/fail), `attempt`, plus `tokens_estimated`, `tokens_actual`, `latency_ms`, `error_message` where they apply. Events go to stdout JSON and to the `postcall_audit` table.

To debug an interaction three days later:

```sql
SELECT stage, event, status, tokens_actual, error_message, created_at
FROM postcall_audit WHERE interaction_id = $1 ORDER BY created_at;
```

That gives the engineer the full timeline. Combined with `postcall_jobs` (state history) and `analysis_results` (every attempt's output), no log grep needed.

### Alert conditions

| Alert | Threshold | Severity |
|---|---|---|
| Platform TPM utilisation | > 85% for 5 min | warn |
| Platform TPM utilisation | > 95% for 1 min | page |
| Hot-lane p95 latency | > 60s for 5 min | page |
| Cold-lane queue depth | > 50,000 | warn |
| DLQ growth rate | > 10/hour | warn |
| DLQ growth rate | > 100/hour | page |
| Recording permanent failure rate | > 0.5% over 1h | warn |
| Per-customer budget overage | > 3× reserved over 5 min | warn |
| Worker liveness | no completion in 2 min while queue non-empty | page |

---

## 10. Data Model

```sql
CREATE TABLE customer_budgets (
    customer_id UUID PRIMARY KEY,
    reserved_tpm INT NOT NULL CHECK (reserved_tpm >= 0),
    reserved_rpm INT NOT NULL CHECK (reserved_rpm >= 0),
    priority_tier VARCHAR(16) NOT NULL DEFAULT 'silver'
        CHECK (priority_tier IN ('gold', 'silver', 'bronze')),
    allow_overflow BOOLEAN NOT NULL DEFAULT TRUE,
    hot_quota_pct NUMERIC(3,2) NOT NULL DEFAULT 0.30,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE postcall_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL UNIQUE REFERENCES interactions(id),
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'queued',
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

ALTER TABLE interactions
    ADD COLUMN correlation_id UUID,
    ADD COLUMN processing_priority VARCHAR(8),
    ADD COLUMN recording_state VARCHAR(32) DEFAULT 'pending',
    ADD COLUMN recording_attempts INT DEFAULT 0;
```

The existing `interactions.interaction_metadata` JSONB stays as the dashboard hot cache. `analysis_results` is the durable history per attempt, so a retry that overwrites the cache doesn't lose the prior version.

---

## 11. Security

The sensitive things are transcripts (PII plus business-confidential), recordings (voice biometrics on top of everything in the transcript), and the lead PII in the `leads` table.

At rest: Postgres KMS, S3 SSE-KMS for the recordings bucket. Redis only stores counters and ephemeral state, no transcript content.

In transit: TLS for Exotel, S3, the LLM provider, and CRM webhooks.

LLM provider: confirmed Zero-Retention agreement so transcripts aren't retained or used for training. Phone numbers and emails are regex-stripped from prompts where they aren't needed for analysis.

Logs go through a redaction pass. Phone numbers and emails are replaced with `[REDACTED]` before emission. The audit table stores IDs only, never the raw transcript.

Tenant isolation is enforced at the query layer. Every worker query is filtered by `customer_id`, and rate-limiter Redis keys are namespaced. Cross-customer leakage is structurally prevented, not just by convention.

---

## 12. API Interface

I didn't change the contract on `POST /session/{sid}/interaction/{iid}/end`. Exotel calls this and changing it would require a coordinated telephony-side change, which isn't worth doing for an internal pipeline rewrite. The response now includes a `correlation_id` field, but that's additive and won't break existing callers.

What changed inside: the endpoint generates a correlation_id, writes a `postcall_jobs` row in the same DB transaction as the status update, reads `additional_data.priority` if the dialler sets it, and stops the empty-payload signal-jobs double-fire.

---

## 13. Trade-offs & Alternatives Considered

| Option | Why considered | Why rejected / chose instead |
|---|---|---|
| Process all 100K in real time with autoscaled workers | "Throw more workers at it" | LLM TPM caps throughput regardless of worker count. Two-lane processing accepts deferral as a real outcome. |
| Single queue with a priority field | Less infrastructure | Celery priorities on a Redis broker are advisory under load. Two physical queues give hard isolation. |
| LLM-based pre-classifier | Better on Hinglish | An LLM call to decide whether to make an LLM call is self-defeating. Keyword heuristic + dialler hint instead. |
| Redis Streams instead of Celery | More durable than Redis lists | Still Redis-only. A Redis loss still loses work. Postgres outbox + Celery as transport. |
| Strict per-customer rate limiting (no overflow) | Simple, hard isolation | Wastes platform headroom when a customer is idle. Reserved + shared overflow pool. |
| Synchronous LLM call from the API endpoint | Simpler control flow | Exotel's 5-second timeout vs. p99 LLM latency of 3.5s+ rules this out. Async outbox + workers. |
| Replace Celery with a custom worker | Cleaner architecture | Time cost too high for the scope. Celery semantics are fine when backed by a durable outbox. |

---

## 14. Known Weaknesses

The keyword pre-classifier will mis-route some calls, particularly Hinglish edge cases. I leaned on the asymmetric cost (demotion is safe, promotion is capped) instead of trying to be clever in v1.

Token estimation is approximate. Refund handles the over-estimate case. An under-estimate can briefly push a customer over budget for one window.

The Postgres outbox adds 2–3 extra writes per interaction. Worth it for durability, but I'd want to measure the actual overhead under campaign load before declaring victory.

DLQ replay is a CLI, not a UI. Functional but not pleasant for ops.

The recording reconciler runs every 5 minutes, so late recordings can be up to 5 minutes behind in S3.

The audit table grows unbounded. It needs partitioning and a 90-day retention policy before this hits production.

What I designed but didn't get to implement:

- The Postgres outbox **schema** is in place, but the worker still uses Celery + the legacy `retry_queue` for non-rate-limit errors. AC3 is therefore partial.
- Hot/cold queue routing is in the design; the endpoint still enqueues to one queue.
- The binary `circuit_breaker` is untouched. AC7 (gradual backpressure) is design-only.
- The rate limiter uses an injectable `get_budget` callable. I didn't wire it to read from `customer_budgets` at runtime; the default returns 5% of platform TPM as a fallback.
- Audit events go to stdout JSON. The `postcall_audit` table exists but nothing writes to it from code yet.
- No unit test for the recording poller. The logic is straightforward and I traced it manually, but AC4 is technically a unit-test AC.

---

## 15. What I Would Do With More Time

In rough priority order:

1. Wire the worker to claim from `postcall_jobs` (closes AC3).
2. Add the recording poller test (closes AC4) and an integration test for hot/cold routing.
3. Real Prometheus metrics + Grafana dashboards for TPM headroom, hot-lane SLA, DLQ growth.
4. A small local classifier (DistilBERT-class) for the Hinglish ambiguous cases.
5. DLQ admin UI with bulk replay.
6. Multi-provider LLM failover so we're not pinned to one vendor's TPM ceiling.
7. End-to-end load harness — 100K calls with realistic transcript distribution from the fixtures plus synthetic.
