
import { pool } from "./db.js";

const createTablesQuery = `
  -- 1. Store accepted webhook events
  CREATE TABLE IF NOT EXISTS webhook_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    job_id TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (event_id, destination_url)
  );

  -- 2. Store pending jobs before publishing to Redis
  CREATE TABLE IF NOT EXISTS webhook_outbox (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE
      REFERENCES webhook_events(job_id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
      CHECK (status IN ('pending', 'published')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
  );

  -- Index for finding pending outbox records efficiently
  CREATE INDEX IF NOT EXISTS idx_webhook_outbox_pending
  ON webhook_outbox(created_at)
  WHERE status = 'pending';

  -- 3. Store every webhook delivery attempt
  CREATE TABLE IF NOT EXISTS webhook_delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,

    status VARCHAR(20) NOT NULL
      CHECK (status IN ('success', 'retrying', 'failed')),

    response_status INTEGER,
    error_message TEXT,
    duration_ms INTEGER,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (job_id, attempt_number)
  );

  -- Indexes for delivery attempt queries
  CREATE INDEX IF NOT EXISTS idx_webhook_attempts_event_id
  ON webhook_delivery_attempts(event_id);

  CREATE INDEX IF NOT EXISTS idx_webhook_attempts_job_id
  ON webhook_delivery_attempts(job_id);
`;

try {
  await pool.query(createTablesQuery);
  console.log("Database tables initialized successfully!");
} catch (error) {
  console.error("Database initialization failed:", error);
  process.exitCode = 1;
} finally {
  await pool.end();
}
