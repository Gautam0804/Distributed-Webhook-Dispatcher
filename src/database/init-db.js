
import { pool } from "./db.js";

export async function initDatabase() {
  // 1. Store accepted webhook events.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS webhook_events (
      id BIGSERIAL PRIMARY KEY,
      event_id TEXT NOT NULL,
      destination_url TEXT NOT NULL,
      job_id TEXT NOT NULL UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (event_id, destination_url)
    );
  `);

  // 2. Store pending jobs before publishing to Redis.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS webhook_outbox (
      id BIGSERIAL PRIMARY KEY,
      job_id TEXT NOT NULL UNIQUE
        REFERENCES webhook_events(job_id) ON DELETE CASCADE,
      payload JSONB NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'published')),
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      claimed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      published_at TIMESTAMPTZ
    );
  `);

  // 3. Migrate existing outbox tables.
  await pool.query(`
    ALTER TABLE webhook_outbox
      ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

    ALTER TABLE webhook_outbox
      DROP CONSTRAINT IF EXISTS webhook_outbox_status_check;

    ALTER TABLE webhook_outbox
      ADD CONSTRAINT webhook_outbox_status_check
      CHECK (status IN ('pending', 'processing', 'published'));
  `);

  // 4. Outbox indexes.
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_webhook_outbox_pending
      ON webhook_outbox(created_at)
      WHERE status = 'pending';

    CREATE INDEX IF NOT EXISTS idx_webhook_outbox_processing
      ON webhook_outbox(claimed_at)
      WHERE status = 'processing';
  `);

  // 5. Store every webhook delivery attempt.
  await pool.query(`
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
      request_started_at TIMESTAMPTZ,
      attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

      UNIQUE (job_id, attempt_number)
    );
  `);

  // 6. Migrate existing delivery attempt tables.
  await pool.query(`
    ALTER TABLE webhook_delivery_attempts
      ADD COLUMN IF NOT EXISTS request_started_at TIMESTAMPTZ;
  `);

  // 7. Delivery attempt indexes.
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_webhook_attempts_event_id
      ON webhook_delivery_attempts(event_id);

    CREATE INDEX IF NOT EXISTS idx_webhook_attempts_job_id
      ON webhook_delivery_attempts(job_id);

    CREATE INDEX IF NOT EXISTS idx_webhook_attempts_request_started_at
      ON webhook_delivery_attempts(request_started_at);
  `);

  console.log("Database tables and migrations initialized successfully!");
}
