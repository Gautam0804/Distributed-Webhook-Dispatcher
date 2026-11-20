
import { pool } from "../database/db.js";
import { webhookQueue } from "../queues/webhook.queue.js";

const STALE_CLAIM_MINUTES = 2;

export async function publishPendingOutboxBatch(batchSize = 10) {
  let publishedCount = 0;

  for (let i = 0; i < batchSize; i++) {
    const client = await pool.connect();
    let record;

    try {
      await client.query("BEGIN");

      const result = await client.query(
        `
          SELECT id, job_id, payload
          FROM webhook_outbox
          WHERE status = 'pending'
             OR (
               status = 'processing'
               AND claimed_at < NOW() - ($1 * INTERVAL '1 minute')
             )
          ORDER BY created_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        `,
        [STALE_CLAIM_MINUTES]
      );

      if (result.rowCount === 0) {
        await client.query("COMMIT");
        break;
      }

      record = result.rows[0];

      await client.query(
        `
          UPDATE webhook_outbox
          SET
            status = 'processing',
            claimed_at = NOW()
          WHERE id = $1
        `,
        [record.id]
      );

      await client.query("COMMIT");

    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }

    try {
      await webhookQueue.add(
        "deliver-webhook",
        record.payload,
        { jobId: record.job_id }
      );

      const updateResult = await pool.query(
        `
          UPDATE webhook_outbox
          SET
            status = 'published',
            attempts = attempts + 1,
            last_error = NULL,
            claimed_at = NULL,
            published_at = NOW()
          WHERE id = $1
            AND status = 'processing'
        `,
        [record.id]
      );

      if (updateResult.rowCount === 1) {
        publishedCount++;

        console.log(
          `Outbox published: ${record.job_id}`
        );
      }

    } catch (error) {
      await pool.query(
        `
          UPDATE webhook_outbox
          SET
            status = 'pending',
            attempts = attempts + 1,
            last_error = $2,
            claimed_at = NULL
          WHERE id = $1
            AND status = 'processing'
        `,
        [record.id, error.message]
      );

      console.error(
        `Outbox publishing failed: ${record.job_id}`,
        error.message
      );
    }
  }

  return publishedCount;
}
