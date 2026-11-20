
import { randomUUID } from "node:crypto";
import { pool } from "../database/db.js";

export async function enqueueWebhook(data) {
  const { eventId, destinationUrl } = data;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const jobId = randomUUID();

    // 1. Insert the event if it does not already exist.
    const insertResult = await client.query(
      `
        INSERT INTO webhook_events (
          event_id,
          destination_url,
          job_id
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (event_id, destination_url) DO NOTHING
        RETURNING job_id
      `,
      [eventId, destinationUrl, jobId]
    );

    // 2. Handle duplicate requests.
    if (insertResult.rowCount === 0) {
      const existingResult = await client.query(
        `
          SELECT job_id
          FROM webhook_events
          WHERE event_id = $1
            AND destination_url = $2
        `,
        [eventId, destinationUrl]
      );

      if (existingResult.rowCount === 0) {
        throw new Error("Existing webhook event could not be found");
      }

      const existingJobId = existingResult.rows[0].job_id;

      await client.query("COMMIT");

      return {
        jobId: existingJobId,
        status: "already_accepted",
        duplicate: true,
      };
    }

    // 3. Store the complete job payload in the outbox.
    await client.query(
      `
        INSERT INTO webhook_outbox (
          job_id,
          payload
        )
        VALUES ($1, $2::jsonb)
      `,
      [jobId, JSON.stringify(data)]
    );

    // 4. Commit both database inserts together.
    await client.query("COMMIT");

    return {
      jobId,
      status: "pending",
      duplicate: false,
    };

  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackError) {
      console.error("Transaction rollback failed:", rollbackError);
    }

    throw error;

  } finally {
    client.release();
  }
}
