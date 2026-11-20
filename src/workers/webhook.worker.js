
import { Worker, UnrecoverableError } from "bullmq";
import axios from "axios";

import { redisConnection } from "../config/redis.js";
import { pool } from "../database/db.js";
import { waitForDomainSlot } from "../services/rate-limiter.service.js";

import {
  resolveSafeDestination,
  UnsafeDestinationError,
} from "../services/url-security.service.js";

const webhookWorker = new Worker(
  "webhook-delivery",

  async (job) => {
    const { destinationUrl, eventId, payload } = job.data;

    const attemptNumber = job.attemptsMade + 1;
    const totalAttempts = job.opts.attempts || 1;

    const startTime = Date.now();

    // Will remain null if the HTTP request never starts.
    let requestStartedAt = null;

    console.log("--------------------------------");
    console.log(`Processing Event: ${eventId}`);
    console.log(`Job ID: ${job.id}`);
    console.log(`Attempt: ${attemptNumber}/${totalAttempts}`);

    try {
      // 1. Apply per-domain rate limiting.
      await waitForDomainSlot(destinationUrl);

      // 2. Validate URL and resolve a safe public IP.
      const safeDestination =
        await resolveSafeDestination(destinationUrl);

      // 3. Capture the exact HTTP request start time.
      requestStartedAt = new Date();

      console.log(
        `HTTP request started at: ${requestStartedAt.toISOString()}`
      );

      // 4. Send webhook to the validated destination.
      const response = await axios.post(
        safeDestination.url.href,
        payload,
        {
          timeout: 5000,
          maxRedirects: 0,
          proxy: false,
          lookup: safeDestination.lookup,
          headers: {
            "Content-Type": "application/json",
            "X-Event-ID": eventId,
          },
        }
      );

      const duration = Date.now() - startTime;

      // 5. Record successful delivery.
      await pool.query(
        `INSERT INTO webhook_delivery_attempts
          (
            job_id,
            event_id,
            destination_url,
            attempt_number,
            status,
            response_status,
            duration_ms,
            request_started_at
          )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (job_id, attempt_number)
         DO UPDATE SET
           status = EXCLUDED.status,
           response_status = EXCLUDED.response_status,
           error_message = NULL,
           duration_ms = EXCLUDED.duration_ms,
           request_started_at = EXCLUDED.request_started_at,
           attempted_at = NOW()`,
        [
          String(job.id),
          eventId,
          destinationUrl,
          attemptNumber,
          "success",
          response.status,
          duration,
          requestStartedAt,
        ]
      );

      console.log("Delivery successful");
      console.log(`HTTP Status: ${response.status}`);
      console.log(`Duration: ${duration}ms`);

      return {
        success: true,
        statusCode: response.status,
      };

    } catch (error) {
      const duration = Date.now() - startTime;
      const statusCode = error.response?.status;

      // 6. Identify security errors using the custom error class.
      const isSecurityError =
        error instanceof UnsafeDestinationError;

      // 7. Classify permanent HTTP errors.
      const isPermanentHttpError =
        statusCode >= 400 &&
        statusCode < 500 &&
        statusCode !== 429;

      const isPermanentError =
        isSecurityError || isPermanentHttpError;

      const isFinalAttempt =
        attemptNumber >= totalAttempts;

      const attemptStatus =
        isPermanentError || isFinalAttempt
          ? "failed"
          : "retrying";

      // 8. Record failed or retrying attempt.
      await pool.query(
        `INSERT INTO webhook_delivery_attempts
          (
            job_id,
            event_id,
            destination_url,
            attempt_number,
            status,
            response_status,
            error_message,
            duration_ms,
            request_started_at
          )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (job_id, attempt_number)
         DO UPDATE SET
           status = EXCLUDED.status,
           response_status = EXCLUDED.response_status,
           error_message = EXCLUDED.error_message,
           duration_ms = EXCLUDED.duration_ms,
           request_started_at = EXCLUDED.request_started_at,
           attempted_at = NOW()`,
        [
          String(job.id),
          eventId,
          destinationUrl,
          attemptNumber,
          attemptStatus,
          statusCode || null,
          error.message,
          duration,
          requestStartedAt,
        ]
      );

      console.error("Delivery failed");
      console.error(`Status: ${attemptStatus}`);
      console.error(`HTTP Status: ${statusCode || "No response"}`);
      console.error(`Error: ${error.message}`);

      // 9. Stop retries for permanent errors.
      if (isPermanentError) {
        throw new UnrecoverableError(
          isSecurityError
            ? `Unsafe destination: ${error.message}`
            : `Permanent HTTP error: ${statusCode}`
        );
      }

      // 10. Allow BullMQ to retry transient errors.
      throw error;
    }
  },

  {
    connection: redisConnection,
    concurrency: 15,
  }
);

// Worker event listeners.
webhookWorker.on("completed", (job) => {
  console.log(`Job ${job.id} completed successfully`);
});

webhookWorker.on("failed", (job, error) => {
  console.error(`Job ${job?.id} failed: ${error.message}`);
});

webhookWorker.on("error", (error) => {
  console.error("Worker error:", error);
});

console.log("Webhook worker is running");

export default webhookWorker;
