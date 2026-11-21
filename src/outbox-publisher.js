
import "dotenv/config";

import { publishPendingOutboxBatch } from "./services/outbox-publisher.service.js";
import { pool } from "./database/db.js";
import { redisConnection } from "./config/redis.js";
import { webhookQueue } from "./queues/webhook.queue.js";
import { initDatabase } from "./database/init-db.js";

const POLL_INTERVAL_MS = 2000;
const BATCH_SIZE = 10;
const LOCK_ID = 741852;

let isShuttingDown = false;
let shutdownPromise = null;
let publisherPromise = null;

let lockClient = null;
let lockAcquired = false;
let lockReleased = false;
let lockClientReleased = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function startPublisher() {
  console.log("Outbox publisher started...");

  while (!isShuttingDown) {
    try {
      const publishedCount =
        await publishPendingOutboxBatch(BATCH_SIZE);

      if (publishedCount > 0) {
        console.log(
          `Published ${publishedCount} outbox record(s).`
        );
      }
    } catch (error) {
      console.error("Outbox publisher error:", error.message);
    }

    if (!isShuttingDown) {
      await sleep(POLL_INTERVAL_MS);
    }
  }

  console.log("Outbox publisher loop stopped.");
}

function releaseLockClient(destroy = false) {
  if (!lockClient || lockClientReleased) return;

  lockClientReleased = true;
  lockClient.release(destroy);
}

async function shutdown(signal) {
  if (shutdownPromise) return shutdownPromise;

  isShuttingDown = true;

  shutdownPromise = (async () => {
    console.log(
      `${signal} received. Shutting down publisher...`
    );

    let shutdownFailed = false;

    try {
      if (publisherPromise) {
        await publisherPromise;
      }

      if (lockAcquired && !lockReleased && lockClient) {
        const result = await lockClient.query(
          "SELECT pg_advisory_unlock($1) AS released",
          [LOCK_ID]
        );

        lockReleased = result.rows[0].released;

        console.log(
          lockReleased
            ? "PostgreSQL advisory lock released."
            : "Advisory lock was not held by this session."
        );
      }
    } catch (error) {
      shutdownFailed = true;
      console.error("Publisher shutdown error:", error);
    } finally {
      releaseLockClient(
        shutdownFailed || (lockAcquired && !lockReleased)
      );
    }

    try {
      await webhookQueue.close();
      console.log("BullMQ queue closed.");
    } catch (error) {
      shutdownFailed = true;
      console.error("BullMQ shutdown failed:", error);
    }

    try {
      await pool.end();
      console.log("PostgreSQL pool closed.");
    } catch (error) {
      shutdownFailed = true;
      console.error("PostgreSQL shutdown failed:", error);
    }

    try {
      if (redisConnection.status !== "end") {
        await redisConnection.quit();
      }

      console.log("Redis connection closed.");
    } catch (error) {
      shutdownFailed = true;
      console.error("Redis shutdown failed:", error);
    }

    if (shutdownFailed) {
      console.error("Publisher shutdown completed with errors.");
      process.exitCode = 1;
    } else {
      console.log("Publisher shut down gracefully.");
      process.exitCode = 0;
    }
  })();

  return shutdownPromise;
}

async function start() {
  try {
    // 1. Verify PostgreSQL connection.
    await pool.query("SELECT 1");
    console.log("PostgreSQL connected.");

    // 2. Initialize database.
    await initDatabase();
    console.log("Database initialized.");

    // 3. Verify Redis connection.
    await redisConnection.ping();
    console.log("Redis connected.");

    // 4. Register shutdown handlers before waiting for the lock.
    process.once("SIGINT", () => {
      void shutdown("SIGINT");
    });

    process.once("SIGTERM", () => {
      void shutdown("SIGTERM");
    });

    // 5. Acquire a dedicated PostgreSQL connection.
    lockClient = await pool.connect();

    // 6. Wait until the advisory lock becomes available.
    while (!isShuttingDown && !lockAcquired) {
      const result = await lockClient.query(
        "SELECT pg_try_advisory_lock($1) AS acquired",
        [LOCK_ID]
      );

      lockAcquired = result.rows[0].acquired;

      if (!lockAcquired) {
        console.log(
          "Another publisher is running. Retrying in 2 seconds..."
        );

        await sleep(POLL_INTERVAL_MS);
      }
    }

    // If shutdown was requested while waiting, exit cleanly.
    if (isShuttingDown) {
      return;
    }

    console.log("Publisher advisory lock acquired.");

    // 7. Start publisher loop.
    publisherPromise = startPublisher();

    await publisherPromise;
  } catch (error) {
    console.error("Publisher startup failed:", error);

    if (lockAcquired && !lockReleased && lockClient) {
      try {
        const result = await lockClient.query(
          "SELECT pg_advisory_unlock($1) AS released",
          [LOCK_ID]
        );

        lockReleased = result.rows[0].released;
      } catch (unlockError) {
        console.error(
          "Could not release advisory lock:",
          unlockError
        );
      }
    }

    releaseLockClient(
      lockAcquired && !lockReleased
    );

    await webhookQueue.close().catch(() => {});
    await pool.end().catch(() => {});
    await redisConnection.quit().catch(() => {});

    process.exitCode = 1;
  }
}

await start();
