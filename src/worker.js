
import "dotenv/config";

import { pool } from "./database/db.js";
import { redisConnection } from "./config/redis.js";
import { initDatabase } from "./database/init-db.js";

let webhookWorker = null;
let shuttingDown = false;
let shutdownPromise = null;

async function startWorker() {
  try {
    // 1. Verify PostgreSQL connection.
    await pool.query("SELECT 1");
    console.log("PostgreSQL connected.");

    // 2. Initialize database tables and migrations.
    await initDatabase();
    console.log("Database initialized.");

    // 3. Verify Redis connection.
    await redisConnection.ping();
    console.log("Redis connected.");

    // 4. Start worker only after dependencies are ready.
    const workerModule = await import("./workers/webhook.worker.js");
    webhookWorker = workerModule.default;

    console.log("Webhook worker started successfully.");

  } catch (error) {
    console.error("Worker startup failed:", error);
    process.exitCode = 1;
    await shutdown("STARTUP_ERROR");
  }
}

async function shutdown(signal) {
  if (shutdownPromise) return shutdownPromise;

  shuttingDown = true;

  shutdownPromise = (async () => {
    console.log(`${signal} received. Shutting down worker...`);

    let shutdownFailed = false;

    try {
      // 1. Stop processing jobs and wait for active jobs.
      if (webhookWorker) {
        await webhookWorker.close();
        console.log("BullMQ worker closed.");
      }
    } catch (error) {
      shutdownFailed = true;
      console.error("Worker close failed:", error);
    }

    try {
      // 2. Close PostgreSQL.
      await pool.end();
      console.log("PostgreSQL pool closed.");
    } catch (error) {
      shutdownFailed = true;
      console.error("PostgreSQL shutdown failed:", error);
    }

    try {
      // 3. Close Redis.
      if (redisConnection.status !== "end") {
        await redisConnection.quit();
      }

      console.log("Redis connection closed.");
    } catch (error) {
      shutdownFailed = true;
      console.error("Redis shutdown failed:", error);
    }

    if (shutdownFailed) {
      console.error("Worker shutdown completed with errors.");
      process.exitCode = 1;
    } else {
      console.log("Worker shut down gracefully.");
      process.exitCode = 0;
    }
  })();

  return shutdownPromise;
}

process.once("SIGTERM", () => {
  void shutdown("SIGTERM");
});

process.once("SIGINT", () => {
  void shutdown("SIGINT");
});

// Start the worker.
await startWorker();
