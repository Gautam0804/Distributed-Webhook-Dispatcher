
import "dotenv/config";
import webhookWorker from "./workers/webhook.worker.js";
import { pool } from "./database/db.js";
import { redisConnection } from "./config/redis.js";

let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`${signal} received. Shutting down worker...`);

  try {
    await webhookWorker.close();
    console.log("BullMQ worker closed.");

    await pool.end();
    console.log("PostgreSQL pool closed.");

    await redisConnection.quit();
    console.log("Redis connection closed.");

    console.log("Worker shut down gracefully.");
    process.exitCode = 0;
  } catch (error) {
    console.error("Worker shutdown failed:", error);
    process.exitCode = 1;
  }
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
