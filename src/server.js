
import "dotenv/config";

import app from "./app.js";
import { pool } from "./database/db.js";
import { redisConnection } from "./config/redis.js";
import { initDatabase } from "./database/init-db.js";

const PORT = Number(process.env.PORT) || 4000;

let server;
let isShuttingDown = false;
let shutdownPromise = null;

async function startServer() {
  try {
    // 1. Verify PostgreSQL connection.
    await pool.query("SELECT 1");
    console.log("PostgreSQL connected.");

    // 2. Initialize database tables and indexes.
    await initDatabase();
    console.log("Database initialized.");

    // 3. Verify Redis connection.
    await redisConnection.ping();
    console.log("Redis connected.");

    // 4. Start HTTP server only after dependencies are ready.
    server = app.listen(PORT, "0.0.0.0", () => {
      console.log(`Server running on port ${PORT}`);
    });

    server.on("error", (error) => {
      console.error("HTTP server error:", error);
      void shutdown("SERVER_ERROR");
    });

  } catch (error) {
    console.error("Server startup failed:", error);
    await shutdown("STARTUP_ERROR");
    process.exitCode = 1;
  }
}

async function shutdown(signal) {
  if (shutdownPromise) return shutdownPromise;

  isShuttingDown = true;

  shutdownPromise = (async () => {
    console.log(`${signal} received. Shutting down server...`);

    try {
      // Stop accepting new requests.
      if (server?.listening) {
        await new Promise((resolve, reject) => {
          server.close((error) => {
            if (error) reject(error);
            else resolve();
          });
        });

        console.log("HTTP server closed.");
      }

      // Close PostgreSQL.
      await pool.end();
      console.log("PostgreSQL pool closed.");

      // Close Redis.
      if (redisConnection.status !== "end") {
        await redisConnection.quit();
      }

      console.log("Redis connection closed.");
      console.log("Server shut down gracefully.");

    } catch (error) {
      console.error("Server shutdown failed:", error);
      process.exitCode = 1;
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

await startServer();
