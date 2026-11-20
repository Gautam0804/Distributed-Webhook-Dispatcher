
import "dotenv/config";

import app from "./app.js";
import { pool } from "./database/db.js";
import { redisConnection } from "./config/redis.js";

const PORT = process.env.PORT || 4000;

const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

let isShuttingDown = false;
let shutdownPromise = null;

async function shutdown(signal) {
  if (shutdownPromise) {
    return shutdownPromise;
  }

  isShuttingDown = true;

  shutdownPromise = (async () => {
    console.log(`${signal} received. Shutting down server...`);

    try {
      // Stop accepting new HTTP connections and wait for existing requests.
      await new Promise((resolve, reject) => {
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });

      console.log("HTTP server closed.");

      // Close PostgreSQL connection pool.
      await pool.end();
      console.log("PostgreSQL pool closed.");

      // Close Redis connection.
      await redisConnection.quit();
      console.log("Redis connection closed.");

      console.log("Server shut down gracefully.");
      process.exitCode = 0;
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

server.on("error", (error) => {
  console.error("HTTP server error:", error);
  process.exitCode = 1;
});
