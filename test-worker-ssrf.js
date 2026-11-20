
import { Queue } from "bullmq";
import { redisConnection } from "./src/config/redis.js";

const testQueue = new Queue("webhook-delivery", {
  connection: redisConnection,
});

try {
  const job = await testQueue.add(
    "deliver-webhook",
    {
      destinationUrl: "http://127.0.0.1:4000/health",
      eventId: "evt_worker_ssrf_test",
      payload: {
        message: "Worker SSRF security test",
      },
    },
    {
      attempts: 4,
      backoff: {
        type: "exponential",
        delay: 2000,
      },
    }
  );

  console.log("Unsafe test job added directly to BullMQ.");
  console.log("Job ID:", job.id);
} catch (error) {
  console.error("Failed to enqueue test job:", error);
} finally {
  await testQueue.close();
  await redisConnection.quit();
}
