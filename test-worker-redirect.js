
import { Queue } from "bullmq";
import { redisConnection } from "./src/config/redis.js";

const queue = new Queue("webhook-delivery", {
  connection: redisConnection,
});

try {
  const job = await queue.add("deliver-webhook", {
    destinationUrl:
      "https://httpbingo.org/redirect-to?url=https://example.com&status_code=302",
    eventId: "evt_worker_redirect_final",
    payload: {
      message: "Testing redirect protection",
    },
  });

  console.log("Redirect test job added.");
  console.log("Job ID:", job.id);
} catch (error) {
  console.error(error);
} finally {
  await queue.close();
  await redisConnection.quit();
}
