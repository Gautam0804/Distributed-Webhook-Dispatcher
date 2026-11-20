
import { Queue } from "bullmq";
import { redisConnection } from "../config/redis.js";

export const webhookQueue = new Queue("webhook-delivery", {
  connection: redisConnection,

  // Isolate this queue from other projects sharing Redis
  prefix: "webhook-dispatcher",

  defaultJobOptions: {
    // 1 initial attempt + 3 retries
    attempts: 4,

    // Retry delays: 2s, 4s, 8s
    backoff: {
      type: "exponential",
      delay: 2000,
    },

    // Keep recent completed and failed jobs
    removeOnComplete: 1000,
    removeOnFail: 5000,
  },
});
