
import { randomUUID } from "node:crypto";
import { redisConnection } from "../config/redis.js";

const WINDOW_MS = 1000;
const MAX_REQUESTS = 5;

// Atomic sliding-window reservation using Redis sorted sets.
const slidingWindowScript = `
  local key = KEYS[1]

  local window = tonumber(ARGV[1])
  local limit = tonumber(ARGV[2])
  local token = ARGV[3]

  -- Use Redis server time, not application server time.
  local time = redis.call("TIME")

  local now = (tonumber(time[1]) * 1000)
              + math.floor(tonumber(time[2]) / 1000)

  -- Remove reservations outside the rolling window.
  redis.call(
    "ZREMRANGEBYSCORE",
    key,
    "-inf",
    now - window
  )

  local count = redis.call("ZCARD", key)

  -- Reserve immediately if capacity is available.
  if count < limit then
    redis.call("ZADD", key, now, token)
    redis.call("PEXPIRE", key, window * 2)

    return 0
  end

  -- Find the oldest reservation.
  local oldest = redis.call(
    "ZRANGE",
    key,
    0,
    0,
    "WITHSCORES"
  )

  local oldestTime = tonumber(oldest[2])

  -- Wait until the oldest reservation leaves the window.
  local waitTime = oldestTime + window - now + 1

  return math.max(1, waitTime)
`;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function waitForDomainSlot(destinationUrl) {
  const domain = new URL(destinationUrl).hostname.toLowerCase();

  // Dedicated namespace for Distributed Webhook Dispatcher.
  const redisKey = `webhook-dispatcher:rate-limit:sliding:${domain}`;

  // Unique identifier for this reservation.
  const token = randomUUID();

  while (true) {
    const waitTime = await redisConnection.eval(
      slidingWindowScript,
      1,
      redisKey,
      WINDOW_MS,
      MAX_REQUESTS,
      token
    );

    if (waitTime === 0) {
      console.log(`Rate limit slot acquired: ${domain}`);
      return;
    }

    console.log(
      `Rate limit reached for ${domain}. Waiting ${waitTime}ms`
    );

    await sleep(waitTime);
  }
}
