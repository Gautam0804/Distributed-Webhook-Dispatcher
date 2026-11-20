# Distributed Webhook Dispatcher

A reliable, asynchronous webhook delivery system built with Node.js, Express, Redis, BullMQ, and PostgreSQL. It accepts webhook events through a REST API, stores them using a transactional outbox pattern, and delivers them asynchronously with automatic retries, per-domain rate limiting, and delivery attempt tracking.

## Features

- **Webhook Ingestion API:** Accepts and validates webhook events through a REST endpoint.
- **Asynchronous Processing:** Uses BullMQ and Redis to process webhook deliveries in the background.
- **Transactional Outbox Pattern:** Persists webhook events and publishing records in PostgreSQL to improve reliability.
- **Automatic Retries:** Retries transient delivery failures up to 3 times using exponential backoff (2s, 4s, and 8s).
- **Error Handling:** Handles HTTP 5xx responses, HTTP 429 rate limits, timeouts, and permanent HTTP errors.
- **Per-Domain Rate Limiting:** Uses an atomic Redis sliding-window algorithm to limit requests to 5 per second per destination domain.
- **Idempotency:** Prevents duplicate event-and-destination combinations from being accepted as new events.
- **Delivery Attempt Logging:** Records attempt number, HTTP status, error details, request start time, and delivery duration.
- **URL Security:** Validates destination URLs and checks DNS resolution to help prevent SSRF attacks.
- **Graceful Shutdown:** Supports controlled shutdown of the API server, worker, and outbox publisher.

## Tech Stack

| Technology | Purpose |
|---|---|
| Node.js | JavaScript runtime |
| Express.js | REST API |
| Redis | Queue infrastructure and rate limiting |
| BullMQ | Background job processing |
| PostgreSQL | Persistent storage |
| Axios | HTTP webhook delivery |
| Zod | Request validation |
| ipaddr.js | IP address validation |
| Docker | Local infrastructure |

## Architecture

```text
                 Client
                   |
                   v
          POST /api/webhooks/dispatch
                   |
                   v
             Express API
                   |
             Zod Validation
                   |
             URL Security
                   |
                   v
             PostgreSQL
             Transaction
              /       \
             v         v
       webhook_events  webhook_outbox
                           |
                           v
                   Outbox Publisher
                           |
                           v
                     Redis / BullMQ
                           |
                           v
                    Background Worker
                           |
                    Domain Rate Limiter
                           |
                    URL / DNS Validation
                           |
                           v
                   Destination Webhook
                           |
                           v
                 Delivery Attempt Logs
                      PostgreSQL
```

## Project Structure

```text
distributed-webhook-dispatcher/
│
├── src/
│   ├── config/
│   │   ├── env.js
│   │   └── redis.js
│   │
│   ├── controllers/
│   │   └── webhook.controller.js
│   │
│   ├── database/
│   │   ├── db.js
│   │   └── init-db.js
│   │
│   ├── queues/
│   │   └── webhook.queue.js
│   │
│   ├── routes/
│   │   └── webhook.routes.js
│   │
│   ├── schemas/
│   │   └── webhook.schema.js
│   │
│   ├── services/
│   │   ├── webhook.service.js
│   │   ├── rate-limiter.service.js
│   │   ├── url-security.service.js
│   │   └── outbox-publisher.service.js
│   │
│   ├── workers/
│   │   └── webhook.worker.js
│   │
│   ├── app.js
│   ├── server.js
│   ├── worker.js
│   └── outbox-publisher.js
│
├── .env
├── .env.example
├── .gitignore
├── package.json
└── README.md
```

## Prerequisites

Install the following:

- Node.js 20 or later
- npm
- Docker Desktop
- Git

## Installation

Clone the repository:

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd distributed-webhook-dispatcher
```

Install dependencies:

```bash
npm install
```

## Environment Configuration

Create a `.env` file in the project root.

```env
PORT=4000

REDIS_HOST=127.0.0.1
REDIS_PORT=6381

DB_HOST=127.0.0.1
DB_PORT=5435
DB_USER=webhook_admin
DB_PASSWORD=your_local_database_password
DB_NAME=webhook_dispatcher
```

Never commit your `.env` file or production credentials.

## Start Infrastructure

Start Redis and PostgreSQL using Docker.

Example local containers:

```bash
docker run -d \
  --name webhook-redis \
  -p 6381:6379 \
  redis:7-alpine
```

```bash
docker run -d \
  --name webhook-postgres \
  -p 5435:5432 \
  -e POSTGRES_USER=webhook_admin \
  -e POSTGRES_PASSWORD=your_local_database_password \
  -e POSTGRES_DB=webhook_dispatcher \
  -v webhook-postgres-data:/var/lib/postgresql/data \
  postgres:16
```

Initialize the database:

```bash
node src/database/init-db.js
```

## Running the Application

The API server, worker, and outbox publisher run as separate processes.

**Terminal 1 — API Server**

```bash
node src/server.js
```

API runs at:

```text
http://localhost:4000
```

**Terminal 2 — Background Worker**

```bash
node src/worker.js
```

**Terminal 3 — Outbox Publisher**

```bash
node src/outbox-publisher.js
```

All three processes must be running for complete webhook processing.

## API Documentation

### Health Check

**GET** `/health`

Example:

```bash
curl http://localhost:4000/health
```

### Dispatch Webhook

**POST** `/api/webhooks/dispatch`

Request body:

```json
{
  "destinationUrl": "https://httpbin.org/post",
  "eventId": "event-001",
  "payload": {
    "message": "Hello from Webhook Dispatcher",
    "userId": 123,
    "event": "user.created"
  }
}
```

Successful response:

```json
{
  "success": true,
  "message": "Webhook accepted and saved to outbox",
  "jobId": "generated-job-id",
  "status": "pending",
  "duplicate": false
}
```

HTTP status:

```text
202 Accepted
```

### Validation Rules

- `destinationUrl` must be a valid URL.
- `eventId` must be a non-empty string.
- `payload` must be a JSON object.
- Unsafe or invalid destinations are rejected.

## Retry Mechanism

BullMQ handles retries for transient delivery failures.

| Attempt | Retry Delay |
|---|---:|
| Initial attempt | Immediate |
| Retry 1 | 2 seconds |
| Retry 2 | 4 seconds |
| Retry 3 | 8 seconds |

Total maximum attempts: **4** (one initial attempt plus three retries).

### Error Handling

| Error | Behavior |
|---|---|
| HTTP 2xx | Delivery successful |
| HTTP 4xx except 429 | Permanent failure |
| HTTP 429 | Retry |
| HTTP 5xx | Retry |
| Request timeout | Retry |
| Network error | Retry |
| Unsafe destination | Permanent failure |

Permanent errors are marked using BullMQ's `UnrecoverableError`, preventing unnecessary retries.

## Rate Limiting

The application implements a Redis-backed sliding-window rate limiter.

Configuration:

```javascript
const WINDOW_MS = 1000;
const MAX_REQUESTS = 5;
```

This enforces a maximum of five reserved request starts per destination domain in any rolling one-second window.

Key characteristics:

- Domain-specific Redis keys.
- Atomic Lua-script execution.
- Redis server time for consistent coordination.
- Shared rate-limit state across workers using the same Redis instance.
- Retries also pass through the rate limiter.

## Database Design

### webhook_events

Stores accepted webhook events and their unique job identifiers.

### webhook_outbox

Stores pending publishing records and tracks publishing state.

### webhook_delivery_attempts

Stores delivery attempt information, including:

- Job ID
- Event ID
- Destination URL
- Attempt number
- Delivery status
- HTTP response status
- Error message
- Request start timestamp
- Delivery duration
- Attempt timestamp

## Idempotency

The application uses a unique database constraint on:

```text
(event_id, destination_url)
```

Repeated submissions with the same event ID and destination URL return the existing job ID rather than creating a new webhook event.

## Testing

The implementation has been tested locally with the following scenarios:

| Test Scenario | Result |
|---|---|
| Successful webhook delivery | Passed |
| Duplicate event submission | Passed |
| HTTP 400 permanent error | Passed |
| HTTP 500 retry handling | Passed |
| HTTP 429 retry handling | Passed |
| Request timeout handling | Passed |
| Concurrent webhook submissions | Passed |
| Per-domain sliding-window rate limiting | Passed |
| Delivery attempt logging | Passed |
| Graceful shutdown | Passed |

### Concurrency Test Results

A test with 15 concurrent webhook jobs produced:

| Metric | Result |
|---|---:|
| Submitted jobs | 15 |
| Total delivery attempts | 24 |
| Successful deliveries | 15 |
| Permanently failed attempts | 0 |
| HTTP 429 responses | 9 |
| Maximum recorded request starts in a rolling second | 5 |

All 15 jobs eventually succeeded after retry handling.

## Security Considerations

The implementation includes:

- HTTP and HTTPS protocol validation.
- Rejection of URLs containing credentials.
- Blocking of selected private and reserved hostnames.
- DNS resolution and public IP validation.
- DNS-pinned HTTP requests.
- Redirects disabled for webhook delivery.
- Proxy use disabled for webhook delivery.

Additional production hardening, including comprehensive IP-range blocking and network-level egress restrictions, should be considered before exposing the service publicly.

## License

This project is developed as a software engineering assignment and learning project.
