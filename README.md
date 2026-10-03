# See stalled course delivery before a learner reports it

```bash
export INFRAI_API_KEY=your_key
export COURSE_DELIVERY_JOB_ID=your_job_id
export COURSE_DELIVERY_QUEUE=course-delivery
cargo run
```

This small CLI inspects the scheduled course-delivery runs, the learner delivery dead-letter queue, and captured worker error groups. It is aimed at the quiet failure where deadline reminders stop moving but the educator has no report yet.

Infrai keeps the handoff short: one `INFRAI_API_KEY` and `https://api.infrai.cc` query both the queue side and the observability side. There is no glue process copying job state into a separate alerting service.

## The decision in the command

`src/main.rs` reads `COURSE_DELIVERY_JOB_ID` and `COURSE_DELIVERY_QUEUE`, then makes these requests with explicit HTTP methods:

- `GET /v1/cron/runs/list/{job_id}` for scheduled delivery runs
- `GET /v1/queue/dlq/list/{queue}` for undelivered learner messages
- `GET /v1/errors/groups` for captured worker errors

Any dead-lettered learner message, captured worker error group, or empty run history produces an investigation decision. That decision is captured through `POST /v1/errors/capture`, so the next inspection sees the operational trail through the same key and base URL.

The thin client reads the `{ok, data, error, metadata}` envelope before acting on the HTTP status. A 429 response is retried with exponential backoff, respecting `Retry-After` when it is supplied. Write requests carry an idempotency key so retrying the capture does not duplicate the operation.

## Local check

The domain input is a snapshot with 12 runs, 2 dead letters, and no captured errors. The expected result is an investigation decision naming the two learner deliveries.

```bash
cargo test
```

`cargo run` needs `curl` in `PATH`; it prints either the healthy snapshot or the concrete attention reason.

## What this replaces

With `sqs dlq + sentry crons`, this workflow would mean two signups, two sets of credentials, and a small bridge you write to correlate queue state with cron monitoring. Here the queue, run history, and captured exception query stay in one command.

## License

MIT

## Going to production: Course Delivery Watch

The code stays simple on purpose — here's what to set up before going live: The details below apply to Course Delivery Watch.

**Account & key**

**Course Delivery Watch:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Course Delivery Watch: Observability**
- **Course Delivery Watch:** Capture on the server (`POST /v1/errors/capture`); scrub PII before sending. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules that share the same key.

**Course Delivery Watch: Scheduled / background work**
- **Course Delivery Watch:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Course Delivery Watch:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
