[Русский](README.md)

# jobqueue

Embedded background job queue for Go. Stores jobs in SQLite —
no Redis, no RabbitMQ. Good for small services and self-hosted apps.

## Install

```bash
go get github.com/Mukller/jobqueue
```

## Quick start

```go
q, _ := jobqueue.New("jobs.db", jobqueue.Options{Workers: 4})
defer q.Close()

q.Register("email", func(ctx context.Context, payload []byte) error {
    var data EmailPayload
    json.Unmarshal(payload, &data)
    return sendEmail(data)
})

q.Start()

q.Enqueue(ctx, jobqueue.Job{
    Type:     "email",
    Payload:  mustJSON(EmailPayload{To: "user@example.com"}),
    Priority: 10,
})
```

## Features

- Persistent jobs in SQLite (survive restarts)
- Priorities (0–100, lower = higher priority)
- Automatic retries with exponential backoff
- Concurrent workers
- Delayed execution (`RunAt`)
- Per-job timeout and deadline
- HTTP API for monitoring

## HTTP monitoring

```bash
curl http://localhost:8081/jobs
curl -X POST http://localhost:8081/retry-stalled
```
