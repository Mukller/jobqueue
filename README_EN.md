# jobqueue

Embedded background job queue for Go. Stores jobs in SQLite — no Redis, no RabbitMQ.

## Quick start

```go
q, _ := jobqueue.New("jobs.db", jobqueue.Options{Workers: 4})
q.Register("email", func(ctx context.Context, payload []byte) error {
    return sendEmail(payload)
})
q.Start()
q.Enqueue(ctx, jobqueue.Job{Type: "email", Payload: data, Priority: 10})
```
