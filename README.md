[English](README_EN.md)

# jobqueue

Встроенная очередь фоновых задач для Go. Хранит задачи в SQLite —
никаких Redis, никаких RabbitMQ. Подходит для небольших сервисов и self-hosted.

## Установка

```bash
go get github.com/Mukller/jobqueue
```

## Быстрый старт

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

## Что умеет

- Персистентные задачи в SQLite (переживают перезапуски)
- Приоритеты (0–100, меньше = выше)
- Автоматические повторы с экспоненциальной задержкой
- Параллельные воркеры
- Отложенный запуск (`RunAt`)
- Таймаут и дедлайн на уровне задачи
- HTTP API для мониторинга

## HTTP мониторинг

```bash
curl http://localhost:8081/jobs
curl -X POST http://localhost:8081/retry-stalled
```
