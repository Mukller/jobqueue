# jobqueue

Встроенная очередь фоновых задач для Go-приложений. Хранит задачи в SQLite — никаких внешних зависимостей вроде Redis или RabbitMQ. Подходит для небольших сервисов, CLI-инструментов, self-hosted приложений.

## Что умеет

- Персистентные задачи в SQLite (переживают перезапуски)
- Приоритеты (0–100, меньше = выше приоритет)
- Автоматические повторы с экспоненциальной задержкой
- Параллельные воркеры (настраиваемо)
- Отложенный запуск (`RunAt`)
- Простой HTTP API для мониторинга
- Таймаут и дедлайн на уровне задачи

## Установка

```bash
go get github.com/you/jobqueue
```

## Быстрый старт

```go
q, _ := jobqueue.New("jobs.db", jobqueue.Options{Workers: 4})
defer q.Close()

// Регистрируем обработчик
q.Register("email", func(ctx context.Context, payload []byte) error {
    var data EmailPayload
    json.Unmarshal(payload, &data)
    return sendEmail(data)
})

q.Start()

// Ставим задачу в очередь
q.Enqueue(ctx, jobqueue.Job{
    Type:     "email",
    Payload:  mustJSON(EmailPayload{To: "user@example.com"}),
    Priority: 10,
})
```

## HTTP мониторинг

```bash
# Статус очереди
curl http://localhost:8081/jobs

# Принудительный запуск застрявших задач
curl -X POST http://localhost:8081/retry-stalled
```
