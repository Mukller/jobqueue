# Contributing

## Как помочь

Принимаю PR с:
- Cron-выражениями для планировщика
- Dead letter queue
- Webhooks при завершении задачи
- Метриками для Prometheus

## Стиль

- `gofmt` обязательно
- Весь SQL — в `queue.go`
- Публичный API — только через `Queue` struct
