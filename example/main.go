package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"jobqueue"
)

func main() {
	q, err := jobqueue.New("example.db", jobqueue.Options{
		Workers:    2,
		MaxRetries: 3,
		PollEvery:  200 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	q.Register("greet", func(ctx context.Context, payload []byte) error {
		fmt.Printf("Привет! Payload: %s\n", payload)
		return nil
	})

	q.Start()

	for i := 0; i < 5; i++ {
		q.Enqueue(context.Background(), jobqueue.Job{
			Type:    "greet",
			Payload: jobqueue.MustJSON(map[string]int{"n": i}),
		})
	}

	q.Enqueue(context.Background(), jobqueue.Job{
		Type:    "greet",
		Payload: []byte(`"отложенная задача"`),
		RunAt:   time.Now().Add(2 * time.Second),
	})

	go q.ServeHTTP(":8081")

	time.Sleep(5 * time.Second)
	fmt.Println("Готово.")
}
