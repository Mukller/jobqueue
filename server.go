package jobqueue

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// ServeHTTP запускает HTTP-сервер для мониторинга очереди.
func (q *Queue) ServeHTTP(addr string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		stats, err := q.Stats()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})

	mux.HandleFunc("POST /retry-stalled", func(w http.ResponseWriter, r *http.Request) {
		n, err := q.RetryStalled(5 * 60 * 1_000_000_000) // 5 минут
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"requeued":%d}`, n)
	})

	fmt.Printf("jobqueue: мониторинг на http://%s/jobs\n", addr)
	return http.ListenAndServe(addr, mux)
}
