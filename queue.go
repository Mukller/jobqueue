package jobqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ─── типы ─────────────────────────────────────────────────────────────────────

type HandlerFunc func(ctx context.Context, payload []byte) error

type Job struct {
	Type     string
	Payload  []byte
	Priority int           // 0 = наивысший
	RunAt    time.Time     // нулевое = немедленно
	Timeout  time.Duration // 0 = 30s по умолчанию
}

type Status int

const (
	StatusPending Status = iota
	StatusRunning
	StatusDone
	StatusFailed
)

func (s Status) String() string {
	return [...]string{"pending", "running", "done", "failed"}[s]
}

type JobRecord struct {
	ID       int64
	Type     string
	Payload  []byte
	Status   Status
	Priority int
	Attempts int
	RunAt    time.Time
	Error    string
}

type Options struct {
	Workers    int           // параллельных воркеров (по умолчанию 2)
	PollEvery  time.Duration // как часто опрашивать БД (по умолчанию 500ms)
	MaxRetries int           // максимум попыток (по умолчанию 3)
}

func (o *Options) fill() {
	if o.Workers <= 0 {
		o.Workers = 2
	}
	if o.PollEvery <= 0 {
		o.PollEvery = 500 * time.Millisecond
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 3
	}
}

// ─── Queue ────────────────────────────────────────────────────────────────────

type Queue struct {
	db       *sql.DB
	opts     Options
	handlers map[string]HandlerFunc
	mu       sync.RWMutex

	cancel context.CancelFunc
	wg     sync.WaitGroup
	sem    chan struct{}
}

func New(dsn string, opts Options) (*Queue, error) {
	opts.fill()
	db, err := sql.Open("sqlite3", dsn+"?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite не любит конкурентные записи

	q := &Queue{
		db:       db,
		opts:     opts,
		handlers: make(map[string]HandlerFunc),
		sem:      make(chan struct{}, opts.Workers),
	}
	return q, q.migrate()
}

func (q *Queue) migrate() error {
	_, err := q.db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			type      TEXT NOT NULL,
			payload   BLOB,
			status    INTEGER NOT NULL DEFAULT 0,
			priority  INTEGER NOT NULL DEFAULT 50,
			attempts  INTEGER NOT NULL DEFAULT 0,
			run_at    INTEGER NOT NULL DEFAULT 0,
			error     TEXT NOT NULL DEFAULT ''
		);
		CREATE INDEX IF NOT EXISTS idx_jobs_pending ON jobs(status, priority, run_at)
			WHERE status = 0;
	`)
	return err
}

// Register привязывает обработчик к типу задачи.
func (q *Queue) Register(jobType string, h HandlerFunc) {
	q.mu.Lock()
	q.handlers[jobType] = h
	q.mu.Unlock()
}

// Enqueue добавляет задачу в очередь.
func (q *Queue) Enqueue(ctx context.Context, job Job) (int64, error) {
	if job.Priority < 0 || job.Priority > 100 {
		job.Priority = 50
	}
	runAt := job.RunAt
	if runAt.IsZero() {
		runAt = time.Now()
	}
	res, err := q.db.ExecContext(ctx, `
		INSERT INTO jobs (type, payload, priority, run_at) VALUES (?, ?, ?, ?)
	`, job.Type, job.Payload, job.Priority, runAt.Unix())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Start запускает воркеры в фоне.
func (q *Queue) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	q.cancel = cancel
	q.wg.Add(1)
	go q.loop(ctx)
}

// Close останавливает воркеры и закрывает БД.
func (q *Queue) Close() error {
	if q.cancel != nil {
		q.cancel()
	}
	q.wg.Wait()
	return q.db.Close()
}

// Stats возвращает количество задач по статусам.
func (q *Queue) Stats() (map[string]int, error) {
	rows, err := q.db.Query(`SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s Status
		var n int
		rows.Scan(&s, &n)
		out[s.String()] = n
	}
	return out, nil
}

// RetryStalled переводит застрявшие "running" задачи обратно в pending.
func (q *Queue) RetryStalled(olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan).Unix()
	res, err := q.db.Exec(`
		UPDATE jobs SET status = 0 WHERE status = 1 AND run_at < ?
	`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ─── внутренний цикл ──────────────────────────────────────────────────────────

func (q *Queue) loop(ctx context.Context) {
	defer q.wg.Done()
	ticker := time.NewTicker(q.opts.PollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.dispatch(ctx)
		}
	}
}

func (q *Queue) dispatch(ctx context.Context) {
	now := time.Now().Unix()
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, type, payload, attempts
		FROM jobs
		WHERE status = 0 AND run_at <= ?
		ORDER BY priority ASC, id ASC
		LIMIT ?
	`, now, q.opts.Workers*2)
	if err != nil {
		slog.Error("jobqueue: dispatch query", "err", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var jobType string
		var payload []byte
		var attempts int
		if err := rows.Scan(&id, &jobType, &payload, &attempts); err != nil {
			continue
		}

		// захватываем слот
		select {
		case q.sem <- struct{}{}:
		default:
			return // воркеры заняты
		}

		// помечаем как running
		_, err := q.db.ExecContext(ctx, `UPDATE jobs SET status = 1 WHERE id = ?`, id)
		if err != nil {
			<-q.sem
			continue
		}

		q.wg.Add(1)
		go func(id int64, jobType string, payload []byte, attempts int) {
			defer func() {
				<-q.sem
				q.wg.Done()
			}()
			q.run(ctx, id, jobType, payload, attempts)
		}(id, jobType, payload, attempts)
	}
}

func (q *Queue) run(ctx context.Context, id int64, jobType string, payload []byte, attempts int) {
	q.mu.RLock()
	h, ok := q.handlers[jobType]
	q.mu.RUnlock()

	if !ok {
		q.db.Exec(`UPDATE jobs SET status = 3, error = ? WHERE id = ?`,
			fmt.Sprintf("нет обработчика для '%s'", jobType), id)
		return
	}

	timeout := 30 * time.Second
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := h(tctx, payload)
	attempts++

	if err == nil {
		q.db.Exec(`UPDATE jobs SET status = 2, attempts = ? WHERE id = ?`, attempts, id)
		return
	}

	slog.Warn("jobqueue: задача упала", "id", id, "type", jobType, "attempt", attempts, "err", err)

	if attempts >= q.opts.MaxRetries {
		q.db.Exec(`UPDATE jobs SET status = 3, attempts = ?, error = ? WHERE id = ?`,
			attempts, err.Error(), id)
		return
	}

	// экспоненциальная задержка: 5s, 25s, 125s...
	delay := time.Duration(math.Pow(5, float64(attempts))) * time.Second
	nextRun := time.Now().Add(delay).Unix()
	q.db.Exec(`
		UPDATE jobs SET status = 0, attempts = ?, run_at = ?, error = ? WHERE id = ?
	`, attempts, nextRun, err.Error(), id)
}

// ─── вспомогательное ──────────────────────────────────────────────────────────

func MustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
