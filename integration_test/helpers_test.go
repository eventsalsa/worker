//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/consumer"
	storemigrations "github.com/eventsalsa/store/migrations"
	storepostgres "github.com/eventsalsa/store/postgres"

	workerpkg "github.com/eventsalsa/worker"
	workermigrations "github.com/eventsalsa/worker/migrations"
	workerpostgres "github.com/eventsalsa/worker/postgres"
)

const (
	testWaitInterval      = 50 * time.Millisecond
	defaultWaitTimeout    = 12 * time.Second
	workerShutdownTimeout = 10 * time.Second
)

type testEventBatch struct {
	AggregateType string
	AggregateID   string
	Count         int
}

type controlledAppend struct {
	tx     *sql.Tx
	events []store.PersistedEvent
}

type failurePlan struct {
	err       error
	remaining int
}

type testConsumer struct {
	name           string
	instanceLabel  string
	aggregateTypes []string
	handleErr      error

	mu              sync.Mutex
	processedEvents []store.PersistedEvent
	attempts        map[int64]int
	failures        map[int64]failurePlan
}

type testConsumerEventRow struct {
	ConsumerName   string
	GlobalPosition int64
	AggregateType  string
	AggregateID    string
	EventType      string
	HandledBy      string
	AttemptNo      int
}

type gapSkipRow struct {
	ConsumerName           string
	GapPosition            int64
	SkipToPosition         int64
	HighestVisiblePosition int64
}

type testWorkerHarness struct {
	label    string
	db       *sql.DB
	worker   *workerpkg.Worker
	cancel   context.CancelFunc
	errCh    chan error
	stopOnce sync.Once
}

func openTestDB(t testing.TB) *sql.DB {
	t.Helper()

	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		dbName = "eventsalsa_worker_test"
	}

	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		password,
		dbName,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}

	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(8)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("ping test db: %v", err)
	}

	return db
}

func setupSchema(t testing.TB, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
DROP TABLE IF EXISTS test_consumer_events CASCADE;
DROP TABLE IF EXISTS worker_leader_election CASCADE;
DROP TABLE IF EXISTS consumer_gap_skips CASCADE;
DROP TABLE IF EXISTS consumer_checkpoints CASCADE;
DROP TABLE IF EXISTS consumer_assignments CASCADE;
DROP TABLE IF EXISTS worker_nodes CASCADE;
DROP TABLE IF EXISTS aggregate_heads CASCADE;
DROP TABLE IF EXISTS events CASCADE;
`)
	if err != nil {
		t.Fatalf("drop existing schema: %v", err)
	}

	tmpDir := t.TempDir()
	storeSQL := generateStoreSQL(t, tmpDir)

	if _, err := db.ExecContext(ctx, string(storeSQL)); err != nil {
		t.Fatalf("execute store migration: %v", err)
	}

	workerSQL := generateWorkerSQL(t, tmpDir)

	if _, err := db.ExecContext(ctx, string(workerSQL)); err != nil {
		t.Fatalf("execute worker migration: %v", err)
	}

	_, err = db.ExecContext(ctx, `
CREATE TABLE test_consumer_events (
consumer_name TEXT NOT NULL,
global_position BIGINT NOT NULL,
aggregate_type TEXT NOT NULL,
aggregate_id TEXT NOT NULL,
event_type TEXT NOT NULL,
handled_by TEXT NOT NULL,
attempt_no INT NOT NULL,
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
PRIMARY KEY (consumer_name, global_position)
)
`)
	if err != nil {
		t.Fatalf("create test read model table: %v", err)
	}
}

func cleanupTables(t testing.TB, db *sql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := db.ExecContext(ctx, `
TRUNCATE TABLE
test_consumer_events,
worker_leader_election,
consumer_gap_skips,
consumer_checkpoints,
consumer_assignments,
worker_nodes,
aggregate_heads,
events
RESTART IDENTITY CASCADE
`); err != nil {
		t.Fatalf("cleanup tables: %v", err)
	}
}

func appendTestEvents(t testing.TB, db *sql.DB, eventStore *storepostgres.Store, count int, aggregateType string) []store.PersistedEvent {
	t.Helper()

	return appendTestEventBatches(t, db, eventStore, testEventBatch{
		AggregateType: aggregateType,
		Count:         count,
	})
}

func appendTestEventBatches(t testing.TB, db *sql.DB, eventStore *storepostgres.Store, batches ...testEventBatch) []store.PersistedEvent {
	t.Helper()

	ctx := context.Background()
	appended := make([]store.PersistedEvent, 0)

	for _, batch := range batches {
		if batch.Count <= 0 {
			continue
		}

		aggregateID := batch.AggregateID
		if aggregateID == "" {
			aggregateID = uuid.NewString()
		}

		events := make([]store.Event, 0, batch.Count)
		for idx := range batch.Count {
			events = append(events, store.Event{
				AggregateType: batch.AggregateType,
				AggregateID:   aggregateID,
				EventID:       uuid.New(),
				EventType:     fmt.Sprintf("%s.event.%d", batch.AggregateType, idx+1),
				EventVersion:  1,
				Payload:       []byte(fmt.Sprintf(`{"aggregate_type":%q,"event_number":%d}`, batch.AggregateType, idx+1)),
				Metadata:      []byte(`{}`),
				CreatedAt:     time.Now().UTC(),
			})
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin append transaction: %v", err)
		}

		result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("append test events: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("commit append transaction: %v", err)
		}

		appended = append(appended, result.Events...)
	}

	sort.Slice(appended, func(i, j int) bool {
		return appended[i].GlobalPosition < appended[j].GlobalPosition
	})

	return appended
}

func beginControlledAppend(t testing.TB, db *sql.DB, eventStore *storepostgres.Store, batch testEventBatch) *controlledAppend {
	t.Helper()

	ctx := context.Background()
	aggregateID := batch.AggregateID
	if aggregateID == "" {
		aggregateID = uuid.NewString()
	}

	events := make([]store.Event, 0, batch.Count)
	for idx := range batch.Count {
		events = append(events, store.Event{
			AggregateType: batch.AggregateType,
			AggregateID:   aggregateID,
			EventID:       uuid.New(),
			EventType:     fmt.Sprintf("%s.event.%d", batch.AggregateType, idx+1),
			EventVersion:  1,
			Payload:       []byte(fmt.Sprintf(`{"aggregate_type":%q,"event_number":%d}`, batch.AggregateType, idx+1)),
			Metadata:      []byte(`{}`),
			CreatedAt:     time.Now().UTC(),
		})
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin controlled append transaction: %v", err)
	}

	result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("controlled append: %v", err)
	}

	return &controlledAppend{
		tx:     tx,
		events: append([]store.PersistedEvent(nil), result.Events...),
	}
}

func (c *controlledAppend) Commit(t testing.TB) {
	t.Helper()

	if err := c.tx.Commit(); err != nil {
		t.Fatalf("commit controlled append transaction: %v", err)
	}
}

func (c *controlledAppend) Rollback(t testing.TB) {
	t.Helper()

	if err := c.tx.Rollback(); err != nil {
		t.Fatalf("rollback controlled append transaction: %v", err)
	}
}

func waitFor(t testing.TB, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()

	waitForErr(t, timeout, func() error {
		if condition() {
			return nil
		}
		return errors.New(msg)
	})
}

func waitForErr(t testing.TB, timeout time.Duration, fn func() error) {
	t.Helper()

	if timeout <= 0 {
		timeout = defaultWaitTimeout
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = fn()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s: %v", timeout, lastErr)
		}
		time.Sleep(testWaitInterval)
	}
}

func newTestConsumer(name, instanceLabel string, aggregateTypes []string) *testConsumer {
	return &testConsumer{
		name:           name,
		instanceLabel:  instanceLabel,
		aggregateTypes: append([]string(nil), aggregateTypes...),
		attempts:       make(map[int64]int),
		failures:       make(map[int64]failurePlan),
	}
}

func (c *testConsumer) Name() string {
	return c.name
}

func (c *testConsumer) AggregateTypes() []string {
	return append([]string(nil), c.aggregateTypes...)
}

func (c *testConsumer) Handle(ctx context.Context, tx *sql.Tx, event store.PersistedEvent) error {
	c.mu.Lock()
	c.attempts[event.GlobalPosition]++
	attemptNo := c.attempts[event.GlobalPosition]
	handleErr := c.handleErr
	plan, shouldFail := c.failures[event.GlobalPosition]
	if shouldFail {
		if plan.remaining > 0 {
			plan.remaining--
		}
		if plan.remaining == 0 {
			delete(c.failures, event.GlobalPosition)
		} else {
			c.failures[event.GlobalPosition] = plan
		}
	}
	c.mu.Unlock()

	if handleErr != nil {
		return handleErr
	}

	if shouldFail {
		return plan.err
	}

	_, err := tx.ExecContext(ctx, `
INSERT INTO test_consumer_events (
consumer_name,
global_position,
aggregate_type,
aggregate_id,
event_type,
handled_by,
attempt_no
) VALUES ($1, $2, $3, $4, $5, $6, $7)
`, c.name, event.GlobalPosition, event.AggregateType, event.AggregateID, event.EventType, c.instanceLabel, attemptNo)
	if err != nil {
		return fmt.Errorf("insert test consumer row: %w", err)
	}

	c.mu.Lock()
	c.processedEvents = append(c.processedEvents, event)
	c.mu.Unlock()

	return nil
}

func (c *testConsumer) FailTimes(position int64, attempts int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failures[position] = failurePlan{err: err, remaining: attempts}
}

func (c *testConsumer) FailUntilCleared(position int64, err error) {
	c.FailTimes(position, -1, err)
}

func (c *testConsumer) ClearFailure(position int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.failures, position)
}

func (c *testConsumer) AttemptCount(position int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.attempts[position]
}

func (c *testConsumer) ProcessedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.processedEvents)
}

func startTestWorker(t *testing.T, label string, consumers []*testConsumer, opts ...workerpkg.Option) *testWorkerHarness {
	t.Helper()

	db := openTestDB(t)
	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumerList := make([]consumer.Consumer, 0, len(consumers))
	for _, consumer := range consumers {
		consumerList = append(consumerList, consumer)
	}

	w := workerpkg.New(db, eventStore, consumerList, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	harness := &testWorkerHarness{
		label:  label,
		db:     db,
		worker: w,
		cancel: cancel,
		errCh:  make(chan error, 1),
	}

	go func() {
		harness.errCh <- w.Start(ctx)
	}()

	t.Cleanup(func() {
		_ = db.Close()
	})
	t.Cleanup(func() {
		harness.stop(t)
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var count int
		err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_nodes WHERE worker_id = $1`, w.ID()).Scan(&count)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("worker %s not registered yet", label)
		}
		return nil
	})

	return harness
}

func (h *testWorkerHarness) stop(tb testing.TB) {
	tb.Helper()

	h.stopOnce.Do(func() {
		h.worker.Stop()
		h.cancel()

		select {
		case err := <-h.errCh:
			if err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, workerpostgres.ErrWorkerRegistrationMissing) {
				tb.Fatalf("worker %s stopped with error: %v", h.label, err)
			}
		case <-time.After(workerShutdownTimeout):
			tb.Fatalf("timeout waiting for worker %s to stop", h.label)
		}
	})
}

func defaultWorkerOptions() []workerpkg.Option {
	return []workerpkg.Option{
		workerpkg.WithBatchSize(50),
		workerpkg.WithPollInterval(50 * time.Millisecond),
		workerpkg.WithMaxPollInterval(200 * time.Millisecond),
		workerpkg.WithDispatcherInterval(50 * time.Millisecond),
		workerpkg.WithHeartbeatInterval(100 * time.Millisecond),
		workerpkg.WithHeartbeatTimeout(500 * time.Millisecond),
		workerpkg.WithRebalanceInterval(200 * time.Millisecond),
		workerpkg.WithBatchPause(10 * time.Millisecond),
	}
}

func latestGlobalPosition(t testing.TB, db *sql.DB, eventStore *storepostgres.Store) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin latest position tx: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	position, err := eventStore.GetLatestGlobalPosition(ctx, tx)
	if err != nil {
		t.Fatalf("get latest global position: %v", err)
	}

	return position
}

func getCheckpoint(t testing.TB, db *sql.DB, consumerName string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	position, err := workerpostgres.GetCheckpoint(ctx, db, workerpostgres.DefaultConsumerCheckpointsTable, consumerName)
	if err != nil {
		t.Fatalf("get checkpoint for %s: %v", consumerName, err)
	}

	return position
}

func getAssignments(t testing.TB, db *sql.DB) []workerpostgres.ConsumerAssignment {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	assignments, err := workerpostgres.GetAssignments(ctx, db, workerpostgres.DefaultConsumerAssignmentsTable)
	if err != nil {
		t.Fatalf("get assignments: %v", err)
	}

	return assignments
}

func assignedConsumerCounts(assignments []workerpostgres.ConsumerAssignment) map[uuid.UUID]int {
	counts := make(map[uuid.UUID]int)
	for _, assignment := range assignments {
		if assignment.Assigned {
			counts[assignment.WorkerID]++
		}
	}
	return counts
}

func getHandledRows(t testing.TB, db *sql.DB, consumerName string) []testConsumerEventRow {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
SELECT consumer_name, global_position, aggregate_type, aggregate_id, event_type, handled_by, attempt_no
FROM test_consumer_events
WHERE consumer_name = $1
ORDER BY global_position ASC
`, consumerName)
	if err != nil {
		t.Fatalf("query handled rows for %s: %v", consumerName, err)
	}
	defer rows.Close()

	result := make([]testConsumerEventRow, 0)
	for rows.Next() {
		var row testConsumerEventRow
		if err := rows.Scan(
			&row.ConsumerName,
			&row.GlobalPosition,
			&row.AggregateType,
			&row.AggregateID,
			&row.EventType,
			&row.HandledBy,
			&row.AttemptNo,
		); err != nil {
			t.Fatalf("scan handled row for %s: %v", consumerName, err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate handled rows for %s: %v", consumerName, err)
	}

	return result
}

func getGapSkipRows(t testing.TB, db *sql.DB, consumerName string) []gapSkipRow {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
SELECT consumer_name, gap_position, skip_to_position, highest_visible_position
FROM consumer_gap_skips
WHERE consumer_name = $1
ORDER BY id ASC
`, consumerName)
	if err != nil {
		t.Fatalf("query gap skip rows for %s: %v", consumerName, err)
	}
	defer rows.Close()

	result := make([]gapSkipRow, 0)
	for rows.Next() {
		var row gapSkipRow
		if err := rows.Scan(
			&row.ConsumerName,
			&row.GapPosition,
			&row.SkipToPosition,
			&row.HighestVisiblePosition,
		); err != nil {
			t.Fatalf("scan gap skip row for %s: %v", consumerName, err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate gap skip rows for %s: %v", consumerName, err)
	}

	return result
}

func handledByAfter(t testing.TB, db *sql.DB, cutoff int64) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
SELECT DISTINCT handled_by
FROM test_consumer_events
WHERE global_position > $1
ORDER BY handled_by ASC
`, cutoff)
	if err != nil {
		t.Fatalf("query handled_by after %d: %v", cutoff, err)
	}
	defer rows.Close()

	labels := make([]string, 0)
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			t.Fatalf("scan handled_by label: %v", err)
		}
		labels = append(labels, label)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate handled_by labels: %v", err)
	}

	return labels
}

func countWorkerRows(t testing.TB, db *sql.DB) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_nodes`).Scan(&count); err != nil {
		t.Fatalf("count worker rows: %v", err)
	}

	return count
}

func workerRowExists(t testing.TB, db *sql.DB, workerID uuid.UUID) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_nodes WHERE worker_id = $1`, workerID).Scan(&count); err != nil {
		t.Fatalf("check worker row %s: %v", workerID, err)
	}

	return count == 1
}

func insertWorkerRow(t testing.TB, db *sql.DB, workerID uuid.UUID, heartbeatAt time.Time) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
INSERT INTO worker_nodes (worker_id, heartbeat_at, created_at, updated_at)
VALUES ($1, $2, NOW(), NOW())
`, workerID, heartbeatAt)
	if err != nil {
		t.Fatalf("insert worker row %s: %v", workerID, err)
	}
}

func assignConsumerToWorker(t testing.TB, db *sql.DB, consumerName string, workerID uuid.UUID) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
UPDATE consumer_assignments
SET worker_id = $1, updated_at = NOW()
WHERE consumer_name = $2
`, workerID, consumerName)
	if err != nil {
		t.Fatalf("assign consumer %s to worker %s: %v", consumerName, workerID, err)
	}
}

func generateStoreSQL(t testing.TB, outputDir string) []byte {
	t.Helper()

	config := &storemigrations.Config{
		OutputFolder:        outputDir,
		OutputFilename:      "store.sql",
		EventsTable:         "events",
		AggregateHeadsTable: "aggregate_heads",
	}
	if err := storemigrations.GeneratePostgres(config); err != nil {
		t.Fatalf("generate store migration: %v", err)
	}

	sqlBytes, err := os.ReadFile(fmt.Sprintf("%s/%s", outputDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("read store migration: %v", err)
	}

	return sqlBytes
}

func generateWorkerSQL(t testing.TB, outputDir string) []byte {
	t.Helper()

	config := &workermigrations.Config{
		OutputFolder:             outputDir,
		OutputFilename:           "worker.sql",
		WorkerNodesTable:         workerpostgres.DefaultWorkerNodesTable,
		ConsumerAssignmentsTable: workerpostgres.DefaultConsumerAssignmentsTable,
		ConsumerCheckpointsTable: workerpostgres.DefaultConsumerCheckpointsTable,
		LeaderElectionTable:      workerpostgres.DefaultLeaderElectionTable,
	}
	if err := workermigrations.GeneratePostgres(config); err != nil {
		t.Fatalf("generate worker migration: %v", err)
	}

	sqlBytes, err := os.ReadFile(fmt.Sprintf("%s/%s", outputDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("read worker migration: %v", err)
	}

	return sqlBytes
}
