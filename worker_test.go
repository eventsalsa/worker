package worker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/consumer"
	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/eventsalsa/worker/dispatcher"
	workerpostgres "github.com/eventsalsa/worker/postgres"
)

type stubWorkerStore struct {
	readErr       error
	positionError error
	events        []store.PersistedEvent
	readBatches   [][]store.PersistedEvent
	latestPos     int64
	readCalls     int
	lastFrom      int64
	lastLimit     int
}

func (s *stubWorkerStore) ReadEvents(_ context.Context, _ *sql.Tx, fromPosition int64, limit int) ([]store.PersistedEvent, error) {
	s.readCalls++
	s.lastFrom = fromPosition
	s.lastLimit = limit

	if s.readErr != nil {
		return nil, s.readErr
	}

	events := s.events
	if len(s.readBatches) > 0 {
		index := s.readCalls - 1
		if index >= len(s.readBatches) {
			index = len(s.readBatches) - 1
		}
		events = s.readBatches[index]
	}

	if limit > 0 && len(events) > limit {
		return append([]store.PersistedEvent(nil), events[:limit]...), nil
	}

	return append([]store.PersistedEvent(nil), events...), nil
}

func (s *stubWorkerStore) GetLatestGlobalPosition(_ context.Context, _ *sql.Tx) (int64, error) {
	if s.positionError != nil {
		return 0, s.positionError
	}
	return s.latestPos, nil
}

type recordingConsumer struct {
	handleErr error
	name      string
	handled   []store.PersistedEvent
	failAt    int
}

func (c *recordingConsumer) Name() string {
	return c.name
}

//nolint:gocritic // hugeParam: mirrors the production consumer contract.
func (c *recordingConsumer) Handle(_ context.Context, _ *sql.Tx, event store.PersistedEvent) error {
	call := len(c.handled) + 1
	if c.failAt > 0 && call == c.failAt {
		return c.handleErr
	}

	c.handled = append(c.handled, event)
	return nil
}

type scopedRecordingConsumer struct {
	recordingConsumer
	aggregateTypes []string
}

func (c *scopedRecordingConsumer) AggregateTypes() []string {
	return append([]string(nil), c.aggregateTypes...)
}

type testDispatcher struct {
	ch <-chan struct{}
}

func (d *testDispatcher) Start(context.Context) error {
	return nil
}

func (d *testDispatcher) WakeupChan() <-chan struct{} {
	return d.ch
}

type stubDBState struct {
	beginErr         error
	execErr          error
	execRowsAffected []int64
	commitErr        error
	commitErrors     []error
	rollbackErr      error
	queryErr         error
	ownerID          string
	checkpointPos    int64
	queryValues      []driver.Value
	execArgs         [][]driver.NamedValue
	execQueries      []string
	execCalls        int
	beginCalls       int
	commitCalls      int
	rollbackCalls    int
	queryCalls       int
	ops              []string
	mu               sync.Mutex
	ownerValid       bool
}

type stubDBDriver struct {
	state *stubDBState
}

type stubDBConn struct {
	state *stubDBState
}

type stubDBTx struct {
	state *stubDBState
}

type stubStmt struct{}

type stubResult struct {
	rowsAffected int64
}

type stubRows struct {
	value    driver.Value
	returned bool
}

var stubDriverCounter atomic.Int64

func (d *stubDBDriver) Open(string) (driver.Conn, error) {
	return &stubDBConn{state: d.state}, nil
}

func (c *stubDBConn) Prepare(string) (driver.Stmt, error) {
	return stubStmt{}, nil
}

func (c *stubDBConn) Close() error {
	return nil
}

func (c *stubDBConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *stubDBConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	c.state.beginCalls++
	c.state.ops = append(c.state.ops, "begin")
	if c.state.beginErr != nil {
		return nil, c.state.beginErr
	}

	return &stubDBTx{state: c.state}, nil
}

func (c *stubDBConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	c.state.execCalls++
	c.state.ops = append(c.state.ops, "exec")
	c.state.execQueries = append(c.state.execQueries, query)
	copiedArgs := append([]driver.NamedValue(nil), args...)
	c.state.execArgs = append(c.state.execArgs, copiedArgs)
	if c.state.execErr != nil {
		return nil, c.state.execErr
	}

	rowsAffected := int64(1)
	if len(c.state.execRowsAffected) > 0 {
		index := c.state.execCalls - 1
		if index >= len(c.state.execRowsAffected) {
			index = len(c.state.execRowsAffected) - 1
		}
		rowsAffected = c.state.execRowsAffected[index]
	}

	return stubResult{rowsAffected: rowsAffected}, nil
}

func (c *stubDBConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	c.state.queryCalls++
	c.state.ops = append(c.state.ops, "query")
	if c.state.queryErr != nil {
		return nil, c.state.queryErr
	}

	if len(c.state.queryValues) > 0 {
		index := c.state.queryCalls - 1
		if index >= len(c.state.queryValues) {
			index = len(c.state.queryValues) - 1
		}
		if c.state.queryValues[index] == nil {
			return &stubRows{returned: true}, nil
		}
		return &stubRows{value: c.state.queryValues[index]}, nil
	}

	if strings.Contains(query, "FROM consumer_checkpoints") {
		return &stubRows{value: c.state.checkpointPos}, nil
	}

	if !c.state.ownerValid {
		return &stubRows{returned: true}, nil
	}

	return &stubRows{value: c.state.ownerID}, nil
}

func (t *stubDBTx) Commit() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()

	t.state.commitCalls++
	t.state.ops = append(t.state.ops, "commit")
	if len(t.state.commitErrors) > 0 {
		index := t.state.commitCalls - 1
		if index >= len(t.state.commitErrors) {
			index = len(t.state.commitErrors) - 1
		}
		return t.state.commitErrors[index]
	}

	return t.state.commitErr
}

func (t *stubDBTx) Rollback() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()

	t.state.rollbackCalls++
	t.state.ops = append(t.state.ops, "rollback")
	return t.state.rollbackErr
}

func (stubStmt) Close() error {
	return nil
}

func (stubStmt) NumInput() int {
	return -1
}

func (stubStmt) Exec([]driver.Value) (driver.Result, error) {
	return stubResult{rowsAffected: 1}, nil
}

func (stubStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("not implemented")
}

func (stubResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r stubResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func (r *stubRows) Columns() []string {
	return []string{"worker_id"}
}

func (r *stubRows) Close() error {
	return nil
}

func (r *stubRows) Next(dest []driver.Value) error {
	if r.returned {
		return io.EOF
	}

	dest[0] = r.value
	r.returned = true
	return nil
}

func openStubDB(t *testing.T, state *stubDBState) *sql.DB {
	t.Helper()

	driverName := fmt.Sprintf("worker_stub_%d", stubDriverCounter.Add(1))
	sql.Register(driverName, &stubDBDriver{state: state})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func TestNewUsesDefaultConfig(t *testing.T) {
	storeStub := &stubWorkerStore{}

	worker := New(
		openStubDB(t, &stubDBState{}),
		storeStub,
		[]consumer.Consumer{&recordingConsumer{name: "alpha"}},
	)

	defaults := DefaultConfig()
	if worker.store != storeStub {
		t.Fatal("worker store was not set")
	}
	if worker.config != defaults {
		t.Fatalf("config = %#v, want %#v", worker.config, defaults)
	}
	if worker.dispatcher == nil {
		t.Fatal("dispatcher was not initialized")
	}
	if len(worker.runningConsumers) != 0 {
		t.Fatalf("runningConsumers length = %d, want 0", len(worker.runningConsumers))
	}
}

func TestNewCopiesConsumersAndUsesConfiguredOptions(t *testing.T) {
	storeStub := &stubWorkerStore{}
	consumers := []consumer.Consumer{
		&recordingConsumer{name: "alpha"},
		&recordingConsumer{name: "beta"},
	}

	worker := New(
		openStubDB(t, &stubDBState{}),
		storeStub,
		consumers,
		WithBatchSize(7),
		WithPollInterval(3*time.Second),
	)

	if worker.store != storeStub {
		t.Fatal("worker store was not set")
	}
	if worker.config.BatchSize != 7 {
		t.Fatalf("BatchSize = %d, want 7", worker.config.BatchSize)
	}
	if worker.config.PollInterval != 3*time.Second {
		t.Fatalf("PollInterval = %v, want %v", worker.config.PollInterval, 3*time.Second)
	}
	if worker.dispatcher == nil {
		t.Fatal("dispatcher was not initialized")
	}
	if len(worker.runningConsumers) != 0 {
		t.Fatalf("runningConsumers length = %d, want 0", len(worker.runningConsumers))
	}

	consumers[0] = &recordingConsumer{name: "mutated"}
	if worker.consumers[0].Name() != "alpha" {
		t.Fatalf("worker consumer slice was not copied, got %q", worker.consumers[0].Name())
	}
}

func TestNewUsesNotifyDispatcherWhenConfigured(t *testing.T) {
	worker := New(
		openStubDB(t, &stubDBState{}),
		&stubWorkerStore{},
		[]consumer.Consumer{&recordingConsumer{name: "alpha"}},
		WithDispatcherStrategy(DispatcherStrategyNotify),
		WithNotifyConnectionString("postgres://worker:test@localhost/db?sslmode=disable"),
		WithNotifyChannel("custom_worker_events"),
	)

	notifyDispatcher, ok := worker.dispatcher.(*dispatcher.NotifyDispatcher)
	if !ok {
		t.Fatalf("dispatcher type = %T, want *dispatcher.NotifyDispatcher", worker.dispatcher)
	}
	if notifyDispatcher == nil {
		t.Fatal("notify dispatcher is nil")
	}
}

func TestWorkerValidate(t *testing.T) {
	validDB := openStubDB(t, &stubDBState{})
	validStore := &stubWorkerStore{}
	validDispatcher := &testDispatcher{ch: make(chan struct{})}
	validConsumers := []consumer.Consumer{&recordingConsumer{name: "alpha"}}

	tests := []struct {
		name   string
		worker *Worker
		errMsg string
	}{
		{
			name:   "nil db",
			worker: &Worker{store: validStore, dispatcher: validDispatcher, consumers: validConsumers},
			errMsg: ErrNilDB.Error(),
		},
		{
			name:   "nil store",
			worker: &Worker{db: validDB, dispatcher: validDispatcher, consumers: validConsumers},
			errMsg: ErrNilStore.Error(),
		},
		{
			name:   "nil dispatcher",
			worker: &Worker{db: validDB, store: validStore, consumers: validConsumers},
			errMsg: ErrNilDispatcher.Error(),
		},
		{
			name:   "nil consumer",
			worker: &Worker{db: validDB, store: validStore, dispatcher: validDispatcher, consumers: []consumer.Consumer{nil}},
			errMsg: "consumer at index 0 is nil",
		},
		{
			name:   "empty consumer name",
			worker: &Worker{db: validDB, store: validStore, dispatcher: validDispatcher, consumers: []consumer.Consumer{&recordingConsumer{}}},
			errMsg: "consumer at index 0 has empty name",
		},
		{
			name: "duplicate consumer name",
			worker: &Worker{
				db:         validDB,
				store:      validStore,
				dispatcher: validDispatcher,
				consumers:  []consumer.Consumer{&recordingConsumer{name: "dup"}, &recordingConsumer{name: "dup"}},
			},
			errMsg: `duplicate consumer name "dup"`,
		},
		{
			name: "notify dispatcher requires connection string",
			worker: &Worker{
				db:         validDB,
				store:      validStore,
				dispatcher: validDispatcher,
				consumers:  validConsumers,
				config:     Config{DispatcherStrategy: DispatcherStrategyNotify},
			},
			errMsg: ErrMissingNotifyConnectionString.Error(),
		},
		{
			name: "valid worker",
			worker: &Worker{
				db:         validDB,
				store:      validStore,
				dispatcher: validDispatcher,
				consumers:  validConsumers,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.worker.validate()
			if tc.errMsg == "" {
				if err != nil {
					t.Fatalf("validate() error = %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("validate() error = nil, want %q", tc.errMsg)
			}
			if err.Error() != tc.errMsg {
				t.Fatalf("validate() error = %q, want %q", err.Error(), tc.errMsg)
			}
		})
	}
}

func TestWorkerStopInvokesCancel(t *testing.T) {
	called := 0
	worker := &Worker{
		cancel: func() {
			called++
		},
	}

	worker.Stop()

	if called != 1 {
		t.Fatalf("cancel called %d times, want 1", called)
	}
}

func TestAssignmentPollInterval(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   time.Duration
	}{
		{
			name:   "uses default when non-positive",
			config: Config{},
			want:   defaultAssignmentPollInterval,
		},
		{
			name:   "clamps sub-second interval",
			config: Config{RebalanceInterval: 500 * time.Millisecond},
			want:   time.Second,
		},
		{
			name:   "uses configured interval",
			config: Config{RebalanceInterval: 3 * time.Second},
			want:   3 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			worker := &Worker{config: tc.config}
			if got := worker.assignmentPollInterval(); got != tc.want {
				t.Fatalf("assignmentPollInterval() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProcessBatchScopedReads(t *testing.T) {
	makeEvent := func(position int64, aggregateType string) store.PersistedEvent {
		return store.PersistedEvent{
			EventID:          uuid.New(),
			GlobalPosition:   position,
			AggregateType:    aggregateType,
			AggregateID:      fmt.Sprintf("%s-%d", aggregateType, position),
			AggregateVersion: position,
		}
	}

	t.Run("global consumer reads the unscoped event stream", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true, checkpointPos: 7}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{makeEvent(1, "order")},
		}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		_, err := worker.processBatch(context.Background(), &recordingConsumer{name: "global"}, 0)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if storeStub.readCalls == 0 {
			t.Fatal("ReadEvents() was not called")
		}
	})

	t.Run("scoped consumer filters matching events in memory after unscoped probe", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(1, "order"),
				makeEvent(2, "user"),
				makeEvent(3, "order"),
			},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "orders"},
			aggregateTypes:    []string{"order"},
		}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		result, err := worker.processBatch(context.Background(), registeredConsumer, 0)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if len(registeredConsumer.handled) != 2 {
			t.Fatalf("handled events = %d, want 2 (only order events)", len(registeredConsumer.handled))
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		if result.checkpoint != 3 {
			t.Fatalf("checkpoint = %d, want 3", result.checkpoint)
		}
	})

	t.Run("scoped consumer with empty types still reads the full stream", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true, checkpointPos: 3}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{makeEvent(1, "order")},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "all"},
		}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		_, err := worker.processBatch(context.Background(), registeredConsumer, 0)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if storeStub.readCalls == 0 {
			t.Fatal("ReadEvents() was not called")
		}
	})

	t.Run("scoped consumer with no matching events does not update checkpoint", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(1, "user"),
				makeEvent(2, "user"),
			},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "orders"},
			aggregateTypes:    []string{"order"},
		}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		result, err := worker.processBatch(context.Background(), registeredConsumer, 0)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		if result.handledCount != 0 {
			t.Fatalf("handledCount = %d, want 0", result.handledCount)
		}
		if result.checkpoint != 2 {
			t.Fatalf("checkpoint = %d, want 2", result.checkpoint)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1 (checkpoint update only)", state.execCalls)
		}
	})
}

func TestNextPollInterval(t *testing.T) {
	tests := []struct {
		name    string
		current time.Duration
		max     time.Duration
		base    time.Duration
		want    time.Duration
	}{
		{
			name:    "uses base when current is non-positive",
			current: 0,
			max:     10 * time.Second,
			base:    time.Second,
			want:    time.Second,
		},
		{
			name:    "uses base when current is negative",
			current: -time.Second,
			max:     10 * time.Second,
			base:    2 * time.Second,
			want:    2 * time.Second,
		},
		{
			name:    "clamps to current when max is non-positive",
			current: 2 * time.Second,
			max:     0,
			base:    time.Second,
			want:    2 * time.Second,
		},
		{
			name:    "doubles until max",
			current: 2 * time.Second,
			max:     10 * time.Second,
			base:    time.Second,
			want:    4 * time.Second,
		},
		{
			name:    "caps at max",
			current: 8 * time.Second,
			max:     10 * time.Second,
			base:    time.Second,
			want:    10 * time.Second,
		},
		{
			name:    "overflow returns max",
			current: time.Duration(1<<62 + 1),
			max:     30 * time.Second,
			base:    time.Second,
			want:    30 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextPollInterval(tc.current, tc.max, tc.base); got != tc.want {
				t.Fatalf("nextPollInterval() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWorkerStaleGapHarborLagAllowsZero(t *testing.T) {
	worker := &Worker{config: Config{StaleGapHarborLag: 0}}

	if got := worker.staleGapHarborLag(); got != 0 {
		t.Fatalf("staleGapHarborLag() = %d, want 0", got)
	}
}

func TestWorkerStaleGapHarborLagCapsToBatchWindow(t *testing.T) {
	worker := &Worker{
		config: Config{
			BatchSize:         5,
			StaleGapHarborLag: 99,
		},
	}

	if got := worker.staleGapHarborLag(); got != 4 {
		t.Fatalf("staleGapHarborLag() = %d, want 4", got)
	}
}

func TestInitializeCleansStaleWorkersBeforeRegistering(t *testing.T) {
	workerID := uuid.New()
	state := &stubDBState{}
	worker := &Worker{
		id:     workerID,
		db:     openStubDB(t, state),
		config: Config{HeartbeatTimeout: 5 * time.Second, Logger: store.NoOpLogger{}},
		consumers: []consumer.Consumer{
			&recordingConsumer{name: "alpha"},
		},
	}

	registered := false
	if err := worker.initialize(context.Background(), &registered); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}
	if !registered {
		t.Fatal("registered = false, want true")
	}
	if state.execCalls != 4 {
		t.Fatalf("execCalls = %d, want 4", state.execCalls)
	}
	if len(state.execQueries) < 2 {
		t.Fatalf("execQueries = %v, want cleanup and register queries", state.execQueries)
	}
	if !strings.Contains(state.execQueries[0], "DELETE FROM worker_nodes") {
		t.Fatalf("first exec query = %q, want worker_nodes cleanup", state.execQueries[0])
	}
	if !strings.Contains(state.execQueries[0], "heartbeat_at < NOW()") {
		t.Fatalf("first exec query = %q, want stale heartbeat cutoff", state.execQueries[0])
	}
	if !strings.Contains(state.execQueries[1], "INSERT INTO worker_nodes") {
		t.Fatalf("second exec query = %q, want worker registration", state.execQueries[1])
	}
}

func TestRunHeartbeatLoopReportsFatalWhenRegistrationRowIsMissing(t *testing.T) {
	state := &stubDBState{execRowsAffected: []int64{0, 0}}
	worker := &Worker{
		id:         uuid.New(),
		db:         openStubDB(t, state),
		config:     Config{HeartbeatInterval: time.Millisecond, Logger: store.NoOpLogger{}},
		fatalErrCh: make(chan error, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		worker.runHeartbeatLoop(ctx)
		close(done)
	}()

	select {
	case err := <-worker.fatalErrCh:
		if !errors.Is(err, workerpostgres.ErrWorkerRegistrationMissing) {
			t.Fatalf("fatal error = %v, want ErrWorkerRegistrationMissing", err)
		}
		if !strings.Contains(err.Error(), "worker registration lost during heartbeat") {
			t.Fatalf("fatal error = %v, want wrapped heartbeat registration error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for heartbeat fatal error")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat loop to exit")
	}

	if state.execCalls < missingRegistrationRetryLimit {
		t.Fatalf("execCalls = %d, want at least %d heartbeat attempts", state.execCalls, missingRegistrationRetryLimit)
	}
	if len(state.execQueries) == 0 || !strings.Contains(state.execQueries[0], "UPDATE worker_nodes") {
		t.Fatalf("execQueries = %v, want heartbeat update query", state.execQueries)
	}
}

func TestWaitForConsumerDelay(t *testing.T) {
	t.Run("returns wakeup when dispatcher signals", func(t *testing.T) {
		ch := make(chan struct{})
		close(ch)

		worker := &Worker{dispatcher: &testDispatcher{ch: ch}}
		woken, ok := worker.waitForConsumerDelay(context.Background(), context.Background(), time.Second)
		if !ok {
			t.Fatal("waitForConsumerDelay() ok = false, want true")
		}
		if !woken {
			t.Fatal("waitForConsumerDelay() woken = false, want true")
		}
	})

	t.Run("returns when timer expires", func(t *testing.T) {
		worker := &Worker{dispatcher: &testDispatcher{ch: make(chan struct{})}}
		woken, ok := worker.waitForConsumerDelay(context.Background(), context.Background(), 5*time.Millisecond)
		if !ok {
			t.Fatal("waitForConsumerDelay() ok = false, want true")
		}
		if woken {
			t.Fatal("waitForConsumerDelay() woken = true, want false")
		}
	})

	t.Run("stops on worker cancellation", func(t *testing.T) {
		workerCtx, cancel := context.WithCancel(context.Background())
		cancel()

		worker := &Worker{dispatcher: &testDispatcher{ch: make(chan struct{})}}
		woken, ok := worker.waitForConsumerDelay(workerCtx, context.Background(), time.Second)
		if ok {
			t.Fatal("waitForConsumerDelay() ok = true, want false")
		}
		if woken {
			t.Fatal("waitForConsumerDelay() woken = true, want false")
		}
	})

	t.Run("stops on assignment cancellation", func(t *testing.T) {
		assignmentCtx, cancel := context.WithCancel(context.Background())
		cancel()

		worker := &Worker{dispatcher: &testDispatcher{ch: make(chan struct{})}}
		woken, ok := worker.waitForConsumerDelay(context.Background(), assignmentCtx, time.Second)
		if ok {
			t.Fatal("waitForConsumerDelay() ok = true, want false")
		}
		if woken {
			t.Fatal("waitForConsumerDelay() woken = true, want false")
		}
	})
}

func TestProcessBatch(t *testing.T) {
	makeEvent := func(position int64, aggregateType string) store.PersistedEvent {
		return store.PersistedEvent{
			EventID:          uuid.New(),
			GlobalPosition:   position,
			AggregateType:    aggregateType,
			AggregateID:      fmt.Sprintf("%s-%d", aggregateType, position),
			AggregateVersion: position,
		}
	}

	t.Run("commits processed batch and saves checkpoint", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true, checkpointPos: 3}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(4, "order"),
				makeEvent(5, "order"),
			},
		}
		registeredConsumer := &recordingConsumer{name: "orders"}

		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 2, Logger: store.NoOpLogger{}},
		}

		result, err := worker.processBatch(context.Background(), registeredConsumer, 3)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		if !result.fullWindow {
			t.Fatal("processBatch() fullWindow = false, want true")
		}
		if result.checkpoint != 5 {
			t.Fatalf("processBatch() checkpoint = %d, want 5", result.checkpoint)
		}
		if result.handledCount != 2 {
			t.Fatalf("handledCount = %d, want 2", result.handledCount)
		}
		if storeStub.lastFrom != 3 {
			t.Fatalf("ReadEvents fromPosition = %d, want 3", storeStub.lastFrom)
		}
		if storeStub.lastLimit != 2 {
			t.Fatalf("ReadEvents limit = %d, want 2", storeStub.lastLimit)
		}
		if len(registeredConsumer.handled) != 2 {
			t.Fatalf("handled events = %d, want 2", len(registeredConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		if state.beginCalls != 2 {
			t.Fatalf("beginCalls = %d, want 2 (probe + batch)", state.beginCalls)
		}
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1", state.execCalls)
		}
		if state.commitCalls != 1 {
			t.Fatalf("commitCalls = %d, want 1", state.commitCalls)
		}
		if state.rollbackCalls != 1 {
			t.Fatalf("rollbackCalls = %d, want 1 (probe rollback)", state.rollbackCalls)
		}
		if len(state.execQueries) != 1 || !strings.Contains(state.execQueries[0], "consumer_checkpoints") {
			t.Fatalf("execQueries = %v, want checkpoint update", state.execQueries)
		}
		if len(state.ops) < 3 || state.ops[0] != "begin" || state.ops[1] != "rollback" || state.ops[2] != "begin" {
			t.Fatalf("ops = %v, want probe tx before batch tx", state.ops)
		}
		if len(state.execArgs) != 1 || len(state.execArgs[0]) != 2 {
			t.Fatalf("execArgs = %#v, want consumer name and checkpoint", state.execArgs)
		}
		if got := state.execArgs[0][0].Value; got != "orders" {
			t.Fatalf("checkpoint consumer arg = %v, want orders", got)
		}
		if got := state.execArgs[0][1].Value; got != int64(5) {
			t.Fatalf("checkpoint position arg = %v, want 5", got)
		}
	})

	t.Run("returns no batch when store has no events", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true, checkpointPos: 7}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  &stubWorkerStore{},
			config: Config{BatchSize: 3, Logger: store.NoOpLogger{}},
		}

		result, err := worker.processBatch(context.Background(), &recordingConsumer{name: "empty"}, 0)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if result.progressed {
			t.Fatal("processBatch() progressed = true, want false")
		}
		if result.blockedByGap {
			t.Fatal("processBatch() blockedByGap = true, want false")
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.rollbackCalls != 1 {
			t.Fatalf("rollbackCalls = %d, want 1", state.rollbackCalls)
		}
	})

	t.Run("checkpoint drift between probe and batch causes a quick re-probe", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{
			queryValues: []driver.Value{
				workerID.String(),
				int64(5),
			},
		}
		registeredConsumer := &recordingConsumer{name: "orders"}
		worker := &Worker{
			id:    workerID,
			db:    openStubDB(t, state),
			store: &stubWorkerStore{events: []store.PersistedEvent{makeEvent(4, "order"), makeEvent(5, "order")}},
			config: Config{
				BatchSize: 2,
				Logger:    store.NoOpLogger{},
			},
		}

		result, err := worker.processBatch(context.Background(), registeredConsumer, 3)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if result.progressed {
			t.Fatal("processBatch() progressed = true, want false")
		}
		if !result.blockedByGap {
			t.Fatal("processBatch() blockedByGap = false, want true")
		}
		if result.checkpoint != 5 {
			t.Fatalf("processBatch() checkpoint = %d, want 5", result.checkpoint)
		}
		if len(registeredConsumer.handled) != 0 {
			t.Fatalf("handled events = %d, want 0", len(registeredConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.commitCalls != 0 {
			t.Fatalf("commitCalls = %d, want 0", state.commitCalls)
		}
	})

	t.Run("scoped consumer only receives matching events via in-memory filter", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true, checkpointPos: 7}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(8, "order"),
				makeEvent(9, "user"),
			},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "orders"},
			aggregateTypes:    []string{"order"},
		}

		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		result, err := worker.processBatch(context.Background(), registeredConsumer, 7)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		if result.checkpoint != 9 {
			t.Fatalf("checkpoint = %d, want 9 (safe frontier)", result.checkpoint)
		}
		if len(registeredConsumer.handled) != 1 {
			t.Fatalf("handled events = %d, want 1", len(registeredConsumer.handled))
		}
		if registeredConsumer.handled[0].AggregateType != "order" {
			t.Fatalf("handled aggregate type = %q, want order", registeredConsumer.handled[0].AggregateType)
		}
	})

	t.Run("returns read error and rolls back", func(t *testing.T) {
		readErr := errors.New("read failed")
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  &stubWorkerStore{readErr: readErr},
			config: Config{BatchSize: 3, Logger: store.NoOpLogger{}},
		}

		_, err := worker.processBatch(context.Background(), &recordingConsumer{name: "reader"}, 0)
		if err == nil || !strings.Contains(err.Error(), "probe frontier for consumer reader: read failed") {
			t.Fatalf("processBatch() error = %v, want wrapped read error", err)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.rollbackCalls != 1 {
			t.Fatalf("rollbackCalls = %d, want 1", state.rollbackCalls)
		}
	})

	t.Run("returns handler error and rolls back", func(t *testing.T) {
		handleErr := errors.New("boom")
		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		registeredConsumer := &recordingConsumer{name: "handler", failAt: 1, handleErr: handleErr}
		worker := &Worker{
			id:    workerID,
			db:    openStubDB(t, state),
			store: &stubWorkerStore{events: []store.PersistedEvent{makeEvent(1, "order")}},
			config: Config{
				BatchSize: 1,
				Logger:    store.NoOpLogger{},
			},
		}

		_, err := worker.processBatch(context.Background(), registeredConsumer, 0)
		if err == nil || !strings.Contains(err.Error(), "handle event") || !strings.Contains(err.Error(), "for consumer handler: boom") {
			t.Fatalf("processBatch() error = %v, want wrapped handler error", err)
		}
		if len(registeredConsumer.handled) != 0 {
			t.Fatalf("handled events = %d, want 0", len(registeredConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.rollbackCalls != 2 {
			t.Fatalf("rollbackCalls = %d, want 2 (probe + batch)", state.rollbackCalls)
		}
	})

	t.Run("returns checkpoint save error and rolls back", func(t *testing.T) {
		workerID := uuid.New()
		state := &stubDBState{
			execErr:    errors.New("checkpoint failed"),
			ownerID:    workerID.String(),
			ownerValid: true,
		}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  &stubWorkerStore{events: []store.PersistedEvent{makeEvent(1, "order")}},
			config: Config{BatchSize: 1, Logger: store.NoOpLogger{}},
		}

		_, err := worker.processBatch(context.Background(), &recordingConsumer{name: "checkpoint"}, 0)
		if err == nil || !strings.Contains(err.Error(), "save checkpoint for consumer checkpoint: save checkpoint for consumer checkpoint: checkpoint failed") {
			t.Fatalf("processBatch() error = %v, want wrapped checkpoint error", err)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1", state.execCalls)
		}
		if state.rollbackCalls != 2 {
			t.Fatalf("rollbackCalls = %d, want 2 (probe + batch)", state.rollbackCalls)
		}
	})

	t.Run("stale gap with default lag advances using the earliest sparse visible row", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(2, "order"),
				makeEvent(3, "order"),
			},
		}
		config := DefaultConfig()
		config.BatchSize = 10
		config.BatchPause = time.Millisecond
		config.StaleGapThreshold = 10 * time.Second
		config.Logger = store.NoOpLogger{}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: config,
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), &recordingConsumer{name: "orders"}, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if first.progressed {
			t.Fatal("first processBatchWithGapState() progressed = true, want false")
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		secondConsumer := &recordingConsumer{name: "orders"}
		second, err := worker.processBatchWithGapState(context.Background(), secondConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v", err)
		}
		if !second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = false, want true")
		}
		if !second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = false, want true")
		}
		if second.checkpoint != 2 {
			t.Fatalf("checkpoint = %d, want 2", second.checkpoint)
		}
		if len(secondConsumer.handled) != 1 {
			t.Fatalf("handled events = %d, want 1", len(secondConsumer.handled))
		}
		if secondConsumer.handled[0].GlobalPosition != 2 {
			t.Fatalf("handled position = %d, want 2", secondConsumer.handled[0].GlobalPosition)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 2 {
			t.Fatalf("execCalls = %d, want 2 (gap skip + checkpoint)", state.execCalls)
		}
		if len(state.execQueries) < 2 || !strings.Contains(state.execQueries[0], "consumer_gap_skips") {
			t.Fatalf("execQueries = %v, want gap skip insert in second batch", state.execQueries)
		}
	})

	t.Run("stale gap revalidation keeps the default-lag sparse fallback", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		storeStub := &stubWorkerStore{
			readBatches: [][]store.PersistedEvent{
				{
					makeEvent(2, "order"),
					makeEvent(3, "order"),
				},
				{
					makeEvent(2, "order"),
					makeEvent(3, "order"),
				},
				{
					makeEvent(2, "order"),
					makeEvent(3, "order"),
				},
			},
		}
		config := DefaultConfig()
		config.BatchSize = 10
		config.BatchPause = time.Millisecond
		config.StaleGapThreshold = 10 * time.Second
		config.Logger = store.NoOpLogger{}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: config,
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), &recordingConsumer{name: "orders"}, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		secondConsumer := &recordingConsumer{name: "orders"}
		second, err := worker.processBatchWithGapState(context.Background(), secondConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v", err)
		}
		if !second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = false, want true")
		}
		if !second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = false, want true")
		}
		if second.checkpoint != 2 {
			t.Fatalf("checkpoint = %d, want 2", second.checkpoint)
		}
		if len(secondConsumer.handled) != 1 {
			t.Fatalf("handled events = %d, want 1", len(secondConsumer.handled))
		}
		if secondConsumer.handled[0].GlobalPosition != 2 {
			t.Fatalf("handled position = %d, want 2", secondConsumer.handled[0].GlobalPosition)
		}
		if storeStub.readCalls != 3 {
			t.Fatalf("ReadEvents calls = %d, want 3 (initial probe + stale probe + revalidation)", storeStub.readCalls)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 2 {
			t.Fatalf("execCalls = %d, want 2 (gap skip + checkpoint)", state.execCalls)
		}
		if len(state.execQueries) < 2 || !strings.Contains(state.execQueries[0], "consumer_gap_skips") {
			t.Fatalf("execQueries = %v, want gap skip insert in second batch", state.execQueries)
		}
	})

	t.Run("stale gap rolls back when the gap skip audit insert fails", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{
			execErr:    errors.New("gap skip failed"),
			ownerID:    workerID.String(),
			ownerValid: true,
		}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(2, "order"),
				makeEvent(3, "order"),
			},
		}
		config := DefaultConfig()
		config.BatchSize = 10
		config.BatchPause = time.Millisecond
		config.StaleGapThreshold = 10 * time.Second
		config.Logger = store.NoOpLogger{}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: config,
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), &recordingConsumer{name: "orders"}, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		_, err = worker.processBatchWithGapState(context.Background(), &recordingConsumer{name: "orders"}, gap, 0)
		if err == nil || !strings.Contains(err.Error(), "record gap skip for consumer orders: gap skip failed") {
			t.Fatalf("processBatchWithGapState() error = %v, want wrapped gap skip error", err)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1 (gap skip insert only)", state.execCalls)
		}
		if len(state.execQueries) != 1 || !strings.Contains(state.execQueries[0], "consumer_gap_skips") {
			t.Fatalf("execQueries = %v, want only gap skip insert", state.execQueries)
		}
		if state.commitCalls != 0 {
			t.Fatalf("commitCalls = %d, want 0", state.commitCalls)
		}
		if state.rollbackCalls != 3 {
			t.Fatalf("rollbackCalls = %d, want 3 (initial probe + stale probe + failed batch)", state.rollbackCalls)
		}
	})

	t.Run("scoped stale gap with zero handled events still advances under the default lag", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(2, "order"),
				makeEvent(3, "order"),
			},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "users"},
			aggregateTypes:    []string{"user"},
		}
		config := DefaultConfig()
		config.BatchSize = 10
		config.BatchPause = time.Millisecond
		config.StaleGapThreshold = 10 * time.Second
		config.Logger = store.NoOpLogger{}
		worker := &Worker{
			id:     workerID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: config,
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		second, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v", err)
		}
		if !second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = false, want true")
		}
		if !second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = false, want true")
		}
		if second.checkpoint != 2 {
			t.Fatalf("checkpoint = %d, want 2", second.checkpoint)
		}
		if len(registeredConsumer.handled) != 0 {
			t.Fatalf("handled events = %d, want 0", len(registeredConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 2 {
			t.Fatalf("execCalls = %d, want 2 (gap skip + checkpoint)", state.execCalls)
		}
	})

	t.Run("stale gap advances to a safe harbor", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(2, "order"),
				makeEvent(3, "order"),
				makeEvent(4, "order"),
			},
		}
		worker := &Worker{
			id:    workerID,
			db:    openStubDB(t, state),
			store: storeStub,
			config: Config{
				BatchSize:         3,
				BatchPause:        time.Millisecond,
				StaleGapThreshold: 10 * time.Second,
				StaleGapHarborLag: 1,
				Logger:            store.NoOpLogger{},
			},
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), &recordingConsumer{name: "orders"}, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if first.progressed {
			t.Fatal("first processBatchWithGapState() progressed = true, want false")
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		secondConsumer := &recordingConsumer{name: "orders"}
		second, err := worker.processBatchWithGapState(context.Background(), secondConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v", err)
		}
		if !second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = false, want true")
		}
		if !second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = false, want true")
		}
		if second.checkpoint != 3 {
			t.Fatalf("checkpoint = %d, want 3", second.checkpoint)
		}
		if len(secondConsumer.handled) != 2 {
			t.Fatalf("handled events = %d, want 2", len(secondConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 2 {
			t.Fatalf("execCalls = %d, want 2 (gap skip + checkpoint)", state.execCalls)
		}
		if len(state.execQueries) < 2 || !strings.Contains(state.execQueries[0], "consumer_gap_skips") {
			t.Fatalf("execQueries = %v, want gap skip insert in second batch", state.execQueries)
		}
	})

	t.Run("stale gap revalidation processes a late commit instead of skipping it", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{ownerID: workerID.String(), ownerValid: true}
		storeStub := &stubWorkerStore{
			readBatches: [][]store.PersistedEvent{
				{
					makeEvent(2, "order"),
					makeEvent(3, "order"),
					makeEvent(4, "order"),
				},
				{
					makeEvent(2, "order"),
					makeEvent(3, "order"),
					makeEvent(4, "order"),
				},
				{
					makeEvent(1, "order"),
					makeEvent(2, "order"),
					makeEvent(3, "order"),
				},
			},
		}
		worker := &Worker{
			id:    workerID,
			db:    openStubDB(t, state),
			store: storeStub,
			config: Config{
				BatchSize:         3,
				BatchPause:        time.Millisecond,
				StaleGapThreshold: 10 * time.Second,
				StaleGapHarborLag: 1,
				Logger:            store.NoOpLogger{},
			},
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), &recordingConsumer{name: "orders"}, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		secondConsumer := &recordingConsumer{name: "orders"}
		second, err := worker.processBatchWithGapState(context.Background(), secondConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v", err)
		}
		if !second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = false, want true")
		}
		if second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = true, want false")
		}
		if second.checkpoint != 3 {
			t.Fatalf("checkpoint = %d, want 3", second.checkpoint)
		}
		if len(secondConsumer.handled) != 3 {
			t.Fatalf("handled events = %d, want 3", len(secondConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1 (checkpoint update only)", state.execCalls)
		}
		if len(state.execQueries) != 1 || strings.Contains(state.execQueries[0], "consumer_gap_skips") {
			t.Fatalf("execQueries = %v, want checkpoint update only", state.execQueries)
		}
	})

	t.Run("stale gap retries serialization failures", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{
			ownerID:      workerID.String(),
			ownerValid:   true,
			commitErrors: []error{&pq.Error{Code: "40001"}, nil},
		}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(2, "order"),
				makeEvent(3, "order"),
				makeEvent(4, "order"),
			},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "users"},
			aggregateTypes:    []string{"user"},
		}
		worker := &Worker{
			id:    workerID,
			db:    openStubDB(t, state),
			store: storeStub,
			config: Config{
				BatchSize:         3,
				BatchPause:        time.Millisecond,
				StaleGapThreshold: 10 * time.Second,
				StaleGapHarborLag: 1,
				Logger:            store.NoOpLogger{},
			},
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		second, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v", err)
		}
		if !second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = false, want true")
		}
		if !second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = false, want true")
		}
		if second.checkpoint != 3 {
			t.Fatalf("checkpoint = %d, want 3", second.checkpoint)
		}
		if len(registeredConsumer.handled) != 0 {
			t.Fatalf("handled events = %d, want 0", len(registeredConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.commitCalls != 2 {
			t.Fatalf("commitCalls = %d, want 2", state.commitCalls)
		}
	})

	t.Run("stale gap treats exhausted serialization retries as blocked", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{
			ownerID:    workerID.String(),
			ownerValid: true,
			commitErrors: []error{
				&pq.Error{Code: "40001"},
				&pq.Error{Code: "40001"},
				&pq.Error{Code: "40001"},
			},
		}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(2, "order"),
				makeEvent(3, "order"),
				makeEvent(4, "order"),
			},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "users"},
			aggregateTypes:    []string{"user"},
		}
		worker := &Worker{
			id:    workerID,
			db:    openStubDB(t, state),
			store: storeStub,
			config: Config{
				BatchSize:         3,
				BatchPause:        time.Millisecond,
				StaleGapThreshold: 10 * time.Second,
				StaleGapHarborLag: 1,
				Logger:            store.NoOpLogger{},
			},
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		second, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v, want nil", err)
		}
		if second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = true, want false")
		}
		if !second.blockedByGap {
			t.Fatal("second processBatchWithGapState() blockedByGap = false, want true")
		}
		if second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = true, want false")
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.commitCalls != staleGapRetryLimit {
			t.Fatalf("commitCalls = %d, want %d", state.commitCalls, staleGapRetryLimit)
		}
	})

	t.Run("stale gap retries serialization failures for driver-agnostic errors", func(t *testing.T) {
		originalNow := timeNow
		defer func() { timeNow = originalNow }()

		base := time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		now := base
		timeNow = func() time.Time { return now }

		workerID := uuid.New()
		state := &stubDBState{
			ownerID:      workerID.String(),
			ownerValid:   true,
			commitErrors: []error{&stubSQLStateError{code: "40001"}, nil},
		}
		storeStub := &stubWorkerStore{
			events: []store.PersistedEvent{
				makeEvent(2, "order"),
				makeEvent(3, "order"),
				makeEvent(4, "order"),
			},
		}
		registeredConsumer := &scopedRecordingConsumer{
			recordingConsumer: recordingConsumer{name: "users"},
			aggregateTypes:    []string{"user"},
		}
		worker := &Worker{
			id:    workerID,
			db:    openStubDB(t, state),
			store: storeStub,
			config: Config{
				BatchSize:         3,
				BatchPause:        time.Millisecond,
				StaleGapThreshold: 10 * time.Second,
				StaleGapHarborLag: 1,
				Logger:            store.NoOpLogger{},
			},
		}

		gap := &gapState{}

		first, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("first processBatchWithGapState() error = %v", err)
		}
		if !first.blockedByGap {
			t.Fatal("first processBatchWithGapState() blockedByGap = false, want true")
		}

		now = base.Add(11 * time.Second)

		second, err := worker.processBatchWithGapState(context.Background(), registeredConsumer, gap, 0)
		if err != nil {
			t.Fatalf("second processBatchWithGapState() error = %v", err)
		}
		if !second.progressed {
			t.Fatal("second processBatchWithGapState() progressed = false, want true")
		}
		if !second.staleSkipped {
			t.Fatal("second processBatchWithGapState() staleSkipped = false, want true")
		}
		if second.checkpoint != 3 {
			t.Fatalf("checkpoint = %d, want 3", second.checkpoint)
		}
		if len(registeredConsumer.handled) != 0 {
			t.Fatalf("handled events = %d, want 0", len(registeredConsumer.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.commitCalls != 2 {
			t.Fatalf("commitCalls = %d, want 2", state.commitCalls)
		}
	})
}

type stubSQLStateError struct {
	code string
}

func (e *stubSQLStateError) Error() string {
	return "mock sqlstate: " + e.code
}

func (e *stubSQLStateError) SQLState() string {
	return e.code
}
