package dispatcher

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eventsalsa/store"
)

type mockPositionQuerier struct {
	positions []int64
	errors    []error
	calls     int
	mu        sync.Mutex
}

func (m *mockPositionQuerier) GetLatestGlobalPosition(_ context.Context, _ *sql.Tx) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := m.calls
	if idx >= len(m.positions) {
		idx = len(m.positions) - 1
	}
	m.calls++

	if idx < len(m.errors) && m.errors[idx] != nil {
		return 0, m.errors[idx]
	}

	return m.positions[idx], nil
}

func (m *mockPositionQuerier) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.calls
}

type stubDriver struct{}

var registerStubDriver sync.Once

func (stubDriver) Open(string) (driver.Conn, error) {
	return stubConn{}, nil
}

type stubConn struct{}

func (stubConn) Prepare(string) (driver.Stmt, error) {
	return stubStmt{}, nil
}

func (stubConn) Close() error {
	return nil
}

func (stubConn) Begin() (driver.Tx, error) {
	return stubTx{}, nil
}

func (stubConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return stubTx{}, nil
}

type stubStmt struct{}

func (stubStmt) Close() error {
	return nil
}

func (stubStmt) NumInput() int {
	return -1
}

func (stubStmt) Exec([]driver.Value) (driver.Result, error) {
	return stubResult{}, nil
}

func (stubStmt) Query([]driver.Value) (driver.Rows, error) {
	return stubRows{}, nil
}

type stubTx struct{}

func (stubTx) Commit() error {
	return nil
}

func (stubTx) Rollback() error {
	return nil
}

type stubResult struct{}

func (stubResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (stubResult) RowsAffected() (int64, error) {
	return 0, nil
}

type stubRows struct{}

func (stubRows) Columns() []string {
	return nil
}

func (stubRows) Close() error {
	return nil
}

func (stubRows) Next([]driver.Value) error {
	return io.EOF
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	const driverName = "dispatcher_stub"

	registerStubDriver.Do(func() {
		sql.Register(driverName, stubDriver{})
	})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func TestPollDispatcherSendsWakeupWhenPositionAdvances(t *testing.T) {
	querier := &mockPositionQuerier{positions: []int64{5, 5, 6}}
	dispatcher := NewPollDispatcher(openTestDB(t), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	waitForWakeup(t, dispatcher.WakeupChan())
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherBroadcastsWakeupToAllWaiters(t *testing.T) {
	querier := &mockPositionQuerier{positions: []int64{5, 5, 6}}
	dispatcher := NewPollDispatcher(openTestDB(t), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	waiterDone := make(chan struct{}, 2)
	for range 2 {
		go func() {
			<-dispatcher.WakeupChan()
			waiterDone <- struct{}{}
		}()
	}

	for range 2 {
		select {
		case <-waiterDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for broadcast wakeup")
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherDoesNotSendWakeupWhenPositionUnchanged(t *testing.T) {
	querier := &mockPositionQuerier{positions: []int64{5, 5, 5, 5}}
	dispatcher := NewPollDispatcher(openTestDB(t), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	select {
	case <-dispatcher.WakeupChan():
		cancel()
		t.Fatal("received unexpected wakeup signal")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherRespectsContextCancellation(t *testing.T) {
	querier := &mockPositionQuerier{positions: []int64{2, 2, 2}}
	dispatcher := NewPollDispatcher(openTestDB(t), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not return after cancellation")
	}
}

func TestPollDispatcherRejectsConcurrentStart(t *testing.T) {
	querier := &mockPositionQuerier{positions: []int64{2, 2, 2}}
	dispatcher := NewPollDispatcher(openTestDB(t), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if querier.CallCount() > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("Start() error = %v, want %v", err, ErrAlreadyRunning)
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherNonBlockingWakeupOnRepeatedSignals(t *testing.T) {
	querier := &mockPositionQuerier{positions: []int64{1, 2, 3, 4, 5, 6, 7}}
	dispatcher := NewPollDispatcher(openTestDB(t), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	waitForWakeup(t, dispatcher.WakeupChan())

	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		if querier.CallCount() >= 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if got := querier.CallCount(); got < 5 {
		cancel()
		t.Fatalf("querier call count = %d, want at least 5 to prove polling continued", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherStartRejectsNilDB(t *testing.T) {
	dispatcher := NewPollDispatcher(nil, &mockPositionQuerier{positions: []int64{1}}, time.Millisecond, store.NoOpLogger{})

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrNilDB) {
		t.Fatalf("Start() error = %v, want %v", err, ErrNilDB)
	}
}

func TestPollDispatcherStartRejectsNilQuerier(t *testing.T) {
	dispatcher := NewPollDispatcher(openTestDB(t), nil, time.Millisecond, store.NoOpLogger{})

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrNilQuerier) {
		t.Fatalf("Start() error = %v, want %v", err, ErrNilQuerier)
	}
}

func TestPollDispatcherStartReturnsInitialQueryError(t *testing.T) {
	dispatcher := NewPollDispatcher(
		openTestDB(t),
		&mockPositionQuerier{positions: []int64{0}, errors: []error{errors.New("latest position failed")}},
		time.Millisecond,
		store.NoOpLogger{},
	)

	err := dispatcher.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want initialization error")
	}
	if !strings.Contains(err.Error(), "initialize poll dispatcher checkpoint: latest position failed") {
		t.Fatalf("Start() error = %v, want wrapped initialization error", err)
	}
}

func waitForWakeup(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wakeup")
	}
}
