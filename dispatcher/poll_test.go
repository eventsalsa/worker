package dispatcher

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"
)

type mockPositionQuerier struct {
	positions []int64
	errors    []error
	calls     int
	mu        sync.Mutex
}

func (m *mockPositionQuerier) GetLatestGlobalPosition(_ context.Context, _ pgx.Tx) (int64, error) {
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

type mockTxBeginner struct {
	beginTxErr error
	txCalls    int32
}

//nolint:gocritic // hugeParam: implements txBeginner interface
func (m *mockTxBeginner) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	atomic.AddInt32(&m.txCalls, 1)
	if m.beginTxErr != nil {
		return nil, m.beginTxErr
	}
	return &mockTx{}, nil
}

type mockTx struct {
	pgx.Tx
}

func (m *mockTx) Rollback(_ context.Context) error {
	return nil
}

func (m *mockTx) Commit(_ context.Context) error {
	return nil
}

func newMockDB() *mockTxBeginner {
	return &mockTxBeginner{}
}

func TestPollDispatcherSendsWakeupWhenPositionAdvances(t *testing.T) {
	querier := &mockPositionQuerier{positions: []int64{5, 5, 6}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

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
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

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
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

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
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

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
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

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
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

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
	dispatcher := NewPollDispatcher(newMockDB(), nil, time.Millisecond, store.NoOpLogger{})

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrNilQuerier) {
		t.Fatalf("Start() error = %v, want %v", err, ErrNilQuerier)
	}
}

func TestPollDispatcherStartReturnsInitialQueryError(t *testing.T) {
	dispatcher := NewPollDispatcher(
		newMockDB(),
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
