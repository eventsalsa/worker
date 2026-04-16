package dispatcher

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/eventsalsa/store"
)

const (
	defaultPollInterval        = 200 * time.Millisecond
	defaultNotifyPollFallback  = time.Second
	defaultListenerMinInterval = 10 * time.Second
	defaultListenerMaxInterval = time.Minute
)

var (
	// ErrNilDB indicates that a dispatcher was created without a database handle.
	ErrNilDB = errors.New("dispatcher requires a database handle")

	// ErrNilQuerier indicates that a dispatcher was created without a position querier.
	ErrNilQuerier = errors.New("dispatcher requires a position querier")

	// ErrEmptyChannel indicates that a notify dispatcher was created without a channel name.
	ErrEmptyChannel = errors.New("dispatcher requires a notification channel")

	// ErrAlreadyRunning indicates that Start was called while the dispatcher was already running.
	ErrAlreadyRunning = errors.New("dispatcher already running")
)

// Dispatcher detects new events and broadcasts wakeup signals to consumer goroutines.
// Implementations must be safe for concurrent use.
type Dispatcher interface {
	// Start begins the dispatcher's detection loop. Blocks until ctx is canceled.
	// The wakeup channel is broadcast by closing the current channel and replacing it
	// with a new one each time new events are detected.
	Start(ctx context.Context) error

	// WakeupChan returns the current wakeup generation channel. Consumers should call
	// WakeupChan each time they enter a select so they observe future broadcasts
	// after the current generation is closed.
	WakeupChan() <-chan struct{}
}

// PositionQuerier queries the latest global position from the event store.
type PositionQuerier interface {
	// GetLatestGlobalPosition returns the highest global_position currently present in the event log.
	// Returns 0 when no events exist.
	GetLatestGlobalPosition(ctx context.Context, tx *sql.Tx) (int64, error)
}

func normalizedPollInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return defaultPollInterval
	}

	return interval
}

func normalizedLogger(logger store.Logger) store.Logger {
	if logger == nil {
		return store.NoOpLogger{}
	}

	return logger
}

type wakeupBroadcaster struct {
	ch chan struct{}
	mu sync.RWMutex
}

func newWakeupBroadcaster() wakeupBroadcaster {
	return wakeupBroadcaster{
		ch: make(chan struct{}),
	}
}

func (b *wakeupBroadcaster) Channel() <-chan struct{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ch
}

func (b *wakeupBroadcaster) Signal() {
	b.mu.Lock()
	defer b.mu.Unlock()

	current := b.ch
	b.ch = make(chan struct{})
	close(current)
}

type startGuard struct {
	mu      sync.Mutex
	running bool
}

func (g *startGuard) tryStart() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.running {
		return false
	}

	g.running = true
	return true
}

func (g *startGuard) stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.running = false
}
