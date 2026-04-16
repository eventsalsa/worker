package worker

import (
	"time"

	"github.com/eventsalsa/store"

	"github.com/eventsalsa/worker/postgres"
)

// DispatcherStrategy selects how the worker wakeup dispatcher detects new events.
type DispatcherStrategy string

const (
	// DispatcherStrategyPoll uses periodic polling to detect new events.
	DispatcherStrategyPoll DispatcherStrategy = "poll"
	// DispatcherStrategyNotify uses PostgreSQL LISTEN/NOTIFY plus reconciliation polling.
	DispatcherStrategyNotify DispatcherStrategy = "notify"
)

// Config holds all configurable values for a Worker.
type Config struct {
	Logger                   store.Logger
	WorkerNodesTable         string
	ConsumerAssignmentsTable string
	ConsumerCheckpointsTable string
	ConsumerGapSkipsTable    string
	DispatcherStrategy       DispatcherStrategy
	NotifyConnectionString   string
	NotifyChannel            string
	BatchSize                int
	MaxConsecutiveFailures   int
	PollInterval             time.Duration
	MaxPollInterval          time.Duration
	DispatcherInterval       time.Duration
	HeartbeatInterval        time.Duration
	HeartbeatTimeout         time.Duration
	RebalanceInterval        time.Duration
	BatchPause               time.Duration
	BatchTimeout             time.Duration
	StaleGapThreshold        time.Duration
	StaleGapHarborLag        int
}

// Option configures a Worker.
type Option func(*Config)

// DefaultConfig returns sensible defaults for worker processing, coordination,
// and observability.
func DefaultConfig() Config {
	return Config{
		BatchSize:                100,
		MaxConsecutiveFailures:   5,
		PollInterval:             time.Second,
		MaxPollInterval:          30 * time.Second,
		DispatcherInterval:       200 * time.Millisecond,
		HeartbeatInterval:        5 * time.Second,
		HeartbeatTimeout:         30 * time.Second,
		RebalanceInterval:        5 * time.Second,
		BatchPause:               200 * time.Millisecond,
		BatchTimeout:             30 * time.Second,
		StaleGapThreshold:        30 * time.Second,
		Logger:                   store.NoOpLogger{},
		WorkerNodesTable:         postgres.DefaultWorkerNodesTable,
		ConsumerAssignmentsTable: postgres.DefaultConsumerAssignmentsTable,
		ConsumerCheckpointsTable: postgres.DefaultConsumerCheckpointsTable,
		ConsumerGapSkipsTable:    postgres.DefaultConsumerGapSkipsTable,
		DispatcherStrategy:       DispatcherStrategyPoll,
		NotifyChannel:            "worker_events",
		StaleGapHarborLag:        8,
	}
}

// WithBatchSize sets the maximum number of events processed per batch.
func WithBatchSize(n int) Option {
	return func(c *Config) {
		c.BatchSize = n
	}
}

// WithPollInterval sets the base interval between consumer polls.
func WithPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.PollInterval = d
	}
}

// WithMaxPollInterval sets the maximum interval used by adaptive polling
// backoff.
func WithMaxPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.MaxPollInterval = d
	}
}

// WithDispatcherInterval sets the interval used by the wakeup dispatcher.
func WithDispatcherInterval(d time.Duration) Option {
	return func(c *Config) {
		c.DispatcherInterval = d
	}
}

// WithHeartbeatInterval sets how often a worker refreshes its heartbeat.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatInterval = d
	}
}

// WithHeartbeatTimeout sets the maximum age of a heartbeat before a worker is
// considered dead.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.HeartbeatTimeout = d
	}
}

// WithRebalanceInterval sets how often the leader checks whether consumer
// assignments need to be recalculated.
func WithRebalanceInterval(d time.Duration) Option {
	return func(c *Config) {
		c.RebalanceInterval = d
	}
}

// WithBatchPause sets the pause between consecutive catch-up batches.
func WithBatchPause(d time.Duration) Option {
	return func(c *Config) {
		c.BatchPause = d
	}
}

// WithBatchTimeout sets the maximum duration for a single batch processing cycle.
// If a batch (including DB operations and event handling) exceeds this duration,
// the context is canceled and the batch is rolled back.
func WithBatchTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.BatchTimeout = d
	}
}

// WithStaleGapThreshold sets how long the worker waits on the same missing
// global position before applying safe-harbor advancement.
func WithStaleGapThreshold(d time.Duration) Option {
	return func(c *Config) {
		c.StaleGapThreshold = d
	}
}

// WithStaleGapHarborLag sets how far behind the visible head the worker stays
// when advancing past a stale gap.
func WithStaleGapHarborLag(n int) Option {
	return func(c *Config) {
		c.StaleGapHarborLag = n
	}
}

// WithMaxConsecutiveFailures sets how many consecutive batch failures a consumer
// tolerates before the worker triggers a fatal shutdown. This prevents the worker
// from appearing healthy (heartbeat alive) while consumers make no progress.
func WithMaxConsecutiveFailures(n int) Option {
	return func(c *Config) {
		c.MaxConsecutiveFailures = n
	}
}

// WithLogger sets the logger used by the worker.
func WithLogger(l store.Logger) Option {
	return func(c *Config) {
		c.Logger = l
	}
}

// WithWorkerNodesTable sets the worker registration table name.
func WithWorkerNodesTable(name string) Option {
	return func(c *Config) {
		c.WorkerNodesTable = name
	}
}

// WithConsumerAssignmentsTable sets the consumer assignment table name.
func WithConsumerAssignmentsTable(name string) Option {
	return func(c *Config) {
		c.ConsumerAssignmentsTable = name
	}
}

// WithConsumerCheckpointsTable sets the consumer checkpoint table name.
func WithConsumerCheckpointsTable(name string) Option {
	return func(c *Config) {
		c.ConsumerCheckpointsTable = name
	}
}

// WithConsumerGapSkipsTable sets the consumer gap skip audit table name.
func WithConsumerGapSkipsTable(name string) Option {
	return func(c *Config) {
		c.ConsumerGapSkipsTable = name
	}
}

// WithDispatcherStrategy sets the worker wakeup dispatcher strategy.
func WithDispatcherStrategy(strategy DispatcherStrategy) Option {
	return func(c *Config) {
		c.DispatcherStrategy = strategy
	}
}

// WithNotifyConnectionString sets the PostgreSQL connection string used by the
// LISTEN/NOTIFY dispatcher.
func WithNotifyConnectionString(connStr string) Option {
	return func(c *Config) {
		c.NotifyConnectionString = connStr
	}
}

// WithNotifyChannel sets the PostgreSQL notification channel used by the notify dispatcher.
func WithNotifyChannel(channel string) Option {
	return func(c *Config) {
		c.NotifyChannel = channel
	}
}
