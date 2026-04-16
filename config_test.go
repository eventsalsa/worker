package worker

import (
	"context"
	"testing"
	"time"

	"github.com/eventsalsa/store"

	"github.com/eventsalsa/worker/postgres"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.BatchSize != 100 {
		t.Fatalf("BatchSize = %d, want 100", config.BatchSize)
	}
	if config.PollInterval != time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, time.Second)
	}
	if config.MaxPollInterval != 30*time.Second {
		t.Fatalf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 30*time.Second)
	}
	if config.DispatcherInterval != 200*time.Millisecond {
		t.Fatalf("DispatcherInterval = %v, want %v", config.DispatcherInterval, 200*time.Millisecond)
	}
	if config.HeartbeatInterval != 5*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 5*time.Second)
	}
	if config.HeartbeatTimeout != 30*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want %v", config.HeartbeatTimeout, 30*time.Second)
	}
	if config.RebalanceInterval != 5*time.Second {
		t.Fatalf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 5*time.Second)
	}
	if config.BatchPause != 200*time.Millisecond {
		t.Fatalf("BatchPause = %v, want %v", config.BatchPause, 200*time.Millisecond)
	}
	if config.BatchTimeout != 30*time.Second {
		t.Fatalf("BatchTimeout = %v, want %v", config.BatchTimeout, 30*time.Second)
	}
	if config.StaleGapThreshold != 30*time.Second {
		t.Fatalf("StaleGapThreshold = %v, want %v", config.StaleGapThreshold, 30*time.Second)
	}
	if config.StaleGapHarborLag != 8 {
		t.Fatalf("StaleGapHarborLag = %d, want 8", config.StaleGapHarborLag)
	}
	if config.MaxConsecutiveFailures != 5 {
		t.Fatalf("MaxConsecutiveFailures = %d, want 5", config.MaxConsecutiveFailures)
	}
	if config.ConsumerGapSkipsTable != postgres.DefaultConsumerGapSkipsTable {
		t.Fatalf("ConsumerGapSkipsTable = %q, want %q", config.ConsumerGapSkipsTable, postgres.DefaultConsumerGapSkipsTable)
	}
	if _, ok := config.Logger.(store.NoOpLogger); !ok {
		t.Fatalf("Logger = %T, want store.NoOpLogger", config.Logger)
	}
}

func TestApplyOptionsComposesMultipleOptions(t *testing.T) {
	logger := configTestLogger{}

	config := applyOptions(
		WithBatchSize(17),
		WithPollInterval(2*time.Second),
		WithMaxPollInterval(9*time.Second),
		WithDispatcherInterval(250*time.Millisecond),
		WithHeartbeatInterval(6*time.Second),
		WithHeartbeatTimeout(45*time.Second),
		WithRebalanceInterval(7*time.Second),
		WithBatchPause(300*time.Millisecond),
		WithStaleGapThreshold(45*time.Second),
		WithStaleGapHarborLag(5),
		WithConsumerGapSkipsTable("custom_gap_skips"),
		WithLogger(logger),
	)

	if config.BatchSize != 17 {
		t.Fatalf("BatchSize = %d, want 17", config.BatchSize)
	}
	if config.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, 2*time.Second)
	}
	if config.MaxPollInterval != 9*time.Second {
		t.Fatalf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 9*time.Second)
	}
	if config.DispatcherInterval != 250*time.Millisecond {
		t.Fatalf("DispatcherInterval = %v, want %v", config.DispatcherInterval, 250*time.Millisecond)
	}
	if config.HeartbeatInterval != 6*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 6*time.Second)
	}
	if config.HeartbeatTimeout != 45*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want %v", config.HeartbeatTimeout, 45*time.Second)
	}
	if config.RebalanceInterval != 7*time.Second {
		t.Fatalf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 7*time.Second)
	}
	if config.BatchPause != 300*time.Millisecond {
		t.Fatalf("BatchPause = %v, want %v", config.BatchPause, 300*time.Millisecond)
	}
	if config.StaleGapThreshold != 45*time.Second {
		t.Fatalf("StaleGapThreshold = %v, want %v", config.StaleGapThreshold, 45*time.Second)
	}
	if config.StaleGapHarborLag != 5 {
		t.Fatalf("StaleGapHarborLag = %d, want 5", config.StaleGapHarborLag)
	}
	if config.ConsumerGapSkipsTable != "custom_gap_skips" {
		t.Fatalf("ConsumerGapSkipsTable = %q, want %q", config.ConsumerGapSkipsTable, "custom_gap_skips")
	}
	if config.Logger != logger {
		t.Fatalf("Logger = %v, want %v", config.Logger, logger)
	}
}

func TestOptionFunctions(t *testing.T) {
	logger := store.NoOpLogger{}
	config := Config{}

	WithBatchSize(17)(&config)
	WithPollInterval(2 * time.Second)(&config)
	WithMaxPollInterval(9 * time.Second)(&config)
	WithDispatcherInterval(250 * time.Millisecond)(&config)
	WithHeartbeatInterval(6 * time.Second)(&config)
	WithHeartbeatTimeout(45 * time.Second)(&config)
	WithRebalanceInterval(7 * time.Second)(&config)
	WithBatchPause(300 * time.Millisecond)(&config)
	WithStaleGapThreshold(45 * time.Second)(&config)
	WithStaleGapHarborLag(5)(&config)
	WithConsumerGapSkipsTable("custom_gap_skips")(&config)
	WithLogger(logger)(&config)

	if config.BatchSize != 17 {
		t.Fatalf("BatchSize = %d, want 17", config.BatchSize)
	}
	if config.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, 2*time.Second)
	}
	if config.MaxPollInterval != 9*time.Second {
		t.Fatalf("MaxPollInterval = %v, want %v", config.MaxPollInterval, 9*time.Second)
	}
	if config.DispatcherInterval != 250*time.Millisecond {
		t.Fatalf("DispatcherInterval = %v, want %v", config.DispatcherInterval, 250*time.Millisecond)
	}
	if config.HeartbeatInterval != 6*time.Second {
		t.Fatalf("HeartbeatInterval = %v, want %v", config.HeartbeatInterval, 6*time.Second)
	}
	if config.HeartbeatTimeout != 45*time.Second {
		t.Fatalf("HeartbeatTimeout = %v, want %v", config.HeartbeatTimeout, 45*time.Second)
	}
	if config.RebalanceInterval != 7*time.Second {
		t.Fatalf("RebalanceInterval = %v, want %v", config.RebalanceInterval, 7*time.Second)
	}
	if config.BatchPause != 300*time.Millisecond {
		t.Fatalf("BatchPause = %v, want %v", config.BatchPause, 300*time.Millisecond)
	}
	if config.StaleGapThreshold != 45*time.Second {
		t.Fatalf("StaleGapThreshold = %v, want %v", config.StaleGapThreshold, 45*time.Second)
	}
	if config.StaleGapHarborLag != 5 {
		t.Fatalf("StaleGapHarborLag = %d, want 5", config.StaleGapHarborLag)
	}
	if config.ConsumerGapSkipsTable != "custom_gap_skips" {
		t.Fatalf("ConsumerGapSkipsTable = %q, want %q", config.ConsumerGapSkipsTable, "custom_gap_skips")
	}
	if config.Logger != logger {
		t.Fatalf("Logger = %v, want %v", config.Logger, logger)
	}
}

func TestApplyOptionsSkipsNilOptions(t *testing.T) {
	defaults := DefaultConfig()
	nilOption := Option(nil)

	config := applyOptions(nilOption, WithBatchSize(23))

	if config.BatchSize != 23 {
		t.Fatalf("BatchSize = %d, want 23", config.BatchSize)
	}
	if config.PollInterval != defaults.PollInterval {
		t.Fatalf("PollInterval = %v, want default %v", config.PollInterval, defaults.PollInterval)
	}
}

func TestApplyOptionsNormalizesInvalidValues(t *testing.T) {
	defaults := DefaultConfig()
	customLogger := &configTestLogger{}

	config := applyOptions(
		WithBatchSize(0),
		WithPollInterval(5*time.Second),
		WithMaxPollInterval(time.Second),
		WithDispatcherInterval(0),
		WithHeartbeatInterval(0),
		WithHeartbeatTimeout(0),
		WithRebalanceInterval(0),
		WithBatchPause(-time.Second),
		WithStaleGapThreshold(0),
		WithStaleGapHarborLag(-1),
		WithConsumerGapSkipsTable(""),
		WithLogger(nil),
		nil,
	)

	if config.BatchSize != defaults.BatchSize {
		t.Fatalf("BatchSize = %d, want default %d", config.BatchSize, defaults.BatchSize)
	}
	if config.PollInterval != 5*time.Second {
		t.Fatalf("PollInterval = %v, want %v", config.PollInterval, 5*time.Second)
	}
	if config.MaxPollInterval != defaults.MaxPollInterval {
		t.Fatalf("MaxPollInterval = %v, want default %v", config.MaxPollInterval, defaults.MaxPollInterval)
	}
	if config.DispatcherInterval != defaults.DispatcherInterval {
		t.Fatalf("DispatcherInterval = %v, want default %v", config.DispatcherInterval, defaults.DispatcherInterval)
	}
	if config.HeartbeatInterval != defaults.HeartbeatInterval {
		t.Fatalf("HeartbeatInterval = %v, want default %v", config.HeartbeatInterval, defaults.HeartbeatInterval)
	}
	if config.HeartbeatTimeout != defaults.HeartbeatTimeout {
		t.Fatalf("HeartbeatTimeout = %v, want default %v", config.HeartbeatTimeout, defaults.HeartbeatTimeout)
	}
	if config.RebalanceInterval != defaults.RebalanceInterval {
		t.Fatalf("RebalanceInterval = %v, want default %v", config.RebalanceInterval, defaults.RebalanceInterval)
	}
	if config.BatchPause != defaults.BatchPause {
		t.Fatalf("BatchPause = %v, want default %v", config.BatchPause, defaults.BatchPause)
	}
	if config.StaleGapThreshold != defaults.StaleGapThreshold {
		t.Fatalf("StaleGapThreshold = %v, want default %v", config.StaleGapThreshold, defaults.StaleGapThreshold)
	}
	if config.StaleGapHarborLag != defaults.StaleGapHarborLag {
		t.Fatalf("StaleGapHarborLag = %d, want default %d", config.StaleGapHarborLag, defaults.StaleGapHarborLag)
	}
	if config.ConsumerGapSkipsTable != defaults.ConsumerGapSkipsTable {
		t.Fatalf("ConsumerGapSkipsTable = %q, want default %q", config.ConsumerGapSkipsTable, defaults.ConsumerGapSkipsTable)
	}
	if _, ok := config.Logger.(store.NoOpLogger); !ok {
		t.Fatalf("Logger = %T, want store.NoOpLogger", config.Logger)
	}

	config = applyOptions(WithLogger(customLogger))
	if config.Logger != customLogger {
		t.Fatalf("Logger = %v, want %v", config.Logger, customLogger)
	}
}

func TestApplyOptionsCapsStaleGapHarborLagToBatchWindow(t *testing.T) {
	config := applyOptions(
		WithBatchSize(5),
		WithStaleGapHarborLag(99),
	)

	if config.StaleGapHarborLag != 4 {
		t.Fatalf("StaleGapHarborLag = %d, want 4", config.StaleGapHarborLag)
	}
}

func TestApplyOptionsAllowsZeroStaleGapHarborLag(t *testing.T) {
	config := applyOptions(WithStaleGapHarborLag(0))

	if config.StaleGapHarborLag != 0 {
		t.Fatalf("StaleGapHarborLag = %d, want 0", config.StaleGapHarborLag)
	}
}

type configTestLogger struct{}

func (configTestLogger) Debug(_ context.Context, _ string, _ ...interface{}) {}
func (configTestLogger) Info(_ context.Context, _ string, _ ...interface{})  {}
func (configTestLogger) Error(_ context.Context, _ string, _ ...interface{}) {}
