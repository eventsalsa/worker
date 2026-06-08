package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"
)

// PollDispatcher checks the latest global position on a fixed interval and emits
// wakeup signals when it detects new events.
type PollDispatcher struct {
	querier  PositionQuerier
	logger   store.Logger
	db       txBeginner
	wakeup   wakeupBroadcaster
	interval time.Duration
	lastPos  int64
	start    startGuard
}

// NewPollDispatcher constructs a poll-based dispatcher.
func NewPollDispatcher(db txBeginner, querier PositionQuerier, interval time.Duration, logger store.Logger) *PollDispatcher {
	return &PollDispatcher{
		db:       db,
		querier:  querier,
		interval: normalizedPollInterval(interval),
		wakeup:   newWakeupBroadcaster(),
		logger:   normalizedLogger(logger),
	}
}

// Start begins the polling loop and blocks until ctx is canceled.
func (d *PollDispatcher) Start(ctx context.Context) error {
	if !d.start.tryStart() {
		return ErrAlreadyRunning
	}
	defer d.start.stop()

	if d.db == nil {
		return ErrNilDB
	}
	if d.querier == nil {
		return ErrNilQuerier
	}

	position, err := d.latestPosition(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("initialize poll dispatcher checkpoint: %w", err)
	}
	d.lastPos = position

	d.logger.Debug(ctx, "poll dispatcher started", "interval", d.interval, "last_position", d.lastPos)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Debug(ctx, "poll dispatcher stopped")
			return nil
		case <-ticker.C:
			position, err := d.latestPosition(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				d.logger.Error(ctx, "poll dispatcher query failed", "error", err)
				continue
			}

			if position <= d.lastPos {
				continue
			}

			d.lastPos = position
			d.signalWakeup()
			d.logger.Debug(ctx, "poll dispatcher detected new events", "last_position", d.lastPos)
		}
	}
}

// WakeupChan returns the wakeup channel shared with consumer goroutines.
func (d *PollDispatcher) WakeupChan() <-chan struct{} {
	return d.wakeup.Channel()
}

func (d *PollDispatcher) latestPosition(ctx context.Context) (int64, error) {
	tx, err := d.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, ctx.Err()
		}
		return 0, err
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger.Error(ctx, "poll dispatcher latest position rollback failed", "error", rollbackErr)
		}
	}()

	return d.querier.GetLatestGlobalPosition(ctx, tx)
}

func (d *PollDispatcher) signalWakeup() {
	d.wakeup.Signal()
}
