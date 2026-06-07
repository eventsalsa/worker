package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"
)

// NotifyDispatcher listens for PostgreSQL NOTIFY events and emits wakeup signals.
// It also performs a slower reconciliation poll so that workers still wake up if
// LISTEN/NOTIFY is unavailable or notifications are missed.
type NotifyDispatcher struct {
	querier PositionQuerier
	logger  store.Logger
	db      txBeginner
	connStr string
	channel string
	wakeup  wakeupBroadcaster
	lastPos int64
	start   startGuard
}

// NewNotifyDispatcher constructs a LISTEN/NOTIFY-based dispatcher.
func NewNotifyDispatcher(connStr, channel string, db txBeginner, querier PositionQuerier, logger store.Logger) *NotifyDispatcher {
	return &NotifyDispatcher{
		connStr: connStr,
		channel: channel,
		querier: querier,
		db:      db,
		wakeup:  newWakeupBroadcaster(),
		logger:  normalizedLogger(logger),
	}
}

// Start begins the LISTEN/NOTIFY loop and blocks until ctx is canceled.
func (d *NotifyDispatcher) Start(ctx context.Context) error {
	if !d.start.tryStart() {
		return ErrAlreadyRunning
	}
	defer d.start.stop()

	if err := d.validate(); err != nil {
		return err
	}

	position, err := d.latestPosition(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("initialize notify dispatcher checkpoint: %w", err)
	}
	d.lastPos = position

	d.logger.Debug(ctx, "notify dispatcher started", "channel", d.channel, "last_position", d.lastPos)

	notifyCh := make(chan struct{}, 1)
	if d.connStr != "" {
		go d.listenNotify(ctx, notifyCh)
	} else {
		d.logger.Info(ctx, "notify dispatcher falling back to reconciliation polling", "reason", "empty connection string")
	}

	reconcileTicker := time.NewTicker(defaultNotifyPollFallback)
	defer reconcileTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Debug(ctx, "notify dispatcher stopped")
			return nil
		case <-reconcileTicker.C:
			if err := d.reconcile(ctx); err != nil && ctx.Err() == nil {
				d.logger.Error(ctx, "notify dispatcher reconciliation failed", "error", err)
			}
		case <-notifyCh:
			if err := d.reconcile(ctx); err != nil && ctx.Err() == nil {
				d.logger.Error(ctx, "notify dispatcher notification reconciliation failed", "error", err)
			}
		}
	}
}

func (d *NotifyDispatcher) validate() error {
	if d.db == nil {
		return ErrNilDB
	}
	if d.querier == nil {
		return ErrNilQuerier
	}
	if d.channel == "" {
		return ErrEmptyChannel
	}
	return nil
}

// WakeupChan returns the wakeup channel shared with consumer goroutines.
func (d *NotifyDispatcher) WakeupChan() <-chan struct{} {
	return d.wakeup.Channel()
}

func (d *NotifyDispatcher) latestPosition(ctx context.Context) (int64, error) {
	tx, err := d.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, err
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger.Error(ctx, "notify dispatcher latest position rollback failed", "error", rollbackErr)
		}
	}()

	return d.querier.GetLatestGlobalPosition(ctx, tx)
}

func (d *NotifyDispatcher) reconcile(ctx context.Context) error {
	position, err := d.latestPosition(ctx)
	if err != nil {
		return err
	}
	if position <= d.lastPos {
		return nil
	}

	d.lastPos = position
	d.signalWakeup()
	d.logger.Debug(ctx, "notify dispatcher detected new events", "last_position", d.lastPos)

	return nil
}

func (d *NotifyDispatcher) signalWakeup() {
	d.wakeup.Signal()
}

func (d *NotifyDispatcher) listenNotify(ctx context.Context, notifyCh chan<- struct{}) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := pgx.Connect(ctx, d.connStr)
		if err != nil {
			d.logger.Error(ctx, "notify dispatcher listener connection failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			}
		}
		backoff = 1 * time.Second

		// Run LISTEN command
		_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", d.channel))
		if err != nil {
			d.logger.Error(ctx, "notify dispatcher listen failed", "channel", d.channel, "error", err)
			_ = conn.Close(ctx)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				continue
			}
		}

		d.logger.Info(ctx, "notify dispatcher listening", "channel", d.channel)

		for {
			notification, err := conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					_ = conn.Close(ctx)
					return
				}
				d.logger.Error(ctx, "notify dispatcher connection lost", "error", err)
				_ = conn.Close(ctx)
				break // break inner loop to reconnect
			}

			if notification != nil && notification.Channel == d.channel {
				select {
				case notifyCh <- struct{}{}:
				default:
					// Already has a pending signal, skip to avoid blocking
				}
			}
		}
	}
}
