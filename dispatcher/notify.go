package dispatcher

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/eventsalsa/store"
	"github.com/lib/pq"
)

// NotifyDispatcher listens for PostgreSQL NOTIFY events and emits wakeup signals.
// It also performs a slower reconciliation poll so that workers still wake up if
// LISTEN/NOTIFY is unavailable or notifications are missed.
type NotifyDispatcher struct {
	querier PositionQuerier
	logger  store.Logger
	db      *sql.DB
	connStr string
	channel string
	wakeup  wakeupBroadcaster
	lastPos int64
	start   startGuard
}

// NewNotifyDispatcher constructs a LISTEN/NOTIFY-based dispatcher.
func NewNotifyDispatcher(connStr, channel string, db *sql.DB, querier PositionQuerier, logger store.Logger) *NotifyDispatcher {
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
func (d *NotifyDispatcher) Start(ctx context.Context) error { //nolint:gocyclo // select loop with fallback paths
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
	if d.channel == "" {
		return ErrEmptyChannel
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

	listener, notifyCh := d.newListener(ctx)
	if listener != nil {
		defer listener.Close()
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
		case notification, ok := <-notifyCh:
			if !ok {
				notifyCh = nil
				continue
			}
			if notification == nil {
				continue
			}
			d.logger.Debug(ctx, "notify dispatcher received notification", "channel", notification.Channel)
			if err := d.reconcile(ctx); err != nil && ctx.Err() == nil {
				d.logger.Error(ctx, "notify dispatcher notification reconciliation failed", "error", err)
			}
		}
	}
}

// WakeupChan returns the wakeup channel shared with consumer goroutines.
func (d *NotifyDispatcher) WakeupChan() <-chan struct{} {
	return d.wakeup.Channel()
}

func (d *NotifyDispatcher) latestPosition(ctx context.Context) (int64, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback() //nolint:errcheck // best-effort rollback on deferred read-only tx
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

func (d *NotifyDispatcher) newListener(ctx context.Context) (listener *pq.Listener, notifyCh <-chan *pq.Notification) {
	if d.connStr == "" {
		d.logger.Info(ctx, "notify dispatcher falling back to reconciliation polling", "reason", "empty connection string")
		return nil, nil
	}

	listener = pq.NewListener(d.connStr, defaultListenerMinInterval, defaultListenerMaxInterval, func(event pq.ListenerEventType, err error) {
		if err != nil {
			d.logger.Error(context.Background(), "notify dispatcher listener event", "event", event, "error", err)
			return
		}
		if event == pq.ListenerEventReconnected {
			d.logger.Info(context.Background(), "notify dispatcher reconnected", "channel", d.channel)
		}
	})

	if err := listener.Listen(d.channel); err != nil {
		d.logger.Error(ctx, "notify dispatcher listen failed, using reconciliation polling", "channel", d.channel, "error", err)
		_ = listener.Close()
		return nil, nil
	}

	return listener, listener.Notify
}
