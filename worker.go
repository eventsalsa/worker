package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/eventsalsa/store"
	"github.com/eventsalsa/store/consumer"
	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/eventsalsa/worker/dispatcher"
	"github.com/eventsalsa/worker/postgres"
)

const (
	defaultAssignmentPollInterval = 2 * time.Second
	defaultShutdownTimeout        = 5 * time.Second
	missingRegistrationRetryLimit = 2
	startupCleanupMultiplier      = 2
	staleGapRetryLimit            = 3
)

var (
	// ErrNilDB indicates that a worker was created without a database handle.
	ErrNilDB = errors.New("worker requires a database handle")

	// ErrNilStore indicates that a worker was created without an event store.
	ErrNilStore = errors.New("worker requires an event store")

	// ErrNilDispatcher indicates that a worker was created without a dispatcher.
	ErrNilDispatcher = errors.New("worker requires a dispatcher")

	// ErrAlreadyStarted indicates that Start was called while the worker is already running.
	ErrAlreadyStarted = errors.New("worker already started")

	// ErrMissingNotifyConnectionString indicates that the notify dispatcher was selected without a connection string.
	ErrMissingNotifyConnectionString = errors.New("worker notify dispatcher requires a connection string")

	// ErrConsecutiveFailures indicates that a consumer exceeded the maximum allowed
	// consecutive batch failures, signaling an infrastructure-level problem.
	ErrConsecutiveFailures = errors.New("consumer exceeded max consecutive failures")

	errConsumerOwnershipLost = errors.New("consumer is no longer assigned to this worker")
)

// workerStore captures the event store behavior needed by Worker.
//
// Scoped consumers (consumer.ScopedConsumer) are filtered in-memory after the
// worker performs an unscoped probe of the global event stream.
type workerStore interface {
	dispatcher.PositionQuerier
	store.EventReader
}

// Worker orchestrates distributed consumer execution for a single worker node.
type Worker struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	config           Config
	dispatcher       dispatcher.Dispatcher
	store            workerStore
	cancel           context.CancelFunc
	processingCancel context.CancelFunc
	fatalErrCh       chan error
	runningConsumers map[string]context.CancelFunc
	consumerDone     map[string]chan struct{}
	db               *sql.DB
	leaderConn       *sql.Conn
	consumers        []consumer.Consumer
	wg               sync.WaitGroup
	mu               sync.Mutex
	id               uuid.UUID
	isLeader         bool
	started          bool
}

type processedBatch struct {
	checkpoint   int64
	handledCount int
	progressed   bool
	blockedByGap bool
	fullWindow   bool
	staleSkipped bool
}

type frontierProbe struct {
	firstSeenAt      time.Time
	rows             []store.PersistedEvent
	checkpoint       int64
	targetCheckpoint int64
	highestVisible   int64
	gapPosition      int64
	fullWindow       bool
	blockedByGap     bool
	staleSkipped     bool
}

// New constructs a Worker with the provided database handle, event store, and consumers.
func New(db *sql.DB, eventStore workerStore, consumers []consumer.Consumer, opts ...Option) *Worker {
	config := applyOptions(opts...)

	return &Worker{
		id:               uuid.New(),
		db:               db,
		store:            eventStore,
		consumers:        append([]consumer.Consumer(nil), consumers...),
		config:           config,
		dispatcher:       newDispatcher(db, eventStore, &config),
		runningConsumers: make(map[string]context.CancelFunc),
		consumerDone:     make(map[string]chan struct{}),
	}
}

// ID returns the unique identifier of this worker node.
func (w *Worker) ID() uuid.UUID {
	return w.id
}

// Start registers the worker, starts its background loops, and blocks until the
// provided context is canceled or a fatal internal error occurs.
func (w *Worker) Start(parent context.Context) (err error) {
	if err := w.validate(); err != nil {
		return err
	}

	controlCtx, controlCancel := context.WithCancel(parent)
	processingCtx, processingCancel := context.WithCancel(context.Background())
	if err := w.markStarted(controlCancel, processingCancel); err != nil {
		controlCancel()
		processingCancel()
		return err
	}
	defer w.markStopped()

	registered := false
	defer func() {
		controlCancel()
		w.shutdown(&registered)
	}()

	if err := w.initialize(controlCtx, &registered); err != nil {
		return err
	}

	w.fatalErrCh = make(chan error, 1)

	w.startBackground(controlCtx, func() { w.runHeartbeatLoop(controlCtx) })
	w.startBackground(controlCtx, func() { w.runLeaderLoop(controlCtx) })
	w.startBackground(controlCtx, func() { w.runAssignmentLoop(controlCtx, processingCtx) })
	w.startBackground(controlCtx, func() {
		if runErr := w.dispatcher.Start(controlCtx); runErr != nil && controlCtx.Err() == nil {
			w.reportFatal(fmt.Errorf("start dispatcher: %w", runErr))
		}
	})

	select {
	case runErr := <-w.fatalErrCh:
		return runErr
	case <-controlCtx.Done():
		return nil
	}
}

func (w *Worker) initialize(ctx context.Context, registered *bool) error {
	cleanupThreshold := w.startupWorkerCleanupThreshold()
	removedRows, err := postgres.CleanupStaleWorkers(ctx, w.db, w.workerNodesTable(), cleanupThreshold)
	if err != nil {
		w.logger().Error(ctx,
			"failed to clean stale worker registrations",
			"worker_id", w.id,
			"threshold", cleanupThreshold,
			"error", err,
		)
	} else if removedRows > 0 {
		w.logger().Info(ctx,
			"cleaned stale worker registrations",
			"worker_id", w.id,
			"threshold", cleanupThreshold,
			"removed_rows", removedRows,
		)
	}

	if err := postgres.RegisterWorker(ctx, w.db, w.workerNodesTable(), w.id); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	*registered = true
	w.logger().Info(ctx, "worker registered", "worker_id", w.id)

	consumerNames := w.consumerNames()
	if err := postgres.EnsureConsumersRegistered(ctx, w.db, w.consumerAssignmentsTable(), consumerNames); err != nil {
		return fmt.Errorf("ensure consumers registered: %w", err)
	}

	for _, consumerName := range consumerNames {
		if err := postgres.EnsureCheckpointExists(ctx, w.db, w.consumerCheckpointsTable(), consumerName); err != nil {
			return fmt.Errorf("ensure checkpoint exists for consumer %s: %w", consumerName, err)
		}
	}

	return nil
}

func (w *Worker) shutdown(registered *bool) {
	logger := w.logger()
	w.stopAllConsumers()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer shutdownCancel()

	if *registered {
		if removeErr := postgres.RemoveWorker(shutdownCtx, w.db, w.workerNodesTable(), w.id); removeErr != nil {
			logger.Error(shutdownCtx, "failed to remove worker registration", "worker_id", w.id, "error", removeErr)
		} else {
			logger.Info(shutdownCtx, "worker removed", "worker_id", w.id)
		}
	}

	if releaseErr := w.releaseLeaderConnection(shutdownCtx); releaseErr != nil {
		logger.Error(shutdownCtx, "failed to release leader connection", "worker_id", w.id, "error", releaseErr)
	}

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(defaultShutdownTimeout):
		if processingCancel := w.getProcessingCancel(); processingCancel != nil {
			logger.Error(shutdownCtx, "worker shutdown timed out; canceling active batches", "worker_id", w.id)
			processingCancel()
		}

		select {
		case <-done:
		case <-time.After(defaultShutdownTimeout):
			logger.Error(shutdownCtx, "worker shutdown did not complete after forced cancellation", "worker_id", w.id)
		}
	}
}

// Stop requests shutdown of a running worker.
func (w *Worker) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// reportFatal sends a fatal error to the worker's error channel and cancels
// the control context to initiate shutdown. Safe for concurrent calls; only
// the first error is captured.
func (w *Worker) reportFatal(err error) {
	if err == nil {
		return
	}

	select {
	case w.fatalErrCh <- err:
	default:
	}

	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (w *Worker) validate() error {
	if w.db == nil {
		return ErrNilDB
	}
	if w.store == nil {
		return ErrNilStore
	}
	if w.dispatcher == nil {
		return ErrNilDispatcher
	}
	if w.config.DispatcherStrategy == DispatcherStrategyNotify && strings.TrimSpace(w.config.NotifyConnectionString) == "" {
		return ErrMissingNotifyConnectionString
	}

	seen := make(map[string]struct{}, len(w.consumers))
	for idx, registeredConsumer := range w.consumers {
		if registeredConsumer == nil {
			return fmt.Errorf("consumer at index %d is nil", idx)
		}

		name := registeredConsumer.Name()
		if name == "" {
			return fmt.Errorf("consumer at index %d has empty name", idx)
		}

		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate consumer name %q", name)
		}

		seen[name] = struct{}{}
	}

	return nil
}

func (w *Worker) markStarted(cancel, processingCancel context.CancelFunc) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.started {
		return ErrAlreadyStarted
	}

	w.cancel = cancel
	w.processingCancel = processingCancel
	w.started = true
	return nil
}

func (w *Worker) markStopped() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.cancel = nil
	w.processingCancel = nil
	w.started = false
}

func (w *Worker) startBackground(ctx context.Context, fn func()) {
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()

		select {
		case <-ctx.Done():
			return
		default:
		}

		fn()
	}()
}

func (w *Worker) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	missingRegistrationCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := postgres.UpdateHeartbeat(ctx, w.db, w.workerNodesTable(), w.id); err != nil && ctx.Err() == nil {
				if errors.Is(err, postgres.ErrWorkerRegistrationMissing) {
					missingRegistrationCount++
					if missingRegistrationCount < missingRegistrationRetryLimit {
						w.logger().Error(ctx,
							"worker registration missing during heartbeat; retrying before shutdown",
							"worker_id", w.id,
							"attempt", missingRegistrationCount,
							"error", err,
						)
						continue
					}
					w.reportFatal(fmt.Errorf("worker registration lost during heartbeat: %w", err))
					return
				}
				missingRegistrationCount = 0
				w.logger().Error(ctx, "failed to update worker heartbeat", "worker_id", w.id, "error", err)
				continue
			}

			missingRegistrationCount = 0
		}
	}
}

func (w *Worker) runLeaderLoop(ctx context.Context) {
	if err := w.leaderCycle(ctx); err != nil && ctx.Err() == nil {
		w.logger().Error(ctx, "leader cycle failed", "worker_id", w.id, "error", err)
	}

	ticker := time.NewTicker(w.config.RebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.leaderCycle(ctx); err != nil && ctx.Err() == nil {
				w.logger().Error(ctx, "leader cycle failed", "worker_id", w.id, "error", err)
			}
		}
	}
}

func (w *Worker) leaderCycle(ctx context.Context) error {
	conn, err := w.ensureLeaderConn(ctx)
	if err != nil {
		return err
	}

	if w.leaderActive() {
		if pingErr := conn.PingContext(ctx); pingErr != nil {
			w.logger().Error(ctx, "leader connection ping failed; relinquishing leadership", "worker_id", w.id, "error", pingErr)
			w.dropLeaderConnection(ctx)
			return nil
		}

		return w.rebalance(ctx, conn)
	}

	acquired, err := tryAcquireLeaderLock(ctx, conn)
	if err != nil {
		w.logger().Error(ctx, "failed to acquire leader lock", "worker_id", w.id, "error", err)
		w.dropLeaderConnection(ctx)
		return nil
	}

	if !acquired {
		return nil
	}

	w.setLeader(true)
	w.logger().Info(ctx, "worker became leader", "worker_id", w.id)

	if err := w.rebalance(ctx, conn); err != nil {
		return fmt.Errorf("rebalance after leader acquisition: %w", err)
	}

	return nil
}

func (w *Worker) rebalance(ctx context.Context, conn *sql.Conn) error {
	liveWorkers, err := postgres.ListLiveWorkers(ctx, conn, w.workerNodesTable(), w.config.HeartbeatTimeout)
	if err != nil {
		return fmt.Errorf("list live workers: %w", err)
	}

	assignments, err := postgres.GetAssignments(ctx, conn, w.consumerAssignmentsTable())
	if err != nil {
		return fmt.Errorf("get assignments: %w", err)
	}

	if !postgres.NeedsRebalance(assignments, liveWorkers) {
		w.logger().Debug(ctx, "rebalance skipped; assignments already balanced", "worker_id", w.id, "live_workers", len(liveWorkers))
		return nil
	}

	consumerNames := make([]string, 0, len(assignments))
	for _, assignment := range assignments {
		consumerNames = append(consumerNames, assignment.ConsumerName)
	}

	nextAssignments := postgres.ComputeAssignments(consumerNames, liveWorkers)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin rebalance transaction: %w", err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			w.logger().Error(ctx, "rebalance rollback failed", "worker_id", w.id, "error", rollbackErr)
		}
	}()

	if err := applyAssignments(ctx, tx, w.consumerAssignmentsTable(), nextAssignments); err != nil {
		return fmt.Errorf("apply assignments: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rebalance transaction: %w", err)
	}
	committed = true

	w.logger().Info(ctx, "consumer assignments updated", "worker_id", w.id, "live_workers", len(liveWorkers), "consumers", len(consumerNames))
	return nil
}

func (w *Worker) runAssignmentLoop(ctx, processingCtx context.Context) {
	if err := w.syncAssignments(ctx, processingCtx); err != nil && ctx.Err() == nil {
		w.logger().Error(ctx, "assignment sync failed", "worker_id", w.id, "error", err)
	}

	ticker := time.NewTicker(w.assignmentPollInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.syncAssignments(ctx, processingCtx); err != nil && ctx.Err() == nil {
				w.logger().Error(ctx, "assignment sync failed", "worker_id", w.id, "error", err)
			}
		}
	}
}

func (w *Worker) syncAssignments(ctx, processingCtx context.Context) error {
	assignments, err := postgres.GetAssignments(ctx, w.db, w.consumerAssignmentsTable())
	if err != nil {
		return fmt.Errorf("get assignments: %w", err)
	}

	desired := make(map[string]consumer.Consumer, len(w.consumers))
	for _, assignment := range assignments {
		if !assignment.Assigned || assignment.WorkerID != w.id {
			continue
		}

		registeredConsumer, ok := w.consumerByName(assignment.ConsumerName)
		if !ok {
			continue
		}

		desired[assignment.ConsumerName] = registeredConsumer
	}

	type consumerStart struct {
		consumer consumer.Consumer
		ctx      context.Context
		done     chan struct{}
	}

	type consumerStop struct {
		cancel context.CancelFunc
		name   string
	}

	var toStart []consumerStart
	var toStop []consumerStop

	w.mu.Lock()
	for name, assignedConsumer := range desired {
		if _, running := w.runningConsumers[name]; running {
			continue
		}

		consumerCtx, consumerCancel := context.WithCancel(context.Background()) //nolint:gosec // cancel stored in runningConsumers for later cleanup
		done := make(chan struct{})
		w.runningConsumers[name] = consumerCancel
		w.consumerDone[name] = done
		toStart = append(toStart, consumerStart{
			consumer: assignedConsumer,
			ctx:      consumerCtx,
			done:     done,
		})
	}

	for name, cancel := range w.runningConsumers {
		if _, shouldRun := desired[name]; shouldRun {
			continue
		}

		toStop = append(toStop, consumerStop{
			name:   name,
			cancel: cancel,
		})
	}
	w.mu.Unlock()

	for _, stopRequest := range toStop {
		w.logger().Info(ctx, "stopping consumer", "worker_id", w.id, "consumer", stopRequest.name)
		stopRequest.cancel()
	}

	for _, startRequest := range toStart {
		w.logger().Info(ctx, "starting consumer", "worker_id", w.id, "consumer", startRequest.consumer.Name())

		w.wg.Add(1)
		go func(consumerCtx context.Context, done chan struct{}, registeredConsumer consumer.Consumer) {
			defer w.wg.Done()
			defer close(done)
			defer w.finishConsumer(registeredConsumer.Name(), done)

			w.runConsumer(ctx, processingCtx, consumerCtx, registeredConsumer)
		}(startRequest.ctx, startRequest.done, startRequest.consumer)
	}

	return nil
}

//nolint:gocyclo // orchestration loop with clear structure
func (w *Worker) runConsumer(
	controlCtx, _, assignmentCtx context.Context,
	registeredConsumer consumer.Consumer,
) {
	logger := w.logger()
	consumerName := registeredConsumer.Name()
	basePollInterval := w.config.PollInterval
	currentPollInterval := basePollInterval
	delay := time.Duration(0)
	consecutiveFailures := 0
	gapTracker := &gapState{}

	logger.Info(controlCtx, "consumer loop started", "worker_id", w.id, "consumer", consumerName)
	defer logger.Info(context.Background(), "consumer loop stopped", "worker_id", w.id, "consumer", consumerName)

	for {
		if delay > 0 {
			woken, ok := w.waitForConsumerDelay(controlCtx, assignmentCtx, delay)
			if !ok {
				return
			}
			if woken {
				currentPollInterval = basePollInterval
			}
		} else {
			select {
			case <-controlCtx.Done():
				return
			case <-assignmentCtx.Done():
				return
			default:
			}
		}

		select {
		case <-controlCtx.Done():
			return
		case <-assignmentCtx.Done():
			return
		default:
		}

		result, err := w.processBatchWithGapState(controlCtx, registeredConsumer, gapTracker)
		if controlCtx.Err() != nil || assignmentCtx.Err() != nil {
			return
		}

		if err != nil {
			if errors.Is(err, errConsumerOwnershipLost) {
				logger.Info(controlCtx, "consumer ownership lost; stopping consumer", "worker_id", w.id, "consumer", consumerName)
				return
			}

			consecutiveFailures++
			logger.Error(controlCtx, "consumer batch failed",
				"worker_id", w.id, "consumer", consumerName,
				"error", err, "consecutive_failures", consecutiveFailures)

			if w.config.MaxConsecutiveFailures > 0 && consecutiveFailures >= w.config.MaxConsecutiveFailures {
				w.reportFatal(fmt.Errorf("%w: consumer %s failed %d times: %w",
					ErrConsecutiveFailures, consumerName, consecutiveFailures, err))
				return
			}

			delay = currentPollInterval
			continue
		}

		consecutiveFailures = 0

		if !result.progressed {
			if result.blockedByGap {
				currentPollInterval = basePollInterval
				delay = w.config.BatchPause
				continue
			}

			currentPollInterval = nextPollInterval(currentPollInterval, w.config.MaxPollInterval, basePollInterval)
			delay = currentPollInterval
			continue
		}

		currentPollInterval = basePollInterval

		if assignmentCtx.Err() != nil {
			return
		}

		if result.fullWindow {
			delay = w.config.BatchPause
			continue
		}

		delay = currentPollInterval
	}
}

func (w *Worker) processBatch(
	parentCtx context.Context,
	registeredConsumer consumer.Consumer,
	checkpointOverride ...int64,
) (processedBatch, error) {
	return w.processBatchWithGapState(parentCtx, registeredConsumer, &gapState{}, checkpointOverride...)
}

//nolint:gocyclo // sequential batch processing steps
func (w *Worker) processBatchWithGapState(
	parentCtx context.Context,
	registeredConsumer consumer.Consumer,
	gapTracker *gapState,
	checkpointOverride ...int64,
) (processedBatch, error) {
	ctx := parentCtx
	if w.config.BatchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parentCtx, w.config.BatchTimeout)
		defer cancel()
	}

	probe, err := w.probeFrontier(ctx, registeredConsumer.Name(), gapTracker, checkpointOverride...)
	if err != nil {
		return processedBatch{}, err
	}

	if probe.targetCheckpoint <= probe.checkpoint {
		if len(probe.rows) == 0 {
			gapTracker.clear()
		}
		return processedBatch{
			blockedByGap: probe.blockedByGap,
			fullWindow:   probe.fullWindow,
			checkpoint:   probe.checkpoint,
		}, nil
	}

	result, err := w.processProbedBatch(ctx, registeredConsumer, &probe)
	if err != nil {
		return processedBatch{}, err
	}

	if result.staleSkipped {
		w.logger().Info(ctx,
			"consumer advanced past stale gap",
			"worker_id", w.id,
			"consumer", registeredConsumer.Name(),
			"gap_position", probe.gapPosition,
			"checkpoint_from", probe.checkpoint,
			"checkpoint_to", probe.targetCheckpoint,
			"stale_for", timeNow().Sub(probe.firstSeenAt),
			"handled_events", result.handledCount,
			"visible_head", probe.highestVisible,
		)
	}

	if result.progressed {
		gapTracker.clear()
	}

	return result, nil
}

func (w *Worker) probeFrontier(
	ctx context.Context,
	consumerName string,
	gapTracker *gapState,
	checkpointOverride ...int64,
) (frontierProbe, error) {
	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return frontierProbe{}, fmt.Errorf("begin frontier probe transaction for consumer %s: %w", consumerName, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			w.logger().Error(ctx, "frontier probe rollback failed", "worker_id", w.id, "consumer", consumerName, "error", rollbackErr)
		}
	}()

	checkpoint := int64(0)
	if len(checkpointOverride) > 0 {
		checkpoint = checkpointOverride[0]
	} else {
		checkpoint, err = postgres.GetCheckpoint(ctx, tx, w.consumerCheckpointsTable(), consumerName)
		if err != nil {
			return frontierProbe{}, fmt.Errorf("get checkpoint for consumer %s: %w", consumerName, err)
		}
	}

	rows, err := w.store.ReadEvents(ctx, tx, checkpoint, w.config.BatchSize)
	if err != nil {
		return frontierProbe{}, fmt.Errorf("probe frontier for consumer %s: %w", consumerName, err)
	}

	probe := buildFrontierProbe(checkpoint, rows, w.config.BatchSize)
	if len(rows) == 0 || probe.targetCheckpoint > checkpoint {
		return probe, nil
	}
	staleFor := gapTracker.observe(probe.gapPosition, probe.highestVisible, timeNow())
	probe.firstSeenAt = gapTracker.firstSeenAt
	if staleFor < w.staleGapThreshold() {
		return probe, nil
	}

	if !gapTracker.staleLogged {
		w.logger().Error(ctx,
			"consumer gap became stale",
			"worker_id", w.id,
			"consumer", consumerName,
			"gap_position", probe.gapPosition,
			"stale_for", staleFor,
			"highest_visible_position", probe.highestVisible,
		)
		gapTracker.staleLogged = true
	}

	safeHarbor, ok := computeGapSkipTarget(probe.gapPosition, rows, w.staleGapHarborLag())
	if !ok || safeHarbor <= checkpoint {
		return probe, nil
	}

	probe.targetCheckpoint = safeHarbor
	probe.staleSkipped = true
	return probe, nil
}

func (w *Worker) processProbedBatch(
	ctx context.Context,
	registeredConsumer consumer.Consumer,
	probe *frontierProbe,
) (processedBatch, error) {
	txOptions := (*sql.TxOptions)(nil)
	attempts := 1
	if probe.staleSkipped {
		txOptions = &sql.TxOptions{Isolation: sql.LevelSerializable}
		attempts = staleGapRetryLimit
	}

	originalProbe := *probe
	for attempt := 0; attempt < attempts; attempt++ {
		attemptProbe := originalProbe
		result, err := w.processProbedBatchAttempt(ctx, registeredConsumer, &attemptProbe, txOptions)
		if err == nil {
			*probe = attemptProbe
			return result, nil
		}
		if !isSerializationFailure(err) || !originalProbe.staleSkipped {
			return processedBatch{}, err
		}
	}

	w.logger().Info(ctx,
		"stale gap advancement hit serializable contention; retrying later",
		"worker_id", w.id,
		"consumer", registeredConsumer.Name(),
		"gap_position", originalProbe.gapPosition,
		"checkpoint", originalProbe.checkpoint,
		"attempts", attempts,
	)
	return processedBatch{
		blockedByGap: true,
		fullWindow:   originalProbe.fullWindow,
		checkpoint:   originalProbe.checkpoint,
	}, nil
}

func (w *Worker) processProbedBatchAttempt(
	ctx context.Context,
	registeredConsumer consumer.Consumer,
	probe *frontierProbe,
	txOptions *sql.TxOptions,
) (processedBatch, error) {
	tx, err := w.db.BeginTx(ctx, txOptions)
	if err != nil {
		return processedBatch{}, fmt.Errorf("begin transaction for consumer %s: %w", registeredConsumer.Name(), err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}

		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			w.logger().Error(ctx, "consumer transaction rollback failed", "worker_id", w.id, "consumer", registeredConsumer.Name(), "error", rollbackErr)
		}
	}()

	assigned, err := w.ensureConsumerOwnership(ctx, tx, registeredConsumer.Name())
	if err != nil {
		return processedBatch{}, fmt.Errorf("check ownership for consumer %s: %w", registeredConsumer.Name(), err)
	}
	if !assigned {
		return processedBatch{}, errConsumerOwnershipLost
	}

	prepared, proceed, err := w.prepareProbeForBatch(ctx, tx, registeredConsumer.Name(), probe)
	if err != nil {
		return processedBatch{}, err
	}
	if !proceed {
		return prepared, nil
	}

	relevantEvents := filterHandledEvents(probe.rows, consumerAggregateTypes(registeredConsumer), probe.targetCheckpoint)
	if err := handleRelevantEvents(ctx, tx, registeredConsumer, relevantEvents); err != nil {
		return processedBatch{}, err
	}

	if err := w.recordStaleGapSkip(ctx, tx, registeredConsumer.Name(), probe); err != nil {
		return processedBatch{}, err
	}

	if err := postgres.SaveCheckpoint(ctx, tx, w.consumerCheckpointsTable(), registeredConsumer.Name(), probe.targetCheckpoint); err != nil {
		return processedBatch{}, fmt.Errorf("save checkpoint for consumer %s: %w", registeredConsumer.Name(), err)
	}

	if err := tx.Commit(); err != nil {
		return processedBatch{}, fmt.Errorf("commit transaction for consumer %s: %w", registeredConsumer.Name(), err)
	}
	committed = true

	return processedBatch{
		progressed:   true,
		fullWindow:   probe.fullWindow,
		checkpoint:   probe.targetCheckpoint,
		handledCount: len(relevantEvents),
		staleSkipped: probe.staleSkipped,
	}, nil
}

func (w *Worker) prepareProbeForBatch(
	ctx context.Context,
	tx *sql.Tx,
	consumerName string,
	probe *frontierProbe,
) (processedBatch, bool, error) {
	currentCheckpoint, err := postgres.GetCheckpointForUpdate(ctx, tx, w.consumerCheckpointsTable(), consumerName)
	if err != nil {
		return processedBatch{}, false, err
	}
	if currentCheckpoint != probe.checkpoint {
		return processedBatch{
			blockedByGap: true,
			fullWindow:   probe.fullWindow,
			checkpoint:   currentCheckpoint,
		}, false, nil
	}

	if !probe.staleSkipped {
		return processedBatch{}, true, nil
	}
	if err := w.revalidateStaleGapSkip(ctx, tx, consumerName, probe); err != nil {
		return processedBatch{}, false, err
	}
	if probe.targetCheckpoint <= probe.checkpoint {
		return processedBatch{
			blockedByGap: probe.blockedByGap,
			fullWindow:   probe.fullWindow,
			checkpoint:   probe.checkpoint,
		}, false, nil
	}

	return processedBatch{}, true, nil
}

func isSerializationFailure(err error) bool {
	if err == nil {
		return false
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "40001" {
		return true
	}

	type sqlStateCarrier interface {
		SQLState() string
	}
	var carrier sqlStateCarrier
	if errors.As(err, &carrier) && carrier.SQLState() == "40001" {
		return true
	}

	return false
}

func (w *Worker) recordStaleGapSkip(
	ctx context.Context,
	tx *sql.Tx,
	consumerName string,
	probe *frontierProbe,
) error {
	if !probe.staleSkipped {
		return nil
	}

	record := postgres.GapSkipRecord{
		FirstSeenAt:            probe.firstSeenAt,
		ConsumerName:           consumerName,
		WorkerID:               w.id,
		GapPosition:            probe.gapPosition,
		SkipToPosition:         probe.targetCheckpoint,
		HighestVisiblePosition: probe.highestVisible,
	}
	if err := postgres.RecordGapSkip(ctx, tx, w.consumerGapSkipsTable(), &record); err != nil {
		return fmt.Errorf("record gap skip for consumer %s: %w", consumerName, err)
	}

	return nil
}

func handleRelevantEvents(
	ctx context.Context,
	tx *sql.Tx,
	registeredConsumer consumer.Consumer,
	relevantEvents []store.PersistedEvent,
) error {
	for i := range relevantEvents {
		if err := registeredConsumer.Handle(ctx, tx, relevantEvents[i]); err != nil {
			return fmt.Errorf("handle event %s for consumer %s: %w", relevantEvents[i].EventID, registeredConsumer.Name(), err)
		}
	}

	return nil
}

func buildFrontierProbe(checkpoint int64, rows []store.PersistedEvent, batchSize int) frontierProbe {
	probe := frontierProbe{
		checkpoint: checkpoint,
		rows:       rows,
		fullWindow: len(rows) == batchSize,
	}
	if len(rows) == 0 {
		return probe
	}

	probe.highestVisible = rows[len(rows)-1].GlobalPosition
	safeCount, safeFrontier := computeSafeFrontier(checkpoint, rows)
	if safeCount > 0 {
		probe.targetCheckpoint = safeFrontier
		probe.rows = rows[:safeCount]
		return probe
	}

	probe.blockedByGap = true
	probe.gapPosition = checkpoint + 1
	return probe
}

func (w *Worker) revalidateStaleGapSkip(
	ctx context.Context,
	tx *sql.Tx,
	consumerName string,
	probe *frontierProbe,
) error {
	firstSeenAt := probe.firstSeenAt

	rows, err := w.store.ReadEvents(ctx, tx, probe.checkpoint, w.config.BatchSize)
	if err != nil {
		return fmt.Errorf("revalidate stale gap frontier for consumer %s: %w", consumerName, err)
	}

	refreshed := buildFrontierProbe(probe.checkpoint, rows, w.config.BatchSize)
	refreshed.firstSeenAt = firstSeenAt
	if !refreshed.blockedByGap {
		*probe = refreshed
		return nil
	}

	safeHarbor, ok := computeGapSkipTarget(refreshed.gapPosition, rows, w.staleGapHarborLag())
	if !ok || safeHarbor <= refreshed.checkpoint {
		*probe = refreshed
		return nil
	}

	refreshed.targetCheckpoint = safeHarbor
	refreshed.staleSkipped = true
	*probe = refreshed
	return nil
}

func consumerAggregateTypes(registeredConsumer consumer.Consumer) []string {
	scopedConsumer, ok := registeredConsumer.(consumer.ScopedConsumer)
	if !ok {
		return nil
	}

	return scopedConsumer.AggregateTypes()
}

func filterHandledEvents(
	rows []store.PersistedEvent,
	aggregateTypes []string,
	upperBound int64,
) []store.PersistedEvent {
	filtered := make([]store.PersistedEvent, 0, len(rows))
	allowed := make(map[string]struct{}, len(aggregateTypes))
	for _, aggregateType := range aggregateTypes {
		allowed[aggregateType] = struct{}{}
	}

	for i := range rows {
		if rows[i].GlobalPosition > upperBound {
			continue
		}
		if len(allowed) > 0 {
			if _, ok := allowed[rows[i].AggregateType]; !ok {
				continue
			}
		}
		filtered = append(filtered, rows[i])
	}

	return filtered
}

func (w *Worker) waitForConsumerDelay(workerCtx, assignmentCtx context.Context, delay time.Duration) (woken, ok bool) {
	if delay <= 0 {
		return false, true
	}

	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case <-workerCtx.Done():
		return false, false
	case <-assignmentCtx.Done():
		return false, false
	case <-w.dispatcher.WakeupChan():
		return true, true
	case <-timer.C:
		return false, true
	}
}

func (w *Worker) stopAllConsumers() {
	w.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(w.runningConsumers))
	for name, cancel := range w.runningConsumers {
		delete(w.runningConsumers, name)
		delete(w.consumerDone, name)
		w.logger().Debug(context.Background(), "canceling consumer", "worker_id", w.id, "consumer", name)
		cancels = append(cancels, cancel)
	}
	w.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (w *Worker) ensureLeaderConn(ctx context.Context) (*sql.Conn, error) {
	w.mu.Lock()
	if w.leaderConn != nil {
		conn := w.leaderConn
		w.mu.Unlock()
		return conn, nil
	}
	w.mu.Unlock()

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open leader connection: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.leaderConn != nil {
		existing := w.leaderConn
		if closeErr := conn.Close(); closeErr != nil {
			w.logger().Error(ctx, "failed to close redundant leader connection", "worker_id", w.id, "error", closeErr)
		}
		return existing, nil
	}

	w.leaderConn = conn
	return conn, nil
}

func (w *Worker) releaseLeaderConnection(ctx context.Context) error {
	w.mu.Lock()
	conn := w.leaderConn
	wasLeader := w.isLeader
	w.leaderConn = nil
	w.isLeader = false
	w.mu.Unlock()

	if conn == nil {
		return nil
	}
	defer func() {
		if err := conn.Close(); err != nil {
			w.logger().Error(ctx, "failed to close leader connection", "worker_id", w.id, "error", err)
		}
	}()

	if !wasLeader {
		return nil
	}

	if err := releaseLeaderLock(ctx, conn); err != nil {
		return err
	}

	w.logger().Info(ctx, "worker released leadership", "worker_id", w.id)
	return nil
}

func (w *Worker) dropLeaderConnection(ctx context.Context) {
	w.mu.Lock()
	conn := w.leaderConn
	wasLeader := w.isLeader
	w.leaderConn = nil
	w.isLeader = false
	w.mu.Unlock()

	if wasLeader {
		w.logger().Info(ctx, "worker lost leadership", "worker_id", w.id)
	}

	if conn != nil {
		if err := conn.Close(); err != nil {
			w.logger().Error(ctx, "failed to close leader connection", "worker_id", w.id, "error", err)
		}
	}
}

func (w *Worker) leaderActive() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.isLeader
}

func (w *Worker) setLeader(isLeader bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.isLeader = isLeader
}

func (w *Worker) assignmentPollInterval() time.Duration {
	interval := w.config.RebalanceInterval
	if interval <= 0 {
		return defaultAssignmentPollInterval
	}

	if interval < time.Second {
		return time.Second
	}

	return interval
}

func (w *Worker) logger() store.Logger {
	if w.config.Logger == nil {
		return store.NoOpLogger{}
	}

	return w.config.Logger
}

func (w *Worker) consumerNames() []string {
	names := make([]string, 0, len(w.consumers))
	for _, registeredConsumer := range w.consumers {
		names = append(names, registeredConsumer.Name())
	}

	return names
}

func (w *Worker) consumerByName(name string) (consumer.Consumer, bool) {
	for _, registeredConsumer := range w.consumers {
		if registeredConsumer.Name() == name {
			return registeredConsumer, true
		}
	}

	return nil, false
}

func (w *Worker) workerNodesTable() string {
	return resolvedTableName(w.config.WorkerNodesTable, postgres.DefaultWorkerNodesTable)
}

func (w *Worker) consumerAssignmentsTable() string {
	return resolvedTableName(w.config.ConsumerAssignmentsTable, postgres.DefaultConsumerAssignmentsTable)
}

func (w *Worker) consumerCheckpointsTable() string {
	return resolvedTableName(w.config.ConsumerCheckpointsTable, postgres.DefaultConsumerCheckpointsTable)
}

func (w *Worker) consumerGapSkipsTable() string {
	return resolvedTableName(w.config.ConsumerGapSkipsTable, postgres.DefaultConsumerGapSkipsTable)
}

func (w *Worker) startupWorkerCleanupThreshold() time.Duration {
	timeout := w.config.HeartbeatTimeout
	if timeout <= 0 {
		timeout = DefaultConfig().HeartbeatTimeout
	}

	if timeout > time.Duration((1<<63-1)/startupCleanupMultiplier) {
		return time.Duration(1<<63 - 1)
	}

	return timeout * startupCleanupMultiplier
}

func (w *Worker) staleGapThreshold() time.Duration {
	if w.config.StaleGapThreshold <= 0 {
		return DefaultConfig().StaleGapThreshold
	}

	return w.config.StaleGapThreshold
}

func (w *Worker) staleGapHarborLag() int {
	lag := w.config.StaleGapHarborLag
	if lag < 0 {
		lag = DefaultConfig().StaleGapHarborLag
	}

	batchSize := w.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultConfig().BatchSize
	}
	maxLag := batchSize - 1
	if maxLag < 0 {
		return 0
	}
	if lag > maxLag {
		return maxLag
	}

	return lag
}

func (w *Worker) finishConsumer(name string, done chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if currentDone, ok := w.consumerDone[name]; ok && currentDone == done {
		delete(w.consumerDone, name)
		delete(w.runningConsumers, name)
	}
}

func (w *Worker) getProcessingCancel() context.CancelFunc {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.processingCancel
}

func applyOptions(opts ...Option) Config { //nolint:gocyclo // sequential validation of independent config fields
	config := DefaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}

	defaults := DefaultConfig()
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.MaxPollInterval < config.PollInterval {
		config.MaxPollInterval = defaults.MaxPollInterval
		if config.MaxPollInterval < config.PollInterval {
			config.MaxPollInterval = config.PollInterval
		}
	}
	if config.DispatcherInterval <= 0 {
		config.DispatcherInterval = defaults.DispatcherInterval
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.HeartbeatTimeout <= 0 {
		config.HeartbeatTimeout = defaults.HeartbeatTimeout
	}
	if config.RebalanceInterval <= 0 {
		config.RebalanceInterval = defaults.RebalanceInterval
	}
	if config.BatchPause < 0 {
		config.BatchPause = defaults.BatchPause
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = defaults.BatchTimeout
	}
	if config.StaleGapThreshold <= 0 {
		config.StaleGapThreshold = defaults.StaleGapThreshold
	}
	if config.StaleGapHarborLag < 0 {
		config.StaleGapHarborLag = defaults.StaleGapHarborLag
	}
	if config.BatchSize > 0 && config.StaleGapHarborLag >= config.BatchSize {
		config.StaleGapHarborLag = config.BatchSize - 1
	}
	if config.MaxConsecutiveFailures <= 0 {
		config.MaxConsecutiveFailures = defaults.MaxConsecutiveFailures
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
	if config.WorkerNodesTable == "" {
		config.WorkerNodesTable = defaults.WorkerNodesTable
	}
	if config.ConsumerAssignmentsTable == "" {
		config.ConsumerAssignmentsTable = defaults.ConsumerAssignmentsTable
	}
	if config.ConsumerCheckpointsTable == "" {
		config.ConsumerCheckpointsTable = defaults.ConsumerCheckpointsTable
	}
	if config.ConsumerGapSkipsTable == "" {
		config.ConsumerGapSkipsTable = defaults.ConsumerGapSkipsTable
	}
	if config.DispatcherStrategy == "" {
		config.DispatcherStrategy = defaults.DispatcherStrategy
	}
	if config.NotifyChannel == "" {
		config.NotifyChannel = defaults.NotifyChannel
	}

	return config
}

func newDispatcher(db *sql.DB, eventStore workerStore, config *Config) dispatcher.Dispatcher {
	if config.DispatcherStrategy == DispatcherStrategyNotify {
		return dispatcher.NewNotifyDispatcher(
			config.NotifyConnectionString,
			config.NotifyChannel,
			db,
			eventStore,
			config.Logger,
		)
	}

	return dispatcher.NewPollDispatcher(db, eventStore, config.DispatcherInterval, config.Logger)
}

func applyAssignments(ctx context.Context, tx *sql.Tx, assignmentsTable string, assignments map[string]uuid.UUID) error {
	if len(assignments) == 0 {
		//nolint:gosec // G201: table name comes from trusted configuration
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET worker_id = NULL, updated_at = NOW()
			WHERE worker_id IS NOT NULL
		`, assignmentsTable)); err != nil {
			return fmt.Errorf("clear existing assignments: %w", err)
		}
		return nil
	}

	if err := postgres.SetAssignments(ctx, tx, assignmentsTable, assignments); err != nil {
		return fmt.Errorf("set assignments: %w", err)
	}

	return nil
}

func (w *Worker) ensureConsumerOwnership(ctx context.Context, tx *sql.Tx, consumerName string) (bool, error) {
	var workerID sql.NullString
	//nolint:gosec // G201: table name comes from trusted configuration
	query := fmt.Sprintf(`
		SELECT worker_id
		FROM %s
		WHERE consumer_name = $1
		FOR UPDATE
	`, w.consumerAssignmentsTable())
	if err := tx.QueryRowContext(ctx, query, consumerName).Scan(&workerID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return workerID.Valid && workerID.String == w.id.String(), nil
}

func nextPollInterval(current, maxInterval, base time.Duration) time.Duration {
	if current <= 0 {
		return base
	}
	if maxInterval <= 0 {
		maxInterval = current
	}

	next := current * 2
	if next < current {
		return maxInterval
	}
	if next > maxInterval {
		return maxInterval
	}

	return next
}

func resolvedTableName(tableName, defaultTableName string) string {
	if strings.TrimSpace(tableName) == "" {
		return defaultTableName
	}

	return tableName
}

func tryAcquireLeaderLock(ctx context.Context, conn *sql.Conn) (bool, error) {
	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", postgres.LeaderLockKey).Scan(&acquired); err != nil {
		return false, fmt.Errorf("acquire leader advisory lock: %w", err)
	}

	return acquired, nil
}

func releaseLeaderLock(ctx context.Context, conn *sql.Conn) error {
	var released bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", postgres.LeaderLockKey).Scan(&released); err != nil {
		return fmt.Errorf("release leader advisory lock: %w", err)
	}
	if !released {
		return fmt.Errorf("release leader advisory lock: lock not held")
	}

	return nil
}
