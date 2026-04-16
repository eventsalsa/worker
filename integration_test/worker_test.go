//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	storepostgres "github.com/eventsalsa/store/postgres"

	workerpkg "github.com/eventsalsa/worker"
	workerpostgres "github.com/eventsalsa/worker/postgres"
)

func TestWorker_SingleWorkerMultipleConsumers(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumerAll := newTestConsumer("consumer-all", "worker-1", nil)
	consumerAccounts := newTestConsumer("consumer-accounts", "worker-1", []string{"Account"})
	consumerOrders := newTestConsumer("consumer-orders", "worker-1", []string{"Order"})

	worker := startTestWorker(t, "worker-1", []*testConsumer{consumerAll, consumerAccounts, consumerOrders}, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 3 {
			return fmt.Errorf("expected 3 assignments, got %d", len(assignments))
		}
		for _, assignment := range assignments {
			if !assignment.Assigned || assignment.WorkerID != worker.worker.ID() {
				return fmt.Errorf("consumer %s not assigned to worker-1 yet", assignment.ConsumerName)
			}
		}
		return nil
	})

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{AggregateType: "Account", Count: 3},
		testEventBatch{AggregateType: "Order", Count: 2},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if rows := getHandledRows(t, controlDB, consumerAll.Name()); len(rows) != 5 {
			return fmt.Errorf("consumer-all handled %d rows, want 5", len(rows))
		}
		if rows := getHandledRows(t, controlDB, consumerAccounts.Name()); len(rows) != 3 {
			return fmt.Errorf("consumer-accounts handled %d rows, want 3", len(rows))
		}
		if rows := getHandledRows(t, controlDB, consumerOrders.Name()); len(rows) != 2 {
			return fmt.Errorf("consumer-orders handled %d rows, want 2", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, consumerAll.Name()); checkpoint != latest {
			return fmt.Errorf("consumer-all checkpoint=%d want %d", checkpoint, latest)
		}
		if checkpoint := getCheckpoint(t, controlDB, consumerAccounts.Name()); checkpoint != latest {
			return fmt.Errorf("consumer-accounts checkpoint=%d want %d (safe global frontier)", checkpoint, latest)
		}
		if checkpoint := getCheckpoint(t, controlDB, consumerOrders.Name()); checkpoint != latest {
			return fmt.Errorf("consumer-orders checkpoint=%d want %d (safe global frontier)", checkpoint, latest)
		}
		return nil
	})
}

func TestRebalance_ScaleUp_WorkersReassignWithoutGapsOrDuplication(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	worker1Consumers := []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
		newTestConsumer("consumer-2", "worker-1", nil),
		newTestConsumer("consumer-3", "worker-1", nil),
		newTestConsumer("consumer-4", "worker-1", nil),
	}
	worker1 := startTestWorker(t, "worker-1", worker1Consumers, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker1.worker.ID()] != 4 {
			return fmt.Errorf("worker-1 has %d consumers, want 4", counts[worker1.worker.ID()])
		}
		return nil
	})

	initial := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{AggregateType: "Account", Count: 3},
		testEventBatch{AggregateType: "Order", Count: 2},
	)
	initialLatest := initial[len(initial)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"consumer-1", "consumer-2", "consumer-3", "consumer-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 5 {
				return fmt.Errorf("%s handled %d rows, want 5", name, len(rows))
			}
		}
		return nil
	})

	worker2Consumers := []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
		newTestConsumer("consumer-2", "worker-2", nil),
		newTestConsumer("consumer-3", "worker-2", nil),
		newTestConsumer("consumer-4", "worker-2", nil),
	}
	worker2 := startTestWorker(t, "worker-2", worker2Consumers, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if len(counts) != 2 {
			return fmt.Errorf("expected 2 workers in assignment map, got %d", len(counts))
		}
		values := []int{counts[worker1.worker.ID()], counts[worker2.worker.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("assignment split=%v want [2 2]", values)
		}
		return nil
	})

	more := appendTestEvents(t, controlDB, eventStore, 3, "Invoice")
	finalLatest := more[len(more)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"consumer-1", "consumer-2", "consumer-3", "consumer-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 8 {
				return fmt.Errorf("%s handled %d rows, want 8", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, finalLatest)
			}
		}
		labels := handledByAfter(t, controlDB, initialLatest)
		if len(labels) != 2 || labels[0] != "worker-1" || labels[1] != "worker-2" {
			return fmt.Errorf("post-rebalance events handled by %v, want both workers", labels)
		}
		return nil
	})
}

func TestRebalance_ScaleDown_WorkerStops_SurvivorOwnsAllConsumers(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
		newTestConsumer("consumer-2", "worker-1", nil),
		newTestConsumer("consumer-3", "worker-1", nil),
		newTestConsumer("consumer-4", "worker-1", nil),
	}, defaultWorkerOptions()...)
	worker2 := startTestWorker(t, "worker-2", []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
		newTestConsumer("consumer-2", "worker-2", nil),
		newTestConsumer("consumer-3", "worker-2", nil),
		newTestConsumer("consumer-4", "worker-2", nil),
	}, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		values := []int{counts[worker1.worker.ID()], counts[worker2.worker.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("assignment split=%v want [2 2]", values)
		}
		return nil
	})

	first := appendTestEvents(t, controlDB, eventStore, 2, "Customer")
	firstLatest := first[len(first)-1].GlobalPosition
	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"consumer-1", "consumer-2", "consumer-3", "consumer-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 2 {
				return fmt.Errorf("%s handled %d rows, want 2", name, len(rows))
			}
		}
		return nil
	})

	worker2.stop(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker1.worker.ID()] != 4 || len(counts) != 1 {
			return fmt.Errorf("survivor assignments=%v, want worker-1 => 4", counts)
		}
		if workers := countWorkerRows(t, controlDB); workers != 1 {
			return fmt.Errorf("worker_nodes has %d rows, want 1", workers)
		}
		return nil
	})

	more := appendTestEvents(t, controlDB, eventStore, 3, "Customer")
	finalLatest := more[len(more)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"consumer-1", "consumer-2", "consumer-3", "consumer-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 5 {
				return fmt.Errorf("%s handled %d rows, want 5", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, finalLatest)
			}
		}
		labels := handledByAfter(t, controlDB, firstLatest)
		if len(labels) != 1 || labels[0] != "worker-1" {
			return fmt.Errorf("post-scale-down events handled by %v, want only worker-1", labels)
		}
		return nil
	})
}

func TestWorkerStartupCleanup_RemovesVeryStaleWorkerRows(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	consumer := newTestConsumer("consumer-cleanup-stale", "worker-1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := workerpostgres.EnsureConsumersRegistered(ctx, controlDB, workerpostgres.DefaultConsumerAssignmentsTable, []string{consumer.Name()}); err != nil {
		t.Fatalf("ensure consumer registered: %v", err)
	}

	staleWorkerID := uuid.New()
	insertWorkerRow(t, controlDB, staleWorkerID, time.Now().Add(-5*time.Second))
	assignConsumerToWorker(t, controlDB, consumer.Name(), staleWorkerID)

	worker := startTestWorker(t, "worker-1", []*testConsumer{consumer},
		append(defaultWorkerOptions(), workerpkg.WithHeartbeatTimeout(2*time.Second))...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if workerRowExists(t, controlDB, staleWorkerID) {
			return fmt.Errorf("stale worker row %s still exists", staleWorkerID)
		}
		if !workerRowExists(t, controlDB, worker.worker.ID()) {
			return fmt.Errorf("worker row %s not registered yet", worker.worker.ID())
		}
		if workers := countWorkerRows(t, controlDB); workers != 1 {
			return fmt.Errorf("worker_nodes has %d rows, want 1", workers)
		}

		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker.worker.ID() {
			return fmt.Errorf("assignments=%v want consumer assigned to new worker", assignments)
		}

		return nil
	})
}

func TestWorkerStartupCleanup_PreservesRowsNewerThanCleanupThreshold(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	consumer := newTestConsumer("consumer-cleanup-preserve", "worker-1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := workerpostgres.EnsureConsumersRegistered(ctx, controlDB, workerpostgres.DefaultConsumerAssignmentsTable, []string{consumer.Name()}); err != nil {
		t.Fatalf("ensure consumer registered: %v", err)
	}

	staleButRetainedWorkerID := uuid.New()
	insertWorkerRow(t, controlDB, staleButRetainedWorkerID, time.Now().Add(-3*time.Second))
	assignConsumerToWorker(t, controlDB, consumer.Name(), staleButRetainedWorkerID)

	worker := startTestWorker(t, "worker-1", []*testConsumer{consumer},
		append(defaultWorkerOptions(), workerpkg.WithHeartbeatTimeout(2*time.Second))...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if !workerRowExists(t, controlDB, staleButRetainedWorkerID) {
			return fmt.Errorf("worker row %s was removed, want retained", staleButRetainedWorkerID)
		}
		if !workerRowExists(t, controlDB, worker.worker.ID()) {
			return fmt.Errorf("worker row %s not registered yet", worker.worker.ID())
		}
		if workers := countWorkerRows(t, controlDB); workers != 2 {
			return fmt.Errorf("worker_nodes has %d rows, want 2", workers)
		}

		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker.worker.ID() {
			return fmt.Errorf("assignments=%v want consumer assigned to new worker", assignments)
		}

		return nil
	})
}

func TestDispatcher_WakeupDispatcher_IdleConsumersWakePromptly(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumer := newTestConsumer("consumer-idle", "worker-1", nil)
	worker := startTestWorker(t, "worker-1", []*testConsumer{
		consumer,
	},
		append(defaultWorkerOptions(),
			workerpkg.WithPollInterval(5*time.Second),
			workerpkg.WithMaxPollInterval(5*time.Second),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker.worker.ID() {
			return fmt.Errorf("consumer not assigned to worker-1 yet")
		}
		return nil
	})

	time.Sleep(1500 * time.Millisecond)

	start := time.Now()
	appended := appendTestEvents(t, controlDB, eventStore, 1, "Wakeup")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, 2*time.Second, func() error {
		rows := getHandledRows(t, controlDB, consumer.Name())
		if len(rows) != 1 {
			return fmt.Errorf("consumer handled %d rows, want 1", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("dispatcher wakeup took %s; expected processing well before the 5s poll interval", elapsed)
	}
}

func TestTransactionalIntegrity_MidBatchFailureRollsBackAndRetriesFromCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumer := newTestConsumer("consumer-fail-once", "worker-1", nil)
	worker := startTestWorker(t, "worker-1", []*testConsumer{consumer}, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker.worker.ID() {
			return fmt.Errorf("consumer not assigned to worker-1 yet")
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Payment")
	consumer.FailUntilCleared(appended[1].GlobalPosition, errors.New("boom"))
	lastPosition := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if consumer.AttemptCount(appended[1].GlobalPosition) < 1 {
			return fmt.Errorf("failing event was not attempted yet")
		}
		if consumer.AttemptCount(appended[0].GlobalPosition) < 1 {
			return fmt.Errorf("first event was not attempted yet")
		}
		if rows := getHandledRows(t, controlDB, consumer.Name()); len(rows) != 0 {
			return fmt.Errorf("read model has %d committed rows, want 0 before retry succeeds", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 while consumer is failing", checkpoint)
		}
		return nil
	})

	consumer.ClearFailure(appended[1].GlobalPosition)

	waitForErr(t, defaultWaitTimeout, func() error {
		rows := getHandledRows(t, controlDB, consumer.Name())
		if len(rows) != 3 {
			return fmt.Errorf("committed rows=%d want 3", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != lastPosition {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, lastPosition)
		}
		if consumer.AttemptCount(appended[0].GlobalPosition) < 2 {
			return fmt.Errorf("first event attempts=%d want at least 2 to prove retry from same checkpoint", consumer.AttemptCount(appended[0].GlobalPosition))
		}
		return nil
	})
}

func TestGapHandling_LowerPositionCommitsLateBeforeThreshold_NoSkip(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumer := newTestConsumer("consumer-gap-late", "worker-1", nil)
	worker := startTestWorker(t, "worker-1", []*testConsumer{consumer},
		append(defaultWorkerOptions(),
			workerpkg.WithStaleGapThreshold(2*time.Second),
			workerpkg.WithBatchPause(100*time.Millisecond),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker.worker.ID() {
			return fmt.Errorf("consumer not assigned to worker-1 yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{AggregateType: "Invoice", Count: 1})
	gapPosition := held.events[0].GlobalPosition
	later := appendTestEvents(t, controlDB, eventStore, 3, "Invoice")
	latest := later[len(later)-1].GlobalPosition

	waitForErr(t, time.Second, func() error {
		if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 while waiting for lower position", checkpoint)
		}
		if rows := getHandledRows(t, controlDB, consumer.Name()); len(rows) != 0 {
			return fmt.Errorf("handled rows=%d want 0 before lower position commits", len(rows))
		}
		return nil
	})

	held.Commit(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		rows := getHandledRows(t, controlDB, consumer.Name())
		if len(rows) != 4 {
			return fmt.Errorf("handled rows=%d want 4", len(rows))
		}
		wantPositions := []int64{gapPosition}
		for _, event := range later {
			wantPositions = append(wantPositions, event.GlobalPosition)
		}
		sort.Slice(wantPositions, func(i, j int) bool { return wantPositions[i] < wantPositions[j] })
		for idx, want := range wantPositions {
			if rows[idx].GlobalPosition != want {
				return fmt.Errorf("handled row %d position=%d want %d", idx, rows[idx].GlobalPosition, want)
			}
		}
		if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		if skips := getGapSkipRows(t, controlDB, consumer.Name()); len(skips) != 0 {
			return fmt.Errorf("gap skips=%d want 0", len(skips))
		}
		return nil
	})
}

func TestGapHandling_StaleGapAfterThreshold_AdvancesBySafeHarbor(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumer := newTestConsumer("consumer-gap-stale", "worker-1", nil)
	worker := startTestWorker(t, "worker-1", []*testConsumer{consumer},
		append(defaultWorkerOptions(),
			workerpkg.WithStaleGapThreshold(250*time.Millisecond),
			workerpkg.WithStaleGapHarborLag(1),
			workerpkg.WithPollInterval(500*time.Millisecond),
			workerpkg.WithMaxPollInterval(500*time.Millisecond),
			workerpkg.WithBatchPause(500*time.Millisecond),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker.worker.ID() {
			return fmt.Errorf("consumer not assigned to worker-1 yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{AggregateType: "Invoice", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 4, "Invoice")
	expectedSkipTo := later[len(later)-2].GlobalPosition
	expectedHighestVisible := later[len(later)-1].GlobalPosition

	waitForErr(t, time.Second, func() error {
		if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 before stale threshold", checkpoint)
		}
		return nil
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, consumer.Name()); len(skips) != 1 {
			return fmt.Errorf("gap skips=%d want 1", len(skips))
		}
		return nil
	})

	worker.stop(t)
	held.Rollback(t)

	skips := getGapSkipRows(t, controlDB, consumer.Name())
	if len(skips) != 1 {
		t.Fatalf("gap skips=%d want 1", len(skips))
	}
	if skips[0].GapPosition != held.events[0].GlobalPosition {
		t.Fatalf("gap position=%d want %d", skips[0].GapPosition, held.events[0].GlobalPosition)
	}
	if skips[0].SkipToPosition != expectedSkipTo {
		t.Fatalf("skip_to_position=%d want %d", skips[0].SkipToPosition, expectedSkipTo)
	}
	if skips[0].HighestVisiblePosition != expectedHighestVisible {
		t.Fatalf("highest_visible_position=%d want %d", skips[0].HighestVisiblePosition, expectedHighestVisible)
	}
	if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != expectedSkipTo {
		t.Fatalf("checkpoint=%d want %d", checkpoint, expectedSkipTo)
	}
	rows := getHandledRows(t, controlDB, consumer.Name())
	if len(rows) != 3 {
		t.Fatalf("handled rows=%d want 3", len(rows))
	}
	for _, row := range rows {
		if row.GlobalPosition > expectedSkipTo {
			t.Fatalf("handled position=%d want <= %d", row.GlobalPosition, expectedSkipTo)
		}
	}
}

func TestGapHandling_StaleGapAfterThreshold_AdvancesWithSparseVisibleWindowUnderDefaultLag(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumer := newTestConsumer("consumer-gap-stale-sparse-default", "worker-1", nil)
	worker := startTestWorker(t, "worker-1", []*testConsumer{consumer},
		append(defaultWorkerOptions(),
			workerpkg.WithStaleGapThreshold(250*time.Millisecond),
			workerpkg.WithPollInterval(500*time.Millisecond),
			workerpkg.WithMaxPollInterval(500*time.Millisecond),
			workerpkg.WithBatchPause(500*time.Millisecond),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker.worker.ID() {
			return fmt.Errorf("consumer not assigned to worker-1 yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{AggregateType: "Invoice", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 1, "Invoice")
	expectedSkipTo := later[0].GlobalPosition

	waitForErr(t, time.Second, func() error {
		if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 before stale threshold", checkpoint)
		}
		return nil
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, consumer.Name()); len(skips) != 1 {
			return fmt.Errorf("gap skips=%d want 1", len(skips))
		}
		return nil
	})

	worker.stop(t)
	held.Rollback(t)

	skips := getGapSkipRows(t, controlDB, consumer.Name())
	if len(skips) != 1 {
		t.Fatalf("gap skips=%d want 1", len(skips))
	}
	if skips[0].GapPosition != held.events[0].GlobalPosition {
		t.Fatalf("gap position=%d want %d", skips[0].GapPosition, held.events[0].GlobalPosition)
	}
	if skips[0].SkipToPosition != expectedSkipTo {
		t.Fatalf("skip_to_position=%d want %d", skips[0].SkipToPosition, expectedSkipTo)
	}
	if skips[0].HighestVisiblePosition != expectedSkipTo {
		t.Fatalf("highest_visible_position=%d want %d", skips[0].HighestVisiblePosition, expectedSkipTo)
	}
	if checkpoint := getCheckpoint(t, controlDB, consumer.Name()); checkpoint != expectedSkipTo {
		t.Fatalf("checkpoint=%d want %d", checkpoint, expectedSkipTo)
	}
	rows := getHandledRows(t, controlDB, consumer.Name())
	if len(rows) != 1 {
		t.Fatalf("handled rows=%d want 1", len(rows))
	}
	if rows[0].GlobalPosition != expectedSkipTo {
		t.Fatalf("handled position=%d want %d", rows[0].GlobalPosition, expectedSkipTo)
	}
}

func TestCheckpointCorrectness_RestartResumesFromPersistedCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
		newTestConsumer("consumer-2", "worker-1", nil),
	}, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker1.worker.ID()] != 2 {
			return fmt.Errorf("worker-1 assignments=%v want 2 consumers", counts)
		}
		return nil
	})

	first := appendTestEvents(t, controlDB, eventStore, 3, "Shipment")
	firstLatest := first[len(first)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"consumer-1", "consumer-2"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 3 {
				return fmt.Errorf("%s handled %d rows, want 3", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != firstLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, firstLatest)
			}
		}
		return nil
	})

	worker1.stop(t)

	second := appendTestEvents(t, controlDB, eventStore, 2, "Shipment")
	finalLatest := second[len(second)-1].GlobalPosition

	worker2 := startTestWorker(t, "worker-2", []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
		newTestConsumer("consumer-2", "worker-2", nil),
	}, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker2.worker.ID()] != 2 || len(counts) != 1 {
			return fmt.Errorf("worker-2 assignments=%v want all consumers", counts)
		}
		return nil
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"consumer-1", "consumer-2"} {
			rows := getHandledRows(t, controlDB, name)
			if len(rows) != 5 {
				return fmt.Errorf("%s handled %d rows, want 5", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, finalLatest)
			}
		}
		labels := handledByAfter(t, controlDB, firstLatest)
		if len(labels) != 1 || labels[0] != "worker-2" {
			return fmt.Errorf("post-restart events handled by %v want only worker-2", labels)
		}
		return nil
	})
}

func TestLeaderFailover_NewLeaderElectedAndRebalancingContinues(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
		newTestConsumer("consumer-2", "worker-1", nil),
		newTestConsumer("consumer-3", "worker-1", nil),
		newTestConsumer("consumer-4", "worker-1", nil),
	}, defaultWorkerOptions()...)
	worker2 := startTestWorker(t, "worker-2", []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
		newTestConsumer("consumer-2", "worker-2", nil),
		newTestConsumer("consumer-3", "worker-2", nil),
		newTestConsumer("consumer-4", "worker-2", nil),
	}, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		values := []int{counts[worker1.worker.ID()], counts[worker2.worker.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("assignment split=%v want [2 2]", values)
		}
		return nil
	})

	worker1.stop(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker2.worker.ID()] != 4 || len(counts) != 1 {
			return fmt.Errorf("post-failover assignments=%v want worker-2 => 4", counts)
		}
		return nil
	})

	worker3 := startTestWorker(t, "worker-3", []*testConsumer{
		newTestConsumer("consumer-1", "worker-3", nil),
		newTestConsumer("consumer-2", "worker-3", nil),
		newTestConsumer("consumer-3", "worker-3", nil),
		newTestConsumer("consumer-4", "worker-3", nil),
	}, defaultWorkerOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if len(counts) != 2 {
			return fmt.Errorf("expected 2 workers after replacement, got %v", counts)
		}
		values := []int{counts[worker2.worker.ID()], counts[worker3.worker.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("replacement split=%v want [2 2]", values)
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Invoice")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"consumer-1", "consumer-2", "consumer-3", "consumer-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 2 {
				return fmt.Errorf("%s handled %d rows, want 2", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, latest)
			}
		}
		labels := handledByAfter(t, controlDB, 0)
		if len(labels) != 2 {
			return fmt.Errorf("handled_by=%v want two active workers", labels)
		}
		return nil
	})
}
