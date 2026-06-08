//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
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

func TestLeaseLeaderFailover_NewLeaderElectedAndRebalancingContinues(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	options := append(defaultWorkerOptions(), workerpkg.WithLeaderStrategy(workerpkg.LeaderStrategyLease))

	worker1 := startTestWorker(t, "worker-1", []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
		newTestConsumer("consumer-2", "worker-1", nil),
		newTestConsumer("consumer-3", "worker-1", nil),
		newTestConsumer("consumer-4", "worker-1", nil),
	}, options...)
	worker2 := startTestWorker(t, "worker-2", []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
		newTestConsumer("consumer-2", "worker-2", nil),
		newTestConsumer("consumer-3", "worker-2", nil),
		newTestConsumer("consumer-4", "worker-2", nil),
	}, options...)

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
	}, options...)

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

func TestLeaseLeader_UncleanCrash_SurvivorTakesOver(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	options := append(defaultWorkerOptions(), workerpkg.WithLeaderStrategy(workerpkg.LeaderStrategyLease))

	// Start only one worker first, it will become leader
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
		newTestConsumer("consumer-2", "worker-1", nil),
	}, options...)

	// Wait for worker1 to become leader
	waitForErr(t, defaultWaitTimeout, func() error {
		leaderID, _, err := workerpostgres.GetLease(context.Background(), controlDB, workerpostgres.DefaultLeaderElectionTable)
		if err != nil {
			return err
		}
		if leaderID != worker1.worker.ID() {
			return fmt.Errorf("expected worker-1 (%s) to be leader, got %s", worker1.worker.ID(), leaderID)
		}
		return nil
	})

	// Start worker2 (standby)
	worker2 := startTestWorker(t, "worker-2", []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
		newTestConsumer("consumer-2", "worker-2", nil),
	}, options...)

	// Confirm worker2 does not become leader (worker1 is still leader)
	time.Sleep(300 * time.Millisecond)
	leaderID, _, err := workerpostgres.GetLease(context.Background(), controlDB, workerpostgres.DefaultLeaderElectionTable)
	if err != nil {
		t.Fatalf("failed to query lease: %v", err)
	}
	if leaderID != worker1.worker.ID() {
		t.Fatalf("expected worker-1 to remain leader, got %s", leaderID)
	}

	// Now stop worker1. To simulate an unclean crash (where it doesn't release the lease),
	// we will manually overwrite the lease table row to assign it to a fake dead leader
	// with an active lease expiring in 400ms.
	worker1.stop(t)

	fakeLeaderID := uuid.New()
	ctx := context.Background()
	_, err = controlDB.Exec(ctx, `
		INSERT INTO worker_nodes (worker_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, fakeLeaderID)
	if err != nil {
		t.Fatalf("failed to register fake leader: %v", err)
	}

	_, err = controlDB.Exec(ctx, `
		INSERT INTO worker_leader_election (lease_key, leader_id, expires_at, updated_at)
		VALUES ('leader', $1, NOW() + INTERVAL '400 milliseconds', NOW())
		ON CONFLICT (lease_key) DO UPDATE
		SET leader_id = EXCLUDED.leader_id,
		    expires_at = EXCLUDED.expires_at,
		    updated_at = EXCLUDED.updated_at
	`, fakeLeaderID)
	if err != nil {
		t.Fatalf("failed to inject fake leader lease: %v", err)
	}

	// Verify worker2 is still not the leader immediately (since the fake lease is still active)
	time.Sleep(100 * time.Millisecond)
	currentLeader, _, err := workerpostgres.GetLease(ctx, controlDB, workerpostgres.DefaultLeaderElectionTable)
	if err != nil {
		t.Fatalf("query current lease: %v", err)
	}
	if currentLeader != fakeLeaderID {
		t.Fatalf("expected lease to still be held by fake leader %s, got %s", fakeLeaderID, currentLeader)
	}

	// Wait for the fake lease to expire and worker2 to take over
	waitForErr(t, 2*time.Second, func() error {
		leader, _, err := workerpostgres.GetLease(ctx, controlDB, workerpostgres.DefaultLeaderElectionTable)
		if err != nil {
			return err
		}
		if leader != worker2.worker.ID() {
			return fmt.Errorf("expected worker-2 (%s) to take over leadership, got %s", worker2.worker.ID(), leader)
		}
		return nil
	})

	// Once worker2 becomes leader, it should rebalance and assign the consumers to itself
	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker2.worker.ID()] != 2 || len(counts) != 1 {
			return fmt.Errorf("expected worker-2 to take all 2 assignments, got %v", counts)
		}
		return nil
	})

	worker2.stop(t)
}

func TestLeaseLeader_CascadingDelete_SchemaConstraint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	workerID := uuid.New()

	// 1. Insert worker node
	_, err := controlDB.Exec(ctx, `
		INSERT INTO worker_nodes (worker_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, workerID)
	if err != nil {
		t.Fatalf("failed to insert worker: %v", err)
	}

	// 2. Insert lease pointing to worker
	_, err = controlDB.Exec(ctx, `
		INSERT INTO worker_leader_election (lease_key, leader_id, expires_at)
		VALUES ('leader', $1, NOW() + INTERVAL '1 hour')
	`, workerID)
	if err != nil {
		t.Fatalf("failed to insert lease: %v", err)
	}

	// 3. Delete worker node
	_, err = controlDB.Exec(ctx, `
		DELETE FROM worker_nodes WHERE worker_id = $1
	`, workerID)
	if err != nil {
		t.Fatalf("failed to delete worker node: %v", err)
	}

	// 4. Get lease and assert it was deleted (which GetLease returns as uuid.Nil because there is no row anymore)
	leaderID, _, err := workerpostgres.GetLease(ctx, controlDB, workerpostgres.DefaultLeaderElectionTable)
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	if leaderID != uuid.Nil {
		t.Fatalf("expected leader lease to be cascadingly deleted (GetLease returns uuid.Nil), got %s", leaderID)
	}
}

func TestLeaseLeader_ReleaseLease_Success(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	workerID := uuid.New()

	// 1. Insert worker node
	_, err := controlDB.Exec(ctx, `
		INSERT INTO worker_nodes (worker_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, workerID)
	if err != nil {
		t.Fatalf("failed to insert worker: %v", err)
	}

	// 2. Try to acquire lease
	acquired, err := workerpostgres.TryAcquireLease(ctx, controlDB, workerpostgres.DefaultLeaderElectionTable, workerID, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !acquired {
		t.Fatalf("expected lease to be acquired")
	}

	// 3. Release lease and verify it does not trigger foreign key constraint error
	err = workerpostgres.ReleaseLease(ctx, controlDB, workerpostgres.DefaultLeaderElectionTable, workerID)
	if err != nil {
		t.Fatalf("failed to release lease: %v", err)
	}

	// 4. Verify leader_id in database is NULL (not uuid.Nil or any other value)
	var leaderIDStr *string
	err = controlDB.QueryRow(ctx, `
		SELECT leader_id FROM worker_leader_election WHERE lease_key = 'leader'
	`).Scan(&leaderIDStr)
	if err != nil {
		t.Fatalf("failed to query leader_id directly: %v", err)
	}
	if leaderIDStr != nil {
		t.Fatalf("expected leader_id database column to be NULL, but got %q", *leaderIDStr)
	}

	// 5. GetLease should return uuid.Nil
	leaderID, _, err := workerpostgres.GetLease(ctx, controlDB, workerpostgres.DefaultLeaderElectionTable)
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	if leaderID != uuid.Nil {
		t.Fatalf("expected GetLease to return uuid.Nil, got %s", leaderID)
	}
}

func TestWorker_SplitBrain_OwnershipLost(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	
	// 1. Start worker-2 first, which will become leader but holds no consumers.
	worker2 := startTestWorker(t, "worker-2", nil, defaultWorkerOptions()...)

	// 2. Start worker-1 with a consumer. Worker-2 (leader) will assign the consumer to worker-1.
	consumer := newTestConsumer("consumer-split-brain", "worker-1", nil)
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{consumer}, defaultWorkerOptions()...)

	// Wait for consumer assignment to worker-1
	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker1.worker.ID() {
			return fmt.Errorf("consumer not assigned to worker-1 yet")
		}
		return nil
	})

	// Append first event
	appended := appendTestEvents(t, controlDB, eventStore, 1, "Invoice")
	latest := appended[len(appended)-1].GlobalPosition

	// Verify worker-1 processes first event
	waitForErr(t, defaultWaitTimeout, func() error {
		rows := getHandledRows(t, controlDB, consumer.Name())
		if len(rows) != 1 || rows[0].GlobalPosition != latest {
			return fmt.Errorf("handled rows=%v want position %d", rows, latest)
		}
		return nil
	})

	// Delete worker-1's registration node row.
	// This prevents the leader from re-assigning it back to worker-1 when rebalancing.
	_, err := controlDB.Exec(context.Background(), "DELETE FROM worker_nodes WHERE worker_id = $1", worker1.worker.ID())
	if err != nil {
		t.Fatalf("failed to delete worker-1 registration: %v", err)
	}

	// Manually reassign ownership in the database to worker-2
	assignConsumerToWorker(t, controlDB, consumer.Name(), worker2.worker.ID())

	// Append second event
	moreAppended := appendTestEvents(t, controlDB, eventStore, 1, "Invoice")
	moreLatest := moreAppended[len(moreAppended)-1].GlobalPosition

	// Verify that worker-1 does NOT process the second event because it lost ownership
	time.Sleep(1200 * time.Millisecond)
	rows := getHandledRows(t, controlDB, consumer.Name())
	if len(rows) != 1 {
		t.Fatalf("expected handled rows to stay at 1, but got %d (worker-1 processed event after losing ownership)", len(rows))
	}
	checkpoint := getCheckpoint(t, controlDB, consumer.Name())
	if checkpoint != latest {
		t.Fatalf("expected checkpoint to stay at %d, but got %d (moreLatest=%d)", latest, checkpoint, moreLatest)
	}

	worker1.stop(t)
	worker2.stop(t)
}

func TestLeaderFailover_AdvisoryLock_UncleanCrash(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	options := append(defaultWorkerOptions(), workerpkg.WithLeaderStrategy(workerpkg.LeaderStrategyAdvisory))

	// Start worker-1 (becomes leader)
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
	}, options...)

	// Start worker-2 (standby)
	worker2 := startTestWorker(t, "worker-2", []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
	}, options...)

	// Wait for worker-1 to be registered as leader
	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker1.worker.ID()] != 1 {
			return fmt.Errorf("worker-1 assignments=%v want 1", counts)
		}
		return nil
	})

	// Retrieve the pg backend PID holding the advisory lock
	var pid int
	err := controlDB.QueryRow(context.Background(), `
		SELECT pid FROM pg_locks 
		WHERE locktype = 'advisory' 
		  AND classid = 1685110 AND objid = 407961287
		LIMIT 1
	`).Scan(&pid)
	if err != nil {
		t.Fatalf("failed to query advisory lock pid: %v", err)
	}

	// Terminate the database session of worker-1's leader connection (unclean crash)
	_, err = controlDB.Exec(context.Background(), "SELECT pg_terminate_backend($1)", pid)
	if err != nil {
		t.Fatalf("failed to terminate leader backend session: %v", err)
	}

	// Cancel worker1's context to simulate the process dying
	worker1.cancel()

	// Verify worker-2 takes over leadership and assignments
	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedConsumerCounts(getAssignments(t, controlDB))
		if counts[worker2.worker.ID()] != 1 || len(counts) != 1 {
			return fmt.Errorf("post-crash assignments=%v want worker-2 => 1", counts)
		}
		return nil
	})

	worker1.stop(t)
	worker2.stop(t)
}

func TestLeaseLeader_HeartbeatRenewalHiccupAndSelfDemotion(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	options := append(defaultWorkerOptions(),
		workerpkg.WithLeaderStrategy(workerpkg.LeaderStrategyLease),
		workerpkg.WithHeartbeatInterval(100*time.Millisecond),
		workerpkg.WithHeartbeatTimeout(800*time.Millisecond),
	)

	// Start worker-1 (becomes leader)
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{
		newTestConsumer("consumer-1", "worker-1", nil),
	}, options...)

	// Wait for worker-1 to become leader
	waitForErr(t, defaultWaitTimeout, func() error {
		leaderID, _, err := workerpostgres.GetLease(context.Background(), controlDB, workerpostgres.DefaultLeaderElectionTable)
		if err != nil {
			return err
		}
		if leaderID != worker1.worker.ID() {
			return fmt.Errorf("expected worker-1 to be leader")
		}
		return nil
	})

	// Start worker-2 (standby)
	worker2 := startTestWorker(t, "worker-2", []*testConsumer{
		newTestConsumer("consumer-1", "worker-2", nil),
	}, options...)

	// Force-delete worker-1 node row. This will cause lease renewal updates to fail due to foreign key constraint violation.
	_, err := controlDB.Exec(context.Background(), "DELETE FROM worker_nodes WHERE worker_id = $1", worker1.worker.ID())
	if err != nil {
		t.Fatalf("failed to delete worker-1: %v", err)
	}

	// Verify that worker-2 takes over as leader after the HeartbeatTimeout has elapsed
	waitForErr(t, 2*time.Second, func() error {
		leaderID, _, err := workerpostgres.GetLease(context.Background(), controlDB, workerpostgres.DefaultLeaderElectionTable)
		if err != nil {
			return err
		}
		if leaderID != worker2.worker.ID() {
			return fmt.Errorf("expected worker-2 to take over leadership, got %s", leaderID)
		}
		return nil
	})

	worker1.stop(t)
	worker2.stop(t)
}

func TestDispatcher_NotifyDispatcher_Reconnection(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	consumer := newTestConsumer("consumer-notify-reconnect", "worker-1", nil)

	// Construct connection string for notifications
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}
	dbName := os.Getenv("POSTGRES_DB")
	if dbName == "" {
		dbName = "eventsalsa_worker_test"
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)

	options := append(defaultWorkerOptions(),
		workerpkg.WithDispatcherStrategy(workerpkg.DispatcherStrategyNotify),
		workerpkg.WithNotifyConnectionString(connStr),
		workerpkg.WithPollInterval(5*time.Second),
		workerpkg.WithMaxPollInterval(5*time.Second),
	)

	// Start worker-1
	worker1 := startTestWorker(t, "worker-1", []*testConsumer{consumer}, options...)

	// Wait for consumer assignment
	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].WorkerID != worker1.worker.ID() {
			return fmt.Errorf("consumer not assigned yet")
		}
		return nil
	})

	// Find the database session PID of the NotifyDispatcher listener
	var pid int
	waitForErr(t, defaultWaitTimeout, func() error {
		err := controlDB.QueryRow(context.Background(), `
			SELECT pid FROM pg_stat_activity 
			WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid() 
			LIMIT 1
		`).Scan(&pid)
		return err
	})

	// Terminate the listener backend session to simulate connection drop
	_, err := controlDB.Exec(context.Background(), "SELECT pg_terminate_backend($1)", pid)
	if err != nil {
		t.Fatalf("failed to terminate listener connection: %v", err)
	}

	// Wait for the NotifyDispatcher to detect and reconnect (1.5 seconds)
	time.Sleep(1500 * time.Millisecond)

	// Append event and verify wakeup occurs promptly (well before the 5s polling interval)
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
		t.Fatalf("dispatcher reconnection wakeup took %s; expected prompt wakeup after reconnection", elapsed)
	}

	worker1.stop(t)
}
