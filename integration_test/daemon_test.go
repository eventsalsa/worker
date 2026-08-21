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

	projectorpkg "github.com/eventsalsa/projector"
	projectorpostgres "github.com/eventsalsa/projector/postgres"
)

func TestProjector_SingleProjectorMultipleProjections(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projectionAll := newTestProjection("projection-all", "projector-1", nil)
	projectionAccounts := newTestProjection("projection-accounts", "projector-1", []string{"Account"})
	projectionOrders := newTestProjection("projection-orders", "projector-1", []string{"Order"})

	projector := startTestProjector(t, "projector-1", []*testProjection{projectionAll, projectionAccounts, projectionOrders}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 3 {
			return fmt.Errorf("expected 3 assignments, got %d", len(assignments))
		}
		for _, assignment := range assignments {
			if !assignment.Assigned || assignment.InstanceID != projector.daemon.ID() {
				return fmt.Errorf("projection %s not assigned to projector-1 yet", assignment.ProjectionName)
			}
		}
		return nil
	})

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Account", Count: 3},
		testEventBatch{StreamType: "Order", Count: 2},
	)
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if rows := getHandledRows(t, controlDB, projectionAll.Name()); len(rows) != 5 {
			return fmt.Errorf("projection-all handled %d rows, want 5", len(rows))
		}
		if rows := getHandledRows(t, controlDB, projectionAccounts.Name()); len(rows) != 3 {
			return fmt.Errorf("projection-accounts handled %d rows, want 3", len(rows))
		}
		if rows := getHandledRows(t, controlDB, projectionOrders.Name()); len(rows) != 2 {
			return fmt.Errorf("projection-orders handled %d rows, want 2", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, projectionAll.Name()); checkpoint != latest {
			return fmt.Errorf("projection-all checkpoint=%d want %d", checkpoint, latest)
		}
		if checkpoint := getCheckpoint(t, controlDB, projectionAccounts.Name()); checkpoint != latest {
			return fmt.Errorf("projection-accounts checkpoint=%d want %d (safe global frontier)", checkpoint, latest)
		}
		if checkpoint := getCheckpoint(t, controlDB, projectionOrders.Name()); checkpoint != latest {
			return fmt.Errorf("projection-orders checkpoint=%d want %d (safe global frontier)", checkpoint, latest)
		}
		return nil
	})
}

func TestRebalance_ScaleUp_ProjectorsReassignWithoutGapsOrDuplication(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projector1Projections := []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
		newTestProjection("projection-2", "projector-1", nil),
		newTestProjection("projection-3", "projector-1", nil),
		newTestProjection("projection-4", "projector-1", nil),
	}
	projector1 := startTestProjector(t, "projector-1", projector1Projections, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector1.daemon.ID()] != 4 {
			return fmt.Errorf("projector-1 has %d projections, want 4", counts[projector1.daemon.ID()])
		}
		return nil
	})

	initial := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Account", Count: 3},
		testEventBatch{StreamType: "Order", Count: 2},
	)
	initialLatest := initial[len(initial)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2", "projection-3", "projection-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 5 {
				return fmt.Errorf("%s handled %d rows, want 5", name, len(rows))
			}
		}
		return nil
	})

	projector2Projections := []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
		newTestProjection("projection-2", "projector-2", nil),
		newTestProjection("projection-3", "projector-2", nil),
		newTestProjection("projection-4", "projector-2", nil),
	}
	projector2 := startTestProjector(t, "projector-2", projector2Projections, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if len(counts) != 2 {
			return fmt.Errorf("expected 2 projectors in assignment map, got %d", len(counts))
		}
		values := []int{counts[projector1.daemon.ID()], counts[projector2.daemon.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("assignment split=%v want [2 2]", values)
		}
		return nil
	})

	more := appendTestEvents(t, controlDB, eventStore, 3, "Invoice")
	finalLatest := more[len(more)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2", "projection-3", "projection-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 8 {
				return fmt.Errorf("%s handled %d rows, want 8", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, finalLatest)
			}
		}
		labels := handledByAfter(t, controlDB, initialLatest)
		if len(labels) != 2 || labels[0] != "projector-1" || labels[1] != "projector-2" {
			return fmt.Errorf("post-rebalance events handled by %v, want both projectors", labels)
		}
		return nil
	})
}

func TestRebalance_ScaleDown_ProjectorStops_SurvivorOwnsAllProjections(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
		newTestProjection("projection-2", "projector-1", nil),
		newTestProjection("projection-3", "projector-1", nil),
		newTestProjection("projection-4", "projector-1", nil),
	}, defaultProjectorOptions()...)
	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
		newTestProjection("projection-2", "projector-2", nil),
		newTestProjection("projection-3", "projector-2", nil),
		newTestProjection("projection-4", "projector-2", nil),
	}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		values := []int{counts[projector1.daemon.ID()], counts[projector2.daemon.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("assignment split=%v want [2 2]", values)
		}
		return nil
	})

	first := appendTestEvents(t, controlDB, eventStore, 2, "Customer")
	firstLatest := first[len(first)-1].GlobalPosition
	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2", "projection-3", "projection-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 2 {
				return fmt.Errorf("%s handled %d rows, want 2", name, len(rows))
			}
		}
		return nil
	})

	projector2.stop(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector1.daemon.ID()] != 4 || len(counts) != 1 {
			return fmt.Errorf("survivor assignments=%v, want projector-1 => 4", counts)
		}
		if projectors := countProjectorRows(t, controlDB); projectors != 1 {
			return fmt.Errorf("projector_instances has %d rows, want 1", projectors)
		}
		return nil
	})

	more := appendTestEvents(t, controlDB, eventStore, 3, "Customer")
	finalLatest := more[len(more)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2", "projection-3", "projection-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 5 {
				return fmt.Errorf("%s handled %d rows, want 5", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, finalLatest)
			}
		}
		labels := handledByAfter(t, controlDB, firstLatest)
		if len(labels) != 1 || labels[0] != "projector-1" {
			return fmt.Errorf("post-scale-down events handled by %v, want only projector-1", labels)
		}
		return nil
	})
}

func TestProjectorStartupCleanup_RemovesVeryStaleProjectorRows(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	projection := newTestProjection("projection-cleanup-stale", "projector-1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := projectorpostgres.EnsureProjectionsRegistered(ctx, controlDB, projectorpostgres.DefaultProjectionAssignmentsTable, []string{projection.Name()}); err != nil {
		t.Fatalf("ensure projection registered: %v", err)
	}

	staleInstanceID := uuid.New()
	insertProjectorRow(t, controlDB, staleInstanceID, time.Now().Add(-5*time.Second))
	assignProjectionToInstance(t, controlDB, projection.Name(), staleInstanceID)

	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(), projectorpkg.WithHeartbeatTimeout(2*time.Second))...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if projectorRowExists(t, controlDB, staleInstanceID) {
			return fmt.Errorf("stale instance row %s still exists", staleInstanceID)
		}
		if !projectorRowExists(t, controlDB, projector.daemon.ID()) {
			return fmt.Errorf("instance row %s not registered yet", projector.daemon.ID())
		}
		if projectors := countProjectorRows(t, controlDB); projectors != 1 {
			return fmt.Errorf("projector_instances has %d rows, want 1", projectors)
		}

		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("assignments=%v want projection assigned to new instance", assignments)
		}

		return nil
	})
}

func TestProjectorStartupCleanup_PreservesRowsNewerThanCleanupThreshold(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	projection := newTestProjection("projection-cleanup-preserve", "projector-1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := projectorpostgres.EnsureProjectionsRegistered(ctx, controlDB, projectorpostgres.DefaultProjectionAssignmentsTable, []string{projection.Name()}); err != nil {
		t.Fatalf("ensure projection registered: %v", err)
	}

	staleButRetainedInstanceID := uuid.New()
	insertProjectorRow(t, controlDB, staleButRetainedInstanceID, time.Now().Add(-3*time.Second))
	assignProjectionToInstance(t, controlDB, projection.Name(), staleButRetainedInstanceID)

	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(), projectorpkg.WithHeartbeatTimeout(2*time.Second))...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if !projectorRowExists(t, controlDB, staleButRetainedInstanceID) {
			return fmt.Errorf("instance row %s was removed, want retained", staleButRetainedInstanceID)
		}
		if !projectorRowExists(t, controlDB, projector.daemon.ID()) {
			return fmt.Errorf("instance row %s not registered yet", projector.daemon.ID())
		}
		if projectors := countProjectorRows(t, controlDB); projectors != 2 {
			return fmt.Errorf("projector_instances has %d rows, want 2", projectors)
		}

		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("assignments=%v want projection assigned to new instance", assignments)
		}

		return nil
	})
}

func TestDispatcher_WakeupDispatcher_IdleProjectionsWakePromptly(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("projection-idle", "projector-1", nil)
	projector := startTestProjector(t, "projector-1", []*testProjection{
		projection,
	},
		append(defaultProjectorOptions(),
			projectorpkg.WithPollInterval(5*time.Second),
			projectorpkg.WithMaxPollInterval(5*time.Second),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("projection not assigned to projector-1 yet")
		}
		return nil
	})

	time.Sleep(1500 * time.Millisecond)

	start := time.Now()
	appended := appendTestEvents(t, controlDB, eventStore, 1, "Wakeup")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, 2*time.Second, func() error {
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 1 {
			return fmt.Errorf("projection handled %d rows, want 1", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
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
	projection := newTestProjection("projection-fail-once", "projector-1", nil)
	projector := startTestProjector(t, "projector-1", []*testProjection{projection}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("projection not assigned to projector-1 yet")
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Payment")
	projection.FailUntilCleared(appended[1].GlobalPosition, errors.New("boom"))
	lastPosition := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		if projection.AttemptCount(appended[1].GlobalPosition) < 1 {
			return fmt.Errorf("failing event was not attempted yet")
		}
		if projection.AttemptCount(appended[0].GlobalPosition) < 1 {
			return fmt.Errorf("first event was not attempted yet")
		}
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 0 {
			return fmt.Errorf("read model has %d committed rows, want 0 before retry succeeds", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 while projection is failing", checkpoint)
		}
		return nil
	})

	projection.ClearFailure(appended[1].GlobalPosition)

	waitForErr(t, defaultWaitTimeout, func() error {
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 3 {
			return fmt.Errorf("committed rows=%d want 3", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != lastPosition {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, lastPosition)
		}
		if projection.AttemptCount(appended[0].GlobalPosition) < 2 {
			return fmt.Errorf("first event attempts=%d want at least 2 to prove retry from same checkpoint", projection.AttemptCount(appended[0].GlobalPosition))
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
	projection := newTestProjection("projection-gap-late", "projector-1", nil)
	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(),
			projectorpkg.WithStaleGapThreshold(2*time.Second),
			projectorpkg.WithBatchPause(100*time.Millisecond),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("projection not assigned to projector-1 yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Invoice", Count: 1})
	gapPosition := held.events[0].GlobalPosition
	later := appendTestEvents(t, controlDB, eventStore, 3, "Invoice")
	latest := later[len(later)-1].GlobalPosition

	waitForErr(t, time.Second, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 while waiting for lower position", checkpoint)
		}
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 0 {
			return fmt.Errorf("handled rows=%d want 0 before lower position commits", len(rows))
		}
		return nil
	})

	held.Commit(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		rows := getHandledRows(t, controlDB, projection.Name())
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
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		if skips := getGapSkipRows(t, controlDB, projection.Name()); len(skips) != 0 {
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
	projection := newTestProjection("projection-gap-stale", "projector-1", nil)
	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(),
			projectorpkg.WithStaleGapThreshold(250*time.Millisecond),
			projectorpkg.WithStaleGapHarborLag(1),
			projectorpkg.WithPollInterval(500*time.Millisecond),
			projectorpkg.WithMaxPollInterval(500*time.Millisecond),
			projectorpkg.WithBatchPause(500*time.Millisecond),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("projection not assigned to projector-1 yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Invoice", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 4, "Invoice")
	expectedSkipTo := later[len(later)-2].GlobalPosition
	expectedHighestVisible := later[len(later)-1].GlobalPosition

	waitForErr(t, time.Second, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 before stale threshold", checkpoint)
		}
		return nil
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, projection.Name()); len(skips) != 1 {
			return fmt.Errorf("gap skips=%d want 1", len(skips))
		}
		return nil
	})

	projector.stop(t)
	held.Rollback(t)

	skips := getGapSkipRows(t, controlDB, projection.Name())
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
	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != expectedSkipTo {
		t.Fatalf("checkpoint=%d want %d", checkpoint, expectedSkipTo)
	}
	rows := getHandledRows(t, controlDB, projection.Name())
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
	projection := newTestProjection("projection-gap-stale-sparse-default", "projector-1", nil)
	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(),
			projectorpkg.WithStaleGapThreshold(250*time.Millisecond),
			projectorpkg.WithPollInterval(500*time.Millisecond),
			projectorpkg.WithMaxPollInterval(500*time.Millisecond),
			projectorpkg.WithBatchPause(500*time.Millisecond),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("projection not assigned to projector-1 yet")
		}
		return nil
	})

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Invoice", Count: 1})
	later := appendTestEvents(t, controlDB, eventStore, 1, "Invoice")
	expectedSkipTo := later[0].GlobalPosition

	waitForErr(t, time.Second, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != 0 {
			return fmt.Errorf("checkpoint=%d want 0 before stale threshold", checkpoint)
		}
		return nil
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		if skips := getGapSkipRows(t, controlDB, projection.Name()); len(skips) != 1 {
			return fmt.Errorf("gap skips=%d want 1", len(skips))
		}
		return nil
	})

	projector.stop(t)
	held.Rollback(t)

	skips := getGapSkipRows(t, controlDB, projection.Name())
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
	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != expectedSkipTo {
		t.Fatalf("checkpoint=%d want %d", checkpoint, expectedSkipTo)
	}
	rows := getHandledRows(t, controlDB, projection.Name())
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
	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
		newTestProjection("projection-2", "projector-1", nil),
	}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector1.daemon.ID()] != 2 {
			return fmt.Errorf("projector-1 assignments=%v want 2 projections", counts)
		}
		return nil
	})

	first := appendTestEvents(t, controlDB, eventStore, 3, "Shipment")
	firstLatest := first[len(first)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 3 {
				return fmt.Errorf("%s handled %d rows, want 3", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != firstLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, firstLatest)
			}
		}
		return nil
	})

	projector1.stop(t)

	second := appendTestEvents(t, controlDB, eventStore, 2, "Shipment")
	finalLatest := second[len(second)-1].GlobalPosition

	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
		newTestProjection("projection-2", "projector-2", nil),
	}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector2.daemon.ID()] != 2 || len(counts) != 1 {
			return fmt.Errorf("projector-2 assignments=%v want all projections", counts)
		}
		return nil
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2"} {
			rows := getHandledRows(t, controlDB, name)
			if len(rows) != 5 {
				return fmt.Errorf("%s handled %d rows, want 5", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, finalLatest)
			}
		}
		labels := handledByAfter(t, controlDB, firstLatest)
		if len(labels) != 1 || labels[0] != "projector-2" {
			return fmt.Errorf("post-restart events handled by %v want only projector-2", labels)
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
	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
		newTestProjection("projection-2", "projector-1", nil),
		newTestProjection("projection-3", "projector-1", nil),
		newTestProjection("projection-4", "projector-1", nil),
	}, defaultProjectorOptions()...)
	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
		newTestProjection("projection-2", "projector-2", nil),
		newTestProjection("projection-3", "projector-2", nil),
		newTestProjection("projection-4", "projector-2", nil),
	}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		values := []int{counts[projector1.daemon.ID()], counts[projector2.daemon.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("assignment split=%v want [2 2]", values)
		}
		return nil
	})

	projector1.stop(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector2.daemon.ID()] != 4 || len(counts) != 1 {
			return fmt.Errorf("post-failover assignments=%v want projector-2 => 4", counts)
		}
		return nil
	})

	projector3 := startTestProjector(t, "projector-3", []*testProjection{
		newTestProjection("projection-1", "projector-3", nil),
		newTestProjection("projection-2", "projector-3", nil),
		newTestProjection("projection-3", "projector-3", nil),
		newTestProjection("projection-4", "projector-3", nil),
	}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if len(counts) != 2 {
			return fmt.Errorf("expected 2 projectors after replacement, got %v", counts)
		}
		values := []int{counts[projector2.daemon.ID()], counts[projector3.daemon.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("replacement split=%v want [2 2]", values)
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Invoice")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2", "projection-3", "projection-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 2 {
				return fmt.Errorf("%s handled %d rows, want 2", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, latest)
			}
		}
		labels := handledByAfter(t, controlDB, 0)
		if len(labels) != 2 {
			return fmt.Errorf("handled_by=%v want two active projectors", labels)
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
	options := append(defaultProjectorOptions(), projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyLease))

	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
		newTestProjection("projection-2", "projector-1", nil),
		newTestProjection("projection-3", "projector-1", nil),
		newTestProjection("projection-4", "projector-1", nil),
	}, options...)
	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
		newTestProjection("projection-2", "projector-2", nil),
		newTestProjection("projection-3", "projector-2", nil),
		newTestProjection("projection-4", "projector-2", nil),
	}, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		values := []int{counts[projector1.daemon.ID()], counts[projector2.daemon.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("assignment split=%v want [2 2]", values)
		}
		return nil
	})

	projector1.stop(t)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector2.daemon.ID()] != 4 || len(counts) != 1 {
			return fmt.Errorf("post-failover assignments=%v want projector-2 => 4", counts)
		}
		return nil
	})

	projector3 := startTestProjector(t, "projector-3", []*testProjection{
		newTestProjection("projection-1", "projector-3", nil),
		newTestProjection("projection-2", "projector-3", nil),
		newTestProjection("projection-3", "projector-3", nil),
		newTestProjection("projection-4", "projector-3", nil),
	}, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if len(counts) != 2 {
			return fmt.Errorf("expected 2 projectors after replacement, got %v", counts)
		}
		values := []int{counts[projector2.daemon.ID()], counts[projector3.daemon.ID()]}
		sort.Ints(values)
		if values[0] != 2 || values[1] != 2 {
			return fmt.Errorf("replacement split=%v want [2 2]", values)
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 2, "Invoice")
	latest := appended[len(appended)-1].GlobalPosition

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"projection-1", "projection-2", "projection-3", "projection-4"} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != 2 {
				return fmt.Errorf("%s handled %d rows, want 2", name, len(rows))
			}
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%d want %d", name, checkpoint, latest)
			}
		}
		labels := handledByAfter(t, controlDB, 0)
		if len(labels) != 2 {
			return fmt.Errorf("handled_by=%v want two active projectors", labels)
		}
		return nil
	})
}

func TestLeaseLeader_UncleanCrash_SurvivorTakesOver(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	options := append(defaultProjectorOptions(), projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyLease))

	// Start only one projector first, it will become leader
	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
		newTestProjection("projection-2", "projector-1", nil),
	}, options...)

	// Wait for projector1 to become leader
	waitForErr(t, defaultWaitTimeout, func() error {
		leaderID, _, err := projectorpostgres.GetLease(context.Background(), controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
		if err != nil {
			return err
		}
		if leaderID != projector1.daemon.ID() {
			return fmt.Errorf("expected projector-1 (%s) to be leader, got %s", projector1.daemon.ID(), leaderID)
		}
		return nil
	})

	// Start projector2 (standby)
	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
		newTestProjection("projection-2", "projector-2", nil),
	}, options...)

	// Confirm projector2 does not become leader (projector1 is still leader)
	time.Sleep(300 * time.Millisecond)
	leaderID, _, err := projectorpostgres.GetLease(context.Background(), controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
	if err != nil {
		t.Fatalf("failed to query lease: %v", err)
	}
	if leaderID != projector1.daemon.ID() {
		t.Fatalf("expected projector-1 to remain leader, got %s", leaderID)
	}

	// Now stop projector1. To simulate an unclean crash (where it doesn't release the lease),
	// we will manually overwrite the lease table row to assign it to a fake dead leader
	// with an active lease expiring in 400ms.
	projector1.stop(t)

	fakeLeaderID := uuid.New()
	ctx := context.Background()
	_, err = controlDB.Exec(ctx, `
		INSERT INTO projector_instances (instance_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, fakeLeaderID)
	if err != nil {
		t.Fatalf("failed to register fake leader: %v", err)
	}

	_, err = controlDB.Exec(ctx, `
		INSERT INTO projector_leader_leases (lease_key, leader_id, expires_at, updated_at)
		VALUES ('leader', $1, NOW() + INTERVAL '400 milliseconds', NOW())
		ON CONFLICT (lease_key) DO UPDATE
		SET leader_id = EXCLUDED.leader_id,
		    expires_at = EXCLUDED.expires_at,
		    updated_at = EXCLUDED.updated_at
	`, fakeLeaderID)
	if err != nil {
		t.Fatalf("failed to inject fake leader lease: %v", err)
	}

	// Verify projector2 is still not the leader immediately (since the fake lease is still active)
	time.Sleep(100 * time.Millisecond)
	currentLeader, _, err := projectorpostgres.GetLease(ctx, controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
	if err != nil {
		t.Fatalf("query current lease: %v", err)
	}
	if currentLeader != fakeLeaderID {
		t.Fatalf("expected lease to still be held by fake leader %s, got %s", fakeLeaderID, currentLeader)
	}

	// Wait for the fake lease to expire and projector2 to take over
	waitForErr(t, 2*time.Second, func() error {
		leader, _, err := projectorpostgres.GetLease(ctx, controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
		if err != nil {
			return err
		}
		if leader != projector2.daemon.ID() {
			return fmt.Errorf("expected projector-2 (%s) to take over leadership, got %s", projector2.daemon.ID(), leader)
		}
		return nil
	})

	// Once projector2 becomes leader, it should rebalance and assign the projections to itself
	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector2.daemon.ID()] != 2 || len(counts) != 1 {
			return fmt.Errorf("expected projector-2 to take all 2 assignments, got %v", counts)
		}
		return nil
	})

	projector2.stop(t)
}

func TestLeaseLeader_CascadingDelete_SchemaConstraint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	instanceID := uuid.New()

	// 1. Insert instance node
	_, err := controlDB.Exec(ctx, `
		INSERT INTO projector_instances (instance_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, instanceID)
	if err != nil {
		t.Fatalf("failed to insert instance: %v", err)
	}

	// 2. Insert lease pointing to instance
	_, err = controlDB.Exec(ctx, `
		INSERT INTO projector_leader_leases (lease_key, leader_id, expires_at)
		VALUES ('leader', $1, NOW() + INTERVAL '1 hour')
	`, instanceID)
	if err != nil {
		t.Fatalf("failed to insert lease: %v", err)
	}

	// 3. Delete instance node
	_, err = controlDB.Exec(ctx, `
		DELETE FROM projector_instances WHERE instance_id = $1
	`, instanceID)
	if err != nil {
		t.Fatalf("failed to delete instance: %v", err)
	}

	// 4. Get lease and assert it was deleted (which GetLease returns as uuid.Nil because there is no row anymore)
	leaderID, _, err := projectorpostgres.GetLease(ctx, controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
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
	instanceID := uuid.New()

	// 1. Insert instance node
	_, err := controlDB.Exec(ctx, `
		INSERT INTO projector_instances (instance_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, instanceID)
	if err != nil {
		t.Fatalf("failed to insert instance: %v", err)
	}

	// 2. Try to acquire lease
	acquired, err := projectorpostgres.TryAcquireLease(ctx, controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable, instanceID, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !acquired {
		t.Fatalf("expected lease to be acquired")
	}

	// 3. Release lease and verify it does not trigger foreign key constraint error
	err = projectorpostgres.ReleaseLease(ctx, controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable, instanceID)
	if err != nil {
		t.Fatalf("failed to release lease: %v", err)
	}

	// 4. Verify leader_id in database is NULL (not uuid.Nil or any other value)
	var leaderIDStr *string
	err = controlDB.QueryRow(ctx, `
		SELECT leader_id FROM projector_leader_leases WHERE lease_key = 'leader'
	`).Scan(&leaderIDStr)
	if err != nil {
		t.Fatalf("failed to query leader_id directly: %v", err)
	}
	if leaderIDStr != nil {
		t.Fatalf("expected leader_id database column to be NULL, but got %q", *leaderIDStr)
	}

	// 5. GetLease should return uuid.Nil
	leaderID, _, err := projectorpostgres.GetLease(ctx, controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
	if err != nil {
		t.Fatalf("failed to get lease: %v", err)
	}
	if leaderID != uuid.Nil {
		t.Fatalf("expected GetLease to return uuid.Nil, got %s", leaderID)
	}
}

func TestProjector_SplitBrain_OwnershipLost(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	// 1. Start projector-1 with a projection. Projector-1 will become leader and assign the projection to itself.
	projection := newTestProjection("projection-split-brain", "projector-1", nil)
	projector1 := startTestProjector(t, "projector-1", []*testProjection{projection}, defaultProjectorOptions()...)

	// Wait for projection assignment to projector-1
	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector1.daemon.ID() {
			return fmt.Errorf("projection not assigned to projector-1 yet")
		}
		return nil
	})

	// Append first event
	appended := appendTestEvents(t, controlDB, eventStore, 1, "Invoice")
	latest := appended[len(appended)-1].GlobalPosition

	// Verify projector-1 processes first event
	waitForErr(t, defaultWaitTimeout, func() error {
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 1 || rows[0].GlobalPosition != latest {
			return fmt.Errorf("handled rows=%v want position %d", rows, latest)
		}
		return nil
	})

	// Delete projector-1's registration instance row.
	// This simulates projector-1 being considered dead or dropped by the cluster.
	_, err := controlDB.Exec(context.Background(), "DELETE FROM projector_instances WHERE instance_id = $1", projector1.daemon.ID())
	if err != nil {
		t.Fatalf("failed to delete projector-1 registration: %v", err)
	}

	// Manually reassign ownership in the database to a different instance ID
	newInstanceID := uuid.New()
	insertProjectorRow(t, controlDB, newInstanceID, time.Now())
	assignProjectionToInstance(t, controlDB, projection.Name(), newInstanceID)

	// Append second event
	moreAppended := appendTestEvents(t, controlDB, eventStore, 1, "Invoice")
	moreLatest := moreAppended[len(moreAppended)-1].GlobalPosition

	// Verify that projector-1 does NOT process the second event because it lost ownership
	time.Sleep(1200 * time.Millisecond)
	rows := getHandledRows(t, controlDB, projection.Name())
	if len(rows) != 1 {
		t.Fatalf("expected handled rows to stay at 1, but got %d (projector-1 processed event after losing ownership)", len(rows))
	}
	checkpoint := getCheckpoint(t, controlDB, projection.Name())
	if checkpoint != latest {
		t.Fatalf("expected checkpoint to stay at %d, but got %d (moreLatest=%d)", latest, checkpoint, moreLatest)
	}

	projector1.stop(t)
}

func TestLeaderFailover_AdvisoryLock_UncleanCrash(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	options := append(defaultProjectorOptions(), projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyAdvisory))

	// Start projector-1 (becomes leader)
	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
	}, options...)

	// Start projector-2 (standby)
	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
	}, options...)

	// Wait for projector-1 to be registered as leader
	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector1.daemon.ID()] != 1 {
			return fmt.Errorf("projector-1 assignments=%v want 1", counts)
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

	// Terminate the database session of projector-1's leader connection (unclean crash)
	_, err = controlDB.Exec(context.Background(), "SELECT pg_terminate_backend($1)", pid)
	if err != nil {
		t.Fatalf("failed to terminate leader backend session: %v", err)
	}

	// Cancel projector1's context to simulate the process dying
	projector1.cancel()

	// Verify projector-2 takes over leadership and assignments
	waitForErr(t, defaultWaitTimeout, func() error {
		counts := assignedProjectionCounts(getAssignments(t, controlDB))
		if counts[projector2.daemon.ID()] != 1 || len(counts) != 1 {
			return fmt.Errorf("post-crash assignments=%v want projector-2 => 1", counts)
		}
		return nil
	})

	projector1.stop(t)
	projector2.stop(t)
}

func TestLeaseLeader_HeartbeatRenewalHiccupAndSelfDemotion(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	options := append(defaultProjectorOptions(),
		projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyLease),
		projectorpkg.WithHeartbeatInterval(100*time.Millisecond),
		projectorpkg.WithHeartbeatTimeout(800*time.Millisecond),
	)

	// Start projector-1 (becomes leader)
	projector1 := startTestProjector(t, "projector-1", []*testProjection{
		newTestProjection("projection-1", "projector-1", nil),
	}, options...)

	// Wait for projector-1 to become leader
	waitForErr(t, defaultWaitTimeout, func() error {
		leaderID, _, err := projectorpostgres.GetLease(context.Background(), controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
		if err != nil {
			return err
		}
		if leaderID != projector1.daemon.ID() {
			return fmt.Errorf("expected projector-1 to be leader")
		}
		return nil
	})

	// Start projector-2 (standby)
	projector2 := startTestProjector(t, "projector-2", []*testProjection{
		newTestProjection("projection-1", "projector-2", nil),
	}, options...)

	// Force-delete projector-1 node row. This will cause lease renewal updates to fail due to foreign key constraint violation.
	_, err := controlDB.Exec(context.Background(), "DELETE FROM projector_instances WHERE instance_id = $1", projector1.daemon.ID())
	if err != nil {
		t.Fatalf("failed to delete projector-1: %v", err)
	}

	// Verify that projector-2 takes over as leader after the HeartbeatTimeout has elapsed
	waitForErr(t, 2*time.Second, func() error {
		leaderID, _, err := projectorpostgres.GetLease(context.Background(), controlDB, projectorpostgres.DefaultProjectorLeaderLeasesTable)
		if err != nil {
			return err
		}
		if leaderID != projector2.daemon.ID() {
			return fmt.Errorf("expected projector-2 to take over leadership, got %s", leaderID)
		}
		return nil
	})

	projector1.stop(t)
	projector2.stop(t)
}

func TestDispatcher_NotifyDispatcher_Reconnection(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("projection-notify-reconnect", "projector-1", nil)

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
		dbName = "eventsalsa_projector_test"
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)

	options := append(defaultProjectorOptions(),
		projectorpkg.WithDispatcherStrategy(projectorpkg.DispatcherStrategyNotify),
		projectorpkg.WithNotifyConnectionString(connStr),
		projectorpkg.WithNotifyChannel("test_events"),
		projectorpkg.WithPollInterval(5*time.Second),
		projectorpkg.WithMaxPollInterval(5*time.Second),
	)

	// Start projector-1
	projector1 := startTestProjector(t, "projector-1", []*testProjection{projection}, options...)

	// Wait for projection assignment
	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned || assignments[0].InstanceID != projector1.daemon.ID() {
			return fmt.Errorf("projection not assigned yet")
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
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 1 {
			return fmt.Errorf("projection handled %d rows, want 1", len(rows))
		}
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%d want %d", checkpoint, latest)
		}
		return nil
	})

	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("dispatcher reconnection wakeup took %s; expected prompt wakeup after reconnection", elapsed)
	}

	projector1.stop(t)
}
