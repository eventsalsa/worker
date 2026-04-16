//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	storepostgres "github.com/eventsalsa/store/postgres"

	workerpostgres "github.com/eventsalsa/worker/postgres"
)

func TestComprehensiveScaleUpAndDown(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	consumerNames := make([]string, 25)
	for i := range consumerNames {
		consumerNames[i] = fmt.Sprintf("consumer-%02d", i+1)
	}

	makeConsumers := func(workerLabel string) []*testConsumer {
		consumers := make([]*testConsumer, 0, len(consumerNames))
		for _, consumerName := range consumerNames {
			consumers = append(consumers, newTestConsumer(consumerName, workerLabel, nil))
		}
		return consumers
	}

	activeWorkers := make([]*testWorkerHarness, 0, 7)
	allPositions := make([]int64, 0, 30)

	runRebalanceAssertions := func() {
		t.Helper()

		time.Sleep(2 * time.Second)

		workerIDs := workerIDsFromHarnesses(activeWorkers)
		waitForErr(t, defaultWaitTimeout, func() error {
			if err := checkBalancedAssignments(t, controlDB, len(activeWorkers), len(consumerNames), workerIDs); err != nil {
				return err
			}
			if err := checkFreshHeartbeats(controlDB, workerIDs, 2*time.Second); err != nil {
				return err
			}
			if len(allPositions) > 0 {
				latest := allPositions[len(allPositions)-1]
				if err := checkConsumersCaughtUp(t, controlDB, consumerNames, len(allPositions), latest); err != nil {
					return err
				}
			}
			return nil
		})

		assertBalancedAssignments(t, controlDB, len(activeWorkers), len(consumerNames), workerIDs)
	}

	runProcessingStep := func(aggregateType string, expectedLabels []string) {
		t.Helper()

		cutoff := int64(0)
		if len(allPositions) > 0 {
			cutoff = allPositions[len(allPositions)-1]
		}

		appended := appendTestEvents(t, controlDB, eventStore, 5, aggregateType)
		for _, event := range appended {
			allPositions = append(allPositions, event.GlobalPosition)
		}
		latest := allPositions[len(allPositions)-1]
		expectedCount := len(allPositions)

		waitForErr(t, defaultWaitTimeout, func() error {
			return checkConsumersProcessedStep(t, controlDB, consumerNames, expectedCount, latest, cutoff, expectedLabels)
		})
	}

	worker1 := startTestWorker(t, "worker-1", makeConsumers("worker-1"), defaultWorkerOptions()...)
	activeWorkers = append(activeWorkers, worker1)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp01", []string{"worker-1"})

	worker2 := startTestWorker(t, "worker-2", makeConsumers("worker-2"), defaultWorkerOptions()...)
	activeWorkers = append(activeWorkers, worker2)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp02", workerLabelsFromHarnesses(activeWorkers))

	worker3 := startTestWorker(t, "worker-3", makeConsumers("worker-3"), defaultWorkerOptions()...)
	activeWorkers = append(activeWorkers, worker3)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp03", workerLabelsFromHarnesses(activeWorkers))

	worker4 := startTestWorker(t, "worker-4", makeConsumers("worker-4"), defaultWorkerOptions()...)
	worker5 := startTestWorker(t, "worker-5", makeConsumers("worker-5"), defaultWorkerOptions()...)
	activeWorkers = append(activeWorkers, worker4, worker5)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp04", workerLabelsFromHarnesses(activeWorkers))

	worker6 := startTestWorker(t, "worker-6", makeConsumers("worker-6"), defaultWorkerOptions()...)
	worker7 := startTestWorker(t, "worker-7", makeConsumers("worker-7"), defaultWorkerOptions()...)
	activeWorkers = append(activeWorkers, worker6, worker7)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp05", workerLabelsFromHarnesses(activeWorkers))

	waitForErr(t, defaultWaitTimeout, func() error {
		return checkFreshHeartbeats(controlDB, workerIDsFromHarnesses(activeWorkers), 2*time.Second)
	})

	worker6.stop(t)
	worker7.stop(t)
	activeWorkers = []*testWorkerHarness{worker1, worker2, worker3, worker4, worker5}
	runRebalanceAssertions()

	worker4.stop(t)
	worker5.stop(t)
	activeWorkers = []*testWorkerHarness{worker1, worker2, worker3}
	runRebalanceAssertions()

	worker2.stop(t)
	worker3.stop(t)
	activeWorkers = []*testWorkerHarness{worker1}
	runRebalanceAssertions()
	runProcessingStep("ScaleDownFinal", []string{"worker-1"})

	latest := allPositions[len(allPositions)-1]
	if position := latestGlobalPosition(t, controlDB, eventStore); position != latest {
		t.Fatalf("latest global position=%d want %d", position, latest)
	}

	assertAllEventsProcessedWithoutGaps(t, controlDB, consumerNames, allPositions, latest)
}

func assertBalancedAssignments(t *testing.T, db *sql.DB, expectedWorkers int, totalConsumers int, workerIDs []uuid.UUID) {
	t.Helper()

	if err := checkBalancedAssignments(t, db, expectedWorkers, totalConsumers, workerIDs); err != nil {
		t.Fatal(err)
	}
}

func checkBalancedAssignments(t *testing.T, db *sql.DB, expectedWorkers int, totalConsumers int, workerIDs []uuid.UUID) error {
	t.Helper()

	assignments := getAssignments(t, db)
	if len(assignments) != totalConsumers {
		return fmt.Errorf("assignment rows=%d want %d", len(assignments), totalConsumers)
	}

	expectedSet := make(map[uuid.UUID]struct{}, len(workerIDs))
	for _, workerID := range workerIDs {
		expectedSet[workerID] = struct{}{}
	}

	assignedCount := 0
	for _, assignment := range assignments {
		if !assignment.Assigned {
			return fmt.Errorf("consumer %s is unassigned", assignment.ConsumerName)
		}
		if _, ok := expectedSet[assignment.WorkerID]; !ok {
			return fmt.Errorf("consumer %s assigned to unexpected worker %s", assignment.ConsumerName, assignment.WorkerID)
		}
		assignedCount++
	}
	if assignedCount != totalConsumers {
		return fmt.Errorf("assigned consumers=%d want %d", assignedCount, totalConsumers)
	}

	counts := assignedConsumerCounts(assignments)
	if len(counts) != expectedWorkers {
		return fmt.Errorf("assigned workers=%d want %d (counts=%v)", len(counts), expectedWorkers, counts)
	}

	minPerWorker := totalConsumers / expectedWorkers
	maxPerWorker := minPerWorker
	if totalConsumers%expectedWorkers != 0 {
		maxPerWorker++
	}

	totalAssigned := 0
	for _, workerID := range workerIDs {
		count := counts[workerID]
		if count < minPerWorker || count > maxPerWorker {
			return fmt.Errorf(
				"worker %s has %d consumers, want %d or %d (counts=%v)",
				workerID,
				count,
				minPerWorker,
				maxPerWorker,
				counts,
			)
		}
		totalAssigned += count
	}
	if totalAssigned != totalConsumers {
		return fmt.Errorf("total assigned=%d want %d", totalAssigned, totalConsumers)
	}

	return nil
}

func assertAllEventsProcessedWithoutGaps(t *testing.T, db *sql.DB, consumerNames []string, expectedPositions []int64, latest int64) {
	t.Helper()

	for _, consumerName := range consumerNames {
		rows := getHandledRows(t, db, consumerName)
		if len(rows) != len(expectedPositions) {
			t.Fatalf("%s handled %d rows, want %d", consumerName, len(rows), len(expectedPositions))
		}

		for index, position := range expectedPositions {
			if rows[index].GlobalPosition != position {
				t.Fatalf(
					"%s row %d has global_position=%d want %d",
					consumerName,
					index,
					rows[index].GlobalPosition,
					position,
				)
			}
		}

		if checkpoint := getCheckpoint(t, db, consumerName); checkpoint != latest {
			t.Fatalf("%s checkpoint=%d want %d", consumerName, checkpoint, latest)
		}
	}
}

func checkConsumersProcessedStep(t *testing.T, db *sql.DB, consumerNames []string, expectedCount int, latest int64, cutoff int64, expectedLabels []string) error {
	t.Helper()

	if err := checkConsumersCaughtUp(t, db, consumerNames, expectedCount, latest); err != nil {
		return err
	}

	labels := handledByAfter(t, db, cutoff)
	sort.Strings(labels)

	expected := append([]string(nil), expectedLabels...)
	sort.Strings(expected)

	if len(labels) != len(expected) {
		return fmt.Errorf("handled_by labels=%v want %v", labels, expected)
	}
	for index := range expected {
		if labels[index] != expected[index] {
			return fmt.Errorf("handled_by labels=%v want %v", labels, expected)
		}
	}

	return nil
}

func checkConsumersCaughtUp(t *testing.T, db *sql.DB, consumerNames []string, expectedCount int, latest int64) error {
	t.Helper()

	for _, consumerName := range consumerNames {
		rows := getHandledRows(t, db, consumerName)
		if len(rows) != expectedCount {
			return fmt.Errorf("%s handled %d rows, want %d", consumerName, len(rows), expectedCount)
		}
		if checkpoint := getCheckpoint(t, db, consumerName); checkpoint != latest {
			return fmt.Errorf("%s checkpoint=%d want %d", consumerName, checkpoint, latest)
		}
	}

	return nil
}

func checkFreshHeartbeats(db *sql.DB, workerIDs []uuid.UUID, maxAge time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
SELECT worker_id, heartbeat_at
FROM %s
ORDER BY worker_id ASC
`, workerpostgres.DefaultWorkerNodesTable)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query worker heartbeats: %w", err)
	}
	defer rows.Close()

	expectedSet := make(map[uuid.UUID]struct{}, len(workerIDs))
	for _, workerID := range workerIDs {
		expectedSet[workerID] = struct{}{}
	}

	heartbeats := make(map[uuid.UUID]time.Time, len(workerIDs))
	for rows.Next() {
		var workerIDStr string
		var heartbeatAt time.Time
		if err := rows.Scan(&workerIDStr, &heartbeatAt); err != nil {
			return fmt.Errorf("scan worker heartbeat: %w", err)
		}

		workerID, err := uuid.Parse(workerIDStr)
		if err != nil {
			return fmt.Errorf("parse worker id %q: %w", workerIDStr, err)
		}

		heartbeats[workerID] = heartbeatAt
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate worker heartbeats: %w", err)
	}

	if len(heartbeats) != len(workerIDs) {
		return fmt.Errorf("worker_nodes rows=%d want %d", len(heartbeats), len(workerIDs))
	}

	now := time.Now()
	for _, workerID := range workerIDs {
		heartbeatAt, ok := heartbeats[workerID]
		if !ok {
			return fmt.Errorf("worker %s missing from worker_nodes", workerID)
		}
		if age := now.Sub(heartbeatAt); age > maxAge {
			return fmt.Errorf("worker %s heartbeat age=%s exceeds %s", workerID, age, maxAge)
		}
	}

	for workerID := range heartbeats {
		if _, ok := expectedSet[workerID]; !ok {
			return fmt.Errorf("unexpected worker %s present in worker_nodes", workerID)
		}
	}

	return nil
}

func workerIDsFromHarnesses(workers []*testWorkerHarness) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(workers))
	for _, worker := range workers {
		ids = append(ids, worker.worker.ID())
	}
	return ids
}

func workerLabelsFromHarnesses(workers []*testWorkerHarness) []string {
	labels := make([]string, 0, len(workers))
	for _, worker := range workers {
		labels = append(labels, worker.label)
	}
	return labels
}
