//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	storepostgres "github.com/eventsalsa/store/postgres"

	projectorpostgres "github.com/eventsalsa/projector/postgres"
)

func TestComprehensiveScaleUpAndDown(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	projectionNames := make([]string, 25)
	for i := range projectionNames {
		projectionNames[i] = fmt.Sprintf("projection-%02d", i+1)
	}

	makeProjections := func(instanceLabel string) []*testProjection {
		projections := make([]*testProjection, 0, len(projectionNames))
		for _, projectionName := range projectionNames {
			projections = append(projections, newTestProjection(projectionName, instanceLabel, nil))
		}
		return projections
	}

	activeProjectors := make([]*testProjectorHarness, 0, 7)
	allPositions := make([]int64, 0, 30)

	runRebalanceAssertions := func() {
		t.Helper()

		time.Sleep(2 * time.Second)

		instanceIDs := instanceIDsFromHarnesses(activeProjectors)
		waitForErr(t, defaultWaitTimeout, func() error {
			if err := checkBalancedAssignments(t, controlDB, len(activeProjectors), len(projectionNames), instanceIDs); err != nil {
				return err
			}
			if err := checkFreshHeartbeats(controlDB, instanceIDs, 2*time.Second); err != nil {
				return err
			}
			if len(allPositions) > 0 {
				latest := allPositions[len(allPositions)-1]
				if err := checkProjectionsCaughtUp(t, controlDB, projectionNames, len(allPositions), latest); err != nil {
					return err
				}
			}
			return nil
		})

		assertBalancedAssignments(t, controlDB, len(activeProjectors), len(projectionNames), instanceIDs)
	}

	runProcessingStep := func(streamType string, expectedLabels []string) {
		t.Helper()

		cutoff := int64(0)
		if len(allPositions) > 0 {
			cutoff = allPositions[len(allPositions)-1]
		}

		appended := appendTestEvents(t, controlDB, eventStore, 5, streamType)
		for _, event := range appended {
			allPositions = append(allPositions, event.GlobalPosition)
		}
		latest := allPositions[len(allPositions)-1]
		expectedCount := len(allPositions)

		waitForErr(t, defaultWaitTimeout, func() error {
			return checkProjectionsProcessedStep(t, controlDB, projectionNames, expectedCount, latest, cutoff, expectedLabels)
		})
	}

	projector1 := startTestProjector(t, "projector-1", makeProjections("projector-1"), defaultProjectorOptions()...)
	activeProjectors = append(activeProjectors, projector1)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp01", []string{"projector-1"})

	projector2 := startTestProjector(t, "projector-2", makeProjections("projector-2"), defaultProjectorOptions()...)
	activeProjectors = append(activeProjectors, projector2)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp02", projectorLabelsFromHarnesses(activeProjectors))

	projector3 := startTestProjector(t, "projector-3", makeProjections("projector-3"), defaultProjectorOptions()...)
	activeProjectors = append(activeProjectors, projector3)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp03", projectorLabelsFromHarnesses(activeProjectors))

	projector4 := startTestProjector(t, "projector-4", makeProjections("projector-4"), defaultProjectorOptions()...)
	projector5 := startTestProjector(t, "projector-5", makeProjections("projector-5"), defaultProjectorOptions()...)
	activeProjectors = append(activeProjectors, projector4, projector5)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp04", projectorLabelsFromHarnesses(activeProjectors))

	projector6 := startTestProjector(t, "projector-6", makeProjections("projector-6"), defaultProjectorOptions()...)
	projector7 := startTestProjector(t, "projector-7", makeProjections("projector-7"), defaultProjectorOptions()...)
	activeProjectors = append(activeProjectors, projector6, projector7)
	runRebalanceAssertions()
	runProcessingStep("ScaleUp05", projectorLabelsFromHarnesses(activeProjectors))

	waitForErr(t, defaultWaitTimeout, func() error {
		return checkFreshHeartbeats(controlDB, instanceIDsFromHarnesses(activeProjectors), 2*time.Second)
	})

	projector6.stop(t)
	projector7.stop(t)
	activeProjectors = []*testProjectorHarness{projector1, projector2, projector3, projector4, projector5}
	runRebalanceAssertions()

	projector4.stop(t)
	projector5.stop(t)
	activeProjectors = []*testProjectorHarness{projector1, projector2, projector3}
	runRebalanceAssertions()

	projector2.stop(t)
	projector3.stop(t)
	activeProjectors = []*testProjectorHarness{projector1}
	runRebalanceAssertions()
	runProcessingStep("ScaleDownFinal", []string{"projector-1"})

	latest := allPositions[len(allPositions)-1]
	if position := latestGlobalPosition(t, controlDB, eventStore); position != latest {
		t.Fatalf("latest global position=%d want %d", position, latest)
	}

	assertAllEventsProcessedWithoutGaps(t, controlDB, projectionNames, allPositions, latest)
}

func assertBalancedAssignments(t *testing.T, db *pgxpool.Pool, expectedInstances int, totalProjections int, instanceIDs []uuid.UUID) {
	t.Helper()

	if err := checkBalancedAssignments(t, db, expectedInstances, totalProjections, instanceIDs); err != nil {
		t.Fatal(err)
	}
}

func checkBalancedAssignments(t *testing.T, db *pgxpool.Pool, expectedInstances int, totalProjections int, instanceIDs []uuid.UUID) error {
	t.Helper()

	assignments := getAssignments(t, db)
	if len(assignments) != totalProjections {
		return fmt.Errorf("assignment rows=%d want %d", len(assignments), totalProjections)
	}

	expectedSet := make(map[uuid.UUID]struct{}, len(instanceIDs))
	for _, instanceID := range instanceIDs {
		expectedSet[instanceID] = struct{}{}
	}

	assignedCount := 0
	for _, assignment := range assignments {
		if !assignment.Assigned {
			return fmt.Errorf("projection %s is unassigned", assignment.ProjectionName)
		}
		if _, ok := expectedSet[assignment.InstanceID]; !ok {
			return fmt.Errorf("projection %s assigned to unexpected instance %s", assignment.ProjectionName, assignment.InstanceID)
		}
		assignedCount++
	}
	if assignedCount != totalProjections {
		return fmt.Errorf("assigned projections=%d want %d", assignedCount, totalProjections)
	}

	counts := assignedProjectionCounts(assignments)
	if len(counts) != expectedInstances {
		return fmt.Errorf("assigned instances=%d want %d (counts=%v)", len(counts), expectedInstances, counts)
	}

	minPerInstance := totalProjections / expectedInstances
	maxPerInstance := minPerInstance
	if totalProjections%expectedInstances != 0 {
		maxPerInstance++
	}

	totalAssigned := 0
	for _, instanceID := range instanceIDs {
		count := counts[instanceID]
		if count < minPerInstance || count > maxPerInstance {
			return fmt.Errorf(
				"instance %s has %d projections, want %d or %d (counts=%v)",
				instanceID,
				count,
				minPerInstance,
				maxPerInstance,
				counts,
			)
		}
		totalAssigned += count
	}
	if totalAssigned != totalProjections {
		return fmt.Errorf("total assigned=%d want %d", totalAssigned, totalProjections)
	}

	return nil
}

func assertAllEventsProcessedWithoutGaps(t *testing.T, db *pgxpool.Pool, projectionNames []string, expectedPositions []int64, latest int64) {
	t.Helper()

	for _, projectionName := range projectionNames {
		rows := getHandledRows(t, db, projectionName)
		if len(rows) != len(expectedPositions) {
			t.Fatalf("%s handled %d rows, want %d", projectionName, len(rows), len(expectedPositions))
		}

		for index, position := range expectedPositions {
			if rows[index].GlobalPosition != position {
				t.Fatalf(
					"%s row %d has global_position=%d want %d",
					projectionName,
					index,
					rows[index].GlobalPosition,
					position,
				)
			}
		}

		if checkpoint := getCheckpoint(t, db, projectionName); checkpoint != latest {
			t.Fatalf("%s checkpoint=%d want %d", projectionName, checkpoint, latest)
		}
	}
}

func checkProjectionsProcessedStep(t *testing.T, db *pgxpool.Pool, projectionNames []string, expectedCount int, latest int64, cutoff int64, expectedLabels []string) error {
	t.Helper()

	if err := checkProjectionsCaughtUp(t, db, projectionNames, expectedCount, latest); err != nil {
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

func checkProjectionsCaughtUp(t *testing.T, db *pgxpool.Pool, projectionNames []string, expectedCount int, latest int64) error {
	t.Helper()

	for _, projectionName := range projectionNames {
		rows := getHandledRows(t, db, projectionName)
		if len(rows) != expectedCount {
			return fmt.Errorf("%s handled %d rows, want %d", projectionName, len(rows), expectedCount)
		}
		if checkpoint := getCheckpoint(t, db, projectionName); checkpoint != latest {
			return fmt.Errorf("%s checkpoint=%d want %d", projectionName, checkpoint, latest)
		}
	}

	return nil
}

func checkFreshHeartbeats(db *pgxpool.Pool, instanceIDs []uuid.UUID, maxAge time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
SELECT instance_id, heartbeat_at
FROM %s
ORDER BY instance_id ASC
`, projectorpostgres.DefaultProjectorInstancesTable)

	rows, err := db.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query instance heartbeats: %w", err)
	}
	defer rows.Close()

	expectedSet := make(map[uuid.UUID]struct{}, len(instanceIDs))
	for _, instanceID := range instanceIDs {
		expectedSet[instanceID] = struct{}{}
	}

	heartbeats := make(map[uuid.UUID]time.Time, len(instanceIDs))
	for rows.Next() {
		var instanceIDStr string
		var heartbeatAt time.Time
		if err := rows.Scan(&instanceIDStr, &heartbeatAt); err != nil {
			return fmt.Errorf("scan instance heartbeat: %w", err)
		}

		instanceID, err := uuid.Parse(instanceIDStr)
		if err != nil {
			return fmt.Errorf("parse instance id %q: %w", instanceIDStr, err)
		}

		heartbeats[instanceID] = heartbeatAt
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate instance heartbeats: %w", err)
	}

	if len(heartbeats) != len(instanceIDs) {
		return fmt.Errorf("projector_instances rows=%d want %d", len(heartbeats), len(instanceIDs))
	}

	now := time.Now()
	for _, instanceID := range instanceIDs {
		heartbeatAt, ok := heartbeats[instanceID]
		if !ok {
			return fmt.Errorf("instance %s missing from projector_instances", instanceID)
		}
		if age := now.Sub(heartbeatAt); age > maxAge {
			return fmt.Errorf("instance %s heartbeat age=%s exceeds %s", instanceID, age, maxAge)
		}
	}

	for instanceID := range heartbeats {
		if _, ok := expectedSet[instanceID]; !ok {
			return fmt.Errorf("unexpected instance %s present in projector_instances", instanceID)
		}
	}

	return nil
}

func instanceIDsFromHarnesses(projectors []*testProjectorHarness) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(projectors))
	for _, projector := range projectors {
		ids = append(ids, projector.daemon.ID())
	}
	return ids
}

func projectorLabelsFromHarnesses(projectors []*testProjectorHarness) []string {
	labels := make([]string, 0, len(projectors))
	for _, projector := range projectors {
		labels = append(labels, projector.label)
	}
	return labels
}
