package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrWorkerRegistrationMissing indicates that a heartbeat update targeted a
// worker row that no longer exists.
var ErrWorkerRegistrationMissing = errors.New("worker registration missing")

// RegisterWorker inserts a new worker node with the given UUID.
func RegisterWorker(ctx context.Context, db DBTX, table string, workerID uuid.UUID) error {
	table = resolveTableName(table, DefaultWorkerNodesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (worker_id, heartbeat_at, created_at, updated_at)
		VALUES ($1, NOW(), NOW(), NOW())
	`, table)

	if _, err := db.ExecContext(ctx, query, workerID); err != nil {
		return fmt.Errorf("register worker %s: %w", workerID, err)
	}

	return nil
}

// CleanupStaleWorkers deletes worker rows older than the given threshold.
func CleanupStaleWorkers(ctx context.Context, db DBTX, table string, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		return 0, fmt.Errorf("cleanup stale workers: olderThan must be positive")
	}

	table = resolveTableName(table, DefaultWorkerNodesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE heartbeat_at < NOW() - ($1 * INTERVAL '1 microsecond')
	`, table)

	result, err := db.ExecContext(ctx, query, olderThan.Microseconds())
	if err != nil {
		return 0, fmt.Errorf("cleanup stale workers older than %s: %w", olderThan, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected cleaning stale workers older than %s: %w", olderThan, err)
	}

	return affected, nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a worker.
func UpdateHeartbeat(ctx context.Context, db DBTX, table string, workerID uuid.UUID) error {
	table = resolveTableName(table, DefaultWorkerNodesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		UPDATE %s
		SET heartbeat_at = NOW(), updated_at = NOW()
		WHERE worker_id = $1
	`, table)

	result, err := db.ExecContext(ctx, query, workerID)
	if err != nil {
		return fmt.Errorf("update heartbeat for worker %s: %w", workerID, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected updating heartbeat for worker %s: %w", workerID, err)
	}
	if affected == 0 {
		return fmt.Errorf("update heartbeat for worker %s: %w", workerID, ErrWorkerRegistrationMissing)
	}

	return nil
}

// ListLiveWorkers returns worker IDs whose heartbeat is within the timeout threshold.
func ListLiveWorkers(ctx context.Context, db DBTX, table string, timeout time.Duration) ([]uuid.UUID, error) {
	table = resolveTableName(table, DefaultWorkerNodesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT worker_id
		FROM %s
		WHERE heartbeat_at >= NOW() - ($1 * INTERVAL '1 microsecond')
		ORDER BY worker_id ASC
	`, table)

	rows, err := db.QueryContext(ctx, query, timeout.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("list live workers: %w", err)
	}
	defer rows.Close()

	var workerIDs []uuid.UUID
	for rows.Next() {
		var workerID uuid.UUID
		if err := rows.Scan(&workerID); err != nil {
			return nil, fmt.Errorf("scan live worker: %w", err)
		}
		workerIDs = append(workerIDs, workerID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live workers: %w", err)
	}

	return workerIDs, nil
}

// RemoveWorker deletes a worker node row.
func RemoveWorker(ctx context.Context, db DBTX, table string, workerID uuid.UUID) error {
	table = resolveTableName(table, DefaultWorkerNodesTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE worker_id = $1
	`, table)

	if _, err := db.ExecContext(ctx, query, workerID); err != nil {
		return fmt.Errorf("remove worker %s: %w", workerID, err)
	}

	return nil
}
