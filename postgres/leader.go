package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// LeaderLockKey is the well-known advisory lock key used for leader election.
const LeaderLockKey int64 = 7237492748123847

// TryAcquireLeaderLock attempts to acquire the advisory lock (non-blocking).
// It returns true when the current database session now holds the lock.
func TryAcquireLeaderLock(ctx context.Context, db *sql.DB) (bool, error) {
	var acquired bool
	if err := db.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", LeaderLockKey).Scan(&acquired); err != nil {
		return false, fmt.Errorf("acquire leader advisory lock: %w", err)
	}

	return acquired, nil
}

// ReleaseLeaderLock releases the advisory lock.
func ReleaseLeaderLock(ctx context.Context, db *sql.DB) error {
	var released bool
	if err := db.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", LeaderLockKey).Scan(&released); err != nil {
		return fmt.Errorf("release leader advisory lock: %w", err)
	}
	if !released {
		return fmt.Errorf("release leader advisory lock: lock not held")
	}

	return nil
}
