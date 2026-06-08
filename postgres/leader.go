package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaderLockKey is the well-known advisory lock key used for leader advisory lock election.
const LeaderLockKey int64 = 7237492748123847

// TryAcquireLeaderLock attempts to acquire the advisory lock (non-blocking) on a dedicated connection.
// It returns true when the connection now holds the lock.
func TryAcquireLeaderLock(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", LeaderLockKey).Scan(&acquired); err != nil {
		return false, fmt.Errorf("acquire leader advisory lock: %w", err)
	}

	return acquired, nil
}

// ReleaseLeaderLock releases the advisory lock on a dedicated connection.
func ReleaseLeaderLock(ctx context.Context, conn *pgxpool.Conn) error {
	var released bool
	if err := conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", LeaderLockKey).Scan(&released); err != nil {
		return fmt.Errorf("release leader advisory lock: %w", err)
	}
	if !released {
		return fmt.Errorf("release leader advisory lock: lock not held")
	}

	return nil
}
