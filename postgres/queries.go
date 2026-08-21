package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB abstracts pgxpool.Pool and pgxpool.Conn for executing queries.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Default table names for projector infrastructure metadata.
const (
	DefaultProjectorInstancesTable    = "projector_instances"
	DefaultProjectionAssignmentsTable = "projection_assignments"
	DefaultProjectionCheckpointsTable = "projection_checkpoints"
	DefaultProjectionGapSkipsTable    = "projection_gap_skips"
	DefaultProjectorLeaderLeasesTable = "projector_leader_leases"
)

func resolveTableName(tableName, defaultTableName string) string {
	if strings.TrimSpace(tableName) == "" {
		return defaultTableName
	}

	return tableName
}
