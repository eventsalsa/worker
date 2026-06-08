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

// Default table names for worker infrastructure metadata.
const (
	DefaultWorkerNodesTable         = "worker_nodes"
	DefaultConsumerAssignmentsTable = "consumer_assignments"
	DefaultConsumerCheckpointsTable = "consumer_checkpoints"
	DefaultConsumerGapSkipsTable    = "consumer_gap_skips"
	DefaultLeaderElectionTable      = "worker_leader_election"
)

func resolveTableName(tableName, defaultTableName string) string {
	if strings.TrimSpace(tableName) == "" {
		return defaultTableName
	}

	return tableName
}
