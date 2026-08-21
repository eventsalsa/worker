package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Config configures projector migration generation.
type Config struct {
	// OutputFolder is the directory where the migration file will be written.
	OutputFolder string

	// OutputFilename is the name of the migration file.
	OutputFilename string

	// ProjectorInstancesTable is the name of the projector instance registration table.
	ProjectorInstancesTable string

	// ProjectionAssignmentsTable is the name of the projection assignment table.
	ProjectionAssignmentsTable string

	// ProjectionCheckpointsTable is the name of the projection checkpoint table.
	ProjectionCheckpointsTable string

	// ProjectionGapSkipsTable is the name of the projection gap skip audit table.
	ProjectionGapSkipsTable string

	// ProjectorLeaderLeasesTable is the name of the leader election lease table.
	ProjectorLeaderLeasesTable string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	timestamp := time.Now().Format("20060102150405")

	return Config{
		OutputFolder:               "migrations",
		OutputFilename:             fmt.Sprintf("%s_init_projector_infrastructure.sql", timestamp),
		ProjectorInstancesTable:    "projector_instances",
		ProjectionAssignmentsTable: "projection_assignments",
		ProjectionCheckpointsTable: "projection_checkpoints",
		ProjectionGapSkipsTable:    "projection_gap_skips",
		ProjectorLeaderLeasesTable: "projector_leader_leases",
	}
}

// GeneratePostgres generates a PostgreSQL migration file.
func GeneratePostgres(config *Config) error {
	normalized := normalizeConfig(config)

	if err := os.MkdirAll(normalized.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generatePostgresSQL(&normalized)

	outputPath := filepath.Join(normalized.OutputFolder, normalized.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generatePostgresSQL(config *Config) string {
	normalized := normalizeConfig(config)

	schemaDDL := schemaStatements(
		normalized.ProjectorInstancesTable,
		normalized.ProjectionAssignmentsTable,
		normalized.ProjectionCheckpointsTable,
		normalized.ProjectionGapSkipsTable,
		normalized.ProjectorLeaderLeasesTable,
	)

	return fmt.Sprintf(`-- Projector Infrastructure Migration
-- Generated: %s
--
-- These tables coordinate distributed projection execution:
-- - %s tracks projector instances and heartbeats
-- - %s maps projections to projector instances
-- - %s stores each projection's last processed global position
-- - %s stores durable stale-gap advancement records
-- - %s manages active leader leases
%s
CREATE TABLE IF NOT EXISTS %s (
    instance_id UUID PRIMARY KEY,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_%s_heartbeat
    ON %s (heartbeat_at);

CREATE TABLE IF NOT EXISTS %s (
    projection_name TEXT PRIMARY KEY,
    instance_id UUID REFERENCES %s(instance_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_%s_instance
    ON %s (instance_id);

CREATE TABLE IF NOT EXISTS %s (
    projection_name TEXT PRIMARY KEY,
    last_position BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS %s (
    id BIGSERIAL PRIMARY KEY,
    projection_name TEXT NOT NULL,
    instance_id UUID NOT NULL,
    gap_position BIGINT NOT NULL,
    skip_to_position BIGINT NOT NULL,
    highest_visible_position BIGINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_%s_projection
    ON %s (projection_name, created_at DESC);

CREATE TABLE IF NOT EXISTS %s (
    lease_key TEXT PRIMARY KEY,
    leader_id UUID REFERENCES %s(instance_id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`,
		time.Now().Format(time.RFC3339),
		normalized.ProjectorInstancesTable,
		normalized.ProjectionAssignmentsTable,
		normalized.ProjectionCheckpointsTable,
		normalized.ProjectionGapSkipsTable,
		normalized.ProjectorLeaderLeasesTable,
		schemaDDL,
		normalized.ProjectorInstancesTable,
		indexNameComponent(normalized.ProjectorInstancesTable), normalized.ProjectorInstancesTable,
		normalized.ProjectionAssignmentsTable,
		normalized.ProjectorInstancesTable,
		indexNameComponent(normalized.ProjectionAssignmentsTable), normalized.ProjectionAssignmentsTable,
		normalized.ProjectionCheckpointsTable,
		normalized.ProjectionGapSkipsTable,
		indexNameComponent(normalized.ProjectionGapSkipsTable), normalized.ProjectionGapSkipsTable,
		normalized.ProjectorLeaderLeasesTable,
		normalized.ProjectorInstancesTable,
	)
}

func normalizeConfig(config *Config) Config {
	normalized := DefaultConfig()
	if config == nil {
		return normalized
	}

	if config.OutputFolder != "" {
		normalized.OutputFolder = config.OutputFolder
	}
	if config.OutputFilename != "" {
		normalized.OutputFilename = config.OutputFilename
	}
	if config.ProjectorInstancesTable != "" {
		normalized.ProjectorInstancesTable = config.ProjectorInstancesTable
	}
	if config.ProjectionAssignmentsTable != "" {
		normalized.ProjectionAssignmentsTable = config.ProjectionAssignmentsTable
	}
	if config.ProjectionCheckpointsTable != "" {
		normalized.ProjectionCheckpointsTable = config.ProjectionCheckpointsTable
	}
	if config.ProjectionGapSkipsTable != "" {
		normalized.ProjectionGapSkipsTable = config.ProjectionGapSkipsTable
	}
	if config.ProjectorLeaderLeasesTable != "" {
		normalized.ProjectorLeaderLeasesTable = config.ProjectorLeaderLeasesTable
	}

	return normalized
}

func schemaStatements(tableNames ...string) string {
	seen := make(map[string]struct{})
	var statements []string

	for _, tableName := range tableNames {
		schemaName := schemaName(tableName)
		if schemaName == "" {
			continue
		}
		if _, ok := seen[schemaName]; ok {
			continue
		}

		seen[schemaName] = struct{}{}
		statements = append(statements, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n", schemaName))
	}

	if len(statements) == 0 {
		return ""
	}

	return strings.Join(statements, "")
}

func schemaName(tableName string) string {
	if idx := strings.LastIndex(tableName, "."); idx >= 0 {
		return tableName[:idx]
	}

	return ""
}

func indexNameComponent(tableName string) string {
	base := tableName
	if idx := strings.LastIndex(base, "."); idx >= 0 {
		base = base[idx+1:]
	}

	base = strings.Trim(base, `"`)
	base = strings.ReplaceAll(base, "-", "_")
	base = strings.ReplaceAll(base, " ", "_")

	return base
}
