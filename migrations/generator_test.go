package migrations

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.OutputFolder != "migrations" {
		t.Fatalf("OutputFolder = %q, want %q", config.OutputFolder, "migrations")
	}

	outputFilenamePattern := regexp.MustCompile(`^\d{14}_init_projector_infrastructure\.sql$`)
	if !outputFilenamePattern.MatchString(config.OutputFilename) {
		t.Fatalf("OutputFilename = %q, want timestamped projector migration filename", config.OutputFilename)
	}

	if config.ProjectorInstancesTable != "projector_instances" {
		t.Fatalf("ProjectorInstancesTable = %q, want %q", config.ProjectorInstancesTable, "projector_instances")
	}

	if config.ProjectionAssignmentsTable != "projection_assignments" {
		t.Fatalf("ProjectionAssignmentsTable = %q, want %q", config.ProjectionAssignmentsTable, "projection_assignments")
	}

	if config.ProjectionCheckpointsTable != "projection_checkpoints" {
		t.Fatalf("ProjectionCheckpointsTable = %q, want %q", config.ProjectionCheckpointsTable, "projection_checkpoints")
	}
	if config.ProjectionGapSkipsTable != "projection_gap_skips" {
		t.Fatalf("ProjectionGapSkipsTable = %q, want %q", config.ProjectionGapSkipsTable, "projection_gap_skips")
	}
	if config.ProjectorLeaderLeasesTable != "projector_leader_leases" {
		t.Fatalf("ProjectorLeaderLeasesTable = %q, want %q", config.ProjectorLeaderLeasesTable, "projector_leader_leases")
	}
}

func TestGeneratePostgresSQL(t *testing.T) {
	config := Config{
		OutputFolder:               t.TempDir(),
		OutputFilename:             "projector_migration.sql",
		ProjectorInstancesTable:    "projector_instances",
		ProjectionAssignmentsTable: "projection_assignments",
		ProjectionCheckpointsTable: "projection_checkpoints",
		ProjectionGapSkipsTable:    "projection_gap_skips",
		ProjectorLeaderLeasesTable: "projector_leader_leases",
	}

	sql := generatePostgresSQL(&config)

	requiredStrings := []string{
		"-- Projector Infrastructure Migration",
		"CREATE TABLE IF NOT EXISTS projector_instances",
		"CREATE TABLE IF NOT EXISTS projection_assignments",
		"CREATE TABLE IF NOT EXISTS projection_checkpoints",
		"CREATE TABLE IF NOT EXISTS projection_gap_skips",
		"CREATE TABLE IF NOT EXISTS projector_leader_leases",
		"instance_id UUID PRIMARY KEY",
		"heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"instance_id UUID REFERENCES projector_instances(instance_id) ON DELETE SET NULL",
		"leader_id UUID REFERENCES projector_instances(instance_id) ON DELETE CASCADE",
		"last_position BIGINT NOT NULL DEFAULT 0",
		"skip_to_position BIGINT NOT NULL",
		"updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing required string: %s", required)
		}
	}

	requiredIndexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_projector_instances_heartbeat",
		"ON projector_instances (heartbeat_at)",
		"CREATE INDEX IF NOT EXISTS idx_projection_assignments_instance",
		"ON projection_assignments (instance_id)",
		"CREATE INDEX IF NOT EXISTS idx_projection_gap_skips_projection",
		"ON projection_gap_skips (projection_name, created_at DESC)",
	}

	for _, required := range requiredIndexes {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing required index definition: %s", required)
		}
	}
}

func TestGeneratePostgresSQL_CustomTableNames(t *testing.T) {
	config := Config{
		OutputFolder:               t.TempDir(),
		OutputFilename:             "custom_projector_migration.sql",
		ProjectorInstancesTable:    "custom_projector_instances",
		ProjectionAssignmentsTable: "custom_projection_assignments",
		ProjectionCheckpointsTable: "custom_projection_checkpoints",
		ProjectionGapSkipsTable:    "custom_projection_gap_skips",
		ProjectorLeaderLeasesTable: "custom_projector_leases",
	}

	sql := generatePostgresSQL(&config)

	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS custom_projector_instances",
		"CREATE TABLE IF NOT EXISTS custom_projection_assignments",
		"CREATE TABLE IF NOT EXISTS custom_projection_checkpoints",
		"CREATE TABLE IF NOT EXISTS custom_projection_gap_skips",
		"CREATE TABLE IF NOT EXISTS custom_projector_leases",
		"instance_id UUID REFERENCES custom_projector_instances(instance_id) ON DELETE SET NULL",
		"leader_id UUID REFERENCES custom_projector_instances(instance_id) ON DELETE CASCADE",
		"CREATE INDEX IF NOT EXISTS idx_custom_projector_instances_heartbeat",
		"CREATE INDEX IF NOT EXISTS idx_custom_projection_assignments_instance",
		"CREATE INDEX IF NOT EXISTS idx_custom_projection_gap_skips_projection",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing custom configuration string: %s", required)
		}
	}
}

func TestGeneratePostgresSQL_SchemaQualifiedTableNames(t *testing.T) {
	config := Config{
		OutputFolder:               t.TempDir(),
		OutputFilename:             "schema_projector_migration.sql",
		ProjectorInstancesTable:    "infra.projector_instances",
		ProjectionAssignmentsTable: "infra.projection_assignments",
		ProjectionCheckpointsTable: "infra.projection_checkpoints",
		ProjectionGapSkipsTable:    "infra.projection_gap_skips",
		ProjectorLeaderLeasesTable: "infra.projector_leader_leases",
	}

	sql := generatePostgresSQL(&config)

	requiredStrings := []string{
		"CREATE SCHEMA IF NOT EXISTS infra;",
		"CREATE TABLE IF NOT EXISTS infra.projector_instances",
		"CREATE INDEX IF NOT EXISTS idx_projector_instances_heartbeat",
		"ON infra.projector_instances (heartbeat_at)",
		"CREATE INDEX IF NOT EXISTS idx_projection_assignments_instance",
		"ON infra.projection_assignments (instance_id)",
		"CREATE INDEX IF NOT EXISTS idx_projection_gap_skips_projection",
		"ON infra.projection_gap_skips (projection_name, created_at DESC)",
	}

	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Errorf("Generated SQL missing schema-qualified string: %s", required)
		}
	}
}

func TestGeneratePostgres_WritesFile(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		OutputFolder:               tmpDir,
		OutputFilename:             "test_projector_migration.sql",
		ProjectorInstancesTable:    "projector_instances",
		ProjectionAssignmentsTable: "projection_assignments",
		ProjectionCheckpointsTable: "projection_checkpoints",
		ProjectionGapSkipsTable:    "projection_gap_skips",
		ProjectorLeaderLeasesTable: "projector_leader_leases",
	}

	if err := GeneratePostgres(&config); err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	outputPath := filepath.Join(tmpDir, config.OutputFilename)
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read generated file: %v", err)
	}

	sql := string(content)
	if sql != generatePostgresSQL(&config) {
		t.Fatal("generated file contents do not match generated SQL")
	}
}
