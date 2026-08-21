package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRunGeneratesMigrationWithDefaults(t *testing.T) {
	outputDir := t.TempDir()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-output", outputDir}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("run exit code = %d, want 0; stderr=%q", exitCode, stderr.String())
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("generated files = %d, want 1", len(entries))
	}

	outputFilename := entries[0].Name()
	if !regexp.MustCompile(`^\d{14}_init_projector_infrastructure\.sql$`).MatchString(outputFilename) {
		t.Fatalf("output filename = %q, want timestamped projector migration filename", outputFilename)
	}

	content, err := os.ReadFile(filepath.Join(outputDir, outputFilename))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	sql := string(content)
	requiredStrings := []string{
		"CREATE TABLE IF NOT EXISTS projector_instances",
		"CREATE TABLE IF NOT EXISTS projection_assignments",
		"CREATE TABLE IF NOT EXISTS projection_checkpoints",
		"CREATE TABLE IF NOT EXISTS projection_gap_skips",
		"CREATE TABLE IF NOT EXISTS projector_leader_leases",
	}
	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Fatalf("generated SQL missing %q", required)
		}
	}

	expectedOutput := "Generated PostgreSQL migration: " + filepath.Join(outputDir, outputFilename) + "\n"
	if stdout.String() != expectedOutput {
		t.Fatalf("stdout = %q, want %q", stdout.String(), expectedOutput)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunGeneratesMigrationWithOverrides(t *testing.T) {
	outputDir := t.TempDir()
	outputFilename := "002_projector_tables.sql"
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{
		"-output", outputDir,
		"-filename", outputFilename,
		"-projector-instances-table", "infra.projector_instances",
		"-projection-assignments-table", "infra.projection_assignments",
		"-projection-checkpoints-table", "infra.projection_checkpoints",
		"-projection-gap-skips-table", "infra.projection_gap_skips",
		"-projector-leader-leases-table", "infra.projector_leader_leases",
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("run exit code = %d, want 0; stderr=%q", exitCode, stderr.String())
	}

	content, err := os.ReadFile(filepath.Join(outputDir, outputFilename))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	sql := string(content)
	requiredStrings := []string{
		"CREATE SCHEMA IF NOT EXISTS infra;",
		"CREATE TABLE IF NOT EXISTS infra.projector_instances",
		"CREATE TABLE IF NOT EXISTS infra.projection_assignments",
		"CREATE TABLE IF NOT EXISTS infra.projection_checkpoints",
		"CREATE TABLE IF NOT EXISTS infra.projection_gap_skips",
		"CREATE TABLE IF NOT EXISTS infra.projector_leader_leases",
	}
	for _, required := range requiredStrings {
		if !strings.Contains(sql, required) {
			t.Fatalf("generated SQL missing %q", required)
		}
	}

	expectedOutput := "Generated PostgreSQL migration: " + filepath.Join(outputDir, outputFilename) + "\n"
	if stdout.String() != expectedOutput {
		t.Fatalf("stdout = %q, want %q", stdout.String(), expectedOutput)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunReturnsParseError(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run([]string{"-does-not-exist"}, &stdout, &stderr)
	if exitCode != 2 {
		t.Fatalf("run exit code = %d, want 2", exitCode)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("stderr = %q, want parse error", stderr.String())
	}
}
