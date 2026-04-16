package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type registrationTestDB struct {
	args   []interface{}
	query  string
	result sql.Result
	err    error
}

func (db *registrationTestDB) ExecContext(_ context.Context, query string, args ...interface{}) (sql.Result, error) {
	db.query = query
	db.args = append([]interface{}(nil), args...)
	if db.err != nil {
		return nil, db.err
	}

	return db.result, nil
}

func (*registrationTestDB) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	panic("unexpected QueryContext call")
}

func (*registrationTestDB) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	panic("unexpected QueryRowContext call")
}

type rowsAffectedResult int64

func (r rowsAffectedResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r rowsAffectedResult) RowsAffected() (int64, error) {
	return int64(r), nil
}

func TestCleanupStaleWorkersReturnsDeletedCountAndUsesConfiguredTable(t *testing.T) {
	db := &registrationTestDB{result: rowsAffectedResult(3)}
	olderThan := 2 * time.Minute

	deleted, err := CleanupStaleWorkers(context.Background(), db, "custom_worker_nodes", olderThan)
	if err != nil {
		t.Fatalf("CleanupStaleWorkers() error = %v", err)
	}
	if deleted != 3 {
		t.Fatalf("CleanupStaleWorkers() deleted = %d, want 3", deleted)
	}
	if !strings.Contains(db.query, "DELETE FROM custom_worker_nodes") {
		t.Fatalf("query = %q, want custom worker_nodes table", db.query)
	}
	if len(db.args) != 1 || db.args[0] != olderThan.Microseconds() {
		t.Fatalf("args = %v, want [%d]", db.args, olderThan.Microseconds())
	}
}

func TestUpdateHeartbeatReturnsErrWorkerRegistrationMissingWhenNoRowsAreAffected(t *testing.T) {
	db := &registrationTestDB{result: rowsAffectedResult(0)}
	workerID := uuid.New()

	err := UpdateHeartbeat(context.Background(), db, "", workerID)
	if !strings.Contains(db.query, "UPDATE worker_nodes") {
		t.Fatalf("query = %q, want worker_nodes heartbeat update", db.query)
	}
	if len(db.args) != 1 || db.args[0] != workerID {
		t.Fatalf("args = %v, want [%s]", db.args, workerID)
	}
	if err == nil {
		t.Fatal("UpdateHeartbeat() error = nil, want ErrWorkerRegistrationMissing")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("update heartbeat for worker %s", workerID)) {
		t.Fatalf("UpdateHeartbeat() error = %v, want wrapped worker id", err)
	}
	if !strings.Contains(err.Error(), ErrWorkerRegistrationMissing.Error()) {
		t.Fatalf("UpdateHeartbeat() error = %v, want ErrWorkerRegistrationMissing", err)
	}
}
