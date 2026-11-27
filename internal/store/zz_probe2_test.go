package store

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestTLOpenRejectsViewsAsTables verifies that an incompatible on-disk
// schema surfaces an error instead of being silently migrated.
func TestTLOpenRejectsViewsAsTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "views.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	statements := []string{
		`CREATE TABLE dummy (a INTEGER, b INTEGER)`,
		`CREATE INDEX inbox_pending ON dummy(a, b)`,
		`CREATE INDEX timers_due ON dummy(a, b)`,
		`CREATE INDEX outbox_pending ON dummy(a, b)`,
		`CREATE VIEW inbox AS SELECT 'x' AS id, 's' AS source_id, 'webhook' AS kind, ` +
			`'' AS session_id, '' AS payload, '' AS resume, 0 AS attempts, ` +
			`0 AS lease_until, 0 AS created_at, 0 AS updated_at, 'pending' AS status`,
		`CREATE VIEW approvals AS SELECT 'a' AS id, 's' AS session_id, ` +
			`'' AS interrupt_id, '' AS request, 'pending' AS status`,
	}
	for _, query := range statements {
		if _, err := raw.Exec(query); err != nil {
			t.Fatalf("setup %q: %v", query, err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(path)
	if st != nil {
		_ = st.Close()
	}
	if err == nil {
		t.Fatal("Open must reject a schema whose inbox is a view")
	}
}
