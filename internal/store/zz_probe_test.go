package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestProbe(t *testing.T) {
	// 1. sql.Open with various malformed DSNs.
	for _, dsn := range []string{":::", "file:x?mode=bogus", "\x00bad", "/definitely/not/a/dir/x.db"} {
		db, err := sql.Open("sqlite", dsn)
		t.Logf("sql.Open(%q) err=%v dbNil=%v", dsn, err, db == nil)
		if db != nil {
			_ = db.Close()
		}
	}

	// 2. Open on a directory path.
	dir := t.TempDir()
	_, err := Open(dir)
	t.Logf("Open(dir) err=%v", err)

	// 3. CREATE TABLE IF NOT EXISTS when a view of the same name exists.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE VIEW inbox AS SELECT 1 AS id, 2 AS status, 3 AS created_at`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS inbox (id TEXT)`); err != nil {
		t.Logf("CREATE TABLE IF NOT EXISTS over view err=%v", err)
	} else {
		t.Log("CREATE TABLE IF NOT EXISTS over view: ok (no-op)")
	}
	rows, err := db.Query(`PRAGMA table_info(inbox)`)
	if err != nil {
		t.Fatal(err)
	}
	cols := []string{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		cols = append(cols, name)
	}
	rows.Close()
	t.Logf("PRAGMA table_info(view) cols=%v", cols)
	if _, err := db.Exec(`ALTER TABLE inbox ADD COLUMN lease_owner TEXT NOT NULL DEFAULT ''`); err != nil {
		t.Logf("ALTER TABLE view ADD COLUMN err=%v", err)
	} else {
		t.Log("ALTER TABLE view ADD COLUMN: unexpectedly ok")
	}
	_ = db.Close()

	// 4. Full Open with a pre-existing approvals view (no resolved_at).
	p := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`CREATE VIEW approvals AS SELECT 'a' AS id, 's' AS session_id, '' AS interrupt_id, '' AS request, 'pending' AS status`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE VIEW inbox AS SELECT 'x' AS id, 'pending' AS status, 0 AS lease_until, '' AS lease_owner, 0 AS created_at`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	st, err := Open(p)
	t.Logf("Open(preexisting-views) err=%v stNil=%v", err, st == nil)
	if st != nil {
		_ = st.Close()
	}

	// 5. ALTER TABLE on view for approvals (used by ensureInboxColumns).
	db2, _ := sql.Open("sqlite", ":memory:")
	db2.SetMaxOpenConns(1)
	_, _ = db2.Exec(`CREATE TABLE inbox (id TEXT, lease_owner TEXT, last_error TEXT)`)
	_, _ = db2.Exec(`CREATE VIEW approvals AS SELECT 'a' AS id, 'pending' AS status`)
	err = ensureInboxColumns(db2)
	t.Logf("ensureInboxColumns(approvals view) err=%v", err)
	_ = db2.Close()

	// 6. Write garbage file then Open.
	bad := filepath.Join(t.TempDir(), "bad.db")
	if err := os.WriteFile(bad, []byte("not a sqlite database"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = Open(bad)
	t.Logf("Open(garbage file) err=%v", err)

	fmt.Println("probe done")
}
