package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/projanvil/aoa/internal/aoa"
)

// newClosedStore returns a Store whose database handle has been closed.
// Every method that touches the DB must then return an error.
func newClosedStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return s
}

func mustExec(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.Exec(query, args...); err != nil {
		t.Fatalf("Exec(%q): %v", query, err)
	}
}

func mustErr(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an error, got nil", what)
	}
}

// TestClosedDatabasePropagatesErrors drives every entry point against a
// closed handle so the "first DB operation failed" branches are exercised.
func TestClosedDatabasePropagatesErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	s := newClosedStore(t)

	if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "e", Kind: aoa.KindWebhook}); err == nil {
		t.Error("Enqueue must fail on a closed DB")
	}
	if _, err := s.AcquireLease(ctx, "w", now, time.Minute); err == nil {
		t.Error("AcquireLease must fail on a closed DB")
	}
	mustErr(t, "RenewLease", s.RenewLease(ctx, "w", now))
	mustErr(t, "ReleaseLease", s.ReleaseLease(ctx, "w"))
	if _, err := s.Claim(ctx, now, time.Minute, 1); err == nil {
		t.Error("Claim must fail on a closed DB")
	}
	mustErr(t, "Ack", s.Ack(ctx, "id"))
	mustErr(t, "Fail", s.Fail(ctx, "id", errors.New("boom"), 3))
	mustErr(t, "AddTimer", s.AddTimer(ctx, Timer{DueAt: now}))
	if _, err := s.DueTimers(ctx, now, 1); err == nil {
		t.Error("DueTimers must fail on a closed DB")
	}
	mustErr(t, "MarkTimerFired", s.MarkTimerFired(ctx, Timer{ID: "t", DueAt: now}))
	mustErr(t, "EnqueueOutbox", s.EnqueueOutbox(ctx, "s", aoa.OutboxMessage, "p"))
	if _, err := s.PendingOutbox(ctx, 1); err == nil {
		t.Error("PendingOutbox must fail on a closed DB")
	}
	mustErr(t, "MarkOutboxSent", s.MarkOutboxSent(ctx, "id"))
	mustErr(t, "MarkOutboxFailed", s.MarkOutboxFailed(ctx, "id"))
	mustErr(t, "UpsertApproval", s.UpsertApproval(ctx, Approval{SessionID: "s"}))
	if _, err := s.ListPendingApprovals(ctx); err == nil {
		t.Error("ListPendingApprovals must fail on a closed DB")
	}
	if _, err := s.GetApproval(ctx, "id"); err == nil {
		t.Error("GetApproval must fail on a closed DB")
	}
	if _, err := s.ResolveApproval(ctx, "id", true); err == nil {
		t.Error("ResolveApproval must fail on a closed DB")
	}
	if _, err := s.ResolveApprovalAndEnqueue(ctx, "id", "approve", "[]"); err == nil {
		t.Error("ResolveApprovalAndEnqueue must fail on a closed DB")
	}
	if _, err := s.Prune(ctx, now); err == nil {
		t.Error("Prune must fail on a closed DB")
	}
}

func TestOpenRejectsUnusableDatabaseFiles(t *testing.T) {
	if _, err := Open(t.TempDir()); err == nil {
		t.Fatal("Open on a directory path must fail")
	}
	bad := filepath.Join(t.TempDir(), "garbage.db")
	if err := os.WriteFile(bad, []byte("not a sqlite database"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(bad); err == nil {
		t.Fatal("Open on a non-database file must fail")
	}
}

// TestOpenSurfacesMigrationFailure builds a legacy on-disk database whose
// inbox table has a case-variant LEASE_OWNER column. PRAGMA reports the
// uppercase name, so the migration tries to add a lowercase column and
// SQLite rejects it as a duplicate. Open must close the DB and return error.
func TestOpenSurfacesMigrationFailure(t *testing.T) {
	p := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`CREATE TABLE inbox (
		id TEXT PRIMARY KEY, source_id TEXT, kind TEXT, session_id TEXT,
		payload TEXT, resume TEXT, status TEXT, attempts INTEGER,
		lease_until INTEGER, LEASE_OWNER TEXT, last_error TEXT,
		created_at INTEGER, updated_at INTEGER
	)`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()

	st, err := Open(p)
	if err == nil {
		_ = st.Close()
		t.Fatal("Open must fail when inbox migration cannot complete")
	}
	if st != nil {
		t.Fatal("Open must return a nil Store on migration failure")
	}
}

// TestEnsureInboxColumnsMigratesLegacySchema verifies the success path of
// the three ALTER TABLE migrations for databases created before those
// columns existed.
func TestEnsureInboxColumnsMigratesLegacySchema(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE inbox (
		id TEXT PRIMARY KEY, source_id TEXT, kind TEXT, session_id TEXT,
		payload TEXT, resume TEXT, status TEXT, attempts INTEGER,
		lease_until INTEGER, created_at INTEGER, updated_at INTEGER
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE approvals (
		id TEXT PRIMARY KEY, session_id TEXT, interrupt_id TEXT,
		request TEXT, status TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	if err := ensureInboxColumns(db); err != nil {
		t.Fatalf("ensureInboxColumns: %v", err)
	}
	for _, tc := range []struct{ table, column string }{
		{"inbox", "lease_owner"},
		{"inbox", "last_error"},
		{"approvals", "resolved_at"},
	} {
		table, column := tc.table, tc.column
		rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for rows.Next() {
			var cid, notnull, pk int
			var name, ctype string
			var dflt any
			if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
				t.Fatal(err)
			}
			if name == column {
				found = true
			}
		}
		_ = rows.Close()
		if !found {
			t.Fatalf("column %q was not added to %q", column, table)
		}
	}
}

// TestEnsureInboxColumnsErrors covers the query-error and ALTER-error
// branches of the migration helper.
func TestEnsureInboxColumnsErrors(t *testing.T) {
	t.Run("query fails on closed db", func(t *testing.T) {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		_ = db.Close()
		mustErr(t, "ensureInboxColumns", ensureInboxColumns(db))
	})

	// A case-variant column makes the corresponding ALTER TABLE fail with
	// "duplicate column name", covering each error-return branch.
	legacy := func(ddlInbox, ddlApprovals string) *sql.DB {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(ddlInbox); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ddlApprovals); err != nil {
			t.Fatal(err)
		}
		return db
	}

	t.Run("lease_owner alter fails", func(t *testing.T) {
		db := legacy(
			`CREATE TABLE inbox (id TEXT, status TEXT, created_at INTEGER, LEASE_OWNER TEXT)`,
			`CREATE TABLE approvals (id TEXT, resolved_at INTEGER)`)
		defer db.Close()
		if err := ensureInboxColumns(db); err == nil || !strings.Contains(err.Error(), "lease_owner") {
			t.Fatalf("ensureInboxColumns error = %v", err)
		}
	})

	t.Run("last_error alter fails", func(t *testing.T) {
		// A lowercase lease_owner already exists, so the first ALTER is
		// skipped and the second one (last_error) collides with LAST_ERROR.
		db := legacy(
			`CREATE TABLE inbox (id TEXT, status TEXT, created_at INTEGER, lease_owner TEXT, LAST_ERROR TEXT)`,
			`CREATE TABLE approvals (id TEXT, resolved_at INTEGER)`)
		defer db.Close()
		if err := ensureInboxColumns(db); err == nil || !strings.Contains(err.Error(), "last_error") {
			t.Fatalf("ensureInboxColumns error = %v", err)
		}
	})

	t.Run("resolved_at alter fails", func(t *testing.T) {
		db := legacy(
			`CREATE TABLE inbox (id TEXT, lease_owner TEXT, last_error TEXT)`,
			`CREATE TABLE approvals (id TEXT, RESOLVED_AT INTEGER)`)
		defer db.Close()
		if err := ensureInboxColumns(db); err == nil || !strings.Contains(err.Error(), "resolved_at") {
			t.Fatalf("ensureInboxColumns error = %v", err)
		}
	})
}

func TestEnqueueErrorPaths(t *testing.T) {
	ctx := context.Background()
	t.Run("missing source id", func(t *testing.T) {
		s := testStore(t)
		if _, err := s.Enqueue(ctx, aoa.Inbound{}); err == nil {
			t.Fatal("Enqueue without SourceID must fail")
		}
	})
	t.Run("dedup insert fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE dedup`)
		if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "s1", Kind: aoa.KindWebhook}); err == nil {
			t.Fatal("Enqueue must fail when the dedup table is gone")
		}
	})
	t.Run("inbox insert fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "s1", Kind: aoa.KindWebhook}); err == nil {
			t.Fatal("Enqueue must fail when the inbox table is gone")
		}
	})
}

func TestAcquireAndRenewAndReleaseLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	t.Run("acquire exec fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE leases`)
		if _, err := s.AcquireLease(ctx, "w", now, time.Minute); err == nil {
			t.Fatal("AcquireLease must fail when the leases table is gone")
		}
	})

	t.Run("renew extends and reports lost lease", func(t *testing.T) {
		s := testStore(t)
		ok, err := s.AcquireLease(ctx, "w", now, time.Minute)
		if err != nil || !ok {
			t.Fatalf("AcquireLease = %v, %v", ok, err)
		}
		if err := s.RenewLease(ctx, "w", now.Add(2*time.Minute)); err != nil {
			t.Fatalf("RenewLease: %v", err)
		}
		var until int64
		if err := s.db.QueryRowContext(ctx, `SELECT until_ms FROM leases WHERE name='inbox'`).Scan(&until); err != nil {
			t.Fatal(err)
		}
		if until != now.Add(2*time.Minute).UnixMilli() {
			t.Fatalf("lease until = %d", until)
		}
		if err := s.RenewLease(ctx, "stranger", now); err == nil {
			t.Fatal("RenewLease by a non-owner must report a lost lease")
		}
	})

	t.Run("release frees the slot", func(t *testing.T) {
		s := testStore(t)
		if ok, err := s.AcquireLease(ctx, "w", now, time.Minute); err != nil || !ok {
			t.Fatalf("AcquireLease = %v, %v", ok, err)
		}
		if err := s.ReleaseLease(ctx, "w"); err != nil {
			t.Fatalf("ReleaseLease: %v", err)
		}
		ok, err := s.AcquireLease(ctx, "replacement", now, time.Minute)
		if err != nil || !ok {
			t.Fatalf("AcquireLease after release = %v, %v", ok, err)
		}
	})
}

func TestClaimErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	t.Run("limit below one is normalized", func(t *testing.T) {
		s := testStore(t)
		events, err := s.Claim(ctx, now, time.Minute, 0)
		if err != nil || len(events) != 0 {
			t.Fatalf("Claim = %v, %v", events, err)
		}
	})

	t.Run("reclaim update fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		if _, err := s.Claim(ctx, now, time.Minute, 1); err == nil {
			t.Fatal("Claim must fail when the reclaim UPDATE has no table")
		}
	})

	t.Run("select fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		mustExec(t, s, `CREATE TABLE inbox (id TEXT, status TEXT, lease_until INTEGER, lease_owner TEXT)`)
		if _, err := s.Claim(ctx, now, time.Minute, 1); err == nil {
			t.Fatal("Claim must fail when the pending SELECT references missing columns")
		}
	})

	t.Run("scan fails on null column", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		mustExec(t, s, `CREATE TABLE inbox (
			id TEXT, source_id TEXT, kind TEXT, session_id TEXT, payload TEXT,
			resume TEXT, attempts INTEGER, created_at INTEGER, updated_at INTEGER,
			lease_until INTEGER, lease_owner TEXT, status TEXT, last_error TEXT)`)
		mustExec(t, s, `INSERT INTO inbox (id, status, lease_until, lease_owner, created_at)
			VALUES ('x', 'pending', 0, '', 1)`)
		if _, err := s.Claim(ctx, now, time.Minute, 1); err == nil {
			t.Fatal("Claim must fail when a selected column is NULL")
		}
	})

	t.Run("claim update fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		// updated_at is deliberately missing so the claim UPDATE fails.
		mustExec(t, s, `CREATE TABLE inbox (
			id TEXT, source_id TEXT, kind TEXT, session_id TEXT, payload TEXT,
			resume TEXT, attempts INTEGER, created_at INTEGER,
			lease_until INTEGER, lease_owner TEXT, status TEXT, last_error TEXT)`)
		mustExec(t, s, `INSERT INTO inbox (id, source_id, kind, session_id, payload, resume,
			attempts, created_at, lease_until, lease_owner, status)
			VALUES ('x', 's', 'webhook', 's', 'p', 'r', 0, 1, 0, '', 'pending')`)
		if _, err := s.Claim(ctx, now, time.Minute, 1); err == nil {
			t.Fatal("Claim must fail when the claim UPDATE references a missing column")
		}
	})

	t.Run("claim update affects more than one row", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		mustExec(t, s, `CREATE TABLE inbox (
			id TEXT, source_id TEXT, kind TEXT, session_id TEXT, payload TEXT,
			resume TEXT, attempts INTEGER, created_at INTEGER, updated_at INTEGER,
			lease_until INTEGER, lease_owner TEXT, status TEXT, last_error TEXT)`)
		mustExec(t, s, `INSERT INTO inbox (id, source_id, kind, session_id, payload, resume,
			attempts, created_at, updated_at, lease_until, lease_owner, status)
			VALUES ('dup', 's1', 'webhook', 's', 'p', 'r', 0, 1, 1, 0, '', 'pending'),
			       ('dup', 's2', 'webhook', 's', 'p', 'r', 0, 2, 2, 0, '', 'pending')`)
		_, err := s.Claim(ctx, now, time.Minute, 1)
		if err == nil || !strings.Contains(err.Error(), "lost to another worker") {
			t.Fatalf("Claim error = %v", err)
		}
	})
}

func TestAckFailAndFenceErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	t.Run("ack exec fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		mustErr(t, "Ack", s.Ack(ctx, "id"))
	})

	t.Run("fail exec fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE inbox`)
		mustErr(t, "Fail", s.Fail(ctx, "id", errors.New("boom"), 3))
	})

	t.Run("fail truncates oversized error text", func(t *testing.T) {
		s := testStore(t)
		if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "e", Kind: aoa.KindWebhook}); err != nil {
			t.Fatal(err)
		}
		claimed, err := s.Claim(ctx, now, time.Minute, 1, "w")
		if err != nil || len(claimed) != 1 {
			t.Fatalf("Claim = %v, %v", claimed, err)
		}
		cause := errors.New(strings.Repeat("x", 2000))
		if err := s.Fail(ctx, claimed[0].ID, cause, 3, "w"); err != nil {
			t.Fatalf("Fail: %v", err)
		}
		var lastError string
		if err := s.db.QueryRowContext(ctx, `SELECT last_error FROM inbox WHERE id=?`, claimed[0].ID).
			Scan(&lastError); err != nil {
			t.Fatal(err)
		}
		if len(lastError) != 1024 {
			t.Fatalf("last_error length = %d, want 1024", len(lastError))
		}
	})

	t.Run("fail rejects stale owner", func(t *testing.T) {
		s := testStore(t)
		if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "e", Kind: aoa.KindWebhook}); err != nil {
			t.Fatal(err)
		}
		claimed, err := s.Claim(ctx, now, time.Minute, 1, "w")
		if err != nil || len(claimed) != 1 {
			t.Fatalf("Claim = %v, %v", claimed, err)
		}
		if err := s.Fail(ctx, claimed[0].ID, errors.New("boom"), 3, "other"); !errors.Is(err, ErrNotClaimed) {
			t.Fatalf("stale Fail error = %v", err)
		}
	})
}

func TestAddTimerDefaultsAndErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	t.Run("empty id and kind get defaults", func(t *testing.T) {
		s := testStore(t)
		if err := s.AddTimer(ctx, Timer{DueAt: now}); err != nil {
			t.Fatal(err)
		}
		due, err := s.DueTimers(ctx, now, 10)
		if err != nil || len(due) != 1 {
			t.Fatalf("DueTimers = %v, %v", due, err)
		}
		if due[0].ID == "" || due[0].Kind != aoa.TimerFollowUp {
			t.Fatalf("timer defaults not applied: %+v", due[0])
		}
	})

	t.Run("insert fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE timers`)
		mustErr(t, "AddTimer", s.AddTimer(ctx, Timer{DueAt: now}))
	})

	t.Run("due timers scan fails on null column", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE timers`)
		mustExec(t, s, `CREATE TABLE timers (
			id TEXT, session_id TEXT, kind TEXT, due_at INTEGER,
			interval_sec INTEGER, payload TEXT, status TEXT)`)
		// session_id is NULL so rows.Scan into a string fails.
		mustExec(t, s, `INSERT INTO timers (id, due_at, interval_sec, status)
			VALUES ('t', 1, 0, 'pending')`)
		if _, err := s.DueTimers(ctx, now, 10); err == nil {
			t.Fatal("DueTimers must fail when a selected column is NULL")
		}
	})
}

func TestOutboxErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("enqueue insert fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE outbox`)
		mustErr(t, "EnqueueOutbox", s.EnqueueOutbox(ctx, "s", aoa.OutboxMessage, "p"))
	})

	t.Run("mark sent exec fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE outbox`)
		mustErr(t, "MarkOutboxSent", s.MarkOutboxSent(ctx, "id"))
	})

	t.Run("mark failed increments attempts", func(t *testing.T) {
		s := testStore(t)
		if err := s.EnqueueOutbox(ctx, "s", aoa.OutboxMessage, "p"); err != nil {
			t.Fatal(err)
		}
		pending, err := s.PendingOutbox(ctx, 10)
		if err != nil || len(pending) != 1 {
			t.Fatalf("PendingOutbox = %v, %v", pending, err)
		}
		if err := s.MarkOutboxFailed(ctx, pending[0].ID); err != nil {
			t.Fatalf("MarkOutboxFailed: %v", err)
		}
		after, err := s.PendingOutbox(ctx, 10)
		if err != nil || len(after) != 1 || after[0].Attempts != 1 {
			t.Fatalf("outbox after failure = %v, %v", after, err)
		}
	})

	t.Run("pending scan fails on null column", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE outbox`)
		mustExec(t, s, `CREATE TABLE outbox (
			id TEXT, session_id TEXT, kind TEXT, payload TEXT, attempts INTEGER,
			status TEXT, created_at INTEGER)`)
		mustExec(t, s, `INSERT INTO outbox (id, status, created_at) VALUES ('o', 'pending', 1)`)
		if _, err := s.PendingOutbox(ctx, 10); err == nil {
			t.Fatal("PendingOutbox must fail when a selected column is NULL")
		}
	})
}

func TestApprovalErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("upsert generates an id", func(t *testing.T) {
		s := testStore(t)
		if err := s.UpsertApproval(ctx, Approval{SessionID: "s", Request: "rollback?"}); err != nil {
			t.Fatal(err)
		}
		list, err := s.ListPendingApprovals(ctx)
		if err != nil || len(list) != 1 || list[0].ID == "" {
			t.Fatalf("ListPendingApprovals = %v, %v", list, err)
		}
	})

	t.Run("upsert exec fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE approvals`)
		mustErr(t, "UpsertApproval", s.UpsertApproval(ctx, Approval{SessionID: "s"}))
	})

	t.Run("list scan fails on null column", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE approvals`)
		mustExec(t, s, `CREATE TABLE approvals (
			id TEXT, session_id TEXT, interrupt_id TEXT, request TEXT,
			status TEXT, resolved_at INTEGER)`)
		mustExec(t, s, `INSERT INTO approvals (id, status) VALUES ('a', 'pending')`)
		if _, err := s.ListPendingApprovals(ctx); err == nil {
			t.Fatal("ListPendingApprovals must fail when a selected column is NULL")
		}
	})

	t.Run("get approval not found", func(t *testing.T) {
		s := testStore(t)
		if _, err := s.GetApproval(ctx, "missing"); err == nil {
			t.Fatal("GetApproval on a missing id must fail")
		}
	})

	t.Run("resolve exec fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE approvals`)
		if _, err := s.ResolveApproval(ctx, "a", true); err == nil {
			t.Fatal("ResolveApproval must fail when the approvals table is gone")
		}
	})
}

func TestResolveApprovalAndEnqueueErrorPaths(t *testing.T) {
	ctx := context.Background()

	t.Run("read approval fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE approvals`)
		if _, err := s.ResolveApprovalAndEnqueue(ctx, "a", "approve", "[]"); err == nil {
			t.Fatal("ResolveApprovalAndEnqueue must fail when approvals is gone")
		}
	})

	t.Run("dedup insert fails", func(t *testing.T) {
		s := testStore(t)
		if err := s.UpsertApproval(ctx, Approval{ID: "a", SessionID: "s"}); err != nil {
			t.Fatal(err)
		}
		mustExec(t, s, `DROP TABLE dedup`)
		if _, err := s.ResolveApprovalAndEnqueue(ctx, "a", "approve", "[]"); err == nil {
			t.Fatal("ResolveApprovalAndEnqueue must fail when dedup is gone")
		}
	})

	t.Run("reply insert fails", func(t *testing.T) {
		s := testStore(t)
		if err := s.UpsertApproval(ctx, Approval{ID: "a", SessionID: "s"}); err != nil {
			t.Fatal(err)
		}
		mustExec(t, s, `DROP TABLE inbox`)
		if _, err := s.ResolveApprovalAndEnqueue(ctx, "a", "approve", "[]"); err == nil {
			t.Fatal("ResolveApprovalAndEnqueue must fail when inbox is gone")
		}
	})

	t.Run("approval update fails", func(t *testing.T) {
		s := testStore(t)
		mustExec(t, s, `DROP TABLE approvals`)
		// No resolved_at column, so the final UPDATE must fail.
		mustExec(t, s, `CREATE TABLE approvals (
			id TEXT PRIMARY KEY, session_id TEXT NOT NULL UNIQUE,
			interrupt_id TEXT NOT NULL DEFAULT '', request TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'pending')`)
		if err := s.UpsertApproval(ctx, Approval{ID: "a", SessionID: "s"}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ResolveApprovalAndEnqueue(ctx, "a", "approve", "[]"); err == nil {
			t.Fatal("ResolveApprovalAndEnqueue must fail when the approval UPDATE has no resolved_at")
		}
	})
}

func TestPruneExecError(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	mustExec(t, s, `DROP TABLE inbox`)
	if _, err := s.Prune(ctx, time.Now()); err == nil {
		t.Fatal("Prune must fail when a delete statement has no table")
	}
}
