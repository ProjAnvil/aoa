package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/projanvil/aoa/internal/aoa"
)

func tlClosedStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return s
}

func tlExpectError(t *testing.T, label string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s must fail on a closed database", label)
	}
}

func TestTLClosedDatabaseErrorPaths(t *testing.T) {
	ctx := context.Background()
	s := tlClosedStore(t)
	now := time.Unix(1_700_000_000, 0)

	tlExpectError(t, "Enqueue", func() error { _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "x", Kind: aoa.KindWebhook}); return err }())
	tlExpectError(t, "AcquireLease", func() error { _, err := s.AcquireLease(ctx, "w", now, time.Minute); return err }())
	tlExpectError(t, "RenewLease", s.RenewLease(ctx, "w", now))
	tlExpectError(t, "ReleaseLease", s.ReleaseLease(ctx, "w"))
	tlExpectError(t, "Claim", func() error { _, err := s.Claim(ctx, now, time.Minute, 1, "w"); return err }())
	tlExpectError(t, "Ack", s.Ack(ctx, "id", "w"))
	tlExpectError(t, "Fail", s.Fail(ctx, "id", errors.New("x"), 3, "w"))
	tlExpectError(t, "AddTimer", s.AddTimer(ctx, Timer{ID: "t", SessionID: "s", Kind: aoa.TimerFollowUp, DueAt: now}))
	tlExpectError(t, "DueTimers", func() error { _, err := s.DueTimers(ctx, now, 1); return err }())
	tlExpectError(t, "MarkTimerFired", s.MarkTimerFired(ctx, Timer{ID: "t", Kind: aoa.TimerFollowUp}))
	tlExpectError(t, "EnqueueOutbox", s.EnqueueOutbox(ctx, "s", aoa.OutboxMessage, "x"))
	tlExpectError(t, "PendingOutbox", func() error { _, err := s.PendingOutbox(ctx, 1); return err }())
	tlExpectError(t, "MarkOutboxSent", s.MarkOutboxSent(ctx, "id"))
	tlExpectError(t, "MarkOutboxFailed", s.MarkOutboxFailed(ctx, "id"))
	tlExpectError(t, "UpsertApproval", s.UpsertApproval(ctx, Approval{ID: "a", SessionID: "s"}))
	tlExpectError(t, "ListPendingApprovals", func() error { _, err := s.ListPendingApprovals(ctx); return err }())
	tlExpectError(t, "GetApproval", func() error { _, err := s.GetApproval(ctx, "a"); return err }())
	tlExpectError(t, "ResolveApproval", func() error { _, err := s.ResolveApproval(ctx, "a", true); return err }())
	tlExpectError(t, "ResolveApprovalAndEnqueue", func() error { _, err := s.ResolveApprovalAndEnqueue(ctx, "a", "approve", "[]"); return err }())
	tlExpectError(t, "Prune", func() error { _, err := s.Prune(ctx, now); return err }())
}

func TestTLLegacySchemaMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := []string{
		`CREATE TABLE inbox (
			id TEXT PRIMARY KEY, source_id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
			session_id TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL DEFAULT '',
			resume TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0, lease_until INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`CREATE TABLE approvals (
			id TEXT PRIMARY KEY, session_id TEXT NOT NULL UNIQUE,
			interrupt_id TEXT NOT NULL DEFAULT '', request TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'pending')`,
	}
	for _, query := range legacy {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, table := range []string{"inbox", "approvals"} {
		rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for rows.Next() {
			var cid, notnull, pk int
			var name, ctype string
			var dflt any
			if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
				t.Fatal(err)
			}
			names[name] = true
		}
		_ = rows.Close()
		if table == "inbox" && (!names["lease_owner"] || !names["last_error"]) {
			t.Fatalf("inbox migration missing columns: %v", names)
		}
		if table == "approvals" && !names["resolved_at"] {
			t.Fatalf("approvals migration missing resolved_at: %v", names)
		}
	}
}

func TestTLApprovalResolutionBranches(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if _, err := s.GetApproval(ctx, "missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetApproval missing = %v", err)
	}
	if _, err := s.ResolveApproval(ctx, "missing", true); !errors.Is(err, ErrApprovalAlreadyResolved) {
		t.Fatalf("ResolveApproval missing = %v", err)
	}
	if err := s.UpsertApproval(ctx, Approval{ID: "a", SessionID: "s", Request: "q"}); err != nil {
		t.Fatal(err)
	}
	approval, err := s.ResolveApproval(ctx, "a", true)
	if err != nil || approval.Status != "approved" {
		t.Fatalf("ResolveApproval = %+v, %v", approval, err)
	}
	var resolvedAt int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT resolved_at FROM approvals WHERE id=?`, "a").Scan(&resolvedAt); err != nil {
		t.Fatal(err)
	}
	if resolvedAt == 0 {
		t.Fatal("ResolveApproval must stamp resolved_at")
	}
	if _, err := s.ResolveApproval(ctx, "a", false); !errors.Is(err, ErrApprovalAlreadyResolved) {
		t.Fatalf("second ResolveApproval = %v", err)
	}
	pending, err := s.ListPendingApprovals(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("ListPendingApprovals = %v, %v", pending, err)
	}
}

func TestTLOutboxFailureAndPrune(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if err := s.EnqueueOutbox(ctx, "s", aoa.OutboxMessage, "hello"); err != nil {
		t.Fatal(err)
	}
	msgs, err := s.PendingOutbox(ctx, 10)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("PendingOutbox = %v, %v", msgs, err)
	}
	if err := s.MarkOutboxFailed(ctx, msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	again, err := s.PendingOutbox(ctx, 10)
	if err != nil || len(again) != 1 || again[0].Attempts != 1 {
		t.Fatalf("failed outbox = %v, %v", again, err)
	}
	if err := s.MarkOutboxSent(ctx, msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	empty, err := s.PendingOutbox(ctx, 10)
	if err != nil || len(empty) != 0 {
		t.Fatalf("sent outbox = %v, %v", empty, err)
	}
	removed, err := s.Prune(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 {
		t.Fatal("Prune must remove terminal rows")
	}
}

func TestTLClaimAndFailBoundary(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "x", Kind: aoa.KindWebhook}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(ctx, now, time.Minute, 1, "w")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim = %v, %v", claimed, err)
	}
	if err := s.Fail(ctx, claimed[0].ID, errors.New("boom"), 1, "w"); err != nil {
		t.Fatal(err)
	}
	var status, lastError string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, last_error FROM inbox WHERE id=?`, claimed[0].ID).Scan(&status, &lastError); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || lastError != "boom" {
		t.Fatalf("status = %q, last_error = %q", status, lastError)
	}
	if _, err := s.Claim(ctx, now.Add(time.Hour), time.Minute, 1, "w"); err != nil {
		t.Fatal(err)
	}
	if err := s.RenewLease(ctx, "w", now.Add(time.Hour)); err == nil {
		t.Fatal("RenewLease without a held lease must fail")
	}
	if err := s.ReleaseLease(ctx, "w"); err != nil {
		t.Fatal(err)
	}
}
