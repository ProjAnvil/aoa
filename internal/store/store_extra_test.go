package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/projanvil/aoa/internal/aoa"
)

func TestAckFenceRejectsStaleOwner(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "e", Kind: aoa.KindWebhook}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(ctx, now, time.Minute, 1, "worker-a")
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim = %v, %v", claimed, err)
	}
	if err := s.Ack(ctx, claimed[0].ID, "worker-b"); !errors.Is(err, ErrNotClaimed) {
		t.Fatalf("stale Ack = %v, want ErrNotClaimed", err)
	}
	if err := s.Ack(ctx, claimed[0].ID, "worker-a"); err != nil {
		t.Fatalf("owner Ack = %v", err)
	}
}

func TestFailWritesLastErrorNotPayload(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := s.Enqueue(ctx, aoa.Inbound{
		SourceID: "e", Kind: aoa.KindWebhook, Payload: "original",
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(ctx, now, time.Minute, 1, "w")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, claimed[0].ID, fmt.Errorf("token=secret failure"), 5, "w"); err != nil {
		t.Fatal(err)
	}
	var payload, lastError string
	if err := s.db.QueryRowContext(ctx,
		`SELECT payload, last_error FROM inbox WHERE id=?`, claimed[0].ID).
		Scan(&payload, &lastError); err != nil {
		t.Fatal(err)
	}
	if payload != "original" || !strings.Contains(lastError, "failure") {
		t.Fatalf("payload = %q, last_error = %q", payload, lastError)
	}
}

func TestMarkTimerFiredIsNotRepeatable(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Unix(1_700_000_000, 0)
	timer := Timer{ID: "t", SessionID: "s", Kind: aoa.TimerFollowUp, DueAt: now}
	if err := s.AddTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTimerFired(ctx, timer); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkTimerFired(ctx, timer); err == nil {
		t.Fatal("second MarkTimerFired must fail")
	}
}

func TestAddTimerValidatesBounds(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	bad := []Timer{
		{ID: "no-due", SessionID: "s", Kind: aoa.TimerFollowUp},
		{ID: "periodic-no-interval", SessionID: "s", Kind: aoa.TimerPeriodic, DueAt: time.Now()},
		{ID: "huge", SessionID: "s", Kind: aoa.TimerPeriodic, DueAt: time.Now(), IntervalSec: 31 * 24 * 3600},
		{ID: "payload", SessionID: "s", Kind: aoa.TimerFollowUp, DueAt: time.Now(), Payload: strings.Repeat("x", 5000)},
	}
	for _, timer := range bad {
		if err := s.AddTimer(ctx, timer); err == nil {
			t.Fatalf("AddTimer(%q) must fail", timer.ID)
		}
	}
	valid := Timer{ID: "ok", SessionID: "s", Kind: aoa.TimerFollowUp, DueAt: time.Now(), Payload: "check"}
	if err := s.AddTimer(ctx, valid); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTimer(ctx, valid); err != nil {
		t.Fatalf("deterministic timer id must be idempotent: %v", err)
	}
}

func TestEnqueueOutboxRejectsOversizedPayload(t *testing.T) {
	s := testStore(t)
	if err := s.EnqueueOutbox(context.Background(), "s", aoa.OutboxMessage, strings.Repeat("x", 17<<10)); err == nil {
		t.Fatal("oversized outbox payload must fail")
	}
}

func TestResolveApprovalAndEnqueueIsAtomic(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if err := s.UpsertApproval(ctx, Approval{ID: "a", SessionID: "s", Request: "rollback?"}); err != nil {
		t.Fatal(err)
	}
	inserted, err := s.ResolveApprovalAndEnqueue(ctx, "a", "approve", `[{"type":"approve"}]`)
	if err != nil || !inserted {
		t.Fatalf("first resolve = %v, %v", inserted, err)
	}
	inserted, err = s.ResolveApprovalAndEnqueue(ctx, "a", "reject", `[{"type":"reject"}]`)
	if err != nil || inserted {
		t.Fatalf("conflicting decision = %v, %v", inserted, err)
	}
	if _, err := s.ResolveApprovalAndEnqueue(ctx, "missing", "approve", "[]"); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("missing approval = %v", err)
	}
	events, err := s.Claim(ctx, time.Now(), time.Minute, 10)
	if err != nil || len(events) != 1 || events[0].Kind != aoa.KindApproval {
		t.Fatalf("events = %v, %v", events, err)
	}
}

func TestPruneKeepsDedupTombstones(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	in := aoa.Inbound{SourceID: "old", Kind: aoa.KindWebhook, Payload: "x"}
	if ok, err := s.Enqueue(ctx, in); err != nil || !ok {
		t.Fatalf("first enqueue = %v, %v", ok, err)
	}
	claimed, err := s.Claim(ctx, time.Now(), time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ack(ctx, claimed[0].ID); err != nil {
		t.Fatal(err)
	}
	// Prune terminal rows but keep the dedup tombstone (cutoff in the past).
	if _, err := s.Prune(ctx, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Enqueue(ctx, in); err != nil || ok {
		t.Fatalf("replay after prune = %v, %v; want duplicate", ok, err)
	}
	// Prune with a future cutoff removes tombstones too.
	if _, err := s.Prune(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Enqueue(ctx, in); err != nil || !ok {
		t.Fatalf("enqueue after tombstone expiry = %v, %v", ok, err)
	}
}
