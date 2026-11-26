package store

import (
	"context"
	"testing"
	"time"

	"github.com/projanvil/aoa/internal/aoa"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestEnqueueDeduplicatesBySourceID(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	in := aoa.Inbound{SourceID: "alert-4821", Kind: aoa.KindWebhook, Payload: "P95 > 800ms"}

	first, err := s.Enqueue(ctx, in)
	if err != nil || !first {
		t.Fatalf("first Enqueue = %v, %v; want true, nil", first, err)
	}
	second, err := s.Enqueue(ctx, in)
	if err != nil || second {
		t.Fatalf("second Enqueue = %v, %v; want false, nil", second, err)
	}
	events, err := s.Claim(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("claimed %d events, want 1", len(events))
	}
	if events[0].Payload != "P95 > 800ms" {
		t.Fatalf("payload = %q", events[0].Payload)
	}
}

func TestLeaseAllowsOnlyOneActiveWorker(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Unix(1_700_000_000, 0)

	ok, err := s.AcquireLease(ctx, "worker-a", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("worker-a AcquireLease = %v, %v; want true, nil", ok, err)
	}
	ok, err = s.AcquireLease(ctx, "worker-b", now, time.Minute)
	if err != nil || ok {
		t.Fatalf("worker-b AcquireLease = %v, %v; want false, nil", ok, err)
	}
	ok, err = s.AcquireLease(ctx, "worker-b", now.Add(2*time.Minute), time.Minute)
	if err != nil || !ok {
		t.Fatalf("worker-b after lease expiry = %v, %v; want true, nil", ok, err)
	}
}

func TestClaimAckAndCrashReplay(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Unix(1_700_000_000, 0)
	if _, err := s.Enqueue(ctx, aoa.Inbound{SourceID: "e1", Kind: aoa.KindUserMessage, Payload: "hi"}); err != nil {
		t.Fatal(err)
	}

	claimed, err := s.Claim(ctx, now, time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim = %v, %v", claimed, err)
	}
	// Simulate a crash before Ack: the lease expires and a later Claim
	// returns the same row again.
	replayed, err := s.Claim(ctx, now.Add(2*time.Minute), time.Minute, 1)
	if err != nil || len(replayed) != 1 {
		t.Fatalf("replay Claim = %v, %v", replayed, err)
	}
	if replayed[0].ID != claimed[0].ID {
		t.Fatalf("replay returned a different row: %q vs %q", replayed[0].ID, claimed[0].ID)
	}
	if replayed[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", replayed[0].Attempts)
	}
	if err := s.Ack(ctx, replayed[0].ID); err != nil {
		t.Fatal(err)
	}
	done, err := s.Claim(ctx, now.Add(3*time.Minute), time.Minute, 1)
	if err != nil || len(done) != 0 {
		t.Fatalf("Claim after Ack = %v, %v; want empty", done, err)
	}
}

func TestFollowUpAndPeriodicTimers(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	now := time.Unix(1_700_000_000, 0)

	follow := Timer{ID: "t1", SessionID: "s1", Kind: aoa.TimerFollowUp, DueAt: now, Payload: "check metrics"}
	if err := s.AddTimer(ctx, follow); err != nil {
		t.Fatal(err)
	}
	periodic := Timer{ID: "t2", SessionID: "s1", Kind: aoa.TimerPeriodic, DueAt: now, IntervalSec: 60, Payload: "digest"}
	if err := s.AddTimer(ctx, periodic); err != nil {
		t.Fatal(err)
	}
	due, err := s.DueTimers(ctx, now, 10)
	if err != nil || len(due) != 2 {
		t.Fatalf("DueTimers = %v, %v; want 2", due, err)
	}
	for _, timer := range due {
		if err := s.MarkTimerFired(ctx, timer); err != nil {
			t.Fatal(err)
		}
	}
	again, err := s.DueTimers(ctx, now, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("DueTimers immediately after firing = %v, %v; want 0", again, err)
	}
	later, err := s.DueTimers(ctx, now.Add(61*time.Second), 10)
	if err != nil || len(later) != 1 || later[0].ID != "t2" {
		t.Fatalf("DueTimers after interval = %v, %v; want only periodic t2", later, err)
	}
}

func TestOutboxAndApprovalLifecycle(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	if err := s.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "done"); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingOutbox(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("PendingOutbox = %v, %v", pending, err)
	}
	if err := s.MarkOutboxSent(ctx, pending[0].ID); err != nil {
		t.Fatal(err)
	}
	after, err := s.PendingOutbox(ctx, 10)
	if err != nil || len(after) != 0 {
		t.Fatalf("PendingOutbox after send = %v, %v; want empty", after, err)
	}

	a := Approval{ID: "a1", SessionID: "s1", InterruptID: "i1", Request: "rollback?"}
	if err := s.UpsertApproval(ctx, a); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListPendingApprovals(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListPendingApprovals = %v, %v", list, err)
	}
	resolved, err := s.ResolveApproval(ctx, "a1", true)
	if err != nil || resolved.Status != "approved" {
		t.Fatalf("ResolveApproval = %v, %v", resolved, err)
	}
	if _, err := s.ResolveApproval(ctx, "a1", true); err != ErrApprovalAlreadyResolved {
		t.Fatalf("second resolve error = %v; want ErrApprovalAlreadyResolved", err)
	}
}
