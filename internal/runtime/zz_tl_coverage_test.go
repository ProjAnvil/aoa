package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/projanvil/aoa/internal/agent"
	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/store"
)

func TLRunAndCancel(t *testing.T, fn func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("loop returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not exit after cancellation")
	}
}

func TestTLRunLoopsExitOnCancel(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	w := h.worker()
	w.cfg.Poll = 5 * time.Millisecond
	s := NewScheduler(SchedulerConfig{Store: h.st, Poll: 5 * time.Millisecond, Clock: h.clock})
	d := NewDispatcher(DispatcherConfig{Store: h.st, Poll: 5 * time.Millisecond})
	TLRunAndCancel(t, w.Run)
	TLRunAndCancel(t, s.Run)
	TLRunAndCancel(t, d.Run)
}

func TestTLDispatcherTickBranches(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())

	// Rate limit: the first message is delivered, the second is skipped.
	for _, p := range []string{"first", "second"} {
		if err := h.st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, p); err != nil {
			t.Fatal(err)
		}
	}
	d := NewDispatcher(DispatcherConfig{Store: h.st, MinInterval: time.Hour, Limit: 10})
	if sent, err := d.Tick(ctx); err != nil || sent != 1 {
		t.Fatalf("rate-limited Tick = %d, %v", sent, err)
	}

	// Webhook success path.
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") == "" {
			t.Error("Idempotency-Key is missing")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	if err := h.st.EnqueueOutbox(ctx, "s1", aoa.OutboxApproval, "card"); err != nil {
		t.Fatal(err)
	}
	withWebhook := NewDispatcher(DispatcherConfig{
		Store: h.st, Webhook: ok.URL, Client: ok.Client(), Limit: 10,
		MinInterval: time.Hour,
	})
	withWebhook.lastSent = time.Now()
	if sent, err := withWebhook.Tick(ctx); err != nil || sent != 1 {
		t.Fatalf("webhook Tick = %d, %v", sent, err)
	}

	// Webhook 5xx goes to MarkOutboxFailed.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	if err := h.st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "fail"); err != nil {
		t.Fatal(err)
	}
	failing := NewDispatcher(DispatcherConfig{
		Store: h.st, Webhook: bad.URL, Client: bad.Client(), Limit: 10,
	})
	failing.lastSent = time.Time{}
	if _, err := failing.Tick(ctx); err != nil {
		t.Fatal(err)
	}

	// Invalid webhook URL and unreachable host.
	for _, target := range []string{"://bad", "http://127.0.0.1:1"} {
		broken := NewDispatcher(DispatcherConfig{Store: h.st, Webhook: target, Limit: 10})
		if err := broken.deliver(ctx, store.OutboxMessage{ID: "x", Payload: "y"}); err == nil {
			t.Fatalf("deliver(%q) must fail", target)
		}
	}

	// Store error paths.
	closed := newHarness(t, agent.NewDemoModel())
	if err := closed.st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDispatcher(DispatcherConfig{Store: closed.st}).Tick(ctx); err == nil {
		t.Fatal("Tick with a closed store must fail")
	}
	if _, err := NewScheduler(SchedulerConfig{Store: closed.st}).Tick(ctx); err == nil {
		t.Fatal("scheduler Tick with a closed store must fail")
	}
	if _, err := closed.worker().Tick(ctx); err == nil {
		t.Fatal("worker Tick with a closed store must fail")
	}
}

func TestTLSchedulerAndWorkerErrorPaths(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	w := h.worker()

	// Lease contention: a second owner cannot claim the inbox.
	if ok, err := h.st.AcquireLease(ctx, "other-owner", h.now, time.Minute); err != nil || !ok {
		t.Fatalf("AcquireLease = %v, %v", ok, err)
	}
	if n, err := w.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("contended Tick = %d, %v", n, err)
	}
	if err := h.st.ReleaseLease(ctx, "other-owner"); err != nil {
		t.Fatal(err)
	}

	// finish must survive failing terminal writes.
	if err := h.st.Close(); err != nil {
		t.Fatal(err)
	}
	w.finish(ctx, aoa.Event{ID: "e", SessionID: "s"}, nil)
	w.finish(ctx, aoa.Event{ID: "e", SessionID: "s"}, errors.New("run failed"))

	// Model failure and malformed resume values.
	failing := newHarness(t, &agent.ScriptedModel{})
	fw := failing.worker()
	events := []aoa.Event{{ID: "ev", SourceID: "src", Kind: aoa.KindWebhook, Payload: "x"}}
	if err := fw.run(ctx, "s1", events); err == nil {
		t.Fatal("run with an exhausted model must fail")
	}
	if err := fw.resume(ctx, "s1", aoa.Event{
		ID: "ev", SourceID: "src", Kind: aoa.KindApproval, ResumeJSON: "not-json",
	}); err == nil {
		t.Fatal("resume with malformed JSON must fail")
	}
}
