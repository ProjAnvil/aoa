package runtime

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/projanvil/langchain-golang/langgraph/types"

	"github.com/projanvil/aoa/internal/agent"
	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/store"
)

// fileStore opens a file-backed store plus a second connection to the same
// database. The second connection lets a test install a temporary trigger so
// a specific store write fails deterministically.
func fileStore(t *testing.T) (*store.Store, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "aoa.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = st.Close() })
	return st, db
}

// brokenMem returns a Memory whose root is a regular file, so every file
// operation (and ContextBundle) fails deterministically.
func brokenMem(t *testing.T) *memory.Memory {
	t.Helper()
	root := t.TempDir()
	mem, err := memory.Open(root)
	if err != nil {
		t.Fatalf("memory.Open: %v", err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("remove memory root: %v", err)
	}
	if err := os.WriteFile(root, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("replace memory root: %v", err)
	}
	return mem
}

// runUntilCancelled starts a polling loop, lets a few ticks fire, then
// cancels the context and verifies the loop exits cleanly.
func runUntilCancelled(t *testing.T, run func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after the context was cancelled")
	}
}

// workerWithStore builds a harness worker backed by the supplied store,
// keeping the rest of the harness defaults.
func workerWithStore(t *testing.T, st *store.Store) *Worker {
	t.Helper()
	h := newHarness(t, agent.NewDemoModel())
	h.st = st
	return h.worker()
}

func closedStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("store.Close: %v", err)
	}
	return st
}

func dueTimer() store.Timer {
	return store.Timer{
		ID: "t-1", SessionID: "s1", Kind: aoa.TimerFollowUp,
		DueAt: time.Unix(0, 0), Payload: "check metrics",
	}
}

// ---------------------------------------------------------------- dispatcher

func TestDispatcherRunStopsOnCancel(t *testing.T) {
	d := NewDispatcher(DispatcherConfig{Store: closedStore(t), Poll: time.Millisecond})
	runUntilCancelled(t, d.Run)
}

func TestDispatcherTickRateLimit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	if err := h.st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "hello"); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(DispatcherConfig{Store: h.st})
	d.lastSent = time.Now()
	n, err := d.Tick(ctx)
	if err != nil || n != 0 {
		t.Fatalf("Tick = %d, %v; want 0, nil", n, err)
	}
	pending, err := h.st.PendingOutbox(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("rate-limited message was consumed: %v, %v", pending, err)
	}
}

func TestDispatcherWebhookSuccess(t *testing.T) {
	ctx := context.Background()
	keys := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := newHarness(t, agent.NewDemoModel())
	if err := h.st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "hello"); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(DispatcherConfig{Store: h.st, Webhook: srv.URL})
	n, err := d.Tick(ctx)
	if err != nil || n != 1 {
		t.Fatalf("Tick = %d, %v; want 1, nil", n, err)
	}
	select {
	case key := <-keys:
		if key == "" {
			t.Fatal("webhook request is missing the Idempotency-Key header")
		}
	default:
		t.Fatal("no webhook request was delivered")
	}
	pending, err := h.st.PendingOutbox(ctx, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending outbox = %v, %v", pending, err)
	}
}

func TestDispatcherWebhookServerError(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	h := newHarness(t, agent.NewDemoModel())
	if err := h.st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "hello"); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(DispatcherConfig{Store: h.st, Webhook: srv.URL})
	n, err := d.Tick(ctx)
	if err != nil || n != 0 {
		t.Fatalf("Tick = %d, %v; want 0, nil", n, err)
	}
	pending, err := h.st.PendingOutbox(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].Attempts < 1 {
		t.Fatalf("failed delivery must stay pending with an attempt: %v, %v", pending, err)
	}
}

func TestDispatcherDeliverRequestErrors(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	msg := store.OutboxMessage{ID: "m1", SessionID: "s1", Kind: aoa.OutboxMessage, Payload: "x"}

	badURL := NewDispatcher(DispatcherConfig{Store: h.st, Webhook: "://bad"})
	if err := badURL.deliver(context.Background(), msg); err == nil {
		t.Fatal("an invalid webhook URL must fail while building the request")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	unreachable := NewDispatcher(DispatcherConfig{Store: h.st, Webhook: url})
	if err := unreachable.deliver(context.Background(), msg); err == nil {
		t.Fatal("a refused webhook connection must fail while posting")
	}
}

func TestDispatcherMarkOutboxSentFails(t *testing.T) {
	ctx := context.Background()
	st, db := fileStore(t)
	if _, err := db.Exec(`CREATE TRIGGER no_sent BEFORE UPDATE ON outbox BEGIN SELECT RAISE(ABORT,'blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "hello"); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(DispatcherConfig{Store: st})
	if _, err := d.Tick(ctx); err == nil {
		t.Fatal("MarkOutboxSent failure must be returned by Tick")
	}
}

func TestDispatcherMarkOutboxFailedFails(t *testing.T) {
	ctx := context.Background()
	st, db := fileStore(t)
	if _, err := db.Exec(`CREATE TRIGGER no_fail BEFORE UPDATE ON outbox BEGIN SELECT RAISE(ABORT,'blocked'); END`); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	if err := st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "hello"); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(DispatcherConfig{Store: st, Webhook: srv.URL})
	if _, err := d.Tick(ctx); err == nil {
		t.Fatal("MarkOutboxFailed failure must be returned by Tick")
	}
}

// ----------------------------------------------------------------- scheduler

func TestSchedulerRunStopsOnCancel(t *testing.T) {
	s := NewScheduler(SchedulerConfig{Store: closedStore(t), Poll: time.Millisecond})
	runUntilCancelled(t, s.Run)
}

func TestSchedulerNowWithoutClock(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	s := NewScheduler(SchedulerConfig{Store: h.st})
	if s.now().IsZero() {
		t.Fatal("now() with a nil Clock must return the wall clock")
	}
}

func TestSchedulerEnqueueFails(t *testing.T) {
	ctx := context.Background()
	st, db := fileStore(t)
	if _, err := db.Exec(`CREATE TRIGGER no_dedup BEFORE INSERT ON dedup BEGIN SELECT RAISE(ABORT,'blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := st.AddTimer(ctx, dueTimer()); err != nil {
		t.Fatal(err)
	}
	s := NewScheduler(SchedulerConfig{Store: st, Clock: func() time.Time { return time.Unix(1_700_000_000, 0) }})
	n, err := s.Tick(ctx)
	if err == nil || n != 0 {
		t.Fatalf("Tick = %d, %v; want 0 with an enqueue error", n, err)
	}
}

func TestSchedulerMarkTimerFiredFails(t *testing.T) {
	ctx := context.Background()
	st, db := fileStore(t)
	if _, err := db.Exec(`CREATE TRIGGER no_fire BEFORE UPDATE ON timers BEGIN SELECT RAISE(ABORT,'blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := st.AddTimer(ctx, dueTimer()); err != nil {
		t.Fatal(err)
	}
	s := NewScheduler(SchedulerConfig{Store: st, Clock: func() time.Time { return time.Unix(1_700_000_000, 0) }})
	n, err := s.Tick(ctx)
	if err == nil || n != 0 {
		t.Fatalf("Tick = %d, %v; want 0 with a mark-fired error", n, err)
	}
}

// -------------------------------------------------------------------- worker

func TestWorkerDefaults(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	w := NewWorker(WorkerConfig{Store: h.st})
	if w.cfg.Owner == "" {
		t.Fatal("NewWorker must generate a default owner")
	}
	if w.cfg.LeaseTTL != 2*time.Minute {
		t.Fatalf("default LeaseTTL = %v, want 2m", w.cfg.LeaseTTL)
	}
	if w.now().IsZero() {
		t.Fatal("now() with a nil Clock must return the wall clock")
	}
}

func TestWorkerRunStopsOnCancel(t *testing.T) {
	w := NewWorker(WorkerConfig{Store: closedStore(t), Poll: time.Millisecond})
	runUntilCancelled(t, w.Run)
}

func TestWorkerLeaseContention(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	ok, err := h.st.AcquireLease(ctx, "other-owner", h.now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("AcquireLease = %v, %v", ok, err)
	}
	n, err := h.worker().Tick(ctx)
	if err != nil || n != 0 {
		t.Fatalf("Tick = %d, %v; want 0, nil when another worker holds the lease", n, err)
	}
}

func TestWorkerTickNoEvents(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	n, err := h.worker().Tick(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("Tick = %d, %v; want 0, nil", n, err)
	}
}

func TestWorkerClaimFails(t *testing.T) {
	ctx := context.Background()
	st, db := fileStore(t)
	if _, err := db.Exec(`CREATE TRIGGER no_claim BEFORE UPDATE ON inbox WHEN NEW.status='claimed' BEGIN SELECT RAISE(ABORT,'blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enqueue(ctx, aoa.Inbound{SourceID: "e1", Kind: aoa.KindUserMessage, Payload: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := workerWithStore(t, st).Tick(ctx); err == nil {
		t.Fatal("Claim failure must be returned by Tick")
	}
}

func TestWorkerReleaseLeaseFails(t *testing.T) {
	ctx := context.Background()
	st, db := fileStore(t)
	if _, err := db.Exec(`CREATE TRIGGER no_release BEFORE UPDATE ON leases WHEN NEW.until_ms=0 BEGIN SELECT RAISE(ABORT,'blocked'); END`); err != nil {
		t.Fatal(err)
	}
	n, err := workerWithStore(t, st).Tick(ctx)
	if err != nil || n != 0 {
		t.Fatalf("Tick = %d, %v; a failed ReleaseLease is only logged", n, err)
	}
}

func TestWorkerHandleGroupRunFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	w := h.worker()
	w.cfg.Memory = brokenMem(t)
	if _, err := h.st.Enqueue(ctx, aoa.Inbound{SourceID: "e1", Kind: aoa.KindUserMessage, Payload: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Tick(ctx); err == nil {
		t.Fatal("a run that fails to build must be reported by Tick")
	}
}

func TestWorkerRunInvokeFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, &agent.ScriptedModel{})
	if _, err := h.st.Enqueue(ctx, aoa.Inbound{SourceID: "e1", Kind: aoa.KindUserMessage, Payload: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.worker().Tick(ctx); err == nil {
		t.Fatal("a model invocation failure must be reported by Tick")
	}
}

func TestWorkerFinishAckAndFail(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	w := h.worker()
	ev := aoa.Event{ID: "missing-id", SourceID: "src", SessionID: "s1", Kind: aoa.KindUserMessage}
	w.finish(context.Background(), ev, errors.New("boom"))
	w.finish(context.Background(), ev, nil)
}

func TestWorkerResumeErrors(t *testing.T) {
	ctx := context.Background()
	newEvent := func(resume string) aoa.Event {
		return aoa.Event{ID: "e1", SourceID: "src", SessionID: "s1", Kind: aoa.KindApproval, ResumeJSON: resume}
	}

	t.Run("malformed resume json", func(t *testing.T) {
		h := newHarness(t, agent.NewDemoModel())
		err := h.worker().resume(ctx, "s1", newEvent("{bad"))
		if err == nil || !strings.Contains(err.Error(), "decode resume answer") {
			t.Fatalf("resume error = %v", err)
		}
	})
	t.Run("build error", func(t *testing.T) {
		h := newHarness(t, agent.NewDemoModel())
		w := h.worker()
		w.cfg.Memory = brokenMem(t)
		err := w.resume(ctx, "s1", newEvent(""))
		if err == nil || !strings.Contains(err.Error(), "build agent") {
			t.Fatalf("resume error = %v", err)
		}
	})
	t.Run("resume without checkpoint", func(t *testing.T) {
		h := newHarness(t, agent.NewDemoModel())
		err := h.worker().resume(ctx, "s1", newEvent(`[{"type":"approve"}]`))
		if err == nil || !strings.Contains(err.Error(), "resume run") {
			t.Fatalf("resume error = %v", err)
		}
	})
}

func TestWorkerSuspendErrorBranches(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	w := h.worker()

	if err := w.suspend(ctx, "s1", []types.Interrupt{{Value: make(chan int), ID: "i1"}}); err != nil {
		t.Fatalf("unmarshalable interrupt value must use the fmt.Sprint fallback: %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.suspend(cancelled, "s1", []types.Interrupt{{Value: "x", ID: "i2"}}); err == nil {
		t.Fatal("UpsertApproval failure must be surfaced")
	}

	if err := w.suspend(ctx, "s1", []types.Interrupt{{Value: strings.Repeat("x", 20000), ID: "i3"}}); err == nil {
		t.Fatal("an oversized approval card must fail EnqueueOutbox")
	}
}

func TestWorkerRenewLeaseCtxDone(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stop := h.worker().renewLease(ctx)
	time.Sleep(5 * time.Millisecond)
	stop()
}

func TestWorkerRenewLeaseFailure(t *testing.T) {
	h := newHarness(t, agent.NewDemoModel())
	w := NewWorker(WorkerConfig{Store: h.st, Owner: "x", LeaseTTL: 3 * time.Millisecond})
	stop := w.renewLease(context.Background())
	time.Sleep(20 * time.Millisecond)
	stop()
}
