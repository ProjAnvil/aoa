package runtime

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/projanvil/langchain-golang/core/messages"
	"github.com/projanvil/langchain-golang/langgraph/checkpoint"

	"github.com/projanvil/aoa/internal/agent"
	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/permission"
	"github.com/projanvil/aoa/internal/store"
)

type fakeProxy struct{ calls int }

func (p *fakeProxy) Do(_ context.Context, _ *http.Request) (*http.Response, error) {
	p.calls++
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("ok")),
	}, nil
}

type harness struct {
	st    *store.Store
	mem   *memory.Memory
	saver checkpoint.Saver
	model *agent.ScriptedModel
	proxy *fakeProxy
	now   time.Time
}

func newHarness(t *testing.T, model *agent.ScriptedModel) *harness {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mem, err := memory.Open(t.TempDir())
	if err != nil {
		t.Fatalf("memory.Open: %v", err)
	}
	return &harness{
		st:    st,
		mem:   mem,
		saver: checkpoint.NewMemorySaver(),
		model: model,
		proxy: &fakeProxy{},
		now:   time.Unix(1_700_000_000, 0),
	}
}

func (h *harness) clock() time.Time { return h.now }

func (h *harness) worker() *Worker {
	return NewWorker(WorkerConfig{
		Store:      h.st,
		Memory:     h.mem,
		Saver:      h.saver,
		Model:      h.model,
		Classifier: permission.DefaultClassifier(),
		Proxy:      h.proxy,
		Owner:      "test-worker",
		LeaseTTL:   time.Minute,
		Clock:      h.clock,
	})
}

func TestBatchedEventsProduceOneOutboxMessage(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	for _, id := range []string{"m1", "m2"} {
		if _, err := h.st.Enqueue(ctx, aoa.Inbound{
			SourceID: id, Kind: aoa.KindUserMessage, Payload: "check the bill",
		}); err != nil {
			t.Fatal(err)
		}
	}

	n, err := h.worker().Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if n != 2 {
		t.Fatalf("consumed %d events, want 2", n)
	}
	out, err := h.st.PendingOutbox(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("outbox rows = %d, want 1 (batched run)", len(out))
	}
	if !strings.Contains(out[0].Payload, "Event received") {
		t.Fatalf("outbox payload = %q", out[0].Payload)
	}
	if h.model.Invokes != 2 {
		t.Fatalf("model calls = %d, want 2 (model -> tool -> model)", h.model.Invokes)
	}
	claimed, err := h.st.Claim(ctx, h.now.Add(time.Minute), time.Minute, 10)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("pending events after Ack = %v, %v; want none", claimed, err)
	}
}

func TestDeferredToolCallSuspendsAndResumes(t *testing.T) {
	ctx := context.Background()
	model := &agent.ScriptedModel{Responses: []messages.Message{
		{
			Role: messages.RoleAI,
			ToolCalls: []messages.ToolCall{{
				ID: "call_rollback", Name: "aoa_rollback",
				Args: map[string]any{"target": "cfg-42"},
			}},
		},
		messages.AI("Rollback finished."),
	}}
	h := newHarness(t, model)
	worker := h.worker()

	if _, err := h.st.Enqueue(ctx, aoa.Inbound{
		SourceID: "alert-4821", Kind: aoa.KindWebhook,
		SessionID: "s1", Payload: "P95 > 800ms",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Tick(ctx); err != nil {
		t.Fatalf("first Tick: %v", err)
	}

	approvals, err := h.st.ListPendingApprovals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(approvals) != 1 {
		t.Fatalf("pending approvals = %d, want 1", len(approvals))
	}
	if approvals[0].SessionID != "s1" {
		t.Fatalf("approval session = %q, want s1", approvals[0].SessionID)
	}
	if h.proxy.calls != 0 {
		t.Fatalf("proxy calls while suspended = %d, want 0", h.proxy.calls)
	}
	cards, err := h.st.PendingOutbox(ctx, 10)
	if err != nil || len(cards) != 1 || cards[0].Kind != aoa.OutboxApproval {
		t.Fatalf("approval cards = %v, %v", cards, err)
	}

	// The human answer comes back as a normal inbox event. It carries the
	// session id so the worker resumes the checkpointed run.
	if _, err := h.st.ResolveApproval(ctx, approvals[0].ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := h.st.Enqueue(ctx, aoa.Inbound{
		SourceID:   "approval-1",
		Kind:       aoa.KindApproval,
		SessionID:  "s1",
		Payload:    "approved",
		ResumeJSON: `[{"type":"approve"}]`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Tick(ctx); err != nil {
		t.Fatalf("resume Tick: %v", err)
	}
	if h.proxy.calls != 1 {
		t.Fatalf("proxy calls after approval = %d, want 1", h.proxy.calls)
	}
	remaining, err := h.st.ListPendingApprovals(ctx)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("approvals after resume = %v, %v; want none", remaining, err)
	}
}

func TestSchedulerFiresTimerIntoInbox(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	if err := h.st.AddTimer(ctx, store.Timer{
		ID: "t-1", SessionID: "s1", Kind: aoa.TimerFollowUp,
		DueAt: h.now.Add(-time.Second), Payload: "check metrics",
	}); err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Store: h.st, Clock: h.clock})
	n, err := sched.Tick(ctx)
	if err != nil || n != 1 {
		t.Fatalf("scheduler Tick = %d, %v; want 1, nil", n, err)
	}
	events, err := h.st.Claim(ctx, h.now, time.Minute, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("inbox events = %v, %v; want 1", events, err)
	}
	if events[0].Kind != aoa.KindTimer || events[0].SessionID != "s1" {
		t.Fatalf("timer event = %+v", events[0])
	}
	// A repeated scan must not create a second wake-up.
	if n, err := sched.Tick(ctx); err != nil || n != 0 {
		t.Fatalf("second scheduler Tick = %d, %v; want 0, nil", n, err)
	}
}

func TestDispatcherDeliversOutbox(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, agent.NewDemoModel())
	if err := h.st.EnqueueOutbox(ctx, "s1", aoa.OutboxMessage, "done"); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(DispatcherConfig{Store: h.st})
	n, err := d.Tick(ctx)
	if err != nil || n != 1 {
		t.Fatalf("dispatcher Tick = %d, %v; want 1, nil", n, err)
	}
	pending, err := h.st.PendingOutbox(ctx, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending outbox after delivery = %v, %v", pending, err)
	}
}
