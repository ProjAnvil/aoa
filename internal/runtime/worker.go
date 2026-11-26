// Package runtime holds the three stateless background loops: worker,
// scheduler, and dispatcher. Any one of them can die; the tables carry the
// state a replacement process needs.
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/projanvil/langchain-golang/core/language"

	"github.com/projanvil/langchain-golang/core/messages"
	"github.com/projanvil/langchain-golang/langgraph/checkpoint"
	graphpkg "github.com/projanvil/langchain-golang/langgraph/graph"
	"github.com/projanvil/langchain-golang/langgraph/types"

	"github.com/projanvil/aoa/internal/agent"
	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/permission"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// WorkerConfig wires the worker to the durable tables and the model.
type WorkerConfig struct {
	Store          *store.Store
	Memory         *memory.Memory
	Saver          checkpoint.Saver
	Model          language.ChatModel
	Classifier     permission.Classifier
	Proxy          permission.Proxy
	Owner          string
	Poll           time.Duration
	LeaseTTL       time.Duration
	BatchSize      int
	MaxAttempts    int
	RunTimeout     time.Duration
	RecursionLimit int
	Clock          func() time.Time
	Logger         *slog.Logger
	Metrics        *telemetry.Metrics
}

// Worker is the only active consumer of the inbox. A lease in the database
// enforces that property across processes.
type Worker struct {
	cfg WorkerConfig
}

// NewWorker applies defaults to the config.
func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.Owner == "" {
		cfg.Owner = uuid.NewString()
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 200 * time.Millisecond
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 2 * time.Minute
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 8
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &telemetry.Metrics{}
	}
	if cfg.RunTimeout <= 0 {
		cfg.RunTimeout = 5 * time.Minute
	}
	if cfg.RecursionLimit <= 0 {
		cfg.RecursionLimit = 64
	}
	return &Worker{cfg: cfg}
}

func (w *Worker) now() time.Time {
	if w.cfg.Clock != nil {
		return w.cfg.Clock()
	}
	return time.Now()
}

// Run polls until the context is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.Poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := w.Tick(ctx); err != nil {
				w.cfg.Logger.Warn("worker tick failed", "error", err)
			}
		}
	}
}

// Tick processes one batch. It returns the number of inbox rows consumed.
func (w *Worker) Tick(ctx context.Context) (int, error) {
	now := w.now()
	ok, err := w.cfg.Store.AcquireLease(ctx, w.cfg.Owner, now, w.cfg.LeaseTTL)
	if err != nil {
		return 0, err
	}
	if !ok {
		w.cfg.Metrics.LeaseContention.Add(1)
		return 0, nil // another worker owns the inbox
	}
	stopRenew := w.renewLease(ctx)
	defer func() {
		stopRenew()
		if err := w.cfg.Store.ReleaseLease(context.Background(), w.cfg.Owner); err != nil {
			w.cfg.Logger.Warn("release lease failed", "error", err)
		}
	}()

	events, err := w.cfg.Store.Claim(ctx, now, w.cfg.LeaseTTL, w.cfg.BatchSize, w.cfg.Owner)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}

	groups := make(map[string][]aoa.Event)
	var order []string
	for _, ev := range events {
		key := ev.SessionID
		if _, seen := groups[key]; !seen {
			order = append(order, key)
		}
		groups[key] = append(groups[key], ev)
	}
	var firstErr error
	for _, key := range order {
		sessionID := key
		if sessionID == "" {
			// Pending messages with no session form one new session. Batching
			// them gives one reply instead of one reply per message.
			sessionID = "s-" + uuid.NewString()
		}
		if err := w.handleGroup(ctx, sessionID, groups[key]); err != nil {
			w.cfg.Logger.Error("run failed", "session", sessionID, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return len(events), firstErr
}

func (w *Worker) renewLease(ctx context.Context) func() {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(w.cfg.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if err := w.cfg.Store.RenewLease(ctx, w.cfg.Owner, w.now().Add(w.cfg.LeaseTTL)); err != nil {
					w.cfg.Logger.Warn("renew lease failed", "error", err)
				}
			}
		}
	}()
	return func() { close(done); <-finished }
}

// handleGroup groups events into a single short run. Multiple messages in one
// batch become one prompt, and approval replies resume the suspended session.
func (w *Worker) handleGroup(ctx context.Context, sessionID string, events []aoa.Event) error {
	var runEvents []aoa.Event
	for _, ev := range events {
		if ev.Kind != aoa.KindApproval {
			runEvents = append(runEvents, ev)
			continue
		}
		err := w.resume(ctx, sessionID, ev)
		w.finish(ctx, ev, err)
	}
	if len(runEvents) == 0 {
		return nil
	}
	err := w.run(ctx, sessionID, runEvents)
	for _, ev := range runEvents {
		w.finish(ctx, ev, err)
	}
	return err
}

func (w *Worker) finish(ctx context.Context, ev aoa.Event, runErr error) {
	// A cancelled run must still write its terminal state. Detach from the
	// request context but keep the deadline short.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	fields := telemetry.Fields("worker", telemetry.TraceFromContext(ctx), ev.SessionID, ev.ID)
	if runErr != nil {
		w.cfg.Metrics.RunsFailed.Add(1)
		if err := w.cfg.Store.Fail(writeCtx, ev.ID, runErr, w.cfg.MaxAttempts, w.cfg.Owner); err != nil {
			w.cfg.Logger.Error("mark event failed", append(fields, "error", err)...)
		}
		return
	}
	if err := w.cfg.Store.Ack(writeCtx, ev.ID, w.cfg.Owner); err != nil {
		w.cfg.Logger.Error("ack event failed", append(fields, "error", err)...)
	}
}

func (w *Worker) scope(sessionID, taskID, traceID string, approved bool) agent.Scope {
	return agent.Scope{
		Store:          w.cfg.Store,
		Memory:         w.cfg.Memory,
		SessionID:      sessionID,
		TaskID:         taskID,
		Classifier:     w.cfg.Classifier,
		Proxy:          w.cfg.Proxy,
		Clock:          w.cfg.Clock,
		Logger:         w.cfg.Logger,
		Metrics:        w.cfg.Metrics,
		TraceID:        traceID,
		RecursionLimit: w.cfg.RecursionLimit,
		Approved:       approved,
	}
}

func taskIDFor(ev aoa.Event) string {
	sum := sha256.Sum256([]byte(ev.SourceID))
	return hex.EncodeToString(sum[:])[:12]
}

func (w *Worker) runOptions(sessionID string) graphpkg.Options {
	return graphpkg.Options{
		ThreadID:       sessionID,
		RecursionLimit: w.cfg.RecursionLimit,
	}
}

// run starts a new turn on an existing session or creates the session's
// first checkpoint. A session spans many runs; a run is one model invocation.
func (w *Worker) run(ctx context.Context, sessionID string, events []aoa.Event) error {
	traceID := telemetry.TraceFromContext(ctx)
	w.cfg.Metrics.RunsTotal.Add(1)
	a, err := agent.Build(w.cfg.Model, w.cfg.Saver,
		w.scope(sessionID, taskIDFor(events[0]), traceID, false))
	if err != nil {
		return fmt.Errorf("build agent: %w", err)
	}
	var lines []string
	for _, ev := range events {
		lines = append(lines, fmt.Sprintf("[%s] %s", ev.Kind, telemetry.Redact(ev.Payload, 4096)))
	}
	prompt := strings.Join(lines, "\n")
	runCtx, cancel := context.WithTimeout(ctx, w.cfg.RunTimeout)
	defer cancel()
	_, interrupts, err := a.InvokeWithStateOptions(runCtx,
		[]messages.Message{messages.Human(prompt)}, w.runOptions(sessionID))
	if err != nil {
		return fmt.Errorf("invoke run: %w", err)
	}
	w.cfg.Logger.Info("run complete", "session_id", sessionID,
		"events", len(events), "interrupts", len(interrupts), "trace_id", traceID)
	return w.suspend(ctx, sessionID, interrupts)
}

// resume continues a suspended tool call. The human answer arrives as an
// inbox event, so the worker stays free while the approval is outstanding.
func (w *Worker) resume(ctx context.Context, sessionID string, ev aoa.Event) error {
	traceID := telemetry.TraceFromContext(ctx)
	w.cfg.Metrics.RunsTotal.Add(1)
	// Approved marks this run as the continuation of a reviewed suspension.
	// Only such a run may execute a Defer-classified tool.
	a, err := agent.Build(w.cfg.Model, w.cfg.Saver,
		w.scope(sessionID, taskIDFor(ev), traceID, true))
	if err != nil {
		return fmt.Errorf("build agent: %w", err)
	}
	var answer any
	if ev.ResumeJSON != "" {
		if err := json.Unmarshal([]byte(ev.ResumeJSON), &answer); err != nil {
			return fmt.Errorf("decode resume answer: %w", err)
		}
	}
	runCtx, cancel := context.WithTimeout(ctx, w.cfg.RunTimeout)
	defer cancel()
	opts := w.runOptions(sessionID)
	opts.Resume = answer
	_, interrupts, err := a.Resume(runCtx, opts)
	if err != nil {
		return fmt.Errorf("resume run: %w", err)
	}
	w.cfg.Logger.Info("run resumed", "session_id", sessionID,
		"interrupts", len(interrupts), "trace_id", traceID)
	return w.suspend(ctx, sessionID, interrupts)
}

// suspend persists a non-blocking pause: save the session checkpoint (the
// saver already did that), write the approval card, and end the run.
func (w *Worker) suspend(ctx context.Context, sessionID string, interrupts []types.Interrupt) error {
	if len(interrupts) == 0 {
		return nil
	}
	for _, it := range interrupts {
		body, err := json.Marshal(it.Value)
		if err != nil {
			body = []byte(fmt.Sprint(it.Value))
		}
		approval := store.Approval{
			ID:          uuid.NewString(),
			SessionID:   sessionID,
			InterruptID: it.ID,
			Request:     string(body),
		}
		if err := w.cfg.Store.UpsertApproval(ctx, approval); err != nil {
			return fmt.Errorf("store approval: %w", err)
		}
		w.cfg.Metrics.Approvals.Add(1)
		card := fmt.Sprintf("approval_id=%s session_id=%s request=%s",
			approval.ID, sessionID, string(body))
		if err := w.cfg.Store.EnqueueOutbox(ctx, sessionID, aoa.OutboxApproval, card); err != nil {
			return fmt.Errorf("queue approval card: %w", err)
		}
	}
	return nil
}
