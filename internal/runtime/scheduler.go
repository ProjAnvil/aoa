package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// SchedulerConfig wires the timer scanner.
type SchedulerConfig struct {
	Store   *store.Store
	Poll    time.Duration
	Limit   int
	Clock   func() time.Time
	Logger  *slog.Logger
	Metrics *telemetry.Metrics
}

// Scheduler turns due timers into inbox events. The model writes a timer row
// during a run; the scheduler, not the model, performs the wake-up.
type Scheduler struct {
	cfg SchedulerConfig
}

// NewScheduler applies defaults.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.Poll <= 0 {
		cfg.Poll = time.Second
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 32
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &telemetry.Metrics{}
	}
	return &Scheduler{cfg: cfg}
}

func (s *Scheduler) now() time.Time {
	if s.cfg.Clock != nil {
		return s.cfg.Clock()
	}
	return time.Now()
}

// Run polls until the context is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := s.Tick(ctx); err != nil {
				s.cfg.Logger.Warn("scheduler tick failed", "error", err)
			}
		}
	}
}

// Tick fires every due timer. Enqueue comes first: a crash after that only
// produces a duplicate CloudEvent id, which the inbox dedups.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	timers, err := s.cfg.Store.DueTimers(ctx, s.now(), s.cfg.Limit)
	if err != nil {
		return 0, err
	}
	fired := 0
	for _, t := range timers {
		in := aoa.Inbound{
			SourceID:  fmt.Sprintf("timer:%s:%d", t.ID, t.DueAt.UnixMilli()),
			Kind:      aoa.KindTimer,
			SessionID: t.SessionID,
			Payload:   fmt.Sprintf("[timer %s] %s", t.Kind, t.Payload),
		}
		if _, err := s.cfg.Store.Enqueue(ctx, in); err != nil {
			return fired, fmt.Errorf("scheduler: enqueue timer %q: %w", t.ID, err)
		}
		if err := s.cfg.Store.MarkTimerFired(ctx, t); err != nil {
			return fired, fmt.Errorf("scheduler: mark timer %q: %w", t.ID, err)
		}
		s.cfg.Metrics.TimersFired.Add(1)
		s.cfg.Logger.Info("timer fired",
			"timer_id", t.ID,
			"kind", t.Kind,
			"session_id", t.SessionID,
		)
		fired++
	}
	return fired, nil
}
