package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// DispatcherConfig wires the outbound delivery loop.
type DispatcherConfig struct {
	Store       *store.Store
	Webhook     string
	Client      *http.Client
	Poll        time.Duration
	Limit       int
	MinInterval time.Duration
	Logger      *slog.Logger
	Metrics     *telemetry.Metrics
}

// Dispatcher is the outbox counterpart of the worker. It is also a rate
// limiter: user attention is scarce, so messages start quiet by default.
type Dispatcher struct {
	cfg      DispatcherConfig
	lastSent time.Time
}

// NewDispatcher applies defaults.
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.Poll <= 0 {
		cfg.Poll = time.Second
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 16
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &telemetry.Metrics{}
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 30 * time.Second
	}
	return &Dispatcher{cfg: cfg}
}

// Run polls until the context is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.cfg.Poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := d.Tick(ctx); err != nil {
				d.cfg.Logger.Warn("dispatcher tick failed", "error", err)
			}
		}
	}
}

// Tick delivers pending outbox rows. Approval cards bypass the rate limit
// because a suspended run cannot continue without them.
func (d *Dispatcher) Tick(ctx context.Context) (int, error) {
	msgs, err := d.cfg.Store.PendingOutbox(ctx, d.cfg.Limit)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, m := range msgs {
		if m.Kind == aoa.OutboxMessage && d.cfg.MinInterval > 0 {
			if !d.lastSent.IsZero() && time.Since(d.lastSent) < d.cfg.MinInterval {
				continue
			}
		}
		if err := d.deliver(ctx, m); err != nil {
			d.cfg.Logger.Warn("delivery failed", "id", m.ID,
				"error", telemetry.Redact(err.Error(), 256))
			if err := d.cfg.Store.MarkOutboxFailed(ctx, m.ID); err != nil {
				return sent, err
			}
			d.cfg.Metrics.OutboxFailed.Add(1)
			continue
		}
		if err := d.cfg.Store.MarkOutboxSent(ctx, m.ID); err != nil {
			return sent, err
		}
		d.cfg.Metrics.OutboxSent.Add(1)
		d.lastSent = time.Now()
		sent++
	}
	return sent, nil
}

func (d *Dispatcher) deliver(ctx context.Context, m store.OutboxMessage) error {
	body, err := json.Marshal(map[string]any{
		"id":         m.ID,
		"session_id": m.SessionID,
		"kind":       m.Kind,
		"payload":    m.Payload,
	})
	if err != nil {
		return fmt.Errorf("dispatcher: encode message: %w", err)
	}
	if d.cfg.Webhook == "" {
		d.cfg.Logger.Info("outbox delivery", "kind", m.Kind,
			"session", m.SessionID, "bytes", len(m.Payload),
			"payload", telemetry.Redact(m.Payload, 256))
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.cfg.Webhook, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("dispatcher: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", m.ID)
	resp, err := d.cfg.Client.Do(req)
	if err != nil {
		return fmt.Errorf("dispatcher: post webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("dispatcher: webhook returned %d", resp.StatusCode)
	}
	return nil
}
