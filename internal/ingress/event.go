// Package ingress adapts external event ecosystems to one canonical event.
// The inbox is the only writer; every adapter is a stateless translator.
package ingress

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// CanonicalEvent is the single event shape all ecosystems map into. Adapters
// never write to the inbox themselves.
type CanonicalEvent struct {
	SourceID   string
	Kind       aoa.Kind
	SessionID  string
	Payload    string
	ResumeJSON string
	TraceID    string
}

// Validate rejects an event that cannot be deduplicated.
func (e CanonicalEvent) Validate() error {
	if e.SourceID == "" {
		return fmt.Errorf("ingress: empty source id")
	}
	if e.Kind == "" {
		return fmt.Errorf("ingress: empty event kind")
	}
	return nil
}

// Sink accepts a canonical event. The bool reports whether a new inbox row
// was created; false means a duplicate delivery was dropped.
type Sink interface {
	Accept(ctx context.Context, ev CanonicalEvent) (bool, error)
}

// SinkFunc adapts a function to the Sink interface.
type SinkFunc func(ctx context.Context, ev CanonicalEvent) (bool, error)

// Accept implements Sink.
func (f SinkFunc) Accept(ctx context.Context, ev CanonicalEvent) (bool, error) {
	return f(ctx, ev)
}

// StoreSink is the production sink. It writes one durable row and records
// counters. Decorator sinks can wrap it without changing adapters.
type StoreSink struct {
	Store   *store.Store
	Metrics *telemetry.Metrics
	Logger  *slog.Logger
}

var _ Sink = (*StoreSink)(nil)

// NewStoreSink applies defaults.
func NewStoreSink(st *store.Store, metrics *telemetry.Metrics, logger *slog.Logger) *StoreSink {
	if metrics == nil {
		metrics = &telemetry.Metrics{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &StoreSink{Store: st, Metrics: metrics, Logger: logger}
}

// Accept persists the event after the dedup prefix is applied.
func (s *StoreSink) Accept(ctx context.Context, ev CanonicalEvent) (bool, error) {
	if err := ev.Validate(); err != nil {
		s.Metrics.EventsRejected.Add(1)
		return false, err
	}
	inserted, err := s.Store.Enqueue(ctx, aoa.Inbound{
		SourceID:   ev.SourceID,
		Kind:       ev.Kind,
		SessionID:  ev.SessionID,
		Payload:    ev.Payload,
		ResumeJSON: ev.ResumeJSON,
	})
	if err != nil {
		s.Metrics.EventsRejected.Add(1)
		return false, err
	}
	if inserted {
		s.Metrics.EventsInbound.Add(1)
	} else {
		s.Metrics.EventsDuplicate.Add(1)
	}
	s.Logger.Debug("inbox enqueue",
		"source_id", ev.SourceID,
		"kind", ev.Kind,
		"session_id", ev.SessionID,
		"inserted", inserted,
		"trace_id", ev.TraceID,
	)
	return inserted, nil
}

// CountingSink is a Decorator used in tests and for audit pipelines.
type CountingSink struct {
	Inner  Sink
	Events []CanonicalEvent
}

// Accept records the event and forwards it.
func (s *CountingSink) Accept(ctx context.Context, ev CanonicalEvent) (bool, error) {
	s.Events = append(s.Events, ev)
	if s.Inner == nil {
		return true, nil
	}
	return s.Inner.Accept(ctx, ev)
}
