package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/telemetry"
)

// BrokerEvent builds a canonical event from an opaque broker message. A
// missing broker id falls back to a body hash so redelivery still dedups.
func BrokerEvent(prefix, brokerID string, payload []byte, kind aoa.Kind, sessionID string) CanonicalEvent {
	id := brokerID
	if id == "" {
		sum := sha256.Sum256(payload)
		id = "hash:" + hex.EncodeToString(sum[:])
	}
	if kind == "" {
		kind = aoa.KindBackground
	}
	return CanonicalEvent{
		SourceID:  prefix + id,
		Kind:      kind,
		SessionID: sessionID,
		Payload:   string(payload),
	}
}

// AcceptWithRetry forwards a broker message to the sink. A rejection Nacks the
// message so the broker redelivers it; an accepted duplicate is Acked.
func AcceptWithRetry(ctx context.Context, sink Sink, ev CanonicalEvent, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	inserted, err := sink.Accept(ctx, ev)
	if err != nil {
		logger.Error("broker message rejected",
			"source_id", ev.SourceID, "error", telemetry.Redact(err.Error(), 256))
		return err
	}
	logger.Debug("broker message accepted",
		"source_id", ev.SourceID, "inserted", inserted,
		"trace_id", ev.TraceID)
	return nil
}

// KindFromSubject maps a dotted topic suffix to an inbox kind.
func KindFromSubject(subject string) aoa.Kind {
	lower := strings.ToLower(subject)
	switch {
	case strings.Contains(lower, "approval"):
		return aoa.KindApproval
	case strings.Contains(lower, "timer"):
		return aoa.KindTimer
	case strings.Contains(lower, "message"):
		return aoa.KindUserMessage
	default:
		return aoa.KindBackground
	}
}
