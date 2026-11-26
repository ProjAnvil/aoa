// Package telemetry provides structured logging, trace correlation, and
// lock-free counters for the service. It never logs secrets or raw payloads.
package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

type ctxKey string

const traceKey ctxKey = "aoa.trace"

// NewLogger builds a structured logger. JSON is the production format; text
// keeps local runs readable.
func NewLogger(w io.Writer, format, level string) *slog.Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: ParseLevel(level)}
	if format == "json" {
		handler = slog.NewJSONHandler(w, opts)
	} else {
		handler = slog.NewTextHandler(w, opts)
	}
	return slog.New(handler)
}

// ParseLevel maps a config string to a slog level. Unknown values mean info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// NewTraceID returns a 16-byte hex trace identifier.
func NewTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}

// WithTrace attaches a trace id to a context.
func WithTrace(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceKey, id)
}

// TraceFromContext returns the trace id or an empty string.
func TraceFromContext(ctx context.Context) string {
	v, _ := ctx.Value(traceKey).(string)
	return v
}

// TraceFromHeader reuses an incoming W3C traceparent when present. A missing
// or malformed value produces a fresh id.
func TraceFromHeader(value string) string {
	parts := strings.Split(value, "-")
	if len(parts) >= 2 && len(parts[1]) == 32 {
		return parts[1]
	}
	return NewTraceID()
}

// Redact truncates a payload and removes token-like material before it
// reaches a log line. Logs carry pointers, not stored facts.
func Redact(s string, limit int) string {
	if limit <= 0 {
		limit = 256
	}
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	return redactMarkers(s)
}

var sensitivePattern = regexp.MustCompile(
	`(?i)(token|password|authorization|api_key|secret|bearer)\s*[=:]\s*[^\s&"',;]+`)

func redactMarkers(s string) string {
	return sensitivePattern.ReplaceAllString(s, "$1=[REDACTED]")
}

// Fields returns the correlation fields every service log line carries.
func Fields(component, traceID, sessionID, eventID string) []any {
	return []any{
		"component", component,
		"trace_id", traceID,
		"session_id", sessionID,
		"event_id", eventID,
	}
}

// Metrics holds process-local counters. A scrape endpoint exposes them;
// no external metrics server is required.
type Metrics struct {
	EventsInbound   atomic.Int64
	EventsDuplicate atomic.Int64
	EventsRejected  atomic.Int64
	RunsTotal       atomic.Int64
	RunsFailed      atomic.Int64
	TimersFired     atomic.Int64
	OutboxSent      atomic.Int64
	OutboxFailed    atomic.Int64
	Approvals       atomic.Int64
	LeaseContention atomic.Int64
}

// Snapshot returns every counter as a plain map.
func (m *Metrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"events_inbound":   m.EventsInbound.Load(),
		"events_duplicate": m.EventsDuplicate.Load(),
		"events_rejected":  m.EventsRejected.Load(),
		"runs_total":       m.RunsTotal.Load(),
		"runs_failed":      m.RunsFailed.Load(),
		"timers_fired":     m.TimersFired.Load(),
		"outbox_sent":      m.OutboxSent.Load(),
		"outbox_failed":    m.OutboxFailed.Load(),
		"approvals":        m.Approvals.Load(),
		"lease_contention": m.LeaseContention.Load(),
	}
}

// Handler exposes counters as JSON for /metrics.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.Snapshot())
	})
}

// statusRecorder captures the response status for access logs.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

// AccessLog is HTTP middleware that emits one structured line per request.
func AccessLog(next http.Handler, logger *slog.Logger, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := TraceFromHeader(r.Header.Get("traceparent"))
		ctx := WithTrace(r.Context(), traceID)
		rec := &statusRecorder{ResponseWriter: w}
		defer func() {
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"trace_id", traceID,
			)
		}()
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		next.ServeHTTP(rec, r.WithContext(ctx))
		_ = metrics
	})
}

// Recover turns a panic in a handler into a 500 without killing the process.
func Recover(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				logger.Error("http panic", "error", v, "path", r.URL.Path,
					"trace_id", TraceFromContext(r.Context()))
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
