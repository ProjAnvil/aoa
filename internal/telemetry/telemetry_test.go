package telemetry

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoggerAndLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "json", "debug")
	logger.Debug("probe", "key", "value")
	if !strings.Contains(buf.String(), `"level":"DEBUG"`) ||
		!strings.Contains(buf.String(), `"key":"value"`) {
		t.Fatalf("json log = %q", buf.String())
	}
	for in, want := range map[string]slog.Level{
		"debug": slog.LevelDebug, "WARN": slog.LevelWarn,
		"error": slog.LevelError, "other": slog.LevelInfo,
	} {
		if got := ParseLevel(in); got != want {
			t.Fatalf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestTraceHelpers(t *testing.T) {
	id := NewTraceID()
	if len(id) != 32 {
		t.Fatalf("trace id = %q", id)
	}
	ctx := WithTrace(t.Context(), id)
	if got := TraceFromContext(ctx); got != id {
		t.Fatalf("TraceFromContext = %q", got)
	}
	parent := strings.Repeat("a", 32)
	if got := TraceFromHeader("00-" + parent + "-01-01"); got != parent {
		t.Fatalf("TraceFromHeader = %q", got)
	}
	if got := TraceFromHeader("broken"); len(got) != 32 {
		t.Fatalf("fresh trace id = %q", got)
	}
}

func TestRedact(t *testing.T) {
	out := Redact("token=abc123 secret=xyz password=pw api_key=k", 1024)
	for _, leak := range []string{"abc123", "xyz", "pw", "api_key=k"} {
		if strings.Contains(out, leak) {
			t.Fatalf("redacted text leaked %q: %s", leak, out)
		}
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("no marker: %s", out)
	}
	if got := Redact(strings.Repeat("x", 500), 32); len(got) != 35 {
		t.Fatalf("truncated length = %d", len(got))
	}
}

func TestMetricsHandler(t *testing.T) {
	m := &Metrics{}
	m.EventsInbound.Add(2)
	m.RunsFailed.Add(1)
	snap := m.Snapshot()
	if snap["events_inbound"] != 2 || snap["runs_failed"] != 1 {
		t.Fatalf("snapshot = %v", snap)
	}
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var decoded map[string]int64
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["events_inbound"] != 2 {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestAccessLogAndRecover(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "json", "info")
	h := AccessLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}), logger, &Metrics{})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
	if w.Code != http.StatusTeapot || !strings.Contains(buf.String(), "trace_id") ||
		!strings.Contains(buf.String(), `"status":418`) {
		t.Fatalf("access log = %q, status = %d", buf.String(), w.Code)
	}

	panicHandler := Recover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}), logger)
	w2 := httptest.NewRecorder()
	panicHandler.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/panic", nil))
	if w2.Code != http.StatusInternalServerError {
		t.Fatalf("panic status = %d", w2.Code)
	}
	if !strings.Contains(buf.String(), "http panic") {
		t.Fatalf("panic not logged: %q", buf.String())
	}
}

func TestFields(t *testing.T) {
	fields := Fields("worker", "t1", "s1", "e1")
	if len(fields) != 8 || fields[1] != "worker" || fields[3] != "t1" {
		t.Fatalf("fields = %v", fields)
	}
}
