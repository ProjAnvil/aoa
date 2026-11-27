package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTLTextLoggerAndStatusRecorderWrite(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "text", "warn")
	logger.Warn("text line", "k", "v")
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("text logger = %q", buf.String())
	}
	if ParseLevel("warning") != slog.LevelWarn {
		t.Fatal("ParseLevel(warning) must map to WARN")
	}

	var access bytes.Buffer
	accessLogger := NewLogger(&access, "json", "info")
	h := AccessLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n, err := w.Write([]byte("body"))
		if err != nil || n != 4 {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}), accessLogger, &Metrics{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/write", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "body" {
		t.Fatalf("response = %d %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(access.String(), `"status":200`) {
		t.Fatalf("access log = %q", access.String())
	}
}

func TestTLTraceAndRedactFallback(t *testing.T) {
	if got := TraceFromContext(context.Background()); got != "" {
		t.Fatalf("empty context trace = %q", got)
	}
	if got := TraceFromHeader("00-" + strings.Repeat("f", 32) + "-01-01"); got != strings.Repeat("f", 32) {
		t.Fatalf("traceparent = %q", got)
	}
	if got := Redact("authorization=abc.def", 0); strings.Contains(got, "abc.def") {
		t.Fatalf("redaction leaked: %q", got)
	}
	if got := Redact("line one\nline two", 512); strings.Contains(got, "\n") {
		t.Fatalf("newline was not removed: %q", got)
	}
}
