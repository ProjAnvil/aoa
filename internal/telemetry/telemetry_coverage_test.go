package telemetry

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewLoggerTextBranch(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "text", "warn")
	logger.Info("suppressed")
	logger.Warn("visible", "key", "value")
	out := buf.String()
	if strings.Contains(out, "suppressed") {
		t.Fatalf("info should be filtered at warn level: %q", out)
	}
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "visible") ||
		!strings.Contains(out, "key=value") {
		t.Fatalf("text log = %q", out)
	}
}

func TestRedactUsesDefaultLimit(t *testing.T) {
	long := strings.Repeat("x", 300)
	got := Redact(long, 0)
	if len(got) != 259 { // 256 bytes plus the ellipsis
		t.Fatalf("default-limit length = %d, want 259", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("default-limit output = %q", got)
	}
	negative := Redact("a\nb", -5)
	if strings.Contains(negative, "\n") {
		t.Fatalf("newline was not replaced: %q", negative)
	}
}

func TestStatusRecorderWrite(t *testing.T) {
	inner := httptest.NewRecorder()
	rec := &statusRecorder{ResponseWriter: inner}
	n, err := rec.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if rec.status != http.StatusOK {
		t.Fatalf("implicit status = %d, want 200", rec.status)
	}
	if inner.Body.String() != "hello" {
		t.Fatalf("body = %q", inner.Body.String())
	}

	// An explicit status must survive a subsequent Write.
	rec2 := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
	rec2.WriteHeader(http.StatusCreated)
	if _, err := rec2.Write([]byte("body")); err != nil {
		t.Fatal(err)
	}
	if rec2.status != http.StatusCreated {
		t.Fatalf("explicit status = %d, want 201", rec2.status)
	}
}

func TestAccessLogRecordsDefaultStatus(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "json", "info")
	h := AccessLog(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}), logger, &Metrics{})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/write-body", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(buf.String(), `"status":200`) ||
		!strings.Contains(buf.String(), `"path":"/write-body"`) {
		t.Fatalf("access log = %q", buf.String())
	}
}

func TestRecoverPassesThroughWithoutPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "json", "info")
	h := Recover(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}), logger)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}
	if strings.Contains(buf.String(), "http panic") {
		t.Fatalf("unexpected panic log: %q", buf.String())
	}
}
