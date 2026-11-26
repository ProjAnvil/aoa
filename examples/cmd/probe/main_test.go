package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestHelloWithFakeOllama(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"model":"qwen2.5:0.5b","created_at":"2026-01-01T00:00:00Z",`+
			`"message":{"role":"assistant","content":"Hello, world!"},"done":true}`)
	}))
	defer srv.Close()
	t.Setenv("OLLAMA_HOST", srv.URL)
	if err := hello([]string{"--model", "qwen2.5:0.5b"}); err != nil {
		t.Fatal(err)
	}
}

func TestAgentHelloWithFakeOllama(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"model":"qwen2.5:0.5b","created_at":"2026-01-01T00:00:00Z",`+
			`"message":{"role":"assistant","content":"Hello, world!"},"done":true}`)
	}))
	defer srv.Close()
	t.Setenv("OLLAMA_HOST", srv.URL)
	if err := agentHello([]string{"--model", "qwen2.5:0.5b"}); err != nil {
		t.Fatal(err)
	}
}

func TestEnvFallback(t *testing.T) {
	t.Setenv("AOA_PROBE_TEST", "value")
	if got := env("AOA_PROBE_TEST", "x"); got != "value" {
		t.Fatalf("env = %q", got)
	}
	if got := env("AOA_PROBE_MISSING", "x"); got != "x" {
		t.Fatalf("env fallback = %q", got)
	}
}

func TestParseEmitDefaultsAndFlags(t *testing.T) {
	f, err := parseEmit("emit", nil)
	if err != nil {
		t.Fatal(err)
	}
	if f.mode != "cloudevents" || f.session != "s1" || f.sourceID == "" ||
		f.target != "http://aoad:8080/events" {
		t.Fatalf("defaults = %+v", f)
	}
	custom, err := parseEmit("emit", []string{
		"--mode", "dapr", "--session", "s9", "--source-id", "fixed",
		"--payload", "alert", "--target", "http://x/events",
	})
	if err != nil {
		t.Fatal(err)
	}
	if custom.mode != "dapr" || custom.session != "s9" || custom.sourceID != "fixed" ||
		custom.payload != "alert" || custom.target != "http://x/events" {
		t.Fatalf("flags = %+v", custom)
	}
}

func TestEmitBodyModes(t *testing.T) {
	cases := []struct {
		mode     string
		contains string
	}{
		{"cloudevents", `"specversion":"1.0"`},
		{"dapr", `"pubsubname":"aoa"`},
		{"webhook", `"source_id":"id-1"`},
		{"admission", `"kind":"AdmissionReview"`},
	}
	for _, tc := range cases {
		f, err := parseEmit("emit", []string{"--mode", tc.mode, "--source-id", "id-1"})
		if err != nil {
			t.Fatal(err)
		}
		body, err := emitBody(f)
		if err != nil {
			t.Fatalf("%s: %v", tc.mode, err)
		}
		if !json.Valid(body) || !strings.Contains(string(body), tc.contains) {
			t.Fatalf("%s body = %s", tc.mode, body)
		}
	}
	if _, err := emitBody(&emitFlags{mode: "unknown"}); err == nil {
		t.Fatal("unknown mode must fail")
	}
}

func TestPostOnceStatusHandling(t *testing.T) {
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		count++
		n := count
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	f, err := parseEmit("emit", []string{"--target", srv.URL, "--source-id", "fixed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := postOnce(f); err != nil {
		t.Fatal(err)
	}
	if err := postOnce(f); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if count != 2 {
		t.Fatalf("requests = %d, want 2", count)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	f.target = bad.URL
	if err := postOnce(f); err == nil {
		t.Fatal("5xx must surface an error")
	}
}

func TestScenarioDedupAndMemory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		_ = json.Unmarshal(buf[:n], &map[string]any{})
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	if err := scenario([]string{"dedup", "--target", srv.URL}); err != nil {
		t.Fatal(err)
	}
	if err := scenario([]string{"memory", "--target", srv.URL}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"timer", "approval"} {
		if err := scenario([]string{name, "--target", srv.URL}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if err := scenario(nil); err == nil {
		t.Fatal("missing scenario name must fail")
	}
	if err := scenario([]string{"unknown", "--target", srv.URL}); err != nil {
		t.Fatalf("unknown scenario: %v", err)
	}
}
