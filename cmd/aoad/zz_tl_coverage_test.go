package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/projanvil/aoa/internal/ingress"
)

func tlRunEnv(t *testing.T, addr string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AOA_DB", ":memory:")
	t.Setenv("AOA_CKPT_DB", filepath.Join(dir, "ckpt.db"))
	t.Setenv("AOA_MEMORY_DIR", filepath.Join(dir, "memory"))
	t.Setenv("AOA_ADDR", addr)
	t.Setenv("AOA_MODEL", "offline")
	t.Setenv("AOA_LOG_FORMAT", "text")
	t.Setenv("AOA_LOG_LEVEL", "info")
	t.Setenv("AOA_PROXY_TOKEN", "test-token")
	t.Setenv("AOA_PROXY_ALLOWLIST", "gateway.internal")
	t.Setenv("AOA_SIGNATURE_KEY", "")
	t.Setenv("AOA_OUTBOX_WEBHOOK", "")
	t.Setenv("AOA_ADAPTERS", "cloudevents,dapr,webhook,kubernetes")
	t.Setenv("AOA_BROKERS", "")
	t.Setenv("AOA_WATERMILL_REDIS", "")
}

func TestTLRunStartupAndShutdown(t *testing.T) {
	tlRunEnv(t, "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not shut down")
	}
}

func TestTLRunWithBrokerListeners(t *testing.T) {
	tlRunEnv(t, "127.0.0.1:0")
	t.Setenv("AOA_BROKERS", "nats,kafka,watermill")
	t.Setenv("AOA_WATERMILL_REDIS", "127.0.0.1:1")
	t.Setenv("AOA_NATS_URL", "nats://127.0.0.1:1")
	t.Setenv("AOA_KAFKA_BROKERS", "127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run with brokers returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run with brokers did not shut down")
	}
}

func TestTLRunEarlyFailures(t *testing.T) {
	cases := []struct {
		name  string
		apply func(t *testing.T)
	}{
		{"bad db", func(t *testing.T) {
			t.Setenv("AOA_DB", filepath.Join(t.TempDir(), "missing", "aoa.db"))
		}},
		{"bad memory dir", func(t *testing.T) {
			blocker := filepath.Join(t.TempDir(), "blocker")
			if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AOA_MEMORY_DIR", filepath.Join(blocker, "memory"))
		}},
		{"bad model", func(t *testing.T) {
			t.Setenv("AOA_MODEL", "not-a-valid-model-spec")
		}},
		{"missing proxy token", func(t *testing.T) {
			t.Setenv("AOA_PROXY_TOKEN", "")
		}},
		{"unknown adapter", func(t *testing.T) {
			t.Setenv("AOA_ADAPTERS", "unknown-adapter")
		}},
		{"duplicate adapter path", func(t *testing.T) {
			t.Setenv("AOA_ADAPTERS", "cloudevents,cloudevents")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tlRunEnv(t, "127.0.0.1:0")
			tc.apply(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := run(ctx); err == nil {
				t.Fatal("run must fail in this configuration")
			}
		})
	}
}

func TestTLRouteForAllNames(t *testing.T) {
	reg := ingress.DefaultRegistry()
	cfg := ingress.Config{}
	want := map[string]string{
		"dapr":        "/dapr/events/aoa-events",
		"webhook":     "/webhook",
		"kubernetes":  "/k8s/events",
		"cloudevents": "/events",
	}
	for name, path := range want {
		d, err := reg.NewDecoder(name, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got := routeFor(name, d); got != path {
			t.Fatalf("routeFor(%q) = %q, want %q", name, got, path)
		}
	}
	ce, err := reg.NewDecoder("cloudevents", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := routeFor("dapr", ce); got != "/dapr/events/aoa-events" {
		t.Fatalf("fallback route = %q", got)
	}
}

func TestTLAdapterOptionsShortKeys(t *testing.T) {
	t.Setenv("AOA_ADAPTER_TOPIC", "topic-x")
	t.Setenv("AOA_ADAPTER_PUBSUB", "pubsub-x")
	t.Setenv("AOA_NATS_URL", "nats://127.0.0.1:1")
	opts := adapterOptions()
	if opts["topic"] == "" || opts["pubsub"] == "" || opts["url"] != "nats://127.0.0.1:1" {
		t.Fatalf("short option aliases missing: %v", opts)
	}
}
