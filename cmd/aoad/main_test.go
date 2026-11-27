package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/projanvil/aoa/internal/ingress"
)

func TestEnvAndList(t *testing.T) {
	t.Setenv("AOA_TEST_VALUE", "set")
	if got := env("AOA_TEST_VALUE", "fallback"); got != "set" {
		t.Fatalf("env = %q", got)
	}
	if got := env("AOA_MISSING_VALUE", "fallback"); got != "fallback" {
		t.Fatalf("env fallback = %q", got)
	}
	t.Setenv("AOA_TEST_LIST", " cloudevents, , dapr ,webhook ")
	got := list("AOA_TEST_LIST")
	if strings.Join(got, ",") != "cloudevents,dapr,webhook" {
		t.Fatalf("list = %v", got)
	}
	if len(list("AOA_EMPTY_LIST")) != 0 {
		t.Fatal("missing list must be empty")
	}
}

func TestBuildModelOffline(t *testing.T) {
	model, err := buildModel("offline")
	if err != nil || model == nil {
		t.Fatalf("offline model = %v, %v", model, err)
	}
	if _, err := buildModel("broken-spec"); err == nil {
		t.Fatal("invalid model spec must fail")
	}
}

func TestAdapterOptions(t *testing.T) {
	t.Setenv("AOA_NATS_URL", "nats://localhost:4222")
	t.Setenv("AOA_KAFKA_TOPIC", "aoa-test")
	opts := adapterOptions()
	if opts["nats_url"] != "nats://localhost:4222" || opts["kafka_topic"] != "aoa-test" {
		t.Fatalf("options = %v", opts)
	}
	// Factories read short keys; adapterOptions emits both forms.
	if opts["url"] != "nats://localhost:4222" || opts["topic"] != "aoa-test" {
		t.Fatalf("short option aliases missing: %v", opts)
	}
}

func TestRouteForAndBuildRoutes(t *testing.T) {
	reg := ingress.DefaultRegistry()
	cfg := ingress.Config{Options: map[string]string{"topic": "custom-topic"}}
	routes, err := buildRoutes(reg, []string{"cloudevents", "dapr", "webhook", "kubernetes"}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 4 {
		t.Fatalf("routes = %d", len(routes))
	}
	wantPaths := map[string]bool{
		"/events": true, "/dapr/events/custom-topic": true,
		"/webhook": true, "/k8s/events": true,
	}
	for _, route := range routes {
		if !wantPaths[route.Path] {
			t.Fatalf("unexpected path %q", route.Path)
		}
		delete(wantPaths, route.Path)
	}
	if len(wantPaths) != 0 {
		t.Fatalf("missing paths: %v", wantPaths)
	}
	if _, err := buildRoutes(reg, []string{"cloudevents", "cloudevents"}, cfg); err == nil {
		t.Fatal("duplicate adapter paths must fail")
	}
	if _, err := buildRoutes(reg, []string{"unknown"}, cfg); err == nil {
		t.Fatal("unknown adapter must fail")
	}
	defaults, err := buildRoutes(reg, nil, cfg)
	if err != nil || len(defaults) != 1 || defaults[0].Path != "/events" {
		t.Fatalf("defaults = %v, %v", defaults, err)
	}
}

// setRunEnv installs a deterministic, fully offline environment for run().
// The returned directory hosts the SQLite checkpoint database and the memory
// folder, both of which must be writable.
func setRunEnv(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AOA_DB", ":memory:")
	t.Setenv("AOA_CKPT_DB", filepath.Join(dir, "ckpt.db"))
	t.Setenv("AOA_MEMORY_DIR", filepath.Join(dir, "memory"))
	t.Setenv("AOA_ADDR", "127.0.0.1:0")
	t.Setenv("AOA_MODEL", "offline")
	t.Setenv("AOA_PROXY_TOKEN", "test-token")
	t.Setenv("AOA_PROXY_ALLOWLIST", "")
	t.Setenv("AOA_LOG_FORMAT", "text")
	t.Setenv("AOA_LOG_LEVEL", "info")
	t.Setenv("AOA_ADAPTERS", "cloudevents")
	t.Setenv("AOA_BROKERS", "")
	t.Setenv("AOA_WATERMILL_REDIS", "")
	t.Setenv("AOA_SIGNATURE_KEY", "")
	t.Setenv("AOA_OUTBOX_WEBHOOK", "")
	// Clear every adapter option key so an ambient environment cannot change
	// the decoder configuration built inside run().
	for _, key := range []string{
		"AOA_ADAPTER_TOPIC", "AOA_ADAPTER_PUBSUB", "AOA_ADAPTER_ROUTE",
		"AOA_NATS_URL", "AOA_NATS_SUBJECT", "AOA_NATS_STREAM", "AOA_NATS_DURABLE",
		"AOA_KAFKA_BROKERS", "AOA_KAFKA_TOPIC", "AOA_KAFKA_GROUP",
		"AOA_WATERMILL_TOPIC",
	} {
		t.Setenv(key, "")
	}
	return dir
}

func TestBuildModelSpecs(t *testing.T) {
	// An empty spec and the "offline" spec both build the scripted demo model.
	for _, spec := range []string{"", "offline"} {
		model, err := buildModel(spec)
		if err != nil || model == nil {
			t.Fatalf("buildModel(%q) = %v, %v", spec, model, err)
		}
	}
	// A spec without a provider separator fails inside ParseModelString.
	if _, err := buildModel("broken"); err == nil {
		t.Fatal("buildModel(broken) must fail")
	}
	// A well-formed provider:model spec reaches chatmodels.Resolve.
	if _, err := buildModel("ollama:qwen2.5:0.5b"); err != nil {
		t.Fatalf("buildModel(valid spec) unexpected error: %v", err)
	}
}

func TestAdapterOptionsAllKeys(t *testing.T) {
	values := map[string]string{
		"AOA_ADAPTER_TOPIC":   "adapter-topic",
		"AOA_ADAPTER_PUBSUB":  "adapter-pubsub",
		"AOA_ADAPTER_ROUTE":   "adapter-route",
		"AOA_NATS_URL":        "nats://localhost:4222",
		"AOA_NATS_SUBJECT":    "nats-subject",
		"AOA_NATS_STREAM":     "nats-stream",
		"AOA_NATS_DURABLE":    "nats-durable",
		"AOA_KAFKA_BROKERS":   "kafka:9092",
		"AOA_KAFKA_TOPIC":     "kafka-topic",
		"AOA_KAFKA_GROUP":     "kafka-group",
		"AOA_WATERMILL_TOPIC": "watermill-topic",
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
	opts := adapterOptions()
	for key, value := range values {
		long := strings.ToLower(strings.TrimPrefix(key, "AOA_"))
		if opts[long] != value {
			t.Fatalf("long option %q = %q, want %q", long, opts[long], value)
		}
	}
	// Short aliases are derived from the prefixed long keys.
	if opts["url"] != values["AOA_NATS_URL"] ||
		opts["subject"] != values["AOA_NATS_SUBJECT"] ||
		opts["brokers"] != values["AOA_KAFKA_BROKERS"] {
		t.Fatalf("short aliases missing: %v", opts)
	}
}

func TestRouteForBranches(t *testing.T) {
	// A decoder that is not *ingress.DaprDecoder forces the type-assertion
	// fallback inside the "dapr" case.
	nonDapr := ingress.DecoderFunc{Label: "plain"}
	cases := []struct {
		name string
		want string
	}{
		{"dapr", "/dapr/events/aoa-events"},
		{"webhook", "/webhook"},
		{"kubernetes", "/k8s/events"},
		{"cloudevents", "/events"},
		{"anything-else", "/events"},
	}
	for _, tc := range cases {
		if got := routeFor(tc.name, nonDapr); got != tc.want {
			t.Fatalf("routeFor(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestRunEndToEnd(t *testing.T) {
	setRunEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()

	// Give the loops and the HTTP server time to start, then request shutdown.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return within 2s after context cancellation")
	}
}

func TestRunEarlyErrors(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T)
	}{
		{"store-open", func(t *testing.T) {
			t.Setenv("AOA_DB", filepath.Join(t.TempDir(), "missing", "aoa.db"))
		}},
		{"memory-open", func(t *testing.T) {
			dir := t.TempDir()
			blocker := filepath.Join(dir, "blocker")
			if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AOA_MEMORY_DIR", filepath.Join(blocker, "memory"))
		}},
		{"saver-open", func(t *testing.T) {
			t.Setenv("AOA_CKPT_DB", filepath.Join(t.TempDir(), "missing", "ckpt.db"))
		}},
		{"model-resolve", func(t *testing.T) {
			t.Setenv("AOA_MODEL", "broken")
		}},
		{"proxy-token", func(t *testing.T) {
			t.Setenv("AOA_PROXY_TOKEN", "")
		}},
		{"build-routes", func(t *testing.T) {
			t.Setenv("AOA_ADAPTERS", "unknown")
		}},
		// A non-empty watermill address exercises the redis subscriber branch
		// (constructed without dialing); an unknown adapter then fails later.
		{"redis-subscriber", func(t *testing.T) {
			t.Setenv("AOA_WATERMILL_REDIS", "127.0.0.1:1")
			t.Setenv("AOA_ADAPTERS", "unknown")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRunEnv(t)
			tc.setup(t)
			// Every case returns before any background loop is launched, so a
			// plain background context is sufficient and nothing leaks.
			if err := run(context.Background()); err == nil {
				t.Fatal("run must fail for this configuration")
			}
		})
	}
}

func TestRunUnknownBroker(t *testing.T) {
	setRunEnv(t)
	t.Setenv("AOA_BROKERS", "unknown")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := run(ctx); err == nil {
		t.Fatal("run must fail when a broker listener cannot be built")
	}
	// Stop the background loops that started before the broker failure.
	cancel()
}
