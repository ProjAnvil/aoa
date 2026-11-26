package main

import (
	"strings"
	"testing"

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
