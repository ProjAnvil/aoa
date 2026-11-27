// Command aoad runs the always-on agent as a stateless set of loops: ingress,
// worker, scheduler, and dispatcher. All state lives in SQLite tables and
// the memory folder, so a replacement process can take over at any point.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/projanvil/langchain-golang/core/language"
	"github.com/projanvil/langchain-golang/langchain/chatmodels"
	checkpointserde "github.com/projanvil/langchain-golang/langgraph/checkpoint/serde"
	checkpointsqlite "github.com/projanvil/langchain-golang/langgraph/checkpoint/sqlite"

	// Blank imports register provider factories through init().
	_ "github.com/projanvil/langchain-golang/partners/anthropic"
	_ "github.com/projanvil/langchain-golang/partners/gemini"
	_ "github.com/projanvil/langchain-golang/partners/ollama"
	_ "github.com/projanvil/langchain-golang/partners/openai"
	_ "github.com/projanvil/langchain-golang/partners/openaicompat"

	"github.com/projanvil/aoa/internal/agent"
	"github.com/projanvil/aoa/internal/httpserver"
	"github.com/projanvil/aoa/internal/ingress"
	"github.com/projanvil/aoa/internal/memory"
	"github.com/projanvil/aoa/internal/permission"
	"github.com/projanvil/aoa/internal/runtime"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func list(key string) []string {
	var out []string
	for _, part := range strings.Split(env(key, ""), ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func buildModel(spec string) (language.ChatModel, error) {
	if spec == "" || spec == "offline" {
		return agent.NewDemoModel(), nil
	}
	parsed, err := chatmodels.ParseModelString(spec)
	if err != nil {
		return nil, err
	}
	return chatmodels.Resolve(parsed)
}

func adapterOptions() map[string]string {
	keys := []string{
		"AOA_ADAPTER_TOPIC", "AOA_ADAPTER_PUBSUB", "AOA_ADAPTER_ROUTE",
		"AOA_NATS_URL", "AOA_NATS_SUBJECT", "AOA_NATS_STREAM", "AOA_NATS_DURABLE",
		"AOA_KAFKA_BROKERS", "AOA_KAFKA_TOPIC", "AOA_KAFKA_GROUP",
		"AOA_WATERMILL_TOPIC",
	}
	out := make(map[string]string)
	for _, key := range keys {
		v := os.Getenv(key)
		if v == "" {
			continue
		}
		long := strings.ToLower(strings.TrimPrefix(key, "AOA_"))
		out[long] = v
		// Adapters read short keys such as "url" or "topic".
		for _, prefix := range []string{"nats_", "kafka_", "watermill_", "adapter_"} {
			if strings.HasPrefix(long, prefix) {
				out[strings.TrimPrefix(long, prefix)] = v
			}
		}
	}
	return out
}

func routeFor(name string, d ingress.Decoder) string {
	switch name {
	case "dapr":
		if provider, ok := d.(*ingress.DaprDecoder); ok {
			return provider.SubscriptionRoute()
		}
		return "/dapr/events/aoa-events"
	case "webhook":
		return "/webhook"
	case "kubernetes":
		return "/k8s/events"
	default:
		return "/events"
	}
}

func buildRoutes(reg *ingress.Registry, names []string, cfg ingress.Config) ([]ingress.Route, error) {
	if len(names) == 0 {
		names = []string{"cloudevents"}
	}
	routes := make([]ingress.Route, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		d, err := reg.NewDecoder(name, cfg)
		if err != nil {
			return nil, err
		}
		path := routeFor(name, d)
		if seen[path] {
			return nil, fmt.Errorf("duplicate ingress path %q", path)
		}
		seen[path] = true
		routes = append(routes, ingress.Route{Path: path, Decoder: d})
	}
	return routes, nil
}

func run(ctx context.Context) error {
	logger := telemetry.NewLogger(os.Stdout, env("AOA_LOG_FORMAT", "json"), env("AOA_LOG_LEVEL", "info"))
	slog.SetDefault(logger)
	metrics := &telemetry.Metrics{}

	st, err := store.Open(env("AOA_DB", "aoa.db"))
	if err != nil {
		return err
	}
	defer st.Close()

	mem, err := memory.Open(env("AOA_MEMORY_DIR", "memory"))
	if err != nil {
		return err
	}

	saver, err := checkpointsqlite.New(env("AOA_CKPT_DB", "aoa-ckpt.db"), checkpointserde.NewJSONSerializer())
	if err != nil {
		return err
	}
	defer saver.Close()

	model, err := buildModel(env("AOA_MODEL", "offline"))
	if err != nil {
		return fmt.Errorf("resolve model: %w", err)
	}

	proxy, err := permission.NewSafeProxy(
		os.Getenv("AOA_PROXY_TOKEN"),
		env("AOA_PROXY_ALLOWLIST", ""),
	)
	if err != nil {
		return err
	}

	worker := runtime.NewWorker(runtime.WorkerConfig{
		Store:          st,
		Memory:         mem,
		Saver:          saver,
		Model:          model,
		Classifier:     permission.DefaultClassifier(),
		Proxy:          proxy,
		Logger:         logger,
		Metrics:        metrics,
		RunTimeout:     5 * time.Minute,
		RecursionLimit: 64,
	})
	scheduler := runtime.NewScheduler(runtime.SchedulerConfig{
		Store: st, Logger: logger, Metrics: metrics,
	})
	dispatcher := runtime.NewDispatcher(runtime.DispatcherConfig{
		Store:       st,
		Webhook:     os.Getenv("AOA_OUTBOX_WEBHOOK"),
		Logger:      logger,
		Metrics:     metrics,
		MinInterval: 30 * time.Second,
	})

	sink := ingress.NewStoreSink(st, metrics, logger)
	reg := ingress.DefaultRegistry()
	adapterCfg := ingress.Config{
		Options: adapterOptions(),
		Store:   st,
		Logger:  logger,
		Metrics: metrics,
	}
	if redisAddr := os.Getenv("AOA_WATERMILL_REDIS"); redisAddr != "" {
		sub, err := ingress.NewRedisStreamSubscriber(redisAddr)
		if err != nil {
			return err
		}
		defer sub.Close()
		adapterCfg.Any = map[string]any{"subscriber": sub}
		logger.Info("watermill redis subscriber enabled", "addr", redisAddr)
	}
	routes, err := buildRoutes(reg, list("AOA_ADAPTERS"), adapterCfg)
	if err != nil {
		return err
	}

	ingressServer := httpserver.NewServer(httpserver.ServerConfig{
		Store:        st,
		Routes:       routes,
		Sink:         sink,
		SignatureKey: os.Getenv("AOA_SIGNATURE_KEY"),
		Logger:       logger,
		Metrics:      metrics,
	})
	addr := env("AOA_ADDR", ":8080")
	srv := &http.Server{
		Addr:              addr,
		Handler:           ingressServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	loops := []func(context.Context) error{worker.Run, scheduler.Run, dispatcher.Run}
	for _, loop := range loops {
		go func(fn func(context.Context) error) {
			if err := fn(ctx); err != nil {
				logger.Error("background loop stopped", "component", "loop", "error", err)
			}
		}(loop)
	}

	var listeners []ingress.Listener
	for _, name := range list("AOA_BROKERS") {
		l, err := reg.NewListener(name, adapterCfg)
		if err != nil {
			return err
		}
		listeners = append(listeners, l)
		go func(listener ingress.Listener) {
			logger.Info("broker listener starting", "adapter", listener.Name())
			if err := listener.Start(ctx, sink); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("broker listener stopped", "adapter", listener.Name(), "error", err)
			}
		}(l)
	}

	go func() {
		logger.Info("ingress listening", "addr", addr, "adapters", reg.DecoderNames())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("ingress stopped", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, l := range listeners {
		if err := l.Stop(shutdownCtx); err != nil {
			logger.Warn("broker listener stop failed", "adapter", l.Name(), "error", err)
		}
	}
	return srv.Shutdown(shutdownCtx)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("aoad failed", "error", err)
		os.Exit(1)
	}
}
