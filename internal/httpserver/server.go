// Package httpserver keeps the public ingress API stable. The protocol work
// lives in internal/ingress; this file is a thin composition shell.
package httpserver

import (
	"log/slog"
	"net/http"

	"github.com/projanvil/aoa/internal/ingress"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// ServerConfig wires the ingress. Routes defaults to one CloudEvents route
// at /events, which is the Knative Eventing Trigger target.
type ServerConfig struct {
	Store           *store.Store
	Routes          []ingress.Route
	Sink            ingress.Sink
	SignatureKey    string
	SignatureHeader string
	MaxBody         int64
	Logger          *slog.Logger
	Metrics         *telemetry.Metrics
}

// Server holds the composed handler.
type Server struct {
	handler http.Handler
}

// NewServer composes the ingress chain. A missing Store produces a handler
// that fails closed instead of panicking at startup.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &telemetry.Metrics{}
	}
	routes := cfg.Routes
	if len(routes) == 0 {
		routes = []ingress.Route{{
			Path:    "/events",
			Decoder: ingress.NewCloudEventsDecoder(ingress.Config{Logger: cfg.Logger, Metrics: cfg.Metrics}),
		}}
	}
	handler, err := ingress.NewHandler(ingress.ServerConfig{
		Store:           cfg.Store,
		Routes:          routes,
		Sink:            cfg.Sink,
		SignatureKey:    cfg.SignatureKey,
		SignatureHeader: cfg.SignatureHeader,
		MaxBody:         cfg.MaxBody,
		Logger:          cfg.Logger,
		Metrics:         cfg.Metrics,
	})
	if err != nil {
		cfg.Logger.Error("ingress disabled", "error", err)
		handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "ingress misconfigured", http.StatusServiceUnavailable)
		})
	}
	return &Server{handler: handler}
}

// Handler returns the composed HTTP handler.
func (s *Server) Handler() http.Handler { return s.handler }
