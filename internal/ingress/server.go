package ingress

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// Route binds one HTTP path to one decoder.
type Route struct {
	Path    string
	Decoder Decoder
}

// ExtraRoute lets an adapter expose protocol-specific endpoints, such as
// Dapr's subscription discovery endpoint.
type ExtraRoute struct {
	Method  string
	Path    string
	Handler http.HandlerFunc
}

// RouteProvider is an optional Adapter capability.
type RouteProvider interface {
	ExtraRoutes() []ExtraRoute
}

// ServerConfig wires the HTTP ingress.
type ServerConfig struct {
	Store           *store.Store
	Routes          []Route
	Sink            Sink
	SignatureKey    string
	SignatureHeader string
	MaxBody         int64
	Logger          *slog.Logger
	Metrics         *telemetry.Metrics
}

type server struct {
	cfg ServerConfig
}

// NewHandler builds the full HTTP router. Every event route goes through the
// same chain: recover, access log, body limit, signature, decode, sink.
func NewHandler(cfg ServerConfig) (http.Handler, error) {
	if cfg.Store == nil {
		return nil, errors.New("ingress: Store is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &telemetry.Metrics{}
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = 1 << 20
	}
	if cfg.Sink == nil {
		cfg.Sink = NewStoreSink(cfg.Store, cfg.Metrics, cfg.Logger)
	}
	s := &server{cfg: cfg}
	mux := http.NewServeMux()

	for _, route := range cfg.Routes {
		if route.Path == "" || route.Decoder == nil {
			return nil, errors.New("ingress: route requires a path and decoder")
		}
		h := Chain(
			http.HandlerFunc(s.adapt(route.Decoder)),
			SignatureRequired(cfg.SignatureKey, cfg.SignatureHeader),
		)
		mux.Handle("POST "+route.Path, h)
		if provider, ok := route.Decoder.(RouteProvider); ok {
			for _, extra := range provider.ExtraRoutes() {
				mux.HandleFunc(extra.Method+" "+extra.Path, extra.Handler)
			}
		}
	}

	mux.Handle("POST /approvals", Chain(
		http.HandlerFunc(s.handleApproval),
		SignatureRequired(cfg.SignatureKey, cfg.SignatureHeader),
	))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": true})
	})
	mux.Handle("GET /metrics", cfg.Metrics.Handler())

	return HTTPStack(mux, cfg.Logger, cfg.Metrics, cfg.MaxBody), nil
}

func (s *server) adapt(d Decoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		traceID := telemetry.TraceFromContext(ctx)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			s.reject(w, d, traceID, "read body", http.StatusBadRequest, err)
			return
		}
		events, err := d.Decode(ctx, r, raw)
		if err != nil {
			s.reject(w, d, traceID, "decode", http.StatusBadRequest, err)
			return
		}
		inserted := false
		for i := range events {
			// Only /approvals may resume a session. External event sources
			// cannot inject a resume value or an approval decision.
			if events[i].Kind == aoa.KindApproval {
				s.reject(w, d, traceID, "approval kind is reserved", http.StatusBadRequest,
					errors.New("approval events must use /approvals"))
				return
			}
			events[i].ResumeJSON = ""
			if events[i].TraceID == "" {
				events[i].TraceID = traceID
			}
			ok, err := s.cfg.Sink.Accept(ctx, events[i])
			if err != nil {
				s.reject(w, d, traceID, "enqueue", http.StatusInternalServerError, err)
				return
			}
			inserted = inserted || ok
		}
		s.cfg.Logger.Info("event batch accepted",
			"adapter", d.Name(),
			"count", len(events),
			"inserted", inserted,
			"trace_id", traceID,
		)
		if responder, ok := d.(Responder); ok {
			responder.WriteResponse(w, events, inserted)
			return
		}
		status := http.StatusOK
		if inserted {
			status = http.StatusAccepted
		}
		writeAccepted(w, status)
	}
}

func (s *server) reject(w http.ResponseWriter, d Decoder, traceID, stage string, status int, cause error) {
	s.cfg.Metrics.EventsRejected.Add(1)
	s.cfg.Logger.Warn("event rejected",
		"adapter", d.Name(),
		"stage", stage,
		"status", status,
		"trace_id", traceID,
		"error", cause,
	)
	http.Error(w, stage+" failed", status)
}

func writeAccepted(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
}

type approvalRequest struct {
	ApprovalID string `json:"approval_id"`
	Decision   string `json:"decision"`
}

func (s *server) handleApproval(w http.ResponseWriter, r *http.Request) {
	traceID := telemetry.TraceFromContext(r.Context())
	var req approvalRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		s.cfg.Logger.Warn("invalid approval body", "trace_id", traceID, "error", err)
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if req.ApprovalID == "" {
		http.Error(w, "approval_id is required", http.StatusBadRequest)
		return
	}
	if req.Decision != "approve" && req.Decision != "reject" {
		http.Error(w, "decision must be approve or reject", http.StatusBadRequest)
		return
	}

	// Use a JSON array, not an object. The graph runtime treats a
	// map[string]any Resume as a dispatch table keyed by interrupt id.
	resume, err := json.Marshal([]any{map[string]any{"type": req.Decision}})
	if err != nil {
		http.Error(w, "encode resume", http.StatusInternalServerError)
		return
	}
	// Resolve and enqueue share one transaction. A crash between them can
	// neither lose the human decision nor create a second conflicting reply.
	inserted, err := s.cfg.Store.ResolveApprovalAndEnqueue(
		r.Context(), req.ApprovalID, req.Decision, string(resume))
	if errors.Is(err, store.ErrApprovalNotFound) {
		http.Error(w, "approval not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.cfg.Logger.Error("resolve approval", "trace_id", traceID, "error", err)
		http.Error(w, "resolve approval", http.StatusInternalServerError)
		return
	}
	if inserted {
		s.cfg.Metrics.EventsInbound.Add(1)
	} else {
		s.cfg.Metrics.EventsDuplicate.Add(1)
	}
	s.cfg.Logger.Info("approval recorded",
		"approval_id", req.ApprovalID,
		"decision", req.Decision,
		"inserted", inserted,
		"trace_id", traceID,
	)
	status := http.StatusOK
	if inserted {
		status = http.StatusAccepted
	}
	writeAccepted(w, status)
}
