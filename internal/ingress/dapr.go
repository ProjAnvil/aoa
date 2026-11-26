package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/projanvil/aoa/internal/aoa"
)

// DaprDecoder adapts Dapr pub/sub. Dapr delivers CloudEvents to the route
// returned by /dapr/subscribe, so this adapter reuses the CloudEvents
// strategy and adds the discovery endpoint.
type DaprDecoder struct {
	cfg      Config
	pubsub   string
	topic    string
	route    string
	fallback string
}

var (
	_ Decoder       = (*DaprDecoder)(nil)
	_ RouteProvider = (*DaprDecoder)(nil)
)

// NewDaprDecoder builds the adapter.
func NewDaprDecoder(cfg Config) *DaprDecoder {
	topic := cfg.Option("topic", "aoa-events")
	return &DaprDecoder{
		cfg:      cfg,
		pubsub:   cfg.Option("pubsub", "aoa"),
		topic:    topic,
		route:    cfg.Option("route", "/dapr/events/"+topic),
		fallback: cfg.Option("source_prefix", "dapr:"),
	}
}

// Name implements Decoder.
func (d *DaprDecoder) Name() string { return "dapr" }

// SubscriptionRoute returns the route the decoder expects.
func (d *DaprDecoder) SubscriptionRoute() string { return d.route }

// ExtraRoutes implements RouteProvider. Dapr calls GET /dapr/subscribe during
// sidecar startup to learn which topics this app consumes.
func (d *DaprDecoder) ExtraRoutes() []ExtraRoute {
	return []ExtraRoute{{
		Method: http.MethodGet,
		Path:   "/dapr/subscribe",
		Handler: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]string{{
				"pubsubname": d.pubsub,
				"topic":      d.topic,
				"route":      d.route,
			}})
		},
	}}
}

// Decode translates a Dapr delivery. CloudEvents mode is preferred; a plain
// JSON envelope is accepted as a fallback for older Dapr sidecars.
func (d *DaprDecoder) Decode(ctx context.Context, r *http.Request, body []byte) ([]CanonicalEvent, error) {
	_, err := DecodeCloudEvent(r, body)
	if err == nil {
		decoder := NewCloudEventsDecoder(Config{Options: map[string]string{"source_prefix": d.fallback}})
		return decoder.Decode(ctx, r, body)
	}
	return d.decodePlain(body)
}

type daprEnvelope struct {
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Topic           string          `json:"topic"`
	PubSubName      string          `json:"pubsubname"`
	DataContentType string          `json:"datacontenttype"`
	Data            json.RawMessage `json:"data"`
	SessionID       string          `json:"session_id"`
}

func (d *DaprDecoder) decodePlain(body []byte) ([]CanonicalEvent, error) {
	var env daprEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	sourceID := env.ID
	if sourceID == "" {
		sum := sha256.Sum256(body)
		sourceID = "hash:" + hex.EncodeToString(sum[:])
	}
	kind := aoa.KindWebhook
	if strings.Contains(strings.ToLower(env.Type), "approval") {
		kind = aoa.KindApproval
	}
	payload := string(env.Data)
	if payload == "" {
		payload = string(body)
	}
	return []CanonicalEvent{{
		SourceID:  d.fallback + sourceID,
		Kind:      kind,
		SessionID: env.SessionID,
		Payload:   payload,
	}}, nil
}
