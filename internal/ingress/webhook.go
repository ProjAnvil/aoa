package ingress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/projanvil/aoa/internal/aoa"
)

// WebhookDecoder adapts a plain JSON or text webhook. Monitoring platforms,
// GitHub, and alert managers usually POST this shape.
type WebhookDecoder struct {
	cfg    Config
	prefix string
}

var _ Decoder = (*WebhookDecoder)(nil)

// NewWebhookDecoder builds the adapter.
func NewWebhookDecoder(cfg Config) *WebhookDecoder {
	return &WebhookDecoder{cfg: cfg, prefix: cfg.Option("source_prefix", "webhook:")}
}

// Name implements Decoder.
func (d *WebhookDecoder) Name() string { return "webhook" }

type webhookBody struct {
	SourceID   string `json:"source_id"`
	SessionID  string `json:"session_id"`
	Payload    string `json:"payload"`
	Kind       string `json:"kind"`
	ResumeJSON string `json:"resume_json"`
}

// Decode maps the request. A missing source id falls back to a body hash so
// at-least-once delivery still deduplicates.
func (d *WebhookDecoder) Decode(_ context.Context, r *http.Request, body []byte) ([]CanonicalEvent, error) {
	var data webhookBody
	if err := json.Unmarshal(body, &data); err != nil {
		// Plain text is a valid webhook payload.
		data.Payload = string(body)
	}
	kind := aoa.Kind(data.Kind)
	if kind == "" {
		kind = aoa.KindForCloudEventType(r.Header.Get("X-AOA-Event-Type"))
	}
	if kind == aoa.KindWebhook && r.Header.Get("X-AOA-Event-Type") == "" {
		kind = aoa.KindWebhook
	}
	sourceID := data.SourceID
	if sourceID == "" {
		sourceID = r.Header.Get("X-AOA-Event-Id")
	}
	if sourceID == "" {
		sum := sha256.Sum256(body)
		sourceID = "hash:" + hex.EncodeToString(sum[:])
	}
	payload := data.Payload
	if payload == "" {
		payload = string(body)
	}
	return []CanonicalEvent{{
		SourceID:   d.prefix + sourceID,
		Kind:       kind,
		SessionID:  data.SessionID,
		Payload:    payload,
		ResumeJSON: data.ResumeJSON,
	}}, nil
}
