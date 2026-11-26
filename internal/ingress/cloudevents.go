package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/projanvil/aoa/internal/aoa"
)

// eventData is the optional aoa payload inside a CloudEvent.
type eventData struct {
	SessionID  string `json:"session_id"`
	Payload    string `json:"payload"`
	ResumeJSON string `json:"resume_json"`
}

// CloudEventsDecoder adapts CloudEvents 1.0 over HTTP. It is the Knative
// Eventing Broker/Trigger format and the Dapr transport format.
type CloudEventsDecoder struct {
	cfg    Config
	prefix string
}

var _ Decoder = (*CloudEventsDecoder)(nil)

// NewCloudEventsDecoder builds the adapter.
func NewCloudEventsDecoder(cfg Config) *CloudEventsDecoder {
	return &CloudEventsDecoder{cfg: cfg, prefix: cfg.Option("source_prefix", "ce:")}
}

// Name implements Decoder.
func (d *CloudEventsDecoder) Name() string { return "cloudevents" }

// Decode translates one CloudEvent into one canonical event. Both binary and
// structured HTTP bindings are supported by the SDK.
func (d *CloudEventsDecoder) Decode(_ context.Context, r *http.Request, body []byte) ([]CanonicalEvent, error) {
	r.Body = io.NopCloser(bytes.NewReader(body))
	ce, err := cloudevents.NewEventFromHTTPRequest(r)
	if err != nil {
		return nil, err
	}
	var data eventData
	if len(ce.Data()) > 0 {
		if err := json.Unmarshal(ce.Data(), &data); err != nil {
			return nil, err
		}
	}
	sessionID := data.SessionID
	if sessionID == "" {
		if v, ok := ce.Extensions()["sessionid"].(string); ok {
			sessionID = v
		}
	}
	payload := data.Payload
	if payload == "" {
		payload = string(ce.Data())
	}
	// (source, id) is the CloudEvents dedup key. id alone is only unique
	// within one producer, so two sources can otherwise collide.
	sourceID := ce.Source() + ":" + ce.ID()
	if ce.ID() == "" {
		sum := sha256.Sum256(body)
		sourceID = ce.Source() + ":hash:" + hex.EncodeToString(sum[:])
	}
	return []CanonicalEvent{{
		SourceID:   d.prefix + sourceID,
		Kind:       aoa.KindForCloudEventType(ce.Type()),
		SessionID:  sessionID,
		Payload:    payload,
		ResumeJSON: data.ResumeJSON,
	}}, nil
}

// DecodeCloudEvent exposes the shared decoder to the Dapr adapter.
func DecodeCloudEvent(r *http.Request, body []byte) (cloudevents.Event, error) {
	r.Body = io.NopCloser(bytes.NewReader(body))
	ce, err := cloudevents.NewEventFromHTTPRequest(r)
	if err != nil {
		return cloudevents.Event{}, err
	}
	return *ce, nil
}
