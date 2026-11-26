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

// KubernetesDecoder adapts two Kubernetes webhook shapes: AdmissionReview v1
// and core/v1 Event notifications. It needs no client-go dependency because
// the API server pushes these over HTTP.
type KubernetesDecoder struct {
	cfg    Config
	prefix string
}

var (
	_ Decoder   = (*KubernetesDecoder)(nil)
	_ Responder = (*KubernetesDecoder)(nil)
)

// NewKubernetesDecoder builds the adapter.
func NewKubernetesDecoder(cfg Config) *KubernetesDecoder {
	return &KubernetesDecoder{cfg: cfg, prefix: cfg.Option("source_prefix", "k8s:")}
}

// Name implements Decoder.
func (d *KubernetesDecoder) Name() string { return "kubernetes" }

type admissionReview struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Request    struct {
		UID       string         `json:"uid"`
		Namespace string         `json:"namespace"`
		Kind      map[string]any `json:"kind"`
		Object    map[string]any `json:"object"`
		Operation string         `json:"operation"`
	} `json:"request"`
}

type coreEvent struct {
	Kind     string `json:"kind"`
	Metadata struct {
		UID  string `json:"uid"`
		Name string `json:"name"`
	} `json:"metadata"`
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	InvolvedObject struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"involvedObject"`
}

// Decode chooses the AdmissionReview or core Event strategy.
func (d *KubernetesDecoder) Decode(_ context.Context, _ *http.Request, body []byte) ([]CanonicalEvent, error) {
	var probe struct {
		Kind    string `json:"kind"`
		Request *struct {
			UID string `json:"uid"`
		} `json:"request"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, err
	}
	if strings.EqualFold(probe.Kind, "AdmissionReview") && probe.Request != nil {
		return d.decodeAdmission(body)
	}
	return d.decodeCoreEvent(body)
}

func (d *KubernetesDecoder) decodeAdmission(body []byte) ([]CanonicalEvent, error) {
	var review admissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		return nil, err
	}
	sourceID := review.Request.UID
	if sourceID == "" {
		sum := sha256.Sum256(body)
		sourceID = "hash:" + hex.EncodeToString(sum[:])
	}
	objectJSON, err := json.Marshal(review.Request.Object)
	if err != nil {
		objectJSON = []byte("{}")
	}
	return []CanonicalEvent{{
		SourceID:  d.prefix + "admission:" + sourceID,
		Kind:      aoa.KindWebhook,
		SessionID: review.Request.Namespace,
		Payload:   review.Request.Operation + " " + string(objectJSON),
	}}, nil
}

func (d *KubernetesDecoder) decodeCoreEvent(body []byte) ([]CanonicalEvent, error) {
	var ev coreEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, err
	}
	sourceID := ev.Metadata.UID
	if sourceID == "" {
		sum := sha256.Sum256(body)
		sourceID = "hash:" + hex.EncodeToString(sum[:])
	}
	return []CanonicalEvent{{
		SourceID:  d.prefix + "event:" + sourceID,
		Kind:      aoa.KindWebhook,
		SessionID: ev.InvolvedObject.Namespace,
		Payload:   ev.Reason + " " + ev.InvolvedObject.Kind + "/" + ev.InvolvedObject.Name + " " + ev.Message,
	}}, nil
}

// WriteResponse implements Responder. An AdmissionReview must answer with an
// AdmissionReview, not with the generic aoa acknowledgement.
func (d *KubernetesDecoder) WriteResponse(w http.ResponseWriter, events []CanonicalEvent, _ bool) {
	uid := ""
	if len(events) > 0 {
		uid = strings.TrimPrefix(events[0].SourceID, d.prefix+"admission:")
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"apiVersion": "admission.k8s.io/v1",
		"kind":       "AdmissionReview",
		"response": map[string]any{
			"uid":     uid,
			"allowed": true,
			"status":  map[string]any{"message": "aoa accepted the event"},
		},
	})
}
