package ingress

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

// Decoder is the Strategy interface for one wire protocol. Implementations
// are pure translators: bytes in, canonical events out.
type Decoder interface {
	Name() string
	Decode(ctx context.Context, r *http.Request, body []byte) ([]CanonicalEvent, error)
}

// Responder is an optional Strategy hook. Adapters whose protocol requires
// a shaped reply (for example a Kubernetes AdmissionReview) implement it.
type Responder interface {
	WriteResponse(w http.ResponseWriter, events []CanonicalEvent, inserted bool)
}

// DecoderFunc adapts a function to Decoder.
type DecoderFunc struct {
	Label string
	Fn    func(ctx context.Context, r *http.Request, body []byte) ([]CanonicalEvent, error)
}

// Name implements Decoder.
func (d DecoderFunc) Name() string { return d.Label }

// Decode implements Decoder.
func (d DecoderFunc) Decode(ctx context.Context, r *http.Request, body []byte) ([]CanonicalEvent, error) {
	return d.Fn(ctx, r, body)
}

// Listener is the Adapter interface for pull-based brokers. The same Sink
// receives events regardless of the transport.
type Listener interface {
	Name() string
	Start(ctx context.Context, sink Sink) error
	Stop(ctx context.Context) error
}

// Config is the factory input shared by decoders and listeners. Any carries
// native client objects that cannot be expressed as environment strings.
type Config struct {
	Name    string
	Options map[string]string
	Any     map[string]any
	Store   *store.Store
	Logger  *slog.Logger
	Metrics *telemetry.Metrics
	Clock   func() string
}

// Option returns a config value or a default.
func (c Config) Option(key, fallback string) string {
	if v := c.Options[key]; v != "" {
		return v
	}
	return fallback
}

// DecoderFactory builds one HTTP decoder.
type DecoderFactory func(Config) (Decoder, error)

// ListenerFactory builds one broker listener.
type ListenerFactory func(Config) (Listener, error)

// Registry is a thread-safe Factory for every supported ecosystem.
type Registry struct {
	mu        sync.RWMutex
	decoders  map[string]DecoderFactory
	listeners map[string]ListenerFactory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		decoders:  make(map[string]DecoderFactory),
		listeners: make(map[string]ListenerFactory),
	}
}

// RegisterDecoder adds a decoder factory. Duplicate names are rejected.
func (r *Registry) RegisterDecoder(name string, f DecoderFactory) error {
	if name == "" || f == nil {
		return fmt.Errorf("ingress: register decoder requires a name and factory")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.decoders[name]; exists {
		return fmt.Errorf("ingress: decoder %q already registered", name)
	}
	r.decoders[name] = f
	return nil
}

// RegisterListener adds a listener factory.
func (r *Registry) RegisterListener(name string, f ListenerFactory) error {
	if name == "" || f == nil {
		return fmt.Errorf("ingress: register listener requires a name and factory")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.listeners[name]; exists {
		return fmt.Errorf("ingress: listener %q already registered", name)
	}
	r.listeners[name] = f
	return nil
}

// NewDecoder builds a decoder by name.
func (r *Registry) NewDecoder(name string, cfg Config) (Decoder, error) {
	r.mu.RLock()
	f, ok := r.decoders[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("ingress: unknown decoder %q (available: %s)", name, strings.Join(r.DecoderNames(), ", "))
	}
	cfg.Name = name
	return f(cfg)
}

// NewListener builds a listener by name.
func (r *Registry) NewListener(name string, cfg Config) (Listener, error) {
	r.mu.RLock()
	f, ok := r.listeners[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("ingress: unknown listener %q (available: %s)", name, strings.Join(r.ListenerNames(), ", "))
	}
	cfg.Name = name
	return f(cfg)
}

// DecoderNames returns sorted decoder names.
func (r *Registry) DecoderNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sortedKeys(r.decoders)
}

// ListenerNames returns sorted listener names.
func (r *Registry) ListenerNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sortedKeys(r.listeners)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// DefaultRegistry registers every adapter shipped with the service.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	must(r.RegisterDecoder("cloudevents", func(cfg Config) (Decoder, error) {
		return NewCloudEventsDecoder(cfg), nil
	}))
	must(r.RegisterDecoder("dapr", func(cfg Config) (Decoder, error) {
		return NewDaprDecoder(cfg), nil
	}))
	must(r.RegisterDecoder("webhook", func(cfg Config) (Decoder, error) {
		return NewWebhookDecoder(cfg), nil
	}))
	must(r.RegisterDecoder("kubernetes", func(cfg Config) (Decoder, error) {
		return NewKubernetesDecoder(cfg), nil
	}))
	must(r.RegisterListener("nats", NewNATSListener))
	must(r.RegisterListener("kafka", NewKafkaListener))
	must(r.RegisterListener("watermill", NewWatermillListener))
	return r
}
