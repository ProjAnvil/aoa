package ingress

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/projanvil/aoa/internal/aoa"
)

// NATSListener adapts a NATS JetStream durable subscription. Messages are
// acknowledged only after the inbox row is durable.
type NATSListener struct {
	cfg     Config
	url     string
	subject string
	stream  string
	durable string

	mu     sync.Mutex
	conn   *nats.Conn
	sub    *nats.Subscription
	cancel context.CancelFunc
}

var _ Listener = (*NATSListener)(nil)

// NewNATSListener is the registry factory for the "nats" adapter.
func NewNATSListener(cfg Config) (Listener, error) {
	url := cfg.Option("url", nats.DefaultURL)
	if url == "" {
		return nil, errors.New("ingress: nats url is empty")
	}
	return &NATSListener{
		cfg:     cfg,
		url:     url,
		subject: cfg.Option("subject", "aoa.events"),
		stream:  cfg.Option("stream", "AOA"),
		durable: cfg.Option("durable", "aoa-worker"),
	}, nil
}

// Name implements Listener.
func (l *NATSListener) Name() string { return "nats" }

// Start connects and subscribes. It returns after the subscription is live;
// message processing runs in the client's own goroutines.
func (l *NATSListener) Start(ctx context.Context, sink Sink) error {
	conn, err := nats.Connect(l.url)
	if err != nil {
		return fmt.Errorf("ingress: nats connect: %w", err)
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return fmt.Errorf("ingress: nats jetstream: %w", err)
	}
	// A durable subscription needs its stream. Create it idempotently so the
	// listener can start before the first publisher.
	_, err = js.AddStream(&nats.StreamConfig{
		Name: l.stream, Subjects: []string{l.subject}, Storage: nats.FileStorage,
	})
	if err != nil && !strings.Contains(err.Error(), "stream name already in use") {
		conn.Close()
		return fmt.Errorf("ingress: nats create stream: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	handler := func(msg *nats.Msg) {
		traceID := ""
		if msg.Header != nil {
			traceID = msg.Header.Get("traceparent")
		}
		ev := BrokerEvent("nats:", msg.Header.Get("Nats-Msg-Id"), msg.Data,
			aoa.KindBackground, "")
		ev.TraceID = traceID
		if md, err := msg.Metadata(); err == nil {
			ev.SourceID = fmt.Sprintf("nats:%s:%d", l.stream, md.Sequence.Stream)
		}
		if err := AcceptWithRetry(runCtx, sink, ev, l.cfg.Logger); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	}
	sub, err := js.Subscribe(l.subject, handler,
		nats.Durable(l.durable), nats.ManualAck(), nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		cancel()
		conn.Close()
		return fmt.Errorf("ingress: nats subscribe: %w", err)
	}
	l.mu.Lock()
	l.conn, l.sub, l.cancel = conn, sub, cancel
	l.mu.Unlock()
	return nil
}

// Stop drains the subscription and closes the connection.
func (l *NATSListener) Stop(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
	}
	if l.sub != nil {
		if err := l.sub.Drain(); err != nil {
			return err
		}
	}
	if l.conn != nil {
		l.conn.Close()
	}
	l.conn, l.sub, l.cancel = nil, nil, nil
	return nil
}
