package ingress

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
)

// WatermillListener adapts any Watermill message.Subscriber (Kafka, NATS,
// RabbitMQ, Redis Streams, SQL, Go channel). The subscriber is injected via
// Config.Any because it is a native client, not a config string.
type WatermillListener struct {
	cfg        Config
	topic      string
	subscriber message.Subscriber

	mu     sync.Mutex
	closed bool
}

var _ Listener = (*WatermillListener)(nil)

// NewWatermillListener is the registry factory for the "watermill" adapter.
func NewWatermillListener(cfg Config) (Listener, error) {
	sub, ok := cfg.Any["subscriber"].(message.Subscriber)
	if !ok || sub == nil {
		return nil, errors.New("ingress: watermill requires Config.Any[\"subscriber\"] implementing message.Subscriber")
	}
	return &WatermillListener{
		cfg:        cfg,
		topic:      cfg.Option("topic", "aoa-events"),
		subscriber: sub,
	}, nil
}

// Name implements Listener.
func (l *WatermillListener) Name() string { return "watermill" }

// Start consumes messages until the context is cancelled. Ack happens only
// after the inbox accepted the event.
func (l *WatermillListener) Start(ctx context.Context, sink Sink) error {
	ch, err := l.subscriber.Subscribe(ctx, l.topic)
	if err != nil {
		return fmt.Errorf("ingress: watermill subscribe: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, open := <-ch:
			if !open {
				return nil
			}
			if msg == nil {
				continue
			}
			ev := BrokerEvent("watermill:", msg.UUID, msg.Payload,
				KindFromSubject(l.topic), msg.Metadata.Get("session_id"))
			ev.TraceID = msg.Metadata.Get("traceparent")
			if err := AcceptWithRetry(ctx, sink, ev, l.cfg.Logger); err != nil {
				msg.Nack()
				return err
			}
			msg.Ack()
		}
	}
}

// Stop closes the underlying subscriber.
func (l *WatermillListener) Stop(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.subscriber.Close()
}
