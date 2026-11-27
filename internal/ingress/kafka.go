package ingress

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
)

// KafkaListener adapts a Kafka consumer group. Offset commits happen only
// after the inbox row is durable, matching the outbox/inbox durability model.
type KafkaListener struct {
	cfg    Config
	broker string
	topic  string
	group  string

	mu        sync.Mutex
	reader    kafkaMessageReader
	newReader func() kafkaMessageReader
}

var _ Listener = (*KafkaListener)(nil)

// kafkaMessageReader is the narrow reader surface Start needs. Tests inject a
// fake so the consumer loop can be exercised without a broker.
type kafkaMessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// NewKafkaListener is the registry factory for the "kafka" adapter.
func NewKafkaListener(cfg Config) (Listener, error) {
	broker := cfg.Option("brokers", "localhost:9092")
	if broker == "" {
		return nil, errors.New("ingress: kafka brokers are empty")
	}
	return &KafkaListener{
		cfg:    cfg,
		broker: broker,
		topic:  cfg.Option("topic", "aoa-events"),
		group:  cfg.Option("group", "aoa-worker"),
	}, nil
}

// Name implements Listener.
func (l *KafkaListener) Name() string { return "kafka" }

// Start begins a blocking read loop. Callers run it in a goroutine.
func (l *KafkaListener) Start(ctx context.Context, sink Sink) error {
	var reader kafkaMessageReader
	if l.newReader != nil {
		reader = l.newReader()
	} else {
		reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:  strings.Split(l.broker, ","),
			Topic:    l.topic,
			GroupID:  l.group,
			MinBytes: 1,
			MaxBytes: 1 << 20,
		})
	}
	l.mu.Lock()
	l.reader = reader
	l.mu.Unlock()
	defer func() {
		_ = reader.Close()
		l.mu.Lock()
		l.reader = nil
		l.mu.Unlock()
	}()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		// Partition offset is a stable broker id. A missing message key
		// cannot produce a weaker dedup key than the offset itself.
		ev := BrokerEvent("kafka:"+l.topic+":", itoa(msg.Offset),
			msg.Value, KindFromSubject(l.topic), "")
		if key := string(msg.Key); key != "" {
			ev.SessionID = key
		}
		for _, h := range msg.Headers {
			if h.Key == "traceparent" {
				ev.TraceID = string(h.Value)
			}
		}
		if err := AcceptWithRetry(ctx, sink, ev, l.cfg.Logger); err != nil {
			return err
		}
	}
}

// Stop closes the reader, which unblocks Start.
func (l *KafkaListener) Stop(_ context.Context) error {
	l.mu.Lock()
	reader := l.reader
	l.mu.Unlock()
	if reader != nil {
		return reader.Close()
	}
	return nil
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	buf := [24]byte{}
	i := len(buf)
	neg := v < 0
	if neg {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
