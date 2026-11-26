package ingress

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/projanvil/aoa/internal/aoa"
)

func cloudEventBodyForTest() []byte {
	e := cloudevents.NewEvent()
	e.SetID("helper")
	e.SetSource("unit")
	e.SetType(aoa.CETypeWebhook)
	_ = e.SetData(cloudevents.ApplicationJSON, map[string]any{"payload": "x"})
	b, _ := json.Marshal(e)
	return b
}

type fakeSubscriber struct {
	ch     chan *message.Message
	mu     sync.Mutex
	closed bool
}

func (s *fakeSubscriber) Subscribe(_ context.Context, _ string) (<-chan *message.Message, error) {
	return s.ch, nil
}

func (s *fakeSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	return nil
}

func TestBrokerListenerFactories(t *testing.T) {
	cfg := Config{Options: map[string]string{
		"url":     "nats://localhost:4222",
		"subject": "aoa.test",
		"brokers": "localhost:9092",
		"topic":   "aoa-test",
		"group":   "g1",
		"durable": "d1",
	}}
	nats, err := NewNATSListener(cfg)
	if err != nil || nats.Name() != "nats" {
		t.Fatalf("nats = %v, %v", nats, err)
	}
	if err := nats.Stop(context.Background()); err != nil {
		t.Fatalf("nats stop without start: %v", err)
	}
	kafka, err := NewKafkaListener(cfg)
	if err != nil || kafka.Name() != "kafka" {
		t.Fatalf("kafka = %v, %v", kafka, err)
	}
	if err := kafka.Stop(context.Background()); err != nil {
		t.Fatalf("kafka stop without start: %v", err)
	}
	if _, err := NewWatermillListener(cfg); err == nil {
		t.Fatal("watermill without a subscriber must fail")
	}
}

func TestWatermillListenerConsumesAndAcks(t *testing.T) {
	sub := &fakeSubscriber{ch: make(chan *message.Message, 1)}
	l, err := NewWatermillListener(Config{
		Options: map[string]string{"topic": "aoa-test"},
		Any:     map[string]any{"subscriber": sub},
	})
	if err != nil {
		t.Fatal(err)
	}
	msg := message.NewMessage("wm-1", []byte("payload"))
	msg.Metadata.Set("session_id", "s1")
	sub.ch <- msg

	var got CanonicalEvent
	sink := SinkFunc(func(_ context.Context, ev CanonicalEvent) (bool, error) {
		got = ev
		return true, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Start(ctx, sink) }()

	select {
	case <-msg.Acked():
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("message was not acked")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got.SourceID != "watermill:wm-1" || got.SessionID != "s1" || got.Payload != "payload" {
		t.Fatalf("event = %+v", got)
	}
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Stop(context.Background()); err != nil {
		t.Fatalf("second stop must be idempotent: %v", err)
	}
}

func TestBrokerEventAndKindMapping(t *testing.T) {
	withID := BrokerEvent("nats:", "msg-1", []byte("body"), "", "")
	if withID.SourceID != "nats:msg-1" {
		t.Fatalf("event = %+v", withID)
	}
	if withID.Kind != aoa.KindBackground {
		t.Fatalf("default kind = %q", withID.Kind)
	}
	hashed := BrokerEvent("kafka:", "", []byte("body"), "", "")
	if !strings.HasPrefix(hashed.SourceID, "kafka:hash:") {
		t.Fatalf("hash fallback = %q", hashed.SourceID)
	}
	for subject, want := range map[string]string{
		"aoa.approval": "approval.decision",
		"aoa.timer":    "timer.fired",
		"aoa.messages": "user.message",
		"aoa.jobs":     "background.job",
	} {
		if got := string(KindFromSubject(subject)); got != want {
			t.Fatalf("KindFromSubject(%q) = %q, want %q", subject, got, want)
		}
	}
}

func TestDecodeCloudEventHelper(t *testing.T) {
	body := cloudEventBodyForTest()
	r := ceRequest(t, body)
	ce, err := DecodeCloudEvent(r, body)
	if err != nil {
		t.Fatal(err)
	}
	if ce.ID() != "helper" || ce.Source() != "unit" {
		t.Fatalf("CloudEvent = %s/%s", ce.Source(), ce.ID())
	}
}
