package ingress

import (
	"context"
	"testing"
)

func TestRedisStreamSubscriberLifecycle(t *testing.T) {
	if _, err := NewRedisStreamSubscriber(""); err == nil {
		t.Fatal("empty address must fail")
	}
	sub, err := NewRedisStreamSubscriber("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	// Close works without a live Redis server and is idempotent.
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Subscribe(context.Background(), "aoa-events"); err == nil {
		t.Fatal("Subscribe after Close must fail")
	}
}

func TestRedisStreamSubscriberStartPosition(t *testing.T) {
	sub, err := NewRedisStreamSubscriber("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if sub.client == nil {
		t.Fatal("redis client is nil")
	}
}
