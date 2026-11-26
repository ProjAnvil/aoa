package ingress

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

// RedisStreamSubscriber is a Watermill Subscriber backed by Redis Streams.
// It lets the watermill adapter run as a real container group without a
// broker-specific Watermill plugin dependency.
type RedisStreamSubscriber struct {
	client  *redis.Client
	mu      sync.Mutex
	cancels []context.CancelFunc
	closed  bool
}

var _ message.Subscriber = (*RedisStreamSubscriber)(nil)

// NewRedisStreamSubscriber connects to a Redis server.
func NewRedisStreamSubscriber(addr string) (*RedisStreamSubscriber, error) {
	if addr == "" {
		return nil, fmt.Errorf("ingress: redis address is empty")
	}
	return &RedisStreamSubscriber{client: redis.NewClient(&redis.Options{Addr: addr})}, nil
}

// Subscribe consumes one stream. The last consumed id is kept in the
// goroutine; a restart replays from the beginning, and the inbox dedup
// tombstones absorb redeliveries.
func (s *RedisStreamSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("ingress: subscriber is closed")
	}
	s.cancels = append(s.cancels, cancel)
	s.mu.Unlock()

	out := make(chan *message.Message, 8)
	go func() {
		defer close(out)
		last := "0"
		for {
			if runCtx.Err() != nil {
				return
			}
			streams, err := s.client.XRead(runCtx, &redis.XReadArgs{
				Streams: []string{topic, last},
				Count:   10,
				Block:   time.Second,
			}).Result()
			if err != nil {
				if err == redis.Nil || runCtx.Err() != nil {
					continue
				}
				select {
				case <-runCtx.Done():
					return
				case <-time.After(500 * time.Millisecond):
					continue
				}
			}
			for _, stream := range streams {
				for _, entry := range stream.Messages {
					last = entry.ID
					msg := message.NewMessage(entry.ID, []byte(fmt.Sprint(entry.Values["payload"])))
					msg.Metadata.Set("session_id", fmt.Sprint(entry.Values["session_id"]))
					select {
					case out <- msg:
					case <-runCtx.Done():
						return
					}
					// Ack before advancing so a crash redelivers the entry.
					select {
					case <-msg.Acked():
					case <-runCtx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

// Close stops every subscription and closes the Redis client.
func (s *RedisStreamSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, cancel := range s.cancels {
		cancel()
	}
	return s.client.Close()
}
