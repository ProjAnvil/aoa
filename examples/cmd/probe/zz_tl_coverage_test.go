package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/nats-io/nats.go"
	"github.com/segmentio/kafka-go"
)

type tlNATSConn struct {
	js     nats.JetStreamContext
	jsErr  error
	closed bool
}

func (c *tlNATSConn) JetStream(...nats.JSOpt) (nats.JetStreamContext, error) {
	return c.js, c.jsErr
}
func (c *tlNATSConn) Close() { c.closed = true }

type tlJetStream struct {
	nats.JetStreamContext
	addErr    error
	pubErr    error
	published []byte
}

func (j *tlJetStream) AddStream(*nats.StreamConfig, ...nats.JSOpt) (*nats.StreamInfo, error) {
	return nil, j.addErr
}
func (j *tlJetStream) Publish(_ string, data []byte, _ ...nats.PubOpt) (*nats.PubAck, error) {
	j.published = data
	return &nats.PubAck{Stream: "AOA", Sequence: 1}, j.pubErr
}

type tlKafkaWriter struct {
	err    error
	msgs   []kafka.Message
	closed bool
}

func (w *tlKafkaWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.msgs = append(w.msgs, msgs...)
	return w.err
}
func (w *tlKafkaWriter) Close() error {
	w.closed = true
	return nil
}

func TestTLPublishNATSWithFake(t *testing.T) {
	js := &tlJetStream{addErr: errors.New("stream name already in use")}
	conn := &tlNATSConn{js: js}
	original := newNATSConn
	newNATSConn = func(string) (natsPublishConn, error) { return conn, nil }
	defer func() { newNATSConn = original }()

	if err := publishNATS([]string{"--url", "nats://fake", "--payload", "hello"}); err != nil {
		t.Fatal(err)
	}
	if string(js.published) != "hello" || !conn.closed {
		t.Fatalf("publish = %q, closed = %v", js.published, conn.closed)
	}

	js.addErr = errors.New("boom")
	if err := publishNATS([]string{}); err == nil {
		t.Fatal("AddStream failure must surface")
	}
	js.addErr = nil
	js.pubErr = errors.New("publish failed")
	if err := publishNATS([]string{}); err == nil {
		t.Fatal("Publish failure must surface")
	}
	newNATSConn = func(string) (natsPublishConn, error) {
		return nil, errors.New("dial failed")
	}
	if err := publishNATS([]string{}); err == nil {
		t.Fatal("dial failure must surface")
	}
	newNATSConn = func(string) (natsPublishConn, error) {
		return &tlNATSConn{jsErr: errors.New("jetstream failed")}, nil
	}
	if err := publishNATS([]string{}); err == nil {
		t.Fatal("JetStream failure must surface")
	}
}

func TestTLPublishKafkaWithFake(t *testing.T) {
	w := &tlKafkaWriter{}
	original := newKafkaWriter
	newKafkaWriter = func([]string, string) kafkaMsgWriter { return w }
	defer func() { newKafkaWriter = original }()

	if err := publishKafka([]string{"--brokers", "a:9092,b:9092", "--payload", "hello"}); err != nil {
		t.Fatal(err)
	}
	if len(w.msgs) != 1 || string(w.msgs[0].Value) != "hello" || !w.closed {
		t.Fatalf("kafka publish = %+v, closed = %v", w.msgs, w.closed)
	}
	w.err = errors.New("write failed")
	if err := publishKafka([]string{}); err == nil {
		t.Fatal("write failure must surface")
	}
}

func TestTLPublishRedisWithMiniRedis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	if err := publishRedis([]string{"--addr", mr.Addr(), "--payload", "hello"}); err != nil {
		t.Fatal(err)
	}
	if err := publishRedis([]string{"--addr", "127.0.0.1:1"}); err == nil {
		t.Fatal("unreachable Redis must fail")
	}
}

func TestTLServeAndOutboxHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	outboxHandler().ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"payload":"x"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("outbox handler = %d", rec.Code)
	}
	if err := serve([]string{"--addr", "not-a-valid-addr"}); err == nil {
		t.Fatal("invalid listen address must fail")
	}
}

func TestTLUsageAndDispatch(t *testing.T) {
	usage()
	if err := dispatch(nil); err == nil {
		t.Fatal("missing command must fail")
	}
	if err := dispatch([]string{"unknown"}); err == nil {
		t.Fatal("unknown command must fail")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	if err := dispatch([]string{"emit", "--target", srv.URL, "--source-id", "fixed"}); err != nil {
		t.Fatal(err)
	}
	if err := dispatch([]string{"scenario", "dedup", "--target", srv.URL}); err != nil {
		t.Fatal(err)
	}
}

func TestTLMainSuccessPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	originalArgs := os.Args
	os.Args = []string{"aoa-probe", "emit", "--target", srv.URL, "--source-id", "fixed"}
	defer func() { os.Args = originalArgs }()
	main()
}

func TestTLHelloFailurePaths(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"error":"not found"}`)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	t.Setenv("OLLAMA_HOST", srv.URL)
	if err := hello([]string{}); err == nil {
		t.Fatal("hello must surface Ollama errors")
	}
	if err := agentHello([]string{}); err == nil {
		t.Fatal("agentHello must surface Ollama errors")
	}

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"model":"x","message":{"role":"assistant","content":""},"done":true}`)
	}))
	defer empty.Close()
	t.Setenv("OLLAMA_HOST", empty.URL)
	if err := hello([]string{}); err == nil {
		t.Fatal("hello must reject an empty reply")
	}
	if err := agentHello([]string{}); err == nil {
		t.Fatal("agentHello must reject an empty reply")
	}
}
