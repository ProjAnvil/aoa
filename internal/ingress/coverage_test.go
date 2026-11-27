package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/alicebob/miniredis/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/telemetry"
)

// ---------- registry, event, sink ----------

func TestCoverageRegistryAndConfig(t *testing.T) {
	cfg := Config{Options: map[string]string{"topic": "x"}}
	if cfg.Option("topic", "fallback") != "x" || cfg.Option("missing", "fallback") != "fallback" {
		t.Fatal("Config.Option failed")
	}
	fn := DecoderFunc{Label: "fn", Fn: func(context.Context, *http.Request, []byte) ([]CanonicalEvent, error) {
		return []CanonicalEvent{{SourceID: "fn:1"}}, nil
	}}
	if fn.Name() != "fn" {
		t.Fatal("DecoderFunc.Name failed")
	}
	if events, err := fn.Decode(context.Background(), nil, nil); err != nil || len(events) != 1 {
		t.Fatalf("DecoderFunc.Decode = %v, %v", events, err)
	}

	r := NewRegistry()
	for _, bad := range []struct {
		name string
		f    DecoderFactory
	}{{"", func(Config) (Decoder, error) { return nil, nil }}, {"x", nil}} {
		if err := r.RegisterDecoder(bad.name, bad.f); err == nil {
			t.Fatalf("RegisterDecoder(%q) must fail", bad.name)
		}
	}
	if err := r.RegisterListener("", func(Config) (Listener, error) { return nil, nil }); err == nil {
		t.Fatal("RegisterListener with empty name must fail")
	}
	if err := r.RegisterListener("x", nil); err == nil {
		t.Fatal("RegisterListener with nil factory must fail")
	}
	dummyDecoder := func(Config) (Decoder, error) { return fn, nil }
	dummyListener := func(Config) (Listener, error) { return &WatermillListener{}, nil }
	if err := r.RegisterDecoder("z", dummyDecoder); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterDecoder("z", dummyDecoder); err == nil {
		t.Fatal("duplicate decoder must fail")
	}
	if err := r.RegisterListener("a", dummyListener); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterListener("a", dummyListener); err == nil {
		t.Fatal("duplicate listener must fail")
	}
	if _, err := r.NewDecoder("missing", Config{}); err == nil {
		t.Fatal("unknown decoder must fail")
	}
	if _, err := r.NewListener("missing", Config{}); err == nil {
		t.Fatal("unknown listener must fail")
	}
	if d, err := r.NewDecoder("z", Config{}); err != nil || d.Name() != "fn" {
		t.Fatalf("NewDecoder = %v, %v", d, err)
	}
	if l, err := r.NewListener("a", Config{}); err != nil || l.Name() != "watermill" {
		t.Fatalf("NewListener = %v, %v", l, err)
	}
	if got := strings.Join(r.DecoderNames(), ","); got != "z" {
		t.Fatalf("DecoderNames = %q", got)
	}
	if got := strings.Join(r.ListenerNames(), ","); got != "a" {
		t.Fatalf("ListenerNames = %q", got)
	}

	def := DefaultRegistry()
	for _, name := range def.DecoderNames() {
		if _, err := def.NewDecoder(name, Config{}); err != nil {
			t.Fatalf("decoder %q: %v", name, err)
		}
	}
	for _, name := range def.ListenerNames() {
		if name == "watermill" {
			continue // requires an injected subscriber
		}
		if _, err := def.NewListener(name, Config{}); err != nil {
			t.Fatalf("listener %q: %v", name, err)
		}
	}
	if _, err := NewWatermillListener(Config{Any: map[string]any{"subscriber": "not-a-subscriber"}}); err == nil {
		t.Fatal("invalid watermill subscriber must fail")
	}
	if (&WebhookDecoder{}).Name() != "webhook" || (&WatermillListener{}).Name() != "watermill" {
		t.Fatal("decoder/listener Name failed")
	}
}

func TestCoverageEventValidationAndSinks(t *testing.T) {
	if err := (CanonicalEvent{}).Validate(); err == nil {
		t.Fatal("empty SourceID must fail")
	}
	if err := (CanonicalEvent{SourceID: "x"}).Validate(); err == nil {
		t.Fatal("empty Kind must fail")
	}
	if err := (CanonicalEvent{SourceID: "x", Kind: aoa.KindWebhook}).Validate(); err != nil {
		t.Fatal(err)
	}
	sink := NewStoreSink(testStore(t), nil, nil)
	if sink.Metrics == nil || sink.Logger == nil {
		t.Fatal("NewStoreSink defaults missing")
	}
	if ok, err := sink.Accept(context.Background(), CanonicalEvent{SourceID: "x", Kind: aoa.KindWebhook}); err != nil || !ok {
		t.Fatalf("Accept = %v, %v", ok, err)
	}

	closed := testStore(t)
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStoreSink(closed, nil, nil).Accept(context.Background(),
		CanonicalEvent{SourceID: "y", Kind: aoa.KindWebhook}); err == nil {
		t.Fatal("closed store must fail Accept")
	}

	inner := SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return false, errors.New("inner failure")
	})
	counting := &CountingSink{Inner: inner}
	if ok, err := counting.Accept(context.Background(), CanonicalEvent{SourceID: "z"}); err == nil || ok {
		t.Fatalf("CountingSink with inner = %v, %v", ok, err)
	}
	if len(counting.Events) != 1 {
		t.Fatal("CountingSink did not record the event")
	}
	if ok, err := (&CountingSink{}).Accept(context.Background(), CanonicalEvent{}); err != nil || !ok {
		t.Fatalf("CountingSink without inner = %v, %v", ok, err)
	}
}

// ---------- middleware ----------

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (errReader) Close() error             { return nil }

func TestCoverageMiddleware(t *testing.T) {
	body := []byte("hello")
	if raw, err := ReadAndReplay(&http.Request{}); err != nil || raw != nil {
		t.Fatalf("ReadAndReplay nil body = %v, %v", raw, err)
	}
	r := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	raw, err := ReadAndReplay(r)
	if err != nil || !bytes.Equal(raw, body) {
		t.Fatalf("ReadAndReplay = %q, %v", raw, err)
	}
	replayed, _ := io.ReadAll(r.Body)
	if !bytes.Equal(replayed, body) {
		t.Fatal("body was not replayed")
	}

	bad := httptest.NewRequest(http.MethodPost, "/events", nil)
	bad.Body = errReader{}
	if _, err := ReadAndReplay(bad); err == nil {
		t.Fatal("ReadAndReplay must surface read errors")
	}

	// BodyLimit default and explicit small limit.
	defaultLimit := BodyLimit(0)(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, err := io.ReadAll(req.Body); err != nil {
			t.Fatalf("default limit read: %v", err)
		}
	}))
	defaultLimit.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))

	small := BodyLimit(2)(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, err := io.ReadAll(req.Body); err == nil {
			t.Fatal("oversized body must fail")
		}
	}))
	small.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))

	// Empty key passes through.
	openRecorder := httptest.NewRecorder()
	Chain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}), SignatureRequired("", "")).ServeHTTP(openRecorder,
		httptest.NewRequest(http.MethodPost, "/", nil))
	if openRecorder.Code != http.StatusTeapot {
		t.Fatalf("empty signature key = %d", openRecorder.Code)
	}

	key := "secret"
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	mac := SignatureHeaderValue(key, http.MethodPost, "/events", ts, body)
	// ValidSignature binds nothing: empty method, path, and timestamp.
	bodyOnlyMAC := SignatureHeaderValue(key, "", "", "", body)
	if !ValidSignature(body, bodyOnlyMAC, key) {
		t.Fatal("ValidSignature failed")
	}
	if ValidSignature(body, "bad", key) {
		t.Fatal("ValidSignature accepted a bad MAC")
	}
	if ValidRequestSignature(body, mac, key, http.MethodPost, "/events", "not-a-number") {
		t.Fatal("non-numeric timestamp must fail")
	}
	if ValidRequestSignature(body, mac, key, http.MethodPost, "/events", "1") {
		t.Fatal("stale timestamp must fail")
	}
	if absInt64(-9) != 9 || absInt64(9) != 9 {
		t.Fatal("absInt64 failed")
	}

	signed := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	signed.Header.Set("X-Custom-Sig", mac)
	signed.Header.Set(TimestampHeader, ts)
	w := httptest.NewRecorder()
	Chain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}), SignatureRequired(key, "X-Custom-Sig")).ServeHTTP(w, signed)
	if w.Code != http.StatusAccepted {
		t.Fatalf("signed request = %d", w.Code)
	}

	unsigned := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	w2 := httptest.NewRecorder()
	Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		SignatureRequired(key, "")).ServeHTTP(w2, unsigned)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned request = %d", w2.Code)
	}

	unreadable := httptest.NewRequest(http.MethodPost, "/events", nil)
	unreadable.Body = errReader{}
	w3 := httptest.NewRecorder()
	Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		SignatureRequired(key, "")).ServeHTTP(w3, unreadable)
	if w3.Code != http.StatusBadRequest {
		t.Fatalf("unreadable body = %d", w3.Code)
	}

	stack := HTTPStack(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), nil, nil, 0)
	w4 := httptest.NewRecorder()
	stack.ServeHTTP(w4, httptest.NewRequest(http.MethodGet, "/", nil))
	if w4.Code != http.StatusOK || w4.Header().Get("X-Content-Type-Options") != "nosniff" ||
		w4.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("HTTPStack = %d %v", w4.Code, w4.Header())
	}
	if Chain(nil) != nil {
		t.Fatal("Chain with no links must return the original handler")
	}
}

// ---------- decoders ----------

func TestCoverageWebhookDecoder(t *testing.T) {
	d := NewWebhookDecoder(Config{})
	req := httptest.NewRequest(http.MethodPost, "/webhook", nil)
	req.Header.Set("X-AOA-Event-Type", aoa.CETypeTimer)
	req.Header.Set("X-AOA-Event-Id", "header-id")
	events, err := d.Decode(context.Background(), req, []byte("plain text"))
	if err != nil || len(events) != 1 {
		t.Fatalf("webhook = %v, %v", events, err)
	}
	if events[0].SourceID != "webhook:header-id" || events[0].Kind != aoa.KindTimer ||
		events[0].Payload != "plain text" {
		t.Fatalf("webhook event = %+v", events[0])
	}

	withKind := []byte(`{"source_id":"j1","session_id":"s1","kind":"background.job","resume_json":"[]"}`)
	jsonEvents, err := d.Decode(context.Background(),
		httptest.NewRequest(http.MethodPost, "/webhook", nil), withKind)
	if err != nil || jsonEvents[0].Kind != aoa.KindBackground ||
		jsonEvents[0].ResumeJSON != "[]" || jsonEvents[0].Payload != string(withKind) {
		t.Fatalf("json webhook = %v, %v", jsonEvents, err)
	}

	noID := []byte(`{"payload":"x"}`)
	hashEvents, err := d.Decode(context.Background(),
		httptest.NewRequest(http.MethodPost, "/webhook", nil), noID)
	if err != nil || !strings.HasPrefix(hashEvents[0].SourceID, "webhook:hash:") {
		t.Fatalf("hash fallback = %v, %v", hashEvents, err)
	}
}

func TestCoverageDaprPlainDecoder(t *testing.T) {
	d := NewDaprDecoder(Config{})
	if _, err := d.decodePlain([]byte("not json")); err == nil {
		t.Fatal("invalid plain envelope must fail")
	}
	body := []byte(`{"type":"com.example.approval","session_id":"s1","data":{"x":1}}`)
	events, err := d.decodePlain(body)
	if err != nil || len(events) != 1 {
		t.Fatalf("plain = %v, %v", events, err)
	}
	if !strings.HasPrefix(events[0].SourceID, "dapr:hash:") ||
		events[0].Kind != aoa.KindApproval || events[0].SessionID != "s1" ||
		events[0].Payload != `{"x":1}` {
		t.Fatalf("plain event = %+v", events[0])
	}
	noData := []byte(`{"id":"d2","type":"x"}`)
	fallback, err := d.decodePlain(noData)
	if err != nil || fallback[0].Payload != string(noData) {
		t.Fatalf("payload fallback = %v, %v", fallback, err)
	}
	req := httptest.NewRequest(http.MethodPost, "/dapr/events/aoa-events", bytes.NewReader(noData))
	if _, err := d.Decode(context.Background(), req, noData); err != nil {
		t.Fatalf("Dapr Decode fallback: %v", err)
	}
	extra := d.ExtraRoutes()
	if len(extra) != 1 {
		t.Fatal("Dapr discovery route missing")
	}
	w := httptest.NewRecorder()
	extra[0].Handler(w, httptest.NewRequest(http.MethodGet, "/dapr/subscribe", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "aoa-events") {
		t.Fatalf("discovery = %d %q", w.Code, w.Body.String())
	}
}

func TestCoverageKubernetesDecoder(t *testing.T) {
	d := NewKubernetesDecoder(Config{})
	if _, err := d.Decode(context.Background(), nil, []byte("broken")); err == nil {
		t.Fatal("invalid Kubernetes body must fail")
	}
	admission := []byte(`{"kind":"AdmissionReview","request":{"uid":"","namespace":"prod","operation":"CREATE","object":{"kind":"Pod"}}}`)
	admissionEvents, err := d.Decode(context.Background(), nil, admission)
	if err != nil || !strings.Contains(admissionEvents[0].SourceID, "hash:") {
		t.Fatalf("admission hash = %v, %v", admissionEvents, err)
	}
	core := []byte(`{"kind":"Event","metadata":{"name":"e1"},"reason":"BackOff","message":"restart"}`)
	coreEvents, err := d.Decode(context.Background(), nil, core)
	if err != nil || !strings.Contains(coreEvents[0].SourceID, "hash:") ||
		coreEvents[0].SessionID != "" {
		t.Fatalf("core event = %v, %v", coreEvents, err)
	}
	w := httptest.NewRecorder()
	d.WriteResponse(w, nil, false)
	if !strings.Contains(w.Body.String(), `"uid":""`) {
		t.Fatalf("WriteResponse with no events = %q", w.Body.String())
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatal("WriteResponse content type missing")
	}
	if _, err := d.Decode(context.Background(), nil, []byte(`{"kind":"Event","metadata":`)); err == nil {
		t.Fatal("truncated core event must fail")
	}
}

func TestCoverageCloudEventsDecoder(t *testing.T) {
	d := NewCloudEventsDecoder(Config{})
	r := httptest.NewRequest(http.MethodPost, "/events", nil)
	if _, err := d.Decode(context.Background(), r, []byte("invalid")); err == nil {
		t.Fatal("invalid CloudEvent must fail")
	}
	if _, err := DecodeCloudEvent(httptest.NewRequest(http.MethodPost, "/events", nil),
		[]byte("invalid")); err == nil {
		t.Fatal("DecodeCloudEvent must fail on invalid input")
	}

	// An id-less structured event exercises the body-hash fallback.
	body := []byte(`{"specversion":"1.0","source":"svc","type":"` + aoa.CETypeTimer +
		`","sessionid":"from-extension","datacontenttype":"application/json","data":{"nested":1}}`)
	r2 := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	r2.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	events, err := d.Decode(context.Background(), r2, body)
	if err != nil || len(events) != 1 {
		t.Fatalf("CloudEvent = %v, %v", events, err)
	}
	if !strings.HasPrefix(events[0].SourceID, "ce:svc:hash:") ||
		events[0].SessionID != "from-extension" || events[0].Kind != aoa.KindTimer {
		t.Fatalf("CloudEvent event = %+v", events[0])
	}

	badData := cloudevents.NewEvent()
	badData.SetID("x")
	badData.SetSource("s")
	badData.SetType(aoa.CETypeWebhook)
	if err := badData.SetData("text/plain", []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	badBody, _ := json.Marshal(badData)
	r3 := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(badBody))
	r3.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	if _, err := d.Decode(context.Background(), r3, badBody); err == nil {
		t.Fatal("non-JSON data must fail")
	}
}

// ---------- server branches ----------

func TestCoverageServerBranches(t *testing.T) {
	if _, err := NewHandler(ServerConfig{}); err == nil {
		t.Fatal("nil Store must fail")
	}
	st := testStore(t)
	for _, route := range [][]Route{{{}}, {{Path: "/events"}}} {
		if _, err := NewHandler(ServerConfig{Store: st, Routes: route}); err == nil {
			t.Fatal("invalid route must fail")
		}
	}

	decodeErr := DecoderFunc{Label: "broken", Fn: func(context.Context, *http.Request, []byte) ([]CanonicalEvent, error) {
		return nil, errors.New("decode failed")
	}}
	h, err := NewHandler(ServerConfig{Store: st, Routes: []Route{
		{Path: "/bad", Decoder: decodeErr},
		{Path: "/sinkerr", Decoder: DecoderFunc{Label: "sinkerr",
			Fn: func(context.Context, *http.Request, []byte) ([]CanonicalEvent, error) {
				return []CanonicalEvent{{SourceID: "x", Kind: aoa.KindWebhook}}, nil
			}}},
	}, Sink: SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return false, errors.New("sink failed")
	})})
	if err != nil {
		t.Fatal(err)
	}
	if got := post(h, "/bad", []byte("x"), nil).Code; got != http.StatusBadRequest {
		t.Fatalf("decode failure = %d", got)
	}
	if got := post(h, "/sinkerr", []byte("x"), nil).Code; got != http.StatusInternalServerError {
		t.Fatalf("sink failure = %d", got)
	}

	// Body read error on the event route.
	unreadable := httptest.NewRequest(http.MethodPost, "/bad", nil)
	unreadable.Body = errReader{}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, unreadable)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unreadable event body = %d", w.Code)
	}

	// Approval branches.
	approveHandler, err := NewHandler(ServerConfig{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		body string
		want int
	}{
		{`{`, http.StatusBadRequest},
		{`{}`, http.StatusBadRequest},
		{`{"approval_id":"a","decision":"maybe"}`, http.StatusBadRequest},
		{`{"approval_id":"missing","decision":"approve"}`, http.StatusNotFound},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, "/approvals", strings.NewReader(tc.body))
		rec := httptest.NewRecorder()
		approveHandler.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("approval %q = %d, want %d", tc.body, rec.Code, tc.want)
		}
	}

	// Store error while resolving an approval.
	closedStore := testStore(t)
	if err := closedStore.Close(); err != nil {
		t.Fatal(err)
	}
	closedHandler, err := NewHandler(ServerConfig{Store: closedStore})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/approvals",
		strings.NewReader(`{"approval_id":"a","decision":"approve"}`))
	rec := httptest.NewRecorder()
	closedHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("closed store approval = %d", rec.Code)
	}

	ready := httptest.NewRecorder()
	approveHandler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK || !strings.Contains(ready.Body.String(), `"ready":true`) {
		t.Fatalf("readyz = %d %q", ready.Code, ready.Body.String())
	}
}

// ---------- broker listeners ----------

type fakeKafkaReader struct {
	queue    []kafka.Message
	err      error
	closeErr error
	closed   bool
}

func (f *fakeKafkaReader) ReadMessage(context.Context) (kafka.Message, error) {
	if len(f.queue) > 0 {
		m := f.queue[0]
		f.queue = f.queue[1:]
		return m, nil
	}
	return kafka.Message{}, f.err
}
func (f *fakeKafkaReader) Close() error {
	f.closed = true
	return f.closeErr
}

func TestCoverageKafkaStart(t *testing.T) {
	if itoa(0) != "0" || itoa(42) != "42" || itoa(-7) != "-7" {
		t.Fatal("itoa failed")
	}
	reader := &fakeKafkaReader{
		queue: []kafka.Message{{
			Offset: 7, Key: []byte("session-1"), Value: []byte("payload"),
			Headers: []kafka.Header{{Key: "traceparent", Value: []byte("trace-1")}},
		}},
		err: context.Canceled,
	}
	l, err := NewKafkaListener(Config{})
	if err != nil {
		t.Fatal(err)
	}
	kl := l.(*KafkaListener)
	kl.newReader = func() kafkaMessageReader { return reader }
	var got CanonicalEvent
	// A cancelled context makes Start treat the terminal read error as a clean stop.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := kl.Start(ctx, SinkFunc(func(_ context.Context, ev CanonicalEvent) (bool, error) {
		got = ev
		return true, nil
	})); err != nil {
		t.Fatal(err)
	}
	if got.SourceID != "kafka:aoa-events:7" || got.SessionID != "session-1" ||
		got.TraceID != "trace-1" {
		t.Fatalf("kafka event = %+v", got)
	}
	if !reader.closed {
		t.Fatal("reader must be closed after Start returns")
	}

	failing := &fakeKafkaReader{queue: []kafka.Message{{Value: []byte("x")}}, err: nil}
	failingListener, _ := NewKafkaListener(Config{})
	fl := failingListener.(*KafkaListener)
	fl.newReader = func() kafkaMessageReader { return failing }
	if err := fl.Start(context.Background(), SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return false, errors.New("rejected")
	})); err == nil {
		t.Fatal("sink rejection must stop the kafka loop")
	}

	stopListener, _ := NewKafkaListener(Config{})
	sl := stopListener.(*KafkaListener)
	stopReader := &fakeKafkaReader{}
	sl.mu.Lock()
	sl.reader = stopReader
	sl.mu.Unlock()
	if err := sl.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !stopReader.closed {
		t.Fatal("Stop must close the reader")
	}

	actualDial := &KafkaListener{}
	_ = actualDial
	if _, err := defaultNATSDial("nats://127.0.0.1:1"); err == nil {
		t.Fatal("unreachable NATS server must fail")
	}
}

type fakeNATSConn struct {
	js     nats.JetStreamContext
	jsErr  error
	closed bool
}

func (f *fakeNATSConn) JetStream(...nats.JSOpt) (nats.JetStreamContext, error) {
	return f.js, f.jsErr
}
func (f *fakeNATSConn) Close() { f.closed = true }

type fakeJetStream struct {
	nats.JetStreamContext
	handler   nats.MsgHandler
	addErr    error
	subErr    error
	subscribe bool
}

func (f *fakeJetStream) AddStream(*nats.StreamConfig, ...nats.JSOpt) (*nats.StreamInfo, error) {
	return nil, f.addErr
}
func (f *fakeJetStream) Subscribe(_ string, cb nats.MsgHandler, _ ...nats.SubOpt) (*nats.Subscription, error) {
	f.handler = cb
	f.subscribe = true
	return nil, f.subErr
}

func natsListenerWithFake(conn *fakeNATSConn) *NATSListener {
	l, _ := NewNATSListener(Config{})
	nl := l.(*NATSListener)
	nl.dial = func(string) (natsConn, error) { return conn, nil }
	return nl
}

func TestCoverageNATSStart(t *testing.T) {
	js := &fakeJetStream{addErr: errors.New("stream name already in use")}
	conn := &fakeNATSConn{js: js}
	l := natsListenerWithFake(conn)
	var got CanonicalEvent
	if err := l.Start(context.Background(), SinkFunc(func(_ context.Context, ev CanonicalEvent) (bool, error) {
		got = ev
		return true, nil
	})); err != nil {
		t.Fatal(err)
	}
	if !js.subscribe || js.handler == nil {
		t.Fatal("Subscribe was not called")
	}
	msg := nats.NewMsg("aoa.events")
	msg.Header.Set("traceparent", "trace-7")
	msg.Header.Set("Nats-Msg-Id", "msg-7")
	msg.Data = []byte("payload")
	js.handler(msg)
	if got.SourceID != "nats:msg-7" || got.TraceID != "trace-7" {
		t.Fatalf("nats event = %+v", got)
	}
	if err := l.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !conn.closed {
		t.Fatal("Stop must close the NATS connection")
	}

	// Sink rejection triggers Nak.
	js2 := &fakeJetStream{}
	conn2 := &fakeNATSConn{js: js2}
	l2 := natsListenerWithFake(conn2)
	if err := l2.Start(context.Background(), SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return false, errors.New("rejected")
	})); err != nil {
		t.Fatal(err)
	}
	js2.handler(nats.NewMsg("aoa.events"))

	// Error branches.
	dialErr, _ := NewNATSListener(Config{})
	dialErr.(*NATSListener).dial = func(string) (natsConn, error) {
		return nil, errors.New("dial failed")
	}
	if err := dialErr.Start(context.Background(), SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return true, nil
	})); err == nil {
		t.Fatal("dial failure must fail Start")
	}
	jsErr := &fakeNATSConn{jsErr: errors.New("jetstream failed")}
	if err := natsListenerWithFake(jsErr).Start(context.Background(), nil); err == nil {
		t.Fatal("JetStream failure must fail Start")
	}
	addErr := &fakeNATSConn{js: &fakeJetStream{addErr: errors.New("boom")}}
	if err := natsListenerWithFake(addErr).Start(context.Background(), nil); err == nil {
		t.Fatal("AddStream failure must fail Start")
	}
	subErr := &fakeNATSConn{js: &fakeJetStream{subErr: errors.New("boom")}}
	if err := natsListenerWithFake(subErr).Start(context.Background(), nil); err == nil {
		t.Fatal("Subscribe failure must fail Start")
	}
}

func TestCoverageWatermillLoop(t *testing.T) {
	sub := &fakeSubscriber{ch: make(chan *message.Message, 2)}
	l, err := NewWatermillListener(Config{Any: map[string]any{"subscriber": sub}})
	if err != nil {
		t.Fatal(err)
	}
	msg := message.NewMessage("wm-coverage", []byte("payload"))
	sub.ch <- nil
	sub.ch <- msg
	close(sub.ch)
	if err := l.Start(context.Background(), SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return true, nil
	})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("message was not acked")
	}

	errSub := &fakeSubscriber{ch: make(chan *message.Message, 1)}
	errSub.ch <- message.NewMessage("wm-fail", []byte("payload"))
	errListener, _ := NewWatermillListener(Config{Any: map[string]any{"subscriber": errSub}})
	if err := errListener.Start(context.Background(), SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return false, errors.New("rejected")
	})); err == nil {
		t.Fatal("sink rejection must stop the watermill loop")
	}
}

func TestCoverageRedisStreamSubscriber(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	sub, err := NewRedisStreamSubscriber(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	if _, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "aoa-events",
		Values: map[string]any{"payload": "hello", "session_id": "s1"},
	}).Result(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, err := sub.Subscribe(ctx, "aoa-events")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case msg, open := <-ch:
		if !open || msg == nil {
			t.Fatal("subscriber channel closed early")
		}
		if string(msg.Payload) != "hello" || msg.Metadata.Get("session_id") != "s1" {
			t.Fatalf("redis message = %+v", msg)
		}
		msg.Ack()
	case <-ctx.Done():
		t.Fatal("timed out waiting for a Redis stream message")
	}

	// A second subscription observes the redis.Nil branch while idle.
	idle, err := sub.Subscribe(ctx, "empty-stream")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case _, open := <-idle:
		if open {
			t.Fatal("idle stream must not emit a message")
		}
	case <-time.After(1500 * time.Millisecond):
	}

	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Subscribe(context.Background(), "aoa-events"); err == nil {
		t.Fatal("Subscribe after Close must fail")
	}
}

func TestCoverageServerDefaults(t *testing.T) {
	st := testStore(t)
	h, err := NewHandler(ServerConfig{Store: st, MaxBody: -1, Logger: slog.Default(),
		Metrics: &telemetry.Metrics{}})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("healthz = %d %q", w.Code, w.Body.String())
	}
}
