package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/store"
	"github.com/projanvil/aoa/internal/telemetry"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func cloudEventBody(t *testing.T, id, source, ceType string, data any) []byte {
	t.Helper()
	e := cloudevents.NewEvent()
	e.SetID(id)
	e.SetSource(source)
	e.SetType(ceType)
	if err := e.SetData(cloudevents.ApplicationJSON, data); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func ceRequest(t *testing.T, body []byte) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	r.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	return r
}

func newHandler(t *testing.T, st *store.Store, routes []Route) http.Handler {
	t.Helper()
	h, err := NewHandler(ServerConfig{Store: st, Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func post(h http.Handler, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	r.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCloudEventsDecoderUsesSourceAndID(t *testing.T) {
	body := cloudEventBody(t, "evt-1", "monitoring", aoa.CETypeWebhook,
		map[string]any{"session_id": "s1", "payload": "P95 > 800ms"})
	r := ceRequest(t, body)
	d := NewCloudEventsDecoder(Config{})
	events, err := d.Decode(context.Background(), r, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].SourceID != "ce:monitoring:evt-1" {
		t.Fatalf("SourceID = %q", events[0].SourceID)
	}
	if events[0].Kind != aoa.KindWebhook || events[0].SessionID != "s1" {
		t.Fatalf("event = %+v", events[0])
	}
}

func TestCloudEventsDecodeRejectsGarbage(t *testing.T) {
	d := NewCloudEventsDecoder(Config{})
	r := ceRequest(t, []byte("not json"))
	if _, err := d.Decode(context.Background(), r, []byte("not json")); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestIngressRoutesAndDeduplication(t *testing.T) {
	st := testStore(t)
	h := newHandler(t, st, []Route{{
		Path: "/events", Decoder: NewCloudEventsDecoder(Config{}),
	}})
	body := cloudEventBody(t, "evt-2", "grafana", aoa.CETypeWebhook,
		map[string]any{"payload": "alert"})

	if got := post(h, "/events", body, nil).Code; got != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", got)
	}
	if got := post(h, "/events", body, nil).Code; got != http.StatusOK {
		t.Fatalf("duplicate status = %d, want 200", got)
	}
	events, err := st.Claim(context.Background(), time.Now(), time.Minute, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("inbox rows = %v, %v; want 1", events, err)
	}
	if got := httptest.NewRecorder(); true {
		r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		h.ServeHTTP(got, r)
		if got.Code != http.StatusOK {
			t.Fatalf("healthz = %d", got.Code)
		}
	}
	m := httptest.NewRecorder()
	h.ServeHTTP(m, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if m.Code != http.StatusOK || !strings.Contains(m.Body.String(), "events_inbound") {
		t.Fatalf("metrics = %d %q", m.Code, m.Body.String())
	}
}

func TestExternalEventsCannotInjectApprovalOrResume(t *testing.T) {
	st := testStore(t)
	h := newHandler(t, st, []Route{{
		Path: "/events", Decoder: NewCloudEventsDecoder(Config{}),
	}})
	body := cloudEventBody(t, "evil", "external", aoa.CETypeApproval,
		map[string]any{"session_id": "s1", "resume_json": `[{"type":"approve"}]`})
	if got := post(h, "/events", body, nil).Code; got != http.StatusBadRequest {
		t.Fatalf("forged approval status = %d, want 400", got)
	}
	events, err := st.Claim(context.Background(), time.Now(), time.Minute, 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("inbox must stay empty, got %v, %v", events, err)
	}
}

func TestSignatureBindsMethodPathAndTimestamp(t *testing.T) {
	st := testStore(t)
	key := "secret"
	h, err := NewHandler(ServerConfig{
		Store:        st,
		Routes:       []Route{{Path: "/events", Decoder: NewCloudEventsDecoder(Config{})}},
		SignatureKey: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := cloudEventBody(t, "sig-1", "monitoring", aoa.CETypeWebhook,
		map[string]any{"payload": "hi"})
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	good := SignatureHeaderValue(key, http.MethodPost, "/events", ts, body)
	headers := map[string]string{"X-AOA-Signature": good, TimestampHeader: ts}
	if got := post(h, "/events", body, headers).Code; got != http.StatusAccepted {
		t.Fatalf("signed request = %d, want 202", got)
	}
	// The same MAC over a different path must fail.
	badPath := map[string]string{"X-AOA-Signature": good, TimestampHeader: ts}
	r := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	r.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	for k, v := range badPath {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound && w.Code != http.StatusUnauthorized {
		t.Fatalf("rebound request = %d", w.Code)
	}
	old := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
	stale := map[string]string{
		"X-AOA-Signature": SignatureHeaderValue(key, http.MethodPost, "/events", old, body),
		TimestampHeader:   old,
	}
	if got := post(h, "/events", body, stale).Code; got != http.StatusUnauthorized {
		t.Fatalf("stale signature = %d, want 401", got)
	}
}

func TestApprovalReplyIsAtomicAndIdempotent(t *testing.T) {
	st := testStore(t)
	h := newHandler(t, st, nil)
	if err := st.UpsertApproval(context.Background(), store.Approval{
		ID: "a1", SessionID: "s1", Request: "rollback?",
	}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"approval_id":"a1","decision":"approve"}`)
	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/approvals", bytes.NewReader(body)))
	if first.Code != http.StatusAccepted {
		t.Fatalf("first reply = %d, want 202", first.Code)
	}
	second := httptest.NewRecorder()
	h.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/approvals", bytes.NewReader(body)))
	if second.Code != http.StatusOK {
		t.Fatalf("duplicate reply = %d, want 200", second.Code)
	}
	events, err := st.Claim(context.Background(), time.Now(), time.Minute, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("inbox rows = %v, %v; want 1", events, err)
	}
	if events[0].ResumeJSON != `[{"type":"approve"}]` {
		t.Fatalf("resume = %q", events[0].ResumeJSON)
	}
	// Finish the first reply so a later Claim cannot reclaim it by lease.
	for _, ev := range events {
		if err := st.Ack(context.Background(), ev.ID); err != nil {
			t.Fatal(err)
		}
	}
	// A conflicting decision must not create a second event.
	conflict := []byte(`{"approval_id":"a1","decision":"reject"}`)
	third := httptest.NewRecorder()
	h.ServeHTTP(third, httptest.NewRequest(http.MethodPost, "/approvals", bytes.NewReader(conflict)))
	if third.Code != http.StatusOK {
		t.Fatalf("conflicting decision = %d, want 200", third.Code)
	}
	all, err := st.Claim(context.Background(), time.Now().Add(time.Minute), time.Minute, 10)
	if err != nil || len(all) != 0 {
		t.Fatalf("extra events after conflict = %v, %v", all, err)
	}
}

func TestDaprDiscoveryAndCloudEventDelivery(t *testing.T) {
	st := testStore(t)
	d := NewDaprDecoder(Config{Options: map[string]string{"topic": "aoa-events"}})
	h := newHandler(t, st, []Route{{Path: d.SubscriptionRoute(), Decoder: d}})

	discovery := httptest.NewRecorder()
	h.ServeHTTP(discovery, httptest.NewRequest(http.MethodGet, "/dapr/subscribe", nil))
	if discovery.Code != http.StatusOK || !strings.Contains(discovery.Body.String(), "aoa-events") {
		t.Fatalf("discovery = %d %q", discovery.Code, discovery.Body.String())
	}
	body := cloudEventBody(t, "dapr-1", "dapr", aoa.CETypeWebhook,
		map[string]any{"session_id": "s2", "payload": "from dapr"})
	w := post(h, d.SubscriptionRoute(), body, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("dapr delivery = %d", w.Code)
	}
}

func TestDaprPlainEnvelopeFallback(t *testing.T) {
	d := NewDaprDecoder(Config{})
	body := []byte(`{"id":"d-1","topic":"aoa","type":"com.example.alert","data":{"latency":900}}`)
	events, err := d.Decode(context.Background(),
		httptest.NewRequest(http.MethodPost, "/dapr/events/aoa", bytes.NewReader(body)), body)
	if err != nil || len(events) != 1 {
		t.Fatalf("events = %v, %v", events, err)
	}
	if events[0].SourceID != "dapr:d-1" || !strings.Contains(events[0].Payload, "latency") {
		t.Fatalf("event = %+v", events[0])
	}
}

func TestWebhookDecoderJSONAndText(t *testing.T) {
	d := NewWebhookDecoder(Config{})
	body := []byte(`{"source_id":"w-1","session_id":"s3","payload":"deploy failed","kind":"webhook"}`)
	events, err := d.Decode(context.Background(),
		httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body)), body)
	if err != nil || len(events) != 1 || events[0].SourceID != "webhook:w-1" {
		t.Fatalf("json events = %v, %v", events, err)
	}
	plain := []byte("plain text alert")
	textEvents, err := d.Decode(context.Background(),
		httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(plain)), plain)
	if err != nil || len(textEvents) != 1 || textEvents[0].Payload != "plain text alert" {
		t.Fatalf("text events = %v, %v", textEvents, err)
	}
}

func TestKubernetesAdmissionReviewResponse(t *testing.T) {
	st := testStore(t)
	d := NewKubernetesDecoder(Config{})
	h := newHandler(t, st, []Route{{Path: "/k8s/events", Decoder: d}})
	body := []byte(`{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","request":{"uid":"uid-1","namespace":"prod","operation":"CREATE","object":{"kind":"Pod"}}}`)
	r := httptest.NewRequest(http.MethodPost, "/k8s/events", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var reply map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	response, _ := reply["response"].(map[string]any)
	if response["uid"] != "uid-1" || response["allowed"] != true {
		t.Fatalf("response = %v", reply)
	}
}

func TestKubernetesCoreEvent(t *testing.T) {
	d := NewKubernetesDecoder(Config{})
	body := []byte(`{"kind":"Event","metadata":{"uid":"e-1"},"reason":"BackOff","message":"restart","involvedObject":{"kind":"Pod","name":"api","namespace":"prod"}}`)
	events, err := d.Decode(context.Background(),
		httptest.NewRequest(http.MethodPost, "/k8s/events", bytes.NewReader(body)), body)
	if err != nil || len(events) != 1 || events[0].SessionID != "prod" {
		t.Fatalf("events = %v, %v", events, err)
	}
}

func TestChainOrderAndRegistry(t *testing.T) {
	var order []string
	link := func(label string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, label)
				next.ServeHTTP(w, r)
			})
		}
	}
	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		order = append(order, "handler")
	}), link("outer"), link("inner"))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	want := "outer,inner,handler"
	if got := strings.Join(order, ","); got != want {
		t.Fatalf("chain order = %q, want %q", got, want)
	}

	reg := DefaultRegistry()
	if len(reg.DecoderNames()) != 4 || len(reg.ListenerNames()) != 3 {
		t.Fatalf("registry = %v / %v", reg.DecoderNames(), reg.ListenerNames())
	}
	if err := reg.RegisterDecoder("cloudevents", func(Config) (Decoder, error) { return nil, nil }); err == nil {
		t.Fatal("duplicate decoder registration must fail")
	}
	if _, err := reg.NewDecoder("missing", Config{}); err == nil {
		t.Fatal("unknown decoder must fail")
	}
	if _, err := reg.NewListener("watermill", Config{}); err == nil {
		t.Fatal("watermill without a subscriber must fail")
	}
	if _, err := reg.NewListener("nats", Config{}); err != nil {
		t.Fatalf("nats factory: %v", err)
	}
	if _, err := reg.NewListener("kafka", Config{}); err != nil {
		t.Fatalf("kafka factory: %v", err)
	}
}

func TestStoreSinkCountsAndValidation(t *testing.T) {
	st := testStore(t)
	metrics := &telemetry.Metrics{}
	sink := NewStoreSink(st, metrics, nil)
	ev := CanonicalEvent{SourceID: "x:1", Kind: aoa.KindWebhook, Payload: "hi"}
	if ok, err := sink.Accept(context.Background(), ev); err != nil || !ok {
		t.Fatalf("first = %v, %v", ok, err)
	}
	if ok, err := sink.Accept(context.Background(), ev); err != nil || ok {
		t.Fatalf("duplicate = %v, %v", ok, err)
	}
	bad := CanonicalEvent{Kind: aoa.KindWebhook}
	if _, err := sink.Accept(context.Background(), bad); err == nil {
		t.Fatal("empty source id must be rejected")
	}
	if metrics.EventsInbound.Load() != 1 || metrics.EventsDuplicate.Load() != 1 ||
		metrics.EventsRejected.Load() != 1 {
		t.Fatalf("metrics = %v", metrics.Snapshot())
	}
	counter := &CountingSink{}
	_, _ = counter.Accept(context.Background(), ev)
	if len(counter.Events) != 1 {
		t.Fatal("CountingSink did not record the event")
	}
}

func TestBrokerHelpers(t *testing.T) {
	ev := BrokerEvent("nats:", "", []byte("payload"), "", "s1")
	if ev.Kind != aoa.KindBackground || !strings.HasPrefix(ev.SourceID, "nats:hash:") {
		t.Fatalf("event = %+v", ev)
	}
	if KindFromSubject("aoa.approval.request") != aoa.KindApproval {
		t.Fatal("approval subject mapping failed")
	}
	if err := AcceptWithRetry(context.Background(), SinkFunc(func(context.Context, CanonicalEvent) (bool, error) {
		return false, fmt.Errorf("rejected")
	}), ev, nil); err == nil {
		t.Fatal("AcceptWithRetry must surface sink errors")
	}
}

func FuzzCloudEventsDecoder(f *testing.F) {
	f.Add(cloudEventBodyForFuzz())
	d := NewCloudEventsDecoder(Config{})
	f.Fuzz(func(t *testing.T, body []byte) {
		r := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
		r.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
		_, _ = d.Decode(context.Background(), r, body)
	})
}

func cloudEventBodyForFuzz() []byte {
	e := cloudevents.NewEvent()
	e.SetID("seed")
	e.SetSource("fuzz")
	e.SetType(aoa.CETypeWebhook)
	_ = e.SetData(cloudevents.ApplicationJSON, map[string]any{"payload": "x"})
	b, _ := json.Marshal(e)
	return b
}
