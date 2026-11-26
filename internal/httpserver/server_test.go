package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/projanvil/aoa/internal/aoa"
	"github.com/projanvil/aoa/internal/store"
)

func TestCloudEventIngressPersistsAndDeduplicates(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	srv := NewServer(ServerConfig{Store: st})

	ce := cloudevents.NewEvent()
	ce.SetID("alert-4821")
	ce.SetSource("monitoring")
	ce.SetType(aoa.CETypeWebhook)
	if err := ce.SetData(cloudevents.ApplicationJSON, map[string]any{
		"session_id": "s1",
		"payload":    "P95 > 800ms",
	}); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(ce)
	if err != nil {
		t.Fatal(err)
	}

	post := func() int {
		req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
		req.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec.Code
	}
	if code := post(); code != http.StatusAccepted {
		t.Fatalf("first POST status = %d, want 202", code)
	}
	if code := post(); code != http.StatusOK {
		t.Fatalf("duplicate POST status = %d, want 200", code)
	}

	events, err := st.Claim(context.Background(), time.Now(), time.Minute, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("inbox rows = %v, %v; want 1", events, err)
	}
	if events[0].Kind != aoa.KindWebhook || events[0].SessionID != "s1" ||
		events[0].Payload != "P95 > 800ms" {
		t.Fatalf("inbox event = %+v", events[0])
	}
}

func TestApprovalReplyBecomesResumeEvent(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	srv := NewServer(ServerConfig{Store: st})
	if err := st.UpsertApproval(context.Background(), store.Approval{
		ID: "a1", SessionID: "s1", Request: "rollback?",
	}); err != nil {
		t.Fatal(err)
	}

	body := []byte(`{"approval_id":"a1","decision":"approve"}`)
	req := httptest.NewRequest(http.MethodPost, "/approvals", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /approvals status = %d, want 202", rec.Code)
	}
	events, err := st.Claim(context.Background(), time.Now(), time.Minute, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("inbox rows = %v, %v; want 1", events, err)
	}
	if events[0].Kind != aoa.KindApproval || events[0].SessionID != "s1" {
		t.Fatalf("approval event = %+v", events[0])
	}
	if events[0].ResumeJSON != `[{"type":"approve"}]` {
		t.Fatalf("resume = %q", events[0].ResumeJSON)
	}
}

func TestSignatureRejectsUnsignedRequest(t *testing.T) {
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	srv := NewServer(ServerConfig{Store: st, SignatureKey: "secret"})

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", cloudevents.ApplicationCloudEventsJSON)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned POST status = %d, want 401", rec.Code)
	}
}
