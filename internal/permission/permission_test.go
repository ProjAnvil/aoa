package permission

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClassifierDecisions(t *testing.T) {
	c := DefaultClassifier()
	cases := []struct {
		name string
		args map[string]any
		want Decision
	}{
		{"aoa_send", map[string]any{"message": "hello"}, Allow},
		{"aoa_rollback", map[string]any{"target": "cfg"}, Defer},
		{"aoa_drop_database", nil, Block},
		{"aoa_send", map[string]any{"message": "please DROP TABLE users"}, Block},
	}
	for _, tc := range cases {
		if got := c.Classify(tc.name, tc.args); got != tc.want {
			t.Fatalf("Classify(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestHeaderProxyInjectsCredential(t *testing.T) {
	p := &HeaderProxy{
		Token: "secret-token",
		Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
				t.Fatalf("authorization = %q", got)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok"))}, nil
		})},
	}
	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	empty := &HeaderProxy{}
	if _, err := empty.Do(context.Background(), req); err == nil {
		t.Fatal("proxy without a credential must fail closed")
	}
}

func TestSafeProxyRejectsUnsafeTargets(t *testing.T) {
	p, err := NewSafeProxy("token", "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	mustParse := func(raw string) *url.URL {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	if err := p.ValidateTarget(mustParse("http://api.example.com/x")); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("plain HTTP = %v", err)
	}
	if err := p.ValidateTarget(mustParse("https://evil.example.com/x")); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("outside allowlist = %v", err)
	}
	if err := p.ValidateTarget(mustParse("file:///etc/passwd")); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("file scheme = %v", err)
	}
	if err := p.ValidateTarget(mustParse("https://user:pass@api.example.com/x")); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("credentials in URL = %v", err)
	}
	if err := p.ValidateTarget(mustParse("https://api.example.com/path")); err != nil {
		t.Fatalf("allowed host rejected: %v", err)
	}

	public, err := NewSafeProxy("token", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := public.ValidateTarget(mustParse("http://example.com")); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("plain HTTP without allowlist = %v", err)
	}
	if err := public.ValidateTarget(mustParse("https://192.168.1.10/x")); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("private literal = %v", err)
	}
}

func TestSafeProxyDoAndBodyLimit(t *testing.T) {
	p, err := NewSafeProxy("token", "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	p.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("0123456789")),
		}, nil
	})}
	p.BodyLimit = 4
	req, err := http.NewRequest(http.MethodGet, "https://api.example.com/v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "0123" {
		t.Fatalf("body = %q, want 4 bytes", body)
	}
	blocked, err := http.NewRequest(http.MethodGet, "https://evil.example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Do(context.Background(), blocked); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("blocked request = %v", err)
	}
}
