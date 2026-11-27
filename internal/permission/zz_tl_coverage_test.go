package permission

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestTLSafeProxyNilURLAndPrivateLiterals(t *testing.T) {
	p, err := NewSafeProxy("token", "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateTarget(nil); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("nil URL = %v", err)
	}
	for _, host := range []string{"[::1]", "[fe80::1]", "10.0.0.5", "169.254.1.1", "0.0.0.0"} {
		u, err := url.Parse("https://" + host + "/x")
		if err != nil {
			t.Fatal(err)
		}
		if err := p.ValidateTarget(u); !errors.Is(err, ErrHostNotAllowed) {
			t.Fatalf("host %q was allowed: %v", host, err)
		}
	}
	if !hostAllowed("sub.api.example.com", []string{"api.example.com"}) {
		t.Fatal("suffix allowlist failed")
	}
	if hostAllowed("api.example.com.evil", []string{"api.example.com"}) {
		t.Fatal("suffix allowlist accepted a lookalike host")
	}
	ip := net.ParseIP("172.16.0.1")
	if !isPrivateIP(ip) {
		t.Fatal("private IPv4 was not detected")
	}
	if isPrivateIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public IPv4 was marked private")
	}
}

func TestTLLimitedBodyEOF(t *testing.T) {
	p, err := NewSafeProxy("token", "api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	p.BodyLimit = 1
	p.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("abcdef"))}, nil
	})}
	req, err := http.NewRequest(http.MethodGet, "https://api.example.com/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "a" {
		t.Fatalf("body = %q, %v", body, err)
	}
}

func TestTLHeaderProxyDefaultClient(t *testing.T) {
	p := &HeaderProxy{Token: "t", Client: &http.Client{Transport: roundTripFunc(
		func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") != "Bearer t" {
				t.Fatal("credential missing")
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}}
	req, _ := http.NewRequest(http.MethodGet, "https://api.example.com", nil)
	resp, err := p.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
}

func TestTLClassifierNestedValues(t *testing.T) {
	c := DefaultClassifier()
	args := map[string]any{
		"nested": map[string]any{"text": "please DROP TABLE users"},
	}
	// The rules classifier only scans top-level strings. Nested values must
	// not be treated as a block phrase.
	if got := c.Classify("aoa_send", args); got != Allow {
		t.Fatalf("nested args = %q", got)
	}
}
