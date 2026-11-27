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

func TestRulesClassifierSkipsNonStringValues(t *testing.T) {
	c := &RulesClassifier{BlockPhrases: []string{"DROP TABLE", "rm -rf /"}}
	// Non-string values (including nested maps) must be skipped, not matched.
	if got := c.Classify("aoa_send", map[string]any{
		"count":  42,
		"flag":   true,
		"nested": map[string]any{"command": "drop table users"},
	}); got != Allow {
		t.Fatalf("Classify(non-string args) = %q, want allow", got)
	}
	// Phrase matching is case-insensitive on string values.
	if got := c.Classify("aoa_send", map[string]any{"cmd": "please DROP TABLE users"}); got != Block {
		t.Fatalf("Classify(phrase) = %q, want block", got)
	}
	if got := c.Classify("aoa_send", map[string]any{"cmd": "rm -rf /tmp"}); got != Block {
		t.Fatalf("Classify(second phrase) = %q, want block", got)
	}
}

func TestHeaderProxyUsesDefaultClientWhenNil(t *testing.T) {
	// http.DefaultClient rejects an unsupported scheme before any network IO.
	p := &HeaderProxy{Token: "t"}
	req, err := http.NewRequest(http.MethodGet, "ftp://example.com/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Do(context.Background(), req); err == nil {
		t.Fatal("Do with a nil Client must fail on an unsupported scheme")
	}
}

// swapDefaultTransport makes http.DefaultClient fully deterministic for the
// duration of a test and restores the original transport afterwards.
func swapDefaultTransport(t *testing.T, rt http.RoundTripper) {
	t.Helper()
	old := http.DefaultClient.Transport
	http.DefaultClient.Transport = rt
	t.Cleanup(func() { http.DefaultClient.Transport = old })
}

func TestSafeProxyDoWithNilClient(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		swapDefaultTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if got := r.Header.Get("Authorization"); got != "Bearer t" {
				t.Fatalf("authorization = %q", got)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Request:    r,
			}, nil
		}))
		p := &SafeProxy{Token: "t"} // nil Client, no allowlist, no body limit
		req, err := http.NewRequest(http.MethodGet, "https://example.com/v1", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := p.Do(context.Background(), req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
	})

	t.Run("transport error", func(t *testing.T) {
		swapDefaultTransport(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("transport failed")
		}))
		p := &SafeProxy{Token: "t"}
		req, err := http.NewRequest(http.MethodGet, "https://example.com/v1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Do(context.Background(), req); err == nil {
			t.Fatal("Do must propagate a transport error")
		}
	})
}

func TestNewSafeProxyRejectsEmptyToken(t *testing.T) {
	if _, err := NewSafeProxy("", "example.com"); err == nil {
		t.Fatal("NewSafeProxy with an empty token must fail")
	}
}

func TestSafeProxyCheckRedirect(t *testing.T) {
	p, err := NewSafeProxy("t", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Client.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
}

func TestSafeProxyValidateTargetExtraCases(t *testing.T) {
	p, err := NewSafeProxy("t", "")
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
	if err := p.ValidateTarget(nil); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("nil URL = %v", err)
	}
	if err := p.ValidateTarget(mustParse("https:///no-host")); !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("empty host = %v", err)
	}
	// A public IP literal is allowed (it is not a private address).
	if err := p.ValidateTarget(mustParse("https://8.8.8.8/resource")); err != nil {
		t.Fatalf("public IP literal rejected: %v", err)
	}
	// The fall-through return of isPrivateIP must report a public IP as safe.
	if isPrivateIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("8.8.8.8 must not be treated as private")
	}
}
