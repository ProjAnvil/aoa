package permission

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrHostNotAllowed is returned when a request target is outside the proxy
// allowlist or resolves to a private address.
var ErrHostNotAllowed = errors.New("permission: request host is not allowed")

// SafeProxy injects credentials and enforces an outbound allowlist. The
// agent can build any URL it likes; only this boundary decides what leaves
// the process.
type SafeProxy struct {
	Token     string
	Allowlist []string
	Client    *http.Client
	BodyLimit int64
}

var _ Proxy = (*SafeProxy)(nil)

// NewSafeProxy builds a proxy. An empty allowlist permits public HTTPS hosts
// only. A comma-separated allowlist restricts both host and scheme.
func NewSafeProxy(token, allowlist string) (*SafeProxy, error) {
	if token == "" {
		return nil, errors.New("permission: proxy token is empty")
	}
	var hosts []string
	for _, item := range strings.Split(allowlist, ",") {
		if h := strings.TrimSpace(strings.ToLower(item)); h != "" {
			hosts = append(hosts, h)
		}
	}
	return &SafeProxy{
		Token:     token,
		Allowlist: hosts,
		BodyLimit: 1 << 20,
		Client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// Do validates the target, injects the credential, and sends the request.
func (p *SafeProxy) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	if err := p.ValidateTarget(req.URL); err != nil {
		return nil, err
	}
	out := req.Clone(ctx)
	out.Header = req.Header.Clone()
	out.Header.Set("Authorization", "Bearer "+p.Token)
	client := p.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(out)
	if err != nil {
		return nil, err
	}
	if p.BodyLimit > 0 && resp.Body != nil {
		resp.Body = &limitedBody{ReadCloser: resp.Body, remaining: p.BodyLimit}
	}
	return resp, nil
}

// ValidateTarget rejects non-HTTP schemes, private IP literals, and hosts
// that are not on the allowlist. Production deployments pair it with an
// egress network policy, because DNS rebinding is outside this check.
func (p *SafeProxy) ValidateTarget(u *url.URL) error {
	if u == nil {
		return fmt.Errorf("%w: nil URL", ErrHostNotAllowed)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "https" {
		return fmt.Errorf("%w: HTTPS is required", ErrHostNotAllowed)
	}
	if u.User != nil {
		return fmt.Errorf("%w: credentials in URL", ErrHostNotAllowed)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return fmt.Errorf("%w: empty host", ErrHostNotAllowed)
	}
	if len(p.Allowlist) > 0 && !hostAllowed(host, p.Allowlist) {
		return fmt.Errorf("%w: %q", ErrHostNotAllowed, host)
	}
	if ip := net.ParseIP(host); ip != nil && isPrivateIP(ip) {
		return fmt.Errorf("%w: private literal", ErrHostNotAllowed)
	}
	return nil
}

func hostAllowed(host string, allowlist []string) bool {
	for _, allowed := range allowlist {
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			return true
		}
	}
	return false
}

func isPrivateIP(ip net.IP) bool {
	private := []string{
		"127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
		"169.254.0.0/16", "::1/128", "fc00::/7", "fe80::/10", "0.0.0.0/8",
	}
	for _, cidr := range private {
		_, block, err := net.ParseCIDR(cidr)
		if err == nil && block.Contains(ip) {
			return true
		}
	}
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()
}

type limitedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *limitedBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}
