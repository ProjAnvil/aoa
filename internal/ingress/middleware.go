package ingress

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/projanvil/aoa/internal/telemetry"
)

// Middleware is one link in the Chain of Responsibility. Chain applies the
// first element as the outermost handler.
type Middleware func(http.Handler) http.Handler

// Chain composes middleware around a handler.
func Chain(h http.Handler, links ...Middleware) http.Handler {
	for i := len(links) - 1; i >= 0; i-- {
		h = links[i](h)
	}
	return h
}

// BodyLimit rejects oversized bodies and replays the body for downstream
// links. It runs once, so decoders always see the exact original bytes.
func BodyLimit(max int64) Middleware {
	if max <= 0 {
		max = 1 << 20
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, max)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ReadAndReplay copies a body and restores it. Signature verification and
// decoding can both consume the same bytes.
func ReadAndReplay(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, nil
}

// SignatureRequired verifies an HMAC-SHA256 header over the exact body.
// An empty key disables the check for local development.
func SignatureRequired(key, header string) Middleware {
	if header == "" {
		header = "X-AOA-Signature"
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}
			raw, err := ReadAndReplay(r)
			if err != nil {
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			if !ValidRequestSignature(raw, r.Header.Get(header), key,
				r.Method, r.URL.Path, r.Header.Get(TimestampHeader)) {
				http.Error(w, "invalid signature", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// TimestampHeader carries the unix seconds used in the signature. A fresh
// timestamp bounds replay of a captured signed request.
const TimestampHeader = "X-AOA-Timestamp"

// ValidSignature performs a constant-time HMAC comparison over the body only.
// It remains for callers that do not use request binding.
func ValidSignature(body []byte, got, key string) bool {
	return ValidRequestSignature(body, got, key, "", "", "")
}

// ValidRequestSignature binds the MAC to method, path, timestamp, and body.
func ValidRequestSignature(body []byte, got, key, method, path, timestamp string) bool {
	if timestamp != "" {
		ts, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil || absInt64(time.Now().Unix()-ts) > 300 {
			return false
		}
	}
	h := hmac.New(sha256.New, []byte(key))
	_, _ = h.Write([]byte(method + "\n" + path + "\n" + timestamp + "\n"))
	_, _ = h.Write(body)
	want := hex.EncodeToString(h.Sum(nil))
	return hmac.Equal([]byte(got), []byte(want))
}

// SignatureHeaderValue computes the client-side header for a signed request.
func SignatureHeaderValue(key, method, path, timestamp string, body []byte) string {
	h := hmac.New(sha256.New, []byte(key))
	_, _ = h.Write([]byte(method + "\n" + path + "\n" + timestamp + "\n"))
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// SecureHeaders sets a small set of response headers.
func SecureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// HTTPStack assembles the standard chain used by every route.
func HTTPStack(next http.Handler, logger *slog.Logger, metrics *telemetry.Metrics, maxBody int64) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = &telemetry.Metrics{}
	}
	recoverLink := func(h http.Handler) http.Handler { return telemetry.Recover(h, logger) }
	accessLink := func(h http.Handler) http.Handler {
		return telemetry.AccessLog(h, logger, metrics)
	}
	return Chain(next,
		recoverLink,
		accessLink,
		BodyLimit(maxBody),
		SecureHeaders,
	)
}
