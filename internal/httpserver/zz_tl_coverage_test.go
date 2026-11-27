package httpserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTLNilStoreFailsClosed(t *testing.T) {
	srv := NewServer(ServerConfig{})
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/events", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil store handler = %d", w.Code)
	}
}
