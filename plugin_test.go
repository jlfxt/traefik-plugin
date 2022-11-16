package plugin_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	plugin "github.com/jlfxt/traefik-plugin"
)

func TestPlugin(t *testing.T) {
	cfg := plugin.CreateConfig()
	cfg.Kafka.BootstrapUrl = "foo.example.local"

	ctx := context.Background()
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {})

	handler, err := plugin.New(ctx, next, cfg, "traefik-plugin")
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Add("X-Plugin-Name", "traefik-plugin")
	req.Header.Add("X-Source", "test")

	handler.ServeHTTP(recorder, req)

	assertHeader(t, recorder.Result(), "X-Request-Id", "1")
}

func assertHeader(t *testing.T, resp *http.Response, key, expected string) {
	t.Helper()

	if resp.Header.Get(key) != expected {
		t.Errorf("invalid header value: %s", resp.Header.Get(key))
	}
}
