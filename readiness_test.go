package caddytailscaleservice

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func TestReadinessRequiresPrimaryRoute(t *testing.T) {
	for _, routed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrouted", true: "routed"}[routed], func(t *testing.T) {
			client := publicationClient(t, routed)
			readiness := Readiness{
				client:      client,
				serviceName: string(publicationServiceName),
			}
			called := false
			next := caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
				called = true
				return nil
			})

			err := readiness.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil), next)

			if called != routed {
				t.Fatalf("health handler called=%t, want %t", called, routed)
			}

			if routed {
				if err != nil {
					t.Fatal(err)
				}

				return
			}

			failure, ok := err.(caddyhttp.HandlerError)
			if !ok || failure.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("unrouted health error=%v, want service unavailable", err)
			}
		})
	}
}
