package caddytailscaleservice

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/tailcfg"
)

func TestIdentityAuthorization(t *testing.T) {
	for _, test := range []struct {
		name           string
		handler        IdentityHandler
		identity       *apitype.WhoIsResponse
		lookupError    error
		forwardedLogin string
		allowed        bool
		expectedLogin  string
	}{
		{name: "direct identity replaces spoofed headers", handler: IdentityHandler{RequireUser: true}, identity: userIdentity(), allowed: true, expectedLogin: "reader@example.invalid"},
		{name: "direct identity requires user", handler: IdentityHandler{RequireUser: true}, identity: &apitype.WhoIsResponse{}},
		{name: "capability denies missing grant", handler: IdentityHandler{Capability: "example.invalid/cap/read"}, identity: userIdentity()},
		{name: "capability allows granted service", handler: IdentityHandler{Capability: "example.invalid/cap/read"}, identity: serviceIdentity(), allowed: true},
		{name: "capability requires user when configured", handler: IdentityHandler{Capability: "example.invalid/cap/read", RequireUser: true}, identity: serviceIdentity()},
		{name: "trusted proxy retains identity", handler: IdentityHandler{TrustedProxyTags: []string{"tag:proxy"}}, identity: proxyIdentity(), forwardedLogin: "forwarded@example.invalid", allowed: true, expectedLogin: "forwarded@example.invalid"},
		{name: "untrusted proxy refuses spoofed identity", handler: IdentityHandler{TrustedProxyTags: []string{"tag:proxy"}}, identity: userIdentity(), forwardedLogin: "forwarded@example.invalid"},
		{name: "trusted proxy requires identity", handler: IdentityHandler{TrustedProxyTags: []string{"tag:proxy"}}, identity: proxyIdentity()},
		{name: "lookup failure denies access", handler: IdentityHandler{RequireUser: true}, lookupError: errors.New("lookup failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set(userLoginHeader, "spoofed@example.invalid")
			request.Header.Set(userNameHeader, "Spoofed")
			request.Header.Set(userProfilePicHeader, "https://spoofed.example.invalid/avatar")
			request.Header.Set(appCapabilitiesHeader, "spoofed")
			request.Header.Set("X-Private-Token", "spoofed-token")
			if len(test.handler.TrustedProxyTags) != 0 {
				request.Header.Set(userLoginHeader, test.forwardedLogin)
			}

			test.handler.StripHeaders = []string{"X-Private-Token"}
			called := false
			next := caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
				called = true
				return nil
			})
			err := test.handler.serveIdentity(httptest.NewRecorder(), request, next,
				func(context.Context, string, tailcfg.ServiceName) (*apitype.WhoIsResponse, error) {
					return test.identity, test.lookupError
				})

			if called != test.allowed || (err == nil) != test.allowed {
				t.Fatalf("called=%t error=%v, want allowed=%t", called, err, test.allowed)
			}

			if request.Header.Get("X-Private-Token") != "" {
				t.Fatal("private header was forwarded")
			}

			if test.allowed && request.Header.Get(userLoginHeader) != test.expectedLogin {
				t.Fatalf("login=%q, want %q", request.Header.Get(userLoginHeader), test.expectedLogin)
			}

			if len(test.handler.TrustedProxyTags) == 0 && (request.Header.Get(appCapabilitiesHeader) == "spoofed" || request.Header.Get(userProfilePicHeader) != "") {
				t.Fatal("spoofed identity headers survived direct authentication")
			}

			if !test.allowed && len(test.handler.TrustedProxyTags) == 0 && request.Header.Get(appCapabilitiesHeader) != "" {
				t.Fatal("capabilities survived denied direct authentication")
			}

			if test.allowed && len(test.handler.TrustedProxyTags) == 0 {
				var capabilities tailcfg.PeerCapMap
				if err := json.Unmarshal([]byte(request.Header.Get(appCapabilitiesHeader)), &capabilities); err != nil {
					t.Fatalf("authenticated capabilities are not JSON: %v", err)
				}

				if capabilities == nil {
					t.Fatal("authenticated capabilities must be a JSON object")
				}

				if test.identity.CapMap != nil && !reflect.DeepEqual(capabilities, test.identity.CapMap) {
					t.Fatal("forwarded capabilities differ from the authenticated policy")
				}
			}
		})
	}
}

func TestIdentityConfiguration(t *testing.T) {
	for _, input := range []string{
		"tailscale_access example.invalid/cap/read",
		"tailscale_access example.invalid/cap/admin user {\n strip_headers X-Private-Token\n}",
		"tailscale_identity {\n trusted_proxy_tags tag:proxy\n}",
	} {
		t.Run(input, func(t *testing.T) {
			var parsed IdentityHandler
			if err := parsed.UnmarshalCaddyfile(caddyfile.NewTestDispenser(input)); err != nil {
				t.Fatal(err)
			}

			encoded, err := json.Marshal(parsed)
			if err != nil {
				t.Fatal(err)
			}

			var loaded IdentityHandler
			if err := json.Unmarshal(encoded, &loaded); err != nil {
				t.Fatal(err)
			}

			if loaded.Capability != parsed.Capability || loaded.RequireUser != parsed.RequireUser || len(loaded.TrustedProxyTags) != len(parsed.TrustedProxyTags) || len(loaded.StripHeaders) != len(parsed.StripHeaders) {
				t.Fatalf("configuration changed after serialization: %s", encoded)
			}
		})
	}
}
