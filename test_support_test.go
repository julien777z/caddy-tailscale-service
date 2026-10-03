package caddytailscaleservice

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"tailscale.com/client/local"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/views"
	"testing"
)

func userIdentity() *apitype.WhoIsResponse {
	return &apitype.WhoIsResponse{UserProfile: &tailcfg.UserProfile{LoginName: "reader@example.invalid", DisplayName: "Reader"}}
}

func serviceIdentity() *apitype.WhoIsResponse {
	return &apitype.WhoIsResponse{CapMap: tailcfg.PeerCapMap{"example.invalid/cap/read": nil}}
}

func proxyIdentity() *apitype.WhoIsResponse {
	return &apitype.WhoIsResponse{Node: &tailcfg.Node{Tags: []string{"tag:proxy"}}}
}

func publicationClient(t *testing.T, routed bool) *local.Client {
	t.Helper()
	routes := views.SliceOf([]netip.Prefix{})
	if routed {
		routes = views.SliceOf([]netip.Prefix{netip.MustParsePrefix("100.64.0.10/32")})
	}
	services := map[tailcfg.ServiceName]tailcfg.ServiceDetails{
		"svc:example": {Name: "svc:example", Addrs: []netip.Addr{netip.MustParseAddr("100.64.0.10")}},
	}
	status := &ipnstate.Status{Self: &ipnstate.PeerStatus{PrimaryRoutes: &routes}}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/localapi/v0/services":
			_ = json.NewEncoder(writer).Encode(services)
		case "/localapi/v0/status":
			_ = json.NewEncoder(writer).Encode(status)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)

	return &local.Client{
		OmitAuth: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
	}
}

type notifyingListener struct {
	net.Listener
	started chan struct{}
	once    sync.Once
}

func (listener *notifyingListener) Accept() (net.Conn, error) {
	listener.once.Do(func() { close(listener.started) })

	return listener.Listener.Accept()
}
