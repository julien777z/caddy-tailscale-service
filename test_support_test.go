package caddytailscaleservice

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"tailscale.com/client/local"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/views"
	"testing"

	"tailscale.com/tsnet"
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

const publicationServiceName tailcfg.ServiceName = "svc:example"

func publicationClient(t *testing.T, routed bool) *local.Client {
	t.Helper()
	routes := views.SliceOf([]netip.Prefix{})
	if routed {
		routes = views.SliceOf([]netip.Prefix{netip.MustParsePrefix("100.64.0.10/32")})
	}
	services := map[tailcfg.ServiceName]tailcfg.ServiceDetails{
		publicationServiceName: {Name: publicationServiceName, Addrs: []netip.Addr{netip.MustParseAddr("100.64.0.10")}},
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

func publicationNode(t *testing.T, routed bool) *serviceNode {
	t.Helper()
	node := &serviceNode{Server: &tsnet.Server{
		Dir: t.TempDir(), Hostname: "example-readiness", ControlURL: "http://127.0.0.1:1",
	}}
	t.Cleanup(func() { _ = node.Close() })
	client, err := node.LocalClient()
	if err != nil {
		t.Fatal(err)
	}

	external := publicationClient(t, routed)
	client.OmitAuth = external.OmitAuth
	client.Dial = external.Dial

	return node
}

type closeNotifyingListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (listener *closeNotifyingListener) Close() error {
	err := listener.Listener.Close()
	listener.once.Do(func() { close(listener.closed) })

	return err
}

type blockedServiceRegistration struct {
	listener    *retryingServiceListener
	created     *closeNotifyingListener
	attempts    atomic.Int32
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func newBlockedServiceRegistration(t *testing.T) *blockedServiceRegistration {
	t.Helper()
	service, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	registration := &blockedServiceRegistration{
		created: &closeNotifyingListener{Listener: service, closed: make(chan struct{})},
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	registration.listener = newRetryingServiceListener(
		&serviceNode{Server: &tsnet.Server{}}, string(publicationServiceName),
		tsnet.ServiceModeTCP{Port: 443}, nil,
	)
	registration.listener.listen = func(*serviceNode, string, tsnet.ServiceModeTCP) (net.Listener, error) {
		registration.attempts.Add(1)
		select {
		case registration.started <- struct{}{}:
		default:
		}
		<-registration.release

		return registration.created, nil
	}
	t.Cleanup(func() {
		_ = registration.listener.Close()
		registration.releaseOnce.Do(func() { close(registration.release) })
		_ = registration.created.Close()
	})

	return registration
}
