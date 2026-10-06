package caddytailscaleservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"tailscale.com/client/local"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/views"
	"testing"
	"time"

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

type temporaryAcceptFailure struct{}

func (temporaryAcceptFailure) Error() string   { return "temporary accept failure" }
func (temporaryAcceptFailure) Temporary() bool { return true }
func (temporaryAcceptFailure) Timeout() bool   { return false }

type faultingAcceptListener struct {
	*closeNotifyingListener
	fault     error
	faultOnce sync.Once
	failed    chan struct{}
}

func (listener *faultingAcceptListener) Accept() (net.Conn, error) {
	var fault error
	listener.faultOnce.Do(func() {
		fault = listener.fault
		close(listener.failed)
	})

	if fault != nil {
		return nil, fault
	}

	return listener.Listener.Accept()
}

type acceptRecoveryScenario struct {
	listener            *retryingServiceListener
	first               *faultingAcceptListener
	replacement         *closeNotifyingListener
	attempts            atomic.Int32
	replaced            chan struct{}
	retirementViolation atomic.Bool
}

func newAcceptRecoveryScenario(t *testing.T, temporary bool) *acceptRecoveryScenario {
	t.Helper()
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	replacement, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}

	var fault error = net.ErrClosed
	if temporary {
		fault = temporaryAcceptFailure{}
	}

	scenario := &acceptRecoveryScenario{
		first: &faultingAcceptListener{
			closeNotifyingListener: &closeNotifyingListener{Listener: first, closed: make(chan struct{})},
			fault:                  fault,
			failed:                 make(chan struct{}),
		},
		replacement: &closeNotifyingListener{Listener: replacement, closed: make(chan struct{})},
		replaced:    make(chan struct{}),
	}
	scenario.listener = newRetryingServiceListener(
		&serviceNode{Server: &tsnet.Server{}}, string(publicationServiceName),
		tsnet.ServiceModeTCP{Port: 443}, nil,
	)
	scenario.listener.retryInterval = time.Millisecond
	scenario.listener.listen = func(*serviceNode, string, tsnet.ServiceModeTCP) (net.Listener, error) {
		if scenario.attempts.Add(1) == 1 {
			return scenario.first, nil
		}

		select {
		case <-scenario.first.closed:
		default:
			scenario.retirementViolation.Store(true)

			return nil, errors.New("previous service registration remains open")
		}

		select {
		case <-scenario.replaced:
		default:
			close(scenario.replaced)
		}

		return scenario.replacement, nil
	}
	t.Cleanup(func() {
		_ = scenario.listener.Close()
		_ = scenario.first.Close()
		_ = scenario.replacement.Close()
	})

	return scenario
}

type gatedCloseListener struct {
	*closeNotifyingListener
	started     chan struct{}
	release     chan struct{}
	gateOnce    sync.Once
	releaseOnce sync.Once
}

func (listener *gatedCloseListener) Close() error {
	listener.gateOnce.Do(func() {
		close(listener.started)
		<-listener.release
	})

	return listener.closeNotifyingListener.Close()
}

func (listener *gatedCloseListener) releaseClose() {
	listener.releaseOnce.Do(func() { close(listener.release) })
}

type activeListenerResult struct {
	listener net.Listener
	err      error
}

type registrationReplacementScenario struct {
	listener               *retryingServiceListener
	first                  *gatedCloseListener
	replacement            *closeNotifyingListener
	firstStarted           chan struct{}
	replacementStarted     chan struct{}
	releaseFirst           chan struct{}
	releaseReplacement     chan struct{}
	releaseFirstOnce       sync.Once
	releaseReplacementOnce sync.Once
	attempts               atomic.Int32
}

func newRegistrationReplacementScenario(t *testing.T) *registrationReplacementScenario {
	t.Helper()
	previousParallelism := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousParallelism) })

	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	replacement, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}

	scenario := &registrationReplacementScenario{
		first: &gatedCloseListener{
			closeNotifyingListener: &closeNotifyingListener{Listener: first, closed: make(chan struct{})},
			started:                make(chan struct{}), release: make(chan struct{}),
		},
		replacement:  &closeNotifyingListener{Listener: replacement, closed: make(chan struct{})},
		firstStarted: make(chan struct{}), replacementStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}), releaseReplacement: make(chan struct{}),
	}
	scenario.listener = newRetryingServiceListener(
		&serviceNode{Server: &tsnet.Server{}}, string(publicationServiceName),
		tsnet.ServiceModeTCP{Port: 443}, nil,
	)
	scenario.listener.listen = func(*serviceNode, string, tsnet.ServiceModeTCP) (net.Listener, error) {
		if scenario.attempts.Add(1) == 1 {
			close(scenario.firstStarted)
			<-scenario.releaseFirst

			return scenario.first, nil
		}

		close(scenario.replacementStarted)
		<-scenario.releaseReplacement

		return scenario.replacement, nil
	}
	t.Cleanup(func() {
		scenario.first.releaseClose()
		scenario.releaseFirstOnce.Do(func() { close(scenario.releaseFirst) })
		scenario.releaseReplacementOnce.Do(func() { close(scenario.releaseReplacement) })
		_ = scenario.listener.Close()
		_ = scenario.first.Close()
		_ = scenario.replacement.Close()
	})

	return scenario
}

func waitForReaderPhase(t *testing.T, label string, mutex bool, results <-chan activeListenerResult) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case result := <-results:
			t.Fatalf("reader %s returned obsolete registration while replacement pending: %v", label, result.err)
		default:
		}

		var profile bytes.Buffer
		if err := pprof.Lookup("goroutine").WriteTo(&profile, 1); err != nil {
			t.Fatal(err)
		}

		for _, block := range strings.Split(profile.String(), "\n\n") {
			if !strings.Contains(block, `"registration-reader":"`+label+`"`) || !strings.Contains(block, "activeListener") {
				continue
			}

			locked := strings.Contains(block, "lockSlow")
			waiting := strings.Contains(block, "runtime.selectgo")
			if (mutex && locked) || (!mutex && waiting && !locked) {
				return
			}
		}
		runtime.Gosched()
	}

	var profile bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&profile, 1)
	t.Fatalf("reader %s did not reach observed mutex=%t phase:\n%s", label, mutex, profile.String())
}

func runRegistrationReader(label string, operation func()) {
	pprof.Do(context.Background(), pprof.Labels("registration-reader", label), func(context.Context) { operation() })
}
