package caddytailscaleservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"tailscale.com/tsnet"
)

func TestCreateServiceNodeUsesDedicatedKeyAndNormalizedTags(t *testing.T) {
	t.Setenv("TS_ADVERTISE_TAGS", "tag:example-staging-internal-proxy tag:example-production-internal-proxy")

	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")
	node, err := createServiceNode("service-node")
	if err != nil {
		t.Fatalf("createServiceNode returned an error: %v", err)
	}

	secret, encodedAttributes, found := strings.Cut(node.ClientSecret, "?")
	if !found || secret != "tskey-client-service" {
		t.Fatalf("ClientSecret = %q, want preauthorized OAuth client secret", node.ClientSecret)
	}

	attributes, err := url.ParseQuery(encodedAttributes)
	if err != nil {
		t.Fatalf("parse client secret attributes: %v", err)
	}
	if attributes.Get("preauthorized") != "true" {
		t.Fatalf("preauthorized = %q, want true", attributes.Get("preauthorized"))
	}

	if len(node.AdvertiseTags) != 2 || node.AdvertiseTags[0] != "tag:example-staging-internal-proxy" || node.AdvertiseTags[1] != "tag:example-production-internal-proxy" {
		t.Fatalf("AdvertiseTags = %v, want normalized tags", node.AdvertiseTags)
	}
	if filepath.Base(node.Dir) != node.Hostname {
		t.Fatalf("Dir = %q, want directory scoped to hostname %q", node.Dir, node.Hostname)
	}
}

func TestCreateServiceNodeSeparatesStateByHostname(t *testing.T) {
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")
	serviceNode, err := createServiceNode("service-node")
	if err != nil {
		t.Fatalf("createServiceNode returned an error: %v", err)
	}

	upstreamNode, err := createServiceNode("upstream-node")
	if err != nil {
		t.Fatalf("createServiceNode returned an error: %v", err)
	}

	if serviceNode.Dir == upstreamNode.Dir {
		t.Fatalf("nodes share state directory %q", serviceNode.Dir)
	}
}

func TestCreateServiceNodeRequiresClientSecret(t *testing.T) {
	t.Setenv("TS_ADVERTISE_TAGS", "tag:example-staging-internal-proxy")

	t.Setenv("TS_OAUTH_SECRET", "")
	_, err := createServiceNode("service-node")
	if err == nil {
		t.Fatal("createServiceNode accepted an empty OAuth client secret")
	}
}

func TestGatewayTailscaleNodeNamesRequireDistinctNodes(t *testing.T) {
	t.Setenv("TAILSCALE_SERVICE_NODE_NAME", "gateway-node")
	t.Setenv("TAILSCALE_UPSTREAM_NODE_NAME", "gateway-node")

	_, _, err := gatewayTailscaleNodeNames()
	if err == nil {
		t.Fatal("gatewayTailscaleNodeNames accepted matching node names")
	}
}

func TestServiceListenerCloseBalancesBothPools(t *testing.T) {
	originalNodes := serviceNodes
	originalListeners := serviceListeners
	originalUpstreamNodes := upstreamNodes
	originalGatewayNodePairs := gatewayNodePairs
	serviceNodes = caddy.NewUsagePool()
	serviceListeners = caddy.NewUsagePool()
	upstreamNodes = caddy.NewUsagePool()
	gatewayNodePairs = caddy.NewUsagePool()
	t.Cleanup(func() {
		serviceNodes = originalNodes
		serviceListeners = originalListeners
		upstreamNodes = originalUpstreamNodes
		gatewayNodePairs = originalGatewayNodePairs
	})

	node := &serviceNode{Server: &tsnet.Server{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serviceNodes.LoadOrStore("service-node", node)
	gatewayNodePairs.LoadOrStore(
		gatewayNodePairKey("service-node", "upstream-node"),
		&gatewayTailscaleNodes{
			service:      node,
			serviceNode:  "service-node",
			upstream:     &serviceNode{Server: &tsnet.Server{}},
			upstreamNode: "upstream-node",
		},
	)
	wrapped := &serviceListener{
		sharedServiceListener: &sharedServiceListener{
			Listener: listener,
			key:      "svc:example:8080",
		},
		serviceNodeName:  "service-node",
		upstreamNodeName: "upstream-node",
		closedCh:         make(chan struct{}),
	}
	serviceListeners.LoadOrStore("svc:example:8080", wrapped.sharedServiceListener)
	upstreamNodes.LoadOrStore("upstream-node", &serviceNode{Server: &tsnet.Server{}})

	if err := wrapped.Close(); err != nil {
		t.Fatalf("serviceListener.Close returned an error: %v", err)
	}
	if _, exists := serviceListeners.References("svc:example:8080"); exists {
		t.Fatal("listener reference remains after release")
	}
	if _, exists := serviceNodes.References("service-node"); exists {
		t.Fatal("service node reference remains after release")
	}
	if _, exists := upstreamNodes.References("upstream-node"); exists {
		t.Fatal("upstream node reference remains after release")
	}
	if _, exists := gatewayNodePairs.References(gatewayNodePairKey("service-node", "upstream-node")); exists {
		t.Fatal("gateway node pair reference remains after release")
	}
}

func TestServiceListenerDefersTailscaleRegistration(t *testing.T) {
	originalServiceNodes := serviceNodes
	originalListeners := serviceListeners
	originalUpstreamNodes := upstreamNodes
	originalGatewayNodePairs := gatewayNodePairs
	serviceNodes = caddy.NewUsagePool()
	serviceListeners = caddy.NewUsagePool()
	upstreamNodes = caddy.NewUsagePool()
	gatewayNodePairs = caddy.NewUsagePool()
	t.Cleanup(func() {
		serviceNodes = originalServiceNodes
		serviceListeners = originalListeners
		upstreamNodes = originalUpstreamNodes
		gatewayNodePairs = originalGatewayNodePairs
	})
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")
	t.Setenv("TS_ADVERTISE_TAGS", "tag:example-staging-internal-proxy")
	t.Setenv("TAILSCALE_SERVICE_NAME", "svc:example-staging")
	t.Setenv("TAILSCALE_SERVICE_NODE_NAME", "example-staging-internal-proxy-host")

	blockedState := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedState, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}

	nodeName := os.Getenv("TAILSCALE_SERVICE_NODE_NAME")
	failedServer := &tsnet.Server{Hostname: nodeName, Dir: blockedState}
	if err := failedServer.Start(); err == nil {
		t.Fatal("SDK accepted a state path that is a file")
	}

	node := &serviceNode{Server: failedServer}
	if _, _, err := serviceNodes.LoadOrNew(nodeName, func() (caddy.Destructor, error) {
		return node, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = serviceNodes.Delete(nodeName) })

	loaded, err := getServiceListener(context.Background(), "", "", "443", 0, net.ListenConfig{})
	if err != nil {
		t.Fatalf("getServiceListener returned an error: %v", err)
	}

	if err := loaded.(*serviceListener).Close(); err != nil {
		t.Fatalf("close deferred listener: %v", err)
	}
}

func TestGatewayTailscaleNodesLoadWithoutTailscaleReadiness(t *testing.T) {
	originalServiceNodes := serviceNodes
	originalUpstreamNodes := upstreamNodes
	originalGatewayNodePairs := gatewayNodePairs
	serviceNodes = caddy.NewUsagePool()
	upstreamNodes = caddy.NewUsagePool()
	gatewayNodePairs = caddy.NewUsagePool()
	t.Cleanup(func() {
		serviceNodes = originalServiceNodes
		upstreamNodes = originalUpstreamNodes
		gatewayNodePairs = originalGatewayNodePairs
	})
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")

	nodes, err := loadGatewayTailscaleNodes("service-node", "upstream-node")
	if err != nil {
		t.Fatalf("loadGatewayTailscaleNodes returned an error: %v", err)
	}
	if nodes.service.Hostname != "service-node" || nodes.upstream.Hostname != "upstream-node" {
		t.Fatalf("gateway node names = service %q upstream %q", nodes.service.Hostname, nodes.upstream.Hostname)
	}
	if err := releaseGatewayTailscaleNodes("service-node", "upstream-node"); err != nil {
		t.Fatalf("gateway node pair cleanup returned an error: %v", err)
	}
}

func TestReadinessProvisionDoesNotRequireTailscaleRegistration(t *testing.T) {
	originalServiceNodes := serviceNodes
	originalUpstreamNodes := upstreamNodes
	originalGatewayNodePairs := gatewayNodePairs
	serviceNodes = caddy.NewUsagePool()
	upstreamNodes = caddy.NewUsagePool()
	gatewayNodePairs = caddy.NewUsagePool()
	t.Cleanup(func() {
		serviceNodes = originalServiceNodes
		upstreamNodes = originalUpstreamNodes
		gatewayNodePairs = originalGatewayNodePairs
	})
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")
	t.Setenv("TAILSCALE_SERVICE_NODE_NAME", "service-node")
	t.Setenv("TAILSCALE_UPSTREAM_NODE_NAME", "upstream-node")

	readiness := new(Readiness)
	if err := readiness.Provision(caddy.Context{Context: context.Background()}); err != nil {
		t.Fatalf("readiness provisioning returned an error: %v", err)
	}
	if readiness.node == nil {
		t.Fatal("readiness provisioning did not retain the service node")
	}
	if err := readiness.Cleanup(); err != nil {
		t.Fatalf("readiness cleanup returned an error: %v", err)
	}
}

func TestProxyProtocolPorts(t *testing.T) {
	for _, test := range []struct {
		name    string
		ports   string
		port    uint16
		version int
		invalid bool
	}{
		{name: "disabled by default", port: 443},
		{name: "configured TLS port", ports: "443 8080", port: 443, version: 2},
		{name: "configured plaintext port", ports: "443 8080", port: 8080, version: 2},
		{name: "unconfigured port", ports: "443", port: 8080},
		{name: "invalid configured port", ports: "65536", port: 443, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("TAILSCALE_PROXY_PROTOCOL_PORTS", test.ports)
			version, err := serviceProxyProtocol(test.port)
			if (err != nil) != test.invalid || version != test.version {
				t.Fatalf("version=%d error=%v, want version=%d invalid=%t", version, err, test.version, test.invalid)
			}
		})
	}
}

func TestSingleGatewayNodeLoadsWithoutUp(t *testing.T) {
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-example")

	nodes, err := loadGatewayTailscaleNodes("single-example", "")
	if err != nil {
		t.Fatal(err)
	}
	if nodes.service.Hostname != "single-example" || nodes.upstream != nil {
		t.Fatalf("unexpected single-node group: %+v", nodes)
	}
	if err := releaseGatewayTailscaleNodes("single-example", ""); err != nil {
		t.Fatal(err)
	}
	if _, exists := serviceNodes.References("single-example"); exists {
		t.Fatal("single node remains after cleanup")
	}
}

func TestListenerOverlap(t *testing.T) {
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-example")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	notifying := &notifyingListener{Listener: listener, started: make(chan struct{})}
	shared := &sharedServiceListener{Listener: notifying, key: t.Name()}
	wrappers := make([]*serviceListener, 2)
	for index := range wrappers {
		serviceListeners.LoadOrStore(shared.key, shared)
		if _, err := loadGatewayTailscaleNodes(t.Name(), ""); err != nil {
			t.Fatal(err)
		}
		wrappers[index] = &serviceListener{sharedServiceListener: shared, serviceNodeName: t.Name(), closedCh: make(chan struct{})}
		t.Cleanup(func() { _ = wrappers[index].Close() })
	}

	oldResult := make(chan error, 1)
	go func() {
		connection, err := wrappers[0].Accept()
		if connection != nil {
			_ = connection.Close()
		}
		oldResult <- err
	}()
	select {
	case <-notifying.started:
	case <-time.After(time.Second):
		t.Fatal("old wrapper did not begin accepting")
	}

	if err := wrappers[0].Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-oldResult:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("closed wrapper returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed wrapper kept Accept blocked")
	}

	newResult := make(chan error, 1)
	go func() {
		connection, err := wrappers[1].Accept()
		if connection != nil {
			_ = connection.Close()
		}
		newResult <- err
	}()
	client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	select {
	case err := <-newResult:
		if err != nil {
			t.Fatalf("surviving wrapper could not accept: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("surviving wrapper did not receive connection")
	}
}

func TestRetryingServiceListenerRecoversFromRegistrationFailure(t *testing.T) {
	service, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })

	var attempts atomic.Int32

	published := make(chan struct{}, 1)
	listener := newRetryingServiceListener(
		&serviceNode{Server: &tsnet.Server{}},
		"svc:example-staging",
		tsnet.ServiceModeTCP{Port: 443},
		func() { published <- struct{}{} },
	)
	listener.retryInterval = time.Millisecond
	listener.listen = func(*serviceNode, string, tsnet.ServiceModeTCP) (net.Listener, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("Tailscale tag is not approved")
		}

		return service, nil
	}

	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if connection != nil {
			_ = connection.Close()
		}
		accepted <- acceptErr
	}()

	deadline := time.Now().Add(time.Second)
	for attempts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if attempts.Load() < 2 {
		t.Fatal("listener did not retry after registration failed")
	}

	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("listener did not publish service addresses after recovery")
	}

	connection, err := net.DialTimeout("tcp", service.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial service listener: %v", err)
	}
	defer connection.Close()

	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("listener did not recover: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener did not accept after registration recovered")
	}
}

func TestServiceNodeReplacesCachedStartupFailure(t *testing.T) {
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")
	t.Setenv("TS_ADVERTISE_TAGS", "tag:example-staging")

	blockedState := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedState, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}

	failedServer := &tsnet.Server{Hostname: t.Name(), Dir: blockedState}
	initialError := failedServer.Start()
	if initialError == nil {
		t.Fatal("SDK accepted a state path that is a file")
	}

	failedServer.Dir = t.TempDir()
	if retryError := failedServer.Start(); retryError != initialError {
		t.Fatalf("SDK did not retain its initialization error: %v", retryError)
	}

	node := &serviceNode{Server: failedServer}
	t.Cleanup(func() { _ = node.Close() })
	if _, err := node.LocalClient(); err == nil {
		t.Fatal("failed initialization was not reported")
	}

	if node.Server == failedServer {
		t.Fatal("retry retained the SDK instance with a cached initialization failure")
	}
	if node.Hostname != t.Name() || !node.Ephemeral {
		t.Fatal("replacement lost the ephemeral host identity")
	}
	if node.ClientSecret == "" || len(node.AdvertiseTags) != 1 {
		t.Fatal("replacement lost provisioning configuration")
	}

	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	closedServer := node.Server

	if _, err := node.LocalClient(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed lifecycle retried initialization: %v", err)
	}
	if node.Server != closedServer {
		t.Fatal("closed lifecycle replaced the SDK instance")
	}
}

func TestPendingRegistrationClosure(t *testing.T) {
	for _, waiters := range []int{1, 8} {
		t.Run(fmt.Sprintf("waiters=%d", waiters), func(t *testing.T) {
			registration := newBlockedServiceRegistration(t)
			results := make(chan error, waiters)
			for range waiters {
				go func() {
					connection, err := registration.listener.Accept()
					if connection != nil {
						_ = connection.Close()
					}
					results <- err
				}()
			}

			select {
			case <-registration.started:
			case <-time.After(time.Second):
				t.Fatal("service registration did not start")
			}
			time.Sleep(50 * time.Millisecond)

			if got := registration.attempts.Load(); got != 1 {
				t.Fatalf("pending registration attempts=%d, want one", got)
			}
			if err := registration.listener.Close(); err != nil {
				t.Fatal(err)
			}

			for range waiters {
				select {
				case err := <-results:
					if !errors.Is(err, net.ErrClosed) {
						t.Fatalf("closed registration returned %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("closure kept a service waiter blocked")
				}
			}

			registration.releaseOnce.Do(func() { close(registration.release) })
			select {
			case <-registration.created.closed:
			case <-time.After(time.Second):
				t.Fatal("late service listener was not closed")
			}
		})
	}
}

func TestAcceptFaultRecovery(t *testing.T) {
	for _, temporary := range []bool{true, false} {
		t.Run(fmt.Sprintf("temporary=%t", temporary), func(t *testing.T) {
			scenario := newAcceptRecoveryScenario(t, temporary)
			accepted := make(chan error, 1)
			go func() {
				connection, err := scenario.listener.Accept()
				if connection != nil {
					_ = connection.Close()
				}
				accepted <- err
			}()

			select {
			case <-scenario.first.failed:
			case <-time.After(time.Second):
				t.Fatal("accept fault was not exercised")
			}

			address := scenario.first.Addr()
			if !temporary {
				deadline := time.Now().Add(time.Second)
				for scenario.attempts.Load() < 2 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}

				if scenario.retirementViolation.Load() {
					t.Fatal("replacement began before service retirement completed")
				}

				select {
				case <-scenario.replaced:
				case <-time.After(time.Second):
					t.Fatal("failed service registration was not replaced")
				}
				address = scenario.replacement.Addr()
			}

			client, err := net.DialTimeout("tcp", address.String(), time.Second)
			if err != nil {
				t.Fatalf("recovered listener did not accept a connection: %v", err)
			}
			defer client.Close()

			select {
			case err := <-accepted:
				if err != nil {
					t.Fatalf("recovered accept returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("recovered listener kept the connection blocked")
			}

			if temporary {
				if scenario.attempts.Load() != 1 {
					t.Fatal("temporary accept failure replaced the active service")
				}

				select {
				case <-scenario.first.closed:
					t.Fatal("temporary accept failure closed the active service")
				default:
				}
			}
		})
	}
}

func TestRegistrationReplacement(t *testing.T) {
	scenario := newRegistrationReplacementScenario(t)
	leading := make(chan activeListenerResult, 1)
	lagging := make(chan activeListenerResult, 1)
	go runRegistrationReader("lagging", func() {
		active, err := scenario.listener.activeListener()
		lagging <- activeListenerResult{listener: active, err: err}
	})
	select {
	case <-scenario.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("initial registration did not start")
	}
	waitForReaderPhase(t, "lagging", false, nil)

	go runRegistrationReader("leading", func() {
		active, err := scenario.listener.activeListener()
		if err != nil {
			leading <- activeListenerResult{err: err}
			return
		}

		if err = scenario.listener.clear(active); err != nil {
			leading <- activeListenerResult{err: err}
			return
		}

		active, err = scenario.listener.activeListener()
		leading <- activeListenerResult{listener: active, err: err}
	})
	waitForReaderPhase(t, "leading", false, nil)

	scenario.releaseFirstOnce.Do(func() { close(scenario.releaseFirst) })
	select {
	case <-scenario.first.started:
	case <-time.After(time.Second):
		t.Fatal("initial registration was not retired")
	}
	waitForReaderPhase(t, "lagging", true, nil)
	scenario.first.releaseClose()
	select {
	case <-scenario.replacementStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement registration did not start")
	}

	waitForReaderPhase(t, "lagging", false, lagging)
	scenario.releaseReplacementOnce.Do(func() { close(scenario.releaseReplacement) })
	for name, results := range map[string]chan activeListenerResult{"leading": leading, "lagging": lagging} {
		select {
		case result := <-results:
			if result.err != nil || result.listener != scenario.replacement {
				t.Fatalf("%s reader adopted obsolete registration: listener=%T error=%v", name, result.listener, result.err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s reader did not acquire replacement", name)
		}
	}

	connection, err := net.DialTimeout("tcp", scenario.replacement.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	accepted, err := scenario.listener.Accept()
	if err != nil {
		t.Fatalf("replacement not usable: %v", err)
	}
	_ = accepted.Close()
}
