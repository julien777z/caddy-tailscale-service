package caddytailscaleservice

import (
	"context"
	"errors"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"tailscale.com/tsnet"
)

func TestCreateServiceNodeUsesDedicatedKeyAndNormalizedTags(t *testing.T) {
	t.Setenv("TS_ADVERTISE_TAGS", "tag:ci tag:gateway")

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

	if len(node.AdvertiseTags) != 2 || node.AdvertiseTags[0] != "tag:ci" || node.AdvertiseTags[1] != "tag:gateway" {
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
	t.Setenv("TS_ADVERTISE_TAGS", "tag:ci")

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
			service:       node,
			startupCancel: func() {},
			serviceNode:   "service-node",
			upstream:      &serviceNode{Server: &tsnet.Server{}},
			upstreamNode:  "upstream-node",
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

func TestLoadReadyGatewayTailscaleNodesWaitsForBothNodes(t *testing.T) {
	originalServiceNodes := serviceNodes
	originalUpstreamNodes := upstreamNodes
	originalGatewayNodePairs := gatewayNodePairs
	originalWaitForTailscaleNode := waitForTailscaleNode
	serviceNodes = caddy.NewUsagePool()
	upstreamNodes = caddy.NewUsagePool()
	gatewayNodePairs = caddy.NewUsagePool()
	started := make(chan string, 2)
	deadlines := make(chan time.Time, 2)
	release := make(chan struct{})
	waitForTailscaleNode = func(ctx context.Context, nodeName string, _ *serviceNode) error {
		deadline, hasDeadline := ctx.Deadline()
		if !hasDeadline {
			deadlines <- time.Time{}
		} else {
			deadlines <- deadline
		}
		started <- nodeName
		<-release

		return nil
	}
	t.Cleanup(func() {
		serviceNodes = originalServiceNodes
		upstreamNodes = originalUpstreamNodes
		gatewayNodePairs = originalGatewayNodePairs
		waitForTailscaleNode = originalWaitForTailscaleNode
	})
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")

	loadedNodes := make(chan *gatewayTailscaleNodes, 1)
	loadedError := make(chan error, 1)
	go func() {
		nodes, err := loadReadyGatewayTailscaleNodes(context.Background(), "service-node", "upstream-node")
		loadedNodes <- nodes
		loadedError <- err
	}()

	startedNodeNames := make(map[string]bool, 2)
	for range 2 {
		select {
		case nodeName := <-started:
			startedNodeNames[nodeName] = true
		case <-time.After(time.Second):
			t.Fatal("Tailscale node readiness did not start concurrently")
		}
	}

	firstDeadline := <-deadlines
	secondDeadline := <-deadlines
	close(release)

	if firstDeadline.IsZero() || secondDeadline.IsZero() {
		t.Fatal("Tailscale node readiness has no deadline")
	}
	if !firstDeadline.Equal(secondDeadline) {
		t.Fatalf("Tailscale node deadlines differ: %v and %v", firstDeadline, secondDeadline)
	}

	nodes := <-loadedNodes
	err := <-loadedError
	if err != nil {
		t.Fatalf("loadReadyGatewayTailscaleNodes returned an error: %v", err)
	}
	if nodes.service.Hostname != "service-node" {
		t.Fatalf("service node hostname = %q", nodes.service.Hostname)
	}
	if !startedNodeNames["service-node"] || !startedNodeNames["upstream-node"] {
		t.Fatalf("started node names = %v", startedNodeNames)
	}
	if err := releaseGatewayTailscaleNodes("service-node", "upstream-node"); err != nil {
		t.Fatalf("gateway node pair cleanup returned an error: %v", err)
	}
}

func TestReadinessProvisionBlocksHealthPublication(t *testing.T) {
	originalServiceNodes := serviceNodes
	originalUpstreamNodes := upstreamNodes
	originalGatewayNodePairs := gatewayNodePairs
	originalWaitForTailscaleNode := waitForTailscaleNode
	serviceNodes = caddy.NewUsagePool()
	upstreamNodes = caddy.NewUsagePool()
	gatewayNodePairs = caddy.NewUsagePool()
	started := make(chan string, 2)
	release := make(chan struct{})
	waitForTailscaleNode = func(_ context.Context, nodeName string, _ *serviceNode) error {
		started <- nodeName
		<-release

		return nil
	}
	t.Cleanup(func() {
		serviceNodes = originalServiceNodes
		upstreamNodes = originalUpstreamNodes
		gatewayNodePairs = originalGatewayNodePairs
		waitForTailscaleNode = originalWaitForTailscaleNode
	})
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")
	t.Setenv("TAILSCALE_SERVICE_NODE_NAME", "service-node")
	t.Setenv("TAILSCALE_UPSTREAM_NODE_NAME", "upstream-node")

	readiness := new(Readiness)
	provisioned := make(chan error, 1)
	go func() {
		provisioned <- readiness.Provision(caddy.Context{Context: context.Background()})
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("readiness provisioning did not wait for both Tailscale nodes")
		}
	}

	select {
	case err := <-provisioned:
		t.Fatalf("readiness provisioning completed before both nodes were ready: %v", err)
	default:
	}

	close(release)
	if err := <-provisioned; err != nil {
		t.Fatalf("readiness provisioning returned an error: %v", err)
	}
	if err := readiness.Cleanup(); err != nil {
		t.Fatalf("readiness cleanup returned an error: %v", err)
	}
	if _, exists := serviceNodes.References("service-node"); exists {
		t.Fatal("service node reference remains after readiness cleanup")
	}
	if _, exists := upstreamNodes.References("upstream-node"); exists {
		t.Fatal("upstream node reference remains after readiness cleanup")
	}
}

func TestLoadReadyGatewayServiceNodeCleansUpWhenUpstreamReadinessFails(t *testing.T) {
	for name, readinessError := range map[string]error{
		"timeout":     context.DeadlineExceeded,
		"unavailable": errors.New("unavailable"),
	} {
		t.Run(name, func(t *testing.T) {
			originalServiceNodes := serviceNodes
			originalUpstreamNodes := upstreamNodes
			originalGatewayNodePairs := gatewayNodePairs
			originalWaitForTailscaleNode := waitForTailscaleNode
			serviceNodes = caddy.NewUsagePool()
			upstreamNodes = caddy.NewUsagePool()
			gatewayNodePairs = caddy.NewUsagePool()
			waitForTailscaleNode = func(_ context.Context, nodeName string, _ *serviceNode) error {
				if nodeName == "upstream-node" {
					return readinessError
				}

				return nil
			}
			t.Cleanup(func() {
				serviceNodes = originalServiceNodes
				upstreamNodes = originalUpstreamNodes
				gatewayNodePairs = originalGatewayNodePairs
				waitForTailscaleNode = originalWaitForTailscaleNode
			})
			t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")

			_, err := loadReadyGatewayTailscaleNodes(context.Background(), "service-node", "upstream-node")
			if err == nil {
				t.Fatal("loadReadyGatewayTailscaleNodes accepted an unready upstream node")
			}
			if _, exists := serviceNodes.References("service-node"); exists {
				t.Fatal("service node reference remains after upstream readiness failure")
			}
			if _, exists := upstreamNodes.References("upstream-node"); exists {
				t.Fatal("upstream node reference remains after readiness failure")
			}
		})
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

func TestSingleNodeReadiness(t *testing.T) {
	originalWait := waitForTailscaleNode
	waitForTailscaleNode = func(context.Context, string, *serviceNode) error { return nil }
	t.Cleanup(func() { waitForTailscaleNode = originalWait })
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-example")

	nodes, err := loadReadyGatewayTailscaleNodes(context.Background(), "single-example", "")
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
	originalWait := waitForTailscaleNode
	waitForTailscaleNode = func(context.Context, string, *serviceNode) error { return nil }
	t.Cleanup(func() { waitForTailscaleNode = originalWait })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	notifying := &notifyingListener{Listener: listener, started: make(chan struct{})}
	shared := &sharedServiceListener{Listener: notifying, key: t.Name()}
	wrappers := make([]*serviceListener, 2)
	for index := range wrappers {
		serviceListeners.LoadOrStore(shared.key, shared)
		if _, err := loadReadyGatewayTailscaleNodes(context.Background(), t.Name(), ""); err != nil {
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

func TestListenerStartupDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() {
		_, err := listenBeforeDeadline(ctx, func() (net.Listener, error) {
			<-release
			return listener, nil
		})
		returned <- err
	}()
	cancel()

	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("listener ignored cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener creation escaped startup deadline")
	}
	close(release)

	// The late listener must be closed, not retained after startup cancellation.
	listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
	_, err = listener.Accept()
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late listener remained open: %v", err)
	}
}
