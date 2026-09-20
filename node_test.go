package caddytailscaleservice

import (
	"net"
	"net/netip"
	"path/filepath"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

func TestCreateServiceNodeUsesDedicatedKeyAndNormalizedTags(t *testing.T) {
	t.Setenv("TS_ADVERTISE_TAGS", "tag:ci tag:gateway")

	t.Setenv("TS_OAUTH_SECRET", "tskey-client-service")
	node, err := createServiceNode("service-node")
	if err != nil {
		t.Fatalf("createServiceNode returned an error: %v", err)
	}

	if node.ClientSecret != "tskey-client-service" {
		t.Fatalf("ClientSecret = %q, want OAuth client secret", node.ClientSecret)
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

func TestIPv4ServiceAddressesSelectsIPv4Addresses(t *testing.T) {
	services := map[tailcfg.ServiceName]tailcfg.ServiceDetails{
		"svc:api": {
			Name:  "svc:api",
			Addrs: []netip.Addr{netip.MustParseAddr("fd7a:115c:a1e0::1"), netip.MustParseAddr("100.64.0.10")},
		},
		"svc:ipv6-only": {
			Name:  "svc:ipv6-only",
			Addrs: []netip.Addr{netip.MustParseAddr("fd7a:115c:a1e0::2")},
		},
	}

	addresses := ipv4ServiceAddresses(services)

	if len(addresses) != 1 || addresses["svc:api"] != "100.64.0.10" {
		t.Fatalf("ipv4ServiceAddresses() = %v", addresses)
	}
}

func TestReleaseServiceReferencesBalancesBothPools(t *testing.T) {
	originalNodes := serviceNodes
	originalListeners := serviceListeners
	serviceNodes = caddy.NewUsagePool()
	serviceListeners = caddy.NewUsagePool()
	t.Cleanup(func() {
		serviceNodes = originalNodes
		serviceListeners = originalListeners
	})

	node := &serviceNode{Server: &tsnet.Server{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serviceNodes.LoadOrStore("service-node", node)
	serviceListeners.LoadOrStore(
		"svc:example:8080",
		&sharedServiceListener{Listener: listener, key: "svc:example:8080"},
	)

	if err := releaseServiceReferences("svc:example:8080", "service-node"); err != nil {
		t.Fatalf("releaseServiceReferences returned an error: %v", err)
	}
	if _, exists := serviceListeners.References("svc:example:8080"); exists {
		t.Fatal("listener reference remains after release")
	}
	if _, exists := serviceNodes.References("service-node"); exists {
		t.Fatal("service node reference remains after release")
	}
}
