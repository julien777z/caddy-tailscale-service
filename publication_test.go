package caddytailscaleservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"tailscale.com/tailcfg"
	"testing"

	"tailscale.com/tsnet"
)

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

func TestPublicationRequiresPrimaryRoute(t *testing.T) {
	for _, scenario := range []struct {
		routed  bool
		publish bool
	}{
		{routed: false, publish: false},
		{routed: true, publish: false},
		{routed: false, publish: true},
		{routed: true, publish: true},
	} {
		t.Run(fmt.Sprintf("routed=%t,publish=%t", scenario.routed, scenario.publish), func(t *testing.T) {
			routed := scenario.routed
			path := ""
			if scenario.publish {
				path = filepath.Join(t.TempDir(), "services.json")
			}

			published, err := writeServiceAddresses(context.Background(), publicationClient(t, routed), path, string(publicationServiceName))
			if err != nil || published != routed {
				t.Fatalf("published=%t error=%v, want %t", published, err, routed)
			}

			if !scenario.publish {
				return
			}

			contents, readErr := os.ReadFile(path)
			if !routed {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatalf("unrouted service was published: %v", readErr)
				}

				return
			}

			if readErr != nil {
				t.Fatal(readErr)
			}

			var addresses map[string]string
			if err := json.Unmarshal(contents, &addresses); err != nil {
				t.Fatal(err)
			}

			if addresses[string(publicationServiceName)] != "100.64.0.10" {
				t.Fatalf("published addresses=%v", addresses)
			}
		})
	}
}

func TestPublicationAfterClosure(t *testing.T) {
	node := &serviceNode{Server: &tsnet.Server{Dir: t.TempDir()}}
	if err := node.Close(); err != nil {
		t.Fatalf("close node: %v", err)
	}

	path := filepath.Join(t.TempDir(), "services.json")
	node.publishServiceAddresses(path, string(publicationServiceName))
	if node.publisherCancel != nil {
		t.Fatal("publisher started after its node closed")
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("closed node published service addresses: %v", err)
	}
}
