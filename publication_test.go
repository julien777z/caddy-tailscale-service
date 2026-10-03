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
	for _, routed := range []bool{false, true} {
		t.Run(fmt.Sprintf("routed=%t", routed), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "services.json")
			published, err := writeServiceAddresses(context.Background(), publicationClient(t, routed), path, "svc:example")
			if err != nil || published != routed {
				t.Fatalf("published=%t error=%v, want %t", published, err, routed)
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
			if addresses["svc:example"] != "100.64.0.10" {
				t.Fatalf("published addresses=%v", addresses)
			}
		})
	}
}
