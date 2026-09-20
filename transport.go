package caddytailscaleservice

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
)

var upstreamNodes = caddy.NewUsagePool()

func init() {
	caddy.RegisterModule(&Transport{})
}

type Transport struct {
	ServerName string `json:"server_name,omitempty"`

	node      *serviceNode
	transport *http.Transport
}

func (Transport) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.transport.tailscale_service",
		New: func() caddy.Module { return new(Transport) },
	}
}

func (transport *Transport) UnmarshalCaddyfile(dispenser *caddyfile.Dispenser) error {
	for dispenser.Next() {
		for dispenser.NextBlock(0) {
			switch dispenser.Val() {
			case "tls_server_name":
				if !dispenser.AllArgs(&transport.ServerName) {
					return dispenser.ArgErr()
				}
			default:
				return dispenser.Errf("unrecognized tailscale_service transport option %q", dispenser.Val())
			}
		}
	}

	return nil
}

func (transport *Transport) Provision(_ caddy.Context) error {
	nodeName := os.Getenv("TAILSCALE_UPSTREAM_NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("TAILSCALE_UPSTREAM_NODE_NAME is required")
	}

	node, _, err := upstreamNodes.LoadOrNew(nodeName, func() (caddy.Destructor, error) {
		return createServiceNode(nodeName)
	})
	if err != nil {
		return err
	}

	transport.node = node.(*serviceNode)
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: transport.ServerName}
	transport.transport = &http.Transport{
		DialContext: transport.node.Dial,
		DialTLSContext: func(
			context context.Context,
			network string,
			address string,
		) (net.Conn, error) {
			connection, dialError := transport.node.Dial(context, network, address)
			if dialError != nil {
				return nil, dialError
			}

			secureConnection := tls.Client(connection, tlsConfig)
			if handshakeError := secureConnection.HandshakeContext(context); handshakeError != nil {
				_ = connection.Close()

				return nil, handshakeError
			}

			return secureConnection, nil
		},
		ForceAttemptHTTP2: true,
	}

	return nil
}

func (transport *Transport) Cleanup() error {
	if transport.node != nil {
		_, err := upstreamNodes.Delete(transport.node.Hostname)

		return err
	}

	return nil
}

func (transport *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	upstreamRequest := request.Clone(request.Context())
	upstreamURL := *request.URL
	upstreamURL.Scheme = "https"
	upstreamRequest.URL = &upstreamURL

	return transport.transport.RoundTrip(upstreamRequest)
}

var (
	_ caddy.CleanerUpper        = (*Transport)(nil)
	_ caddy.Provisioner         = (*Transport)(nil)
	_ caddyfile.Unmarshaler     = (*Transport)(nil)
	_ http.RoundTripper         = (*Transport)(nil)
	_ reverseproxy.TLSTransport = (*Transport)(nil)
)

func (transport *Transport) TLSEnabled() bool {
	return true
}

func (transport *Transport) EnableTLS(_ *reverseproxy.TLSConfig) error {
	return nil
}
