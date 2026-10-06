package caddytailscaleservice

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
	"tailscale.com/tailcfg"
)

func init() {
	caddy.RegisterModule(&Transport{})
}

type Transport struct {
	ServiceName string `json:"service_name,omitempty"`
	ServerName  string `json:"server_name,omitempty"`
	Plaintext   bool   `json:"plaintext,omitempty"`

	node             *serviceNode
	serviceNodeName  string
	upstreamNodeName string
	outbound         *outboundServiceNode
	transport        *http.Transport
}

const serviceDialTimeout = 10 * time.Second

var outboundServiceNodes = caddy.NewUsagePool()

type outboundServiceNode struct {
	name   string
	mu     sync.Mutex
	node   *serviceNode
	closed bool
}

func (outbound *outboundServiceNode) Destruct() error {
	outbound.mu.Lock()
	defer outbound.mu.Unlock()

	outbound.closed = true

	return outbound.node.Close()
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
			case "service":
				if !dispenser.AllArgs(&transport.ServiceName) {
					return dispenser.ArgErr()
				}
			case "plaintext":
				if dispenser.NextArg() {
					return dispenser.ArgErr()
				}
				transport.Plaintext = true
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
	if transport.ServiceName == "" {
		serviceNodeName, upstreamNodeName, err := gatewayTailscaleNodeNames()
		if err != nil {
			return err
		}
		if upstreamNodeName == "" {
			return fmt.Errorf("TAILSCALE_UPSTREAM_NODE_NAME is required for hostname transport")
		}

		nodes, err := loadGatewayTailscaleNodes(serviceNodeName, upstreamNodeName)
		if err != nil {
			return err
		}

		transport.node = nodes.upstream
		transport.serviceNodeName = serviceNodeName
		transport.upstreamNodeName = upstreamNodeName
		transport.configureHTTPTransport(transport.node.Dial)
		return nil
	}

	nodeName := os.Getenv("TAILSCALE_SERVICE_NODE_NAME")
	if nodeName == "" {
		return fmt.Errorf("TAILSCALE_SERVICE_NODE_NAME is required")
	}

	outboundName := nodeName + "-outbound"
	node, _, err := outboundServiceNodes.LoadOrNew(outboundName, func() (caddy.Destructor, error) {
		serviceNode, createError := createServiceNode(outboundName)
		if createError != nil {
			return nil, createError
		}

		return &outboundServiceNode{name: outboundName, node: serviceNode}, nil
	})
	if err != nil {
		return err
	}

	transport.outbound = node.(*outboundServiceNode)
	transport.configureHTTPTransport(transport.dialService)

	return nil
}

func (transport *Transport) configureHTTPTransport(dial func(context.Context, string, string) (net.Conn, error)) {
	transport.transport = &http.Transport{
		DialContext:       dial,
		ForceAttemptHTTP2: true,
	}
	if transport.Plaintext {
		return
	}

	transport.transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: transport.ServerName,
	}
	transport.transport.TLSHandshakeTimeout = serviceDialTimeout
}

func (transport *Transport) dialService(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	outbound := transport.outbound
	outbound.mu.Lock()
	node := outbound.node
	outbound.mu.Unlock()

	connection, err := transport.dialServiceFromNode(ctx, node, network, address)
	if err == nil || ctx.Err() != nil {
		return connection, err
	}

	retryNode, retryErr := outbound.reconnect(node, err)
	if retryErr != nil {
		return nil, retryErr
	}

	node = retryNode

	return transport.dialServiceFromNode(ctx, node, network, address)
}

func (outbound *outboundServiceNode) reconnect(failedNode *serviceNode, dialErr error) (*serviceNode, error) {
	outbound.mu.Lock()
	defer outbound.mu.Unlock()

	if outbound.closed {
		return nil, dialErr
	}

	if outbound.node != failedNode {
		return outbound.node, nil
	}

	var networkError net.Error
	if !errors.Is(dialErr, context.DeadlineExceeded) && !(errors.As(dialErr, &networkError) && networkError.Timeout()) {
		return nil, dialErr
	}

	newNode, err := createServiceNode(outbound.name)
	if err != nil {
		return nil, errors.Join(dialErr, err)
	}

	if err := failedNode.Close(); err != nil {
		return nil, errors.Join(dialErr, err)
	}

	outbound.node = newNode
	caddy.Log().Warn("reconnect Tailscale upstream node", zap.String("node", outbound.name))

	return newNode, nil
}

func (transport *Transport) dialServiceFromNode(
	ctx context.Context,
	node *serviceNode,
	network string,
	address string,
) (net.Conn, error) {
	dialContext, cancel := context.WithTimeout(ctx, serviceDialTimeout)
	defer cancel()

	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("parse upstream address: %w", err)
	}

	if _, err := node.Up(dialContext); err != nil {
		return nil, fmt.Errorf("connect Tailscale upstream node: %w", err)
	}

	client, err := node.LocalClient()
	if err != nil {
		return nil, fmt.Errorf("create Tailscale local client: %w", err)
	}

	services, err := client.GetServices(dialContext)
	if err != nil {
		return nil, fmt.Errorf("look up Tailscale services: %w", err)
	}

	service, available := services[tailcfg.ServiceName(transport.ServiceName)]
	if !available {
		return nil, fmt.Errorf("Tailscale upstream service %s is unavailable", transport.ServiceName)
	}
	for _, serviceAddress := range service.Addrs {
		if serviceAddress.Is4() {
			return node.Dial(dialContext, network, net.JoinHostPort(serviceAddress.String(), port))
		}
	}

	return nil, fmt.Errorf("Tailscale upstream service %s has no IPv4 address", transport.ServiceName)
}

func (transport *Transport) Cleanup() error {
	if transport.transport != nil {
		transport.transport.CloseIdleConnections()
	}
	if transport.serviceNodeName != "" {
		return releaseGatewayTailscaleNodes(transport.serviceNodeName, transport.upstreamNodeName)
	}

	if transport.outbound == nil {
		return nil
	}

	_, err := outboundServiceNodes.Delete(transport.outbound.name)

	return err
}

func (transport *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	upstreamRequest := request.Clone(request.Context())
	upstreamURL := *request.URL
	upstreamURL.Scheme = "https"
	if transport.Plaintext {
		upstreamURL.Scheme = "http"
	}
	upstreamRequest.URL = &upstreamURL

	response, err := transport.transport.RoundTrip(upstreamRequest)
	if err != nil {
		caddy.Log().Error("Tailscale upstream request failed", zap.String("upstream", upstreamURL.Host))
	}

	return response, err
}

var (
	_ caddy.CleanerUpper        = (*Transport)(nil)
	_ caddy.Provisioner         = (*Transport)(nil)
	_ caddyfile.Unmarshaler     = (*Transport)(nil)
	_ http.RoundTripper         = (*Transport)(nil)
	_ reverseproxy.TLSTransport = (*Transport)(nil)
)

func (transport *Transport) TLSEnabled() bool {
	return !transport.Plaintext
}

func (transport *Transport) EnableTLS(_ *reverseproxy.TLSConfig) error {
	return nil
}
