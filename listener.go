package caddytailscaleservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
	"tailscale.com/client/local"
	_ "tailscale.com/feature/oauthkey"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

var serviceNodes = caddy.NewUsagePool()
var serviceListeners = caddy.NewUsagePool()
var upstreamNodes = caddy.NewUsagePool()
var gatewayNodePairs = caddy.NewUsagePool()

const servicePublicationTimeout = time.Minute

var servicePublicationInterval = time.Second

func init() {
	caddy.RegisterNetwork("tailscale-service", getServiceListener)
	caddy.RegisterModule(&Readiness{})
	httpcaddyfile.RegisterHandlerDirective("tailscale_readiness", parseReadinessCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("tailscale_readiness", httpcaddyfile.Before, "respond")
}

func getServiceListener(
	_ context.Context,
	_ string,
	_ string,
	portRange string,
	portOffset uint,
	_ net.ListenConfig,
) (any, error) {
	port, err := strconv.ParseUint(portRange, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("parse Tailscale service port: %w", err)
	}

	effectivePort := port + uint64(portOffset)
	if effectivePort == 0 || effectivePort > 65535 {
		return nil, fmt.Errorf("Tailscale service port is out of range")
	}

	proxyVersion, err := serviceProxyProtocol(uint16(effectivePort))
	if err != nil {
		return nil, err
	}

	serviceName := os.Getenv("TAILSCALE_SERVICE_NAME")
	serviceNodeName, upstreamNodeName, err := gatewayTailscaleNodeNames()
	if err != nil {
		return nil, err
	}

	if serviceName == "" {
		return nil, fmt.Errorf("TAILSCALE_SERVICE_NAME is required")
	}

	nodes, err := loadGatewayTailscaleNodes(serviceNodeName, upstreamNodeName)
	if err != nil {
		return nil, err
	}

	listenerKey := fmt.Sprintf("%s:%s:%d:%d", serviceNodeName, serviceName, effectivePort, proxyVersion)
	listener, _, err := serviceListeners.LoadOrNew(listenerKey, func() (caddy.Destructor, error) {
		serviceMode := tsnet.ServiceModeTCP{
			Port:                 uint16(effectivePort),
			PROXYProtocolVersion: proxyVersion,
		}
		servicesPath := os.Getenv("TAILSCALE_SERVICES_FILE")
		serviceListener := newRetryingServiceListener(
			nodes.service,
			serviceName,
			serviceMode,
			func() {
				if servicesPath != "" {
					nodes.service.publishServiceAddresses(servicesPath, serviceName)
				}
			},
		)

		return &sharedServiceListener{Listener: serviceListener, key: listenerKey}, nil
	})
	if err != nil {
		cleanupErr := releaseGatewayTailscaleNodes(serviceNodeName, upstreamNodeName)

		return nil, errors.Join(err, cleanupErr)
	}

	return &serviceListener{
		sharedServiceListener: listener.(*sharedServiceListener),
		serviceNodeName:       serviceNodeName,
		upstreamNodeName:      upstreamNodeName,
		closedCh:              make(chan struct{}),
	}, nil
}

type gatewayTailscaleNodes struct {
	service      *serviceNode
	serviceNode  string
	upstream     *serviceNode
	upstreamNode string
}

func loadGatewayTailscaleNodes(
	serviceNodeName string,
	upstreamNodeName string,
) (*gatewayTailscaleNodes, error) {
	pairKey := gatewayNodePairKey(serviceNodeName, upstreamNodeName)
	nodePair, _, err := gatewayNodePairs.LoadOrNew(pairKey, func() (caddy.Destructor, error) {
		service, err := loadTailscaleNode(serviceNodes, serviceNodeName)
		if err != nil {
			return nil, err
		}

		var upstream *serviceNode
		if upstreamNodeName != "" {
			upstream, err = loadTailscaleNode(upstreamNodes, upstreamNodeName)
			if err != nil {
				_, cleanupErr := serviceNodes.Delete(serviceNodeName)

				return nil, errors.Join(err, cleanupErr)
			}
		}

		return &gatewayTailscaleNodes{
			service:      service,
			serviceNode:  serviceNodeName,
			upstream:     upstream,
			upstreamNode: upstreamNodeName,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return nodePair.(*gatewayTailscaleNodes), nil
}

func loadTailscaleNode(nodes *caddy.UsagePool, nodeName string) (*serviceNode, error) {
	node, _, err := nodes.LoadOrNew(nodeName, func() (caddy.Destructor, error) {
		return createServiceNode(nodeName)
	})
	if err != nil {
		return nil, err
	}

	return node.(*serviceNode), nil
}

func gatewayNodePairKey(serviceNodeName string, upstreamNodeName string) string {
	return serviceNodeName + "\x00" + upstreamNodeName
}

func (nodes *gatewayTailscaleNodes) Destruct() error {
	return releaseTailscaleNodes(nodes.serviceNode, nodes.upstreamNode)
}

func gatewayTailscaleNodeNames() (string, string, error) {
	serviceNodeName := os.Getenv("TAILSCALE_SERVICE_NODE_NAME")
	upstreamNodeName := os.Getenv("TAILSCALE_UPSTREAM_NODE_NAME")
	if serviceNodeName == "" {
		return "", "", fmt.Errorf("Tailscale node configuration is required")
	}
	if serviceNodeName == upstreamNodeName {
		return "", "", fmt.Errorf("Tailscale service and upstream nodes must differ")
	}

	return serviceNodeName, upstreamNodeName, nil
}

type Readiness struct {
	client           *local.Client
	node             *serviceNode
	serviceName      string
	serviceNodeName  string
	upstreamNodeName string
}

func (Readiness) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.tailscale_readiness",
		New: func() caddy.Module { return new(Readiness) },
	}
}

func parseReadinessCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	readiness := new(Readiness)
	if err := readiness.UnmarshalCaddyfile(h.Dispenser); err != nil {
		return nil, err
	}

	return readiness, nil
}

func (readiness *Readiness) UnmarshalCaddyfile(dispenser *caddyfile.Dispenser) error {
	dispenser.Next()
	if dispenser.NextArg() {
		return dispenser.ArgErr()
	}

	if dispenser.NextBlock(0) {
		return dispenser.ArgErr()
	}

	return nil
}

func (readiness *Readiness) Provision(_ caddy.Context) error {
	serviceNodeName, upstreamNodeName, err := gatewayTailscaleNodeNames()
	if err != nil {
		return err
	}

	nodes, err := loadGatewayTailscaleNodes(serviceNodeName, upstreamNodeName)
	if err != nil {
		return err
	}

	readiness.node = nodes.service
	readiness.serviceName = os.Getenv("TAILSCALE_SERVICE_NAME")

	readiness.serviceNodeName = serviceNodeName
	readiness.upstreamNodeName = upstreamNodeName

	return nil
}

func (readiness *Readiness) Cleanup() error {
	if readiness.serviceNodeName == "" {
		return nil
	}

	return releaseGatewayTailscaleNodes(readiness.serviceNodeName, readiness.upstreamNodeName)
}

func (readiness Readiness) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
	next caddyhttp.Handler,
) error {
	client := readiness.client
	if client == nil {
		if readiness.node == nil {
			return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("Tailscale service node is unavailable"))
		}

		var err error
		client, err = readiness.node.LocalClient()
		if err != nil {
			return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("Tailscale service node is unavailable"))
		}
	}

	ready, err := writeServiceAddresses(r.Context(), client, "", readiness.serviceName)
	if err != nil {
		return caddyhttp.Error(http.StatusServiceUnavailable, err)
	}

	if !ready {
		return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("Tailscale service route is not active"))
	}

	return next.ServeHTTP(w, r)
}

func releaseGatewayTailscaleNodes(serviceNodeName string, upstreamNodeName string) error {
	_, err := gatewayNodePairs.Delete(gatewayNodePairKey(serviceNodeName, upstreamNodeName))

	return err
}

func releaseTailscaleNodes(serviceNodeName string, upstreamNodeName string) error {
	_, serviceNodeErr := serviceNodes.Delete(serviceNodeName)
	var upstreamNodeErr error
	if upstreamNodeName != "" {
		_, upstreamNodeErr = upstreamNodes.Delete(upstreamNodeName)
	}

	return errors.Join(serviceNodeErr, upstreamNodeErr)
}

type serviceNode struct {
	*tsnet.Server
	publisherMu     sync.Mutex
	publisherCancel context.CancelFunc
}

func createServiceNode(name string) (*serviceNode, error) {
	clientSecret := os.Getenv("TS_OAUTH_SECRET")
	if clientSecret == "" {
		return nil, fmt.Errorf("Tailscale OAuth client secret is required")
	}
	clientSecret, err := preauthorizedClientSecret(clientSecret)
	if err != nil {
		return nil, err
	}

	return &serviceNode{Server: &tsnet.Server{
		AdvertiseTags: strings.Fields(os.Getenv("TS_ADVERTISE_TAGS")),
		ClientSecret:  clientSecret,
		Dir:           filepath.Join(os.TempDir(), "tsnet-caddy", name),
		Ephemeral:     true,
		Hostname:      name,
	}}, nil
}

func preauthorizedClientSecret(clientSecret string) (string, error) {
	secret, encodedAttributes, _ := strings.Cut(clientSecret, "?")
	attributes, err := url.ParseQuery(encodedAttributes)
	if err != nil {
		return "", fmt.Errorf("parse Tailscale OAuth client secret attributes: %w", err)
	}

	attributes.Set("preauthorized", "true")

	return secret + "?" + attributes.Encode(), nil
}

func (node *serviceNode) Destruct() error {
	node.publisherMu.Lock()
	defer node.publisherMu.Unlock()

	if node.publisherCancel != nil {
		node.publisherCancel()
	}

	return node.Close()
}

type acceptedConnection struct {
	connection net.Conn
	err        error
}

var listenTailscaleService = func(
	node *serviceNode,
	serviceName string,
	serviceMode tsnet.ServiceModeTCP,
) (net.Listener, error) {
	return node.ListenService(serviceName, serviceMode)
}

type retryingServiceListener struct {
	serviceMode tsnet.ServiceModeTCP
	serviceName string
	node        *serviceNode
	publish     func()

	closed    chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	listener  net.Listener
}

func newRetryingServiceListener(
	node *serviceNode,
	serviceName string,
	serviceMode tsnet.ServiceModeTCP,
	publish func(),
) *retryingServiceListener {
	return &retryingServiceListener{
		serviceMode: serviceMode,
		serviceName: serviceName,
		node:        node,
		publish:     publish,
		closed:      make(chan struct{}),
	}
}

func (listener *retryingServiceListener) Accept() (net.Conn, error) {
	for {
		active, err := listener.activeListener()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil, net.ErrClosed
			}

			caddy.Log().Warn(
				"Tailscale service listener unavailable; retrying",
				zap.String("service", listener.serviceName),
				zap.Error(err),
			)
			if !listener.waitForRetry() {
				return nil, net.ErrClosed
			}

			continue
		}

		connection, acceptErr := active.Accept()
		if acceptErr == nil {
			return connection, nil
		}
		if errors.Is(acceptErr, net.ErrClosed) && listener.isClosed() {
			return nil, net.ErrClosed
		}

		listener.clear(active)
		caddy.Log().Warn(
			"Tailscale service listener stopped; retrying",
			zap.String("service", listener.serviceName),
			zap.Error(acceptErr),
		)
	}
}

func (listener *retryingServiceListener) activeListener() (net.Listener, error) {
	listener.mu.Lock()
	if listener.listener != nil {
		active := listener.listener
		listener.mu.Unlock()

		return active, nil
	}
	listener.mu.Unlock()

	startupContext, cancel := context.WithTimeout(context.Background(), servicePublicationTimeout)
	defer cancel()

	created, err := listenBeforeDeadline(startupContext, func() (net.Listener, error) {
		return listenTailscaleService(listener.node, listener.serviceName, listener.serviceMode)
	})
	if err != nil {
		return nil, err
	}

	listener.mu.Lock()
	if listener.isClosed() {
		listener.mu.Unlock()
		_ = created.Close()

		return nil, net.ErrClosed
	}
	if listener.listener != nil {
		active := listener.listener
		listener.mu.Unlock()
		_ = created.Close()

		return active, nil
	}

	listener.listener = created
	publish := listener.publish
	listener.mu.Unlock()

	if publish != nil {
		publish()
	}

	return created, nil
}

func (listener *retryingServiceListener) waitForRetry() bool {
	timer := time.NewTimer(servicePublicationInterval)
	defer timer.Stop()

	select {
	case <-listener.closed:
		return false
	case <-timer.C:
		return true
	}
}

func (listener *retryingServiceListener) clear(active net.Listener) {
	listener.mu.Lock()
	defer listener.mu.Unlock()

	if listener.listener == active {
		listener.listener = nil
	}
}

func (listener *retryingServiceListener) Close() error {
	var closeErr error
	listener.closeOnce.Do(func() {
		close(listener.closed)

		listener.mu.Lock()
		defer listener.mu.Unlock()
		if listener.listener != nil {
			closeErr = listener.listener.Close()
		}
	})

	return closeErr
}

func (listener *retryingServiceListener) Addr() net.Addr {
	listener.mu.Lock()
	defer listener.mu.Unlock()

	if listener.listener != nil {
		return listener.listener.Addr()
	}

	return retryingServiceAddress(listener.serviceName)
}

func (listener *retryingServiceListener) isClosed() bool {
	select {
	case <-listener.closed:
		return true
	default:
		return false
	}
}

type retryingServiceAddress string

func (address retryingServiceAddress) Network() string {
	return "tailscale-service"
}

func (address retryingServiceAddress) String() string {
	return string(address)
}

type sharedServiceListener struct {
	net.Listener
	key       string
	startOnce sync.Once
	accepted  chan acceptedConnection
	done      chan struct{}
}

func (listener *sharedServiceListener) start() {
	listener.startOnce.Do(func() {
		listener.accepted = make(chan acceptedConnection)
		listener.done = make(chan struct{})

		go func() {
			defer close(listener.accepted)

			for {
				connection, err := listener.Listener.Accept()
				result := acceptedConnection{connection: connection, err: err}

				select {
				case listener.accepted <- result:
				case <-listener.done:
					if connection != nil {
						_ = connection.Close()
					}
					return
				}

				if err != nil {
					var networkError net.Error
					if !errors.As(err, &networkError) || !networkError.Temporary() {
						return
					}
				}
			}
		}()
	})
}

func (listener *sharedServiceListener) Destruct() error {
	listener.start()
	close(listener.done)

	return listener.Listener.Close()
}

type serviceListener struct {
	closed   atomic.Bool
	closedCh chan struct{}
	*sharedServiceListener
	serviceNodeName  string
	upstreamNodeName string
}

func (listener *serviceListener) Accept() (net.Conn, error) {
	if listener.closed.Load() {
		return nil, net.ErrClosed
	}

	listener.sharedServiceListener.start()
	select {
	case <-listener.closedCh:
		return nil, net.ErrClosed
	case result, open := <-listener.accepted:
		if !open {
			return nil, net.ErrClosed
		}

		if listener.closed.Load() {
			if result.connection != nil {
				_ = result.connection.Close()
			}

			return nil, net.ErrClosed
		}

		return result.connection, result.err
	}
}

func (listener *serviceListener) Close() error {
	if listener.closed.CompareAndSwap(false, true) {
		close(listener.closedCh)
		_, listenerErr := serviceListeners.Delete(listener.key)
		nodeErr := releaseGatewayTailscaleNodes(listener.serviceNodeName, listener.upstreamNodeName)

		return errors.Join(listenerErr, nodeErr)
	}

	return nil
}

func (listener *serviceListener) Unwrap() net.Listener {
	return listener.Listener
}

func listenBeforeDeadline(ctx context.Context, listen func() (net.Listener, error)) (net.Listener, error) {
	result := make(chan acceptedListener)
	go func() {
		listener, err := listen()
		select {
		case result <- acceptedListener{listener: listener, err: err}:
		case <-ctx.Done():
			if listener != nil {
				_ = listener.Close()
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case accepted := <-result:
		if ctx.Err() != nil {
			if accepted.listener != nil {
				_ = accepted.listener.Close()
			}

			return nil, ctx.Err()
		}

		return accepted.listener, accepted.err
	}
}

type acceptedListener struct {
	listener net.Listener
	err      error
}

func writeServiceAddresses(
	ctx context.Context,
	client *local.Client,
	path string,
	requiredService string,
) (bool, error) {
	services, err := client.GetServices(ctx)
	if err != nil {
		return false, fmt.Errorf("read Tailscale services: %w", err)
	}

	addresses := ipv4ServiceAddresses(services)
	serviceAddress, available := addresses[requiredService]
	if !available {
		return false, nil
	}

	status, err := client.StatusWithoutPeers(ctx)
	if err != nil {
		return false, fmt.Errorf("read Tailscale service routes: %w", err)
	}
	if status.Self == nil || status.Self.PrimaryRoutes == nil {
		return false, nil
	}

	route := netip.PrefixFrom(netip.MustParseAddr(serviceAddress), 32)
	if !status.Self.PrimaryRoutes.ContainsFunc(func(primary netip.Prefix) bool { return primary == route }) {
		return false, nil
	}

	if path == "" {
		return true, nil
	}

	contents, _ := json.Marshal(addresses)

	temporaryPath := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, fmt.Errorf("create Tailscale services directory: %w", err)
	}
	if err := os.WriteFile(temporaryPath, contents, 0o600); err != nil {
		return false, fmt.Errorf("write Tailscale service addresses: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return false, fmt.Errorf("publish Tailscale service addresses: %w", err)
	}

	return true, nil
}

func ipv4ServiceAddresses(
	services map[tailcfg.ServiceName]tailcfg.ServiceDetails,
) map[string]string {
	addresses := make(map[string]string)
	for name, service := range services {
		for _, address := range service.Addrs {
			if address.Is4() {
				addresses[name.String()] = address.String()

				break
			}
		}
	}

	return addresses
}

func (node *serviceNode) publishServiceAddresses(
	path string,
	serviceName string,
) {
	node.publisherMu.Lock()
	defer node.publisherMu.Unlock()

	if node.publisherCancel != nil {
		return
	}

	// Node provisioning has completed Up, which initializes and retains this client.
	client, _ := node.LocalClient()

	publisherContext, cancel := context.WithCancel(context.Background())
	node.publisherCancel = cancel
	go func() {
		startupContext, startupCancel := context.WithTimeout(publisherContext, servicePublicationTimeout)
		defer startupCancel()
		startupTicker := time.NewTicker(servicePublicationInterval)
		defer startupTicker.Stop()

		for {
			published, err := writeServiceAddresses(startupContext, client, path, serviceName)
			if err != nil {
				caddy.Log().Error("publish Tailscale service addresses", zap.Error(err))
				return
			}

			if published {
				break
			}

			select {
			case <-startupContext.Done():
				caddy.Log().Error("publish Tailscale service addresses", zap.Error(startupContext.Err()))
				return
			case <-startupTicker.C:
			}
		}

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-publisherContext.Done():
				return
			case <-ticker.C:
				published, err := writeServiceAddresses(
					publisherContext,
					client,
					path,
					serviceName,
				)
				if err != nil {
					caddy.Log().Error("refresh Tailscale service addresses", zap.Error(err))
				} else if !published {
					caddy.Log().Error(
						"refresh Tailscale service addresses",
						zap.String("service", serviceName),
					)
				}
			}
		}
	}()

}

func serviceProxyProtocol(port uint16) (int, error) {
	version := 0

	for _, configured := range strings.Fields(os.Getenv("TAILSCALE_PROXY_PROTOCOL_PORTS")) {
		configuredPort, err := strconv.ParseUint(configured, 10, 16)
		if err != nil || configuredPort == 0 {
			return 0, fmt.Errorf("invalid Tailscale PROXY protocol port %q", configured)
		}

		if uint16(configuredPort) == port {
			version = 2
		}
	}

	return version, nil
}
