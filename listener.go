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
const servicePublicationInterval = time.Second

func init() {
	caddy.RegisterNetwork("tailscale-service", getServiceListener)
	caddy.RegisterModule(&Readiness{})
	httpcaddyfile.RegisterHandlerDirective("tailscale_readiness", parseReadinessCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("tailscale_readiness", httpcaddyfile.Before, "respond")
}

func getServiceListener(
	ctx context.Context,
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

	nodes, err := loadReadyGatewayTailscaleNodes(ctx, serviceNodeName, upstreamNodeName)
	if err != nil {
		return nil, err
	}
	serviceNode := nodes.service
	startupContext := nodes.startupContext
	if nodes.active.Load() {
		var cancel context.CancelFunc
		startupContext, cancel = context.WithTimeout(ctx, servicePublicationTimeout)
		defer cancel()
	}

	listenerKey := fmt.Sprintf("%s:%s:%d:%d", serviceNodeName, serviceName, effectivePort, proxyVersion)
	listener, _, err := serviceListeners.LoadOrNew(listenerKey, func() (caddy.Destructor, error) {
		serviceMode := tsnet.ServiceModeTCP{
			Port:                 uint16(effectivePort),
			PROXYProtocolVersion: proxyVersion,
		}
		serviceListener, listenError := listenBeforeDeadline(startupContext, func() (net.Listener, error) {
			return serviceNode.ListenService(serviceName, serviceMode)
		})
		if listenError != nil {
			return nil, listenError
		}

		return &sharedServiceListener{Listener: serviceListener, key: listenerKey}, nil
	})
	if err != nil {
		cleanupErr := releaseGatewayTailscaleNodes(serviceNodeName, upstreamNodeName)

		return nil, errors.Join(err, cleanupErr)
	}

	if servicesFile := os.Getenv("TAILSCALE_SERVICES_FILE"); servicesFile != "" {
		if err := serviceNode.publishServiceAddresses(startupContext, servicesFile, serviceName); err != nil {
			_, listenerErr := serviceListeners.Delete(listenerKey)
			return nil, errors.Join(err, listenerErr, releaseGatewayTailscaleNodes(serviceNodeName, upstreamNodeName))
		}
	}

	nodes.active.Store(true)

	return &serviceListener{
		sharedServiceListener: listener.(*sharedServiceListener),
		serviceNodeName:       serviceNodeName,
		upstreamNodeName:      upstreamNodeName,
		closedCh:              make(chan struct{}),
	}, nil
}

type gatewayTailscaleNodes struct {
	service        *serviceNode
	startupCancel  context.CancelFunc
	startupContext context.Context
	active         atomic.Bool
	serviceNode    string
	upstream       *serviceNode
	upstreamNode   string
}

func loadReadyGatewayTailscaleNodes(
	ctx context.Context,
	serviceNodeName string,
	upstreamNodeName string,
) (*gatewayTailscaleNodes, error) {
	pairKey := gatewayNodePairKey(serviceNodeName, upstreamNodeName)
	nodePair, _, err := gatewayNodePairs.LoadOrNew(pairKey, func() (caddy.Destructor, error) {
		startupContext, cancel := context.WithTimeout(ctx, servicePublicationTimeout)
		nodes, loadErr := createReadyGatewayTailscaleNodes(
			startupContext,
			serviceNodeName,
			upstreamNodeName,
		)
		if loadErr != nil {
			cancel()

			return nil, loadErr
		}

		nodes.startupCancel = cancel
		nodes.startupContext = startupContext
		nodes.serviceNode = serviceNodeName
		nodes.upstreamNode = upstreamNodeName

		return nodes, nil
	})
	if err != nil {
		return nil, err
	}

	return nodePair.(*gatewayTailscaleNodes), nil
}

func createReadyGatewayTailscaleNodes(
	ctx context.Context,
	serviceNodeName string,
	upstreamNodeName string,
) (*gatewayTailscaleNodes, error) {

	type nodeReadinessResult struct {
		name string
		node *serviceNode
		err  error
	}

	count := 1
	if upstreamNodeName != "" {
		count++
	}

	results := make(chan nodeReadinessResult, count)
	loadNode := func(name string, nodes *caddy.UsagePool) {
		node, err := loadReadyTailscaleNodeBeforeDeadline(ctx, nodes, name)
		results <- nodeReadinessResult{name: name, node: node, err: err}
	}

	go loadNode(serviceNodeName, serviceNodes)
	if upstreamNodeName != "" {
		go loadNode(upstreamNodeName, upstreamNodes)
	}

	var service *serviceNode
	var upstream *serviceNode
	var readinessErr error
	for range count {
		result := <-results
		if result.err != nil {
			readinessErr = errors.Join(readinessErr, result.err)

			continue
		}

		if result.name == serviceNodeName {
			service = result.node

			continue
		}

		upstream = result.node
	}

	if readinessErr != nil {
		var cleanupErr error
		if service != nil {
			_, cleanupErr = serviceNodes.Delete(serviceNodeName)
		}
		if upstream != nil {
			_, upstreamCleanupErr := upstreamNodes.Delete(upstreamNodeName)
			cleanupErr = errors.Join(cleanupErr, upstreamCleanupErr)
		}

		return nil, errors.Join(readinessErr, cleanupErr)
	}

	return &gatewayTailscaleNodes{service: service, upstream: upstream}, nil
}

func gatewayNodePairKey(serviceNodeName string, upstreamNodeName string) string {
	return serviceNodeName + "\x00" + upstreamNodeName
}

func (nodes *gatewayTailscaleNodes) Destruct() error {
	nodes.startupCancel()

	return releaseTailscaleNodes(nodes.serviceNode, nodes.upstreamNode)
}

func loadReadyTailscaleNodeBeforeDeadline(
	ctx context.Context,
	nodes *caddy.UsagePool,
	nodeName string,
) (*serviceNode, error) {
	node, _, err := nodes.LoadOrNew(nodeName, func() (caddy.Destructor, error) {
		return createServiceNode(nodeName)
	})
	if err != nil {
		return nil, err
	}

	serviceNode := node.(*serviceNode)
	if err := waitForTailscaleNode(ctx, nodeName, serviceNode); err != nil {
		_, cleanupErr := nodes.Delete(nodeName)

		return nil, errors.Join(err, cleanupErr)
	}

	return serviceNode, nil
}

var waitForTailscaleNode = waitForTailscaleNodeRunning

func waitForTailscaleNodeRunning(
	ctx context.Context,
	nodeName string,
	node *serviceNode,
) error {
	status, err := node.Up(ctx)
	if err != nil {
		failureKind := "unavailable"
		if errors.Is(err, context.DeadlineExceeded) {
			failureKind = "timeout"
		}
		failure := err.Error()
		if secret := os.Getenv("TS_OAUTH_SECRET"); secret != "" {
			secret, _, _ = strings.Cut(secret, "?")
			failure = strings.ReplaceAll(failure, secret, "[redacted]")
		}

		caddy.Log().Error(
			"Tailscale node readiness failed",
			zap.String("node", nodeName),
			zap.String("failure_kind", failureKind),
			zap.String("cause", failure),
		)

		return fmt.Errorf("Tailscale node %s did not reach Running", nodeName)
	}

	caddy.Log().Info(
		"Tailscale node ready",
		zap.String("node", nodeName),
		zap.Int("address_count", len(status.TailscaleIPs)),
	)

	return nil
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

func (readiness *Readiness) Provision(ctx caddy.Context) error {
	serviceNodeName, upstreamNodeName, err := gatewayTailscaleNodeNames()
	if err != nil {
		return err
	}

	if _, err := loadReadyGatewayTailscaleNodes(ctx, serviceNodeName, upstreamNodeName); err != nil {
		return err
	}

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

func (Readiness) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
	next caddyhttp.Handler,
) error {
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
	ctx context.Context,
	path string,
	serviceName string,
) error {
	node.publisherMu.Lock()
	defer node.publisherMu.Unlock()

	if node.publisherCancel != nil {
		return nil
	}

	client, err := node.LocalClient()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(servicePublicationInterval)
	defer ticker.Stop()

	for {
		published, err := writeServiceAddresses(ctx, client, path, serviceName)
		if err != nil {
			return err
		}
		if published {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	publisherContext, cancel := context.WithCancel(context.Background())
	node.publisherCancel = cancel
	go func() {
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

	return nil
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
