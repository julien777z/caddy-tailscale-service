package caddytailscaleservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

var serviceNodes = caddy.NewUsagePool()
var serviceListeners = caddy.NewUsagePool()

const servicePublicationTimeout = time.Minute
const servicePublicationInterval = time.Second

func init() {
	caddy.RegisterNetwork("tailscale-service", getServiceListener)
}

func getServiceListener(
	ctx context.Context,
	network string,
	_ string,
	portRange string,
	portOffset uint,
	_ net.ListenConfig,
) (any, error) {
	if network != "tailscale-service" {
		return nil, fmt.Errorf("unsupported Tailscale service network: %s", network)
	}

	port, err := strconv.ParseUint(portRange, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("parse Tailscale service port: %w", err)
	}

	serviceName := os.Getenv("TAILSCALE_SERVICE_NAME")
	serviceNodeName := os.Getenv("TAILSCALE_SERVICE_NODE_NAME")

	if serviceName == "" || serviceNodeName == "" {
		return nil, fmt.Errorf("Tailscale service configuration is required")
	}

	node, _, err := serviceNodes.LoadOrNew(serviceNodeName, func() (caddy.Destructor, error) {
		return createServiceNode(serviceNodeName)
	})
	if err != nil {
		return nil, err
	}

	listenerKey := fmt.Sprintf("%s:%d", serviceName, port+uint64(portOffset))
	listener, _, err := serviceListeners.LoadOrNew(listenerKey, func() (caddy.Destructor, error) {
		serviceListener, listenError := node.(*serviceNode).ListenService(
			serviceName,
			tsnet.ServiceModeTCP{Port: uint16(port) + uint16(portOffset)},
		)
		if listenError != nil {
			return nil, listenError
		}

		return &sharedServiceListener{Listener: serviceListener, key: listenerKey}, nil
	})
	if err != nil {
		_, cleanupErr := serviceNodes.Delete(serviceNodeName)

		return nil, errors.Join(err, cleanupErr)
	}
	if servicesFile := os.Getenv("TAILSCALE_SERVICES_FILE"); servicesFile != "" {
		if err := node.(*serviceNode).publishServiceAddresses(ctx, servicesFile, serviceName); err != nil {
			return nil, errors.Join(
				err,
				releaseServiceReferences(listenerKey, serviceNodeName),
			)
		}
	}

	return &serviceListener{
		sharedServiceListener: listener.(*sharedServiceListener),
		serviceNodeName:       serviceNodeName,
	}, nil
}

func releaseServiceReferences(listenerKey string, serviceNodeName string) error {
	_, listenerErr := serviceListeners.Delete(listenerKey)
	_, nodeErr := serviceNodes.Delete(serviceNodeName)

	return errors.Join(listenerErr, nodeErr)
}

func writeServiceAddresses(
	ctx context.Context,
	node *serviceNode,
	path string,
	requiredService string,
) (bool, error) {
	client, err := node.LocalClient()
	if err != nil {
		return false, fmt.Errorf("create Tailscale local client: %w", err)
	}

	services, err := client.GetServices(ctx)
	if err != nil {
		return false, fmt.Errorf("read Tailscale services: %w", err)
	}

	addresses := ipv4ServiceAddresses(services)
	if _, available := addresses[requiredService]; !available {
		return false, nil
	}

	contents, err := json.Marshal(addresses)
	if err != nil {
		return false, fmt.Errorf("serialize Tailscale service addresses: %w", err)
	}

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

type serviceNode struct {
	*tsnet.Server
	publisherCancel  context.CancelFunc
	publisherStarted atomic.Bool
}

func (node *serviceNode) publishServiceAddresses(
	ctx context.Context,
	path string,
	serviceName string,
) error {
	if !node.publisherStarted.CompareAndSwap(false, true) {
		return nil
	}

	timeout := time.NewTimer(servicePublicationTimeout)
	ticker := time.NewTicker(servicePublicationInterval)
	defer timeout.Stop()
	defer ticker.Stop()

	for {
		published, err := writeServiceAddresses(ctx, node, path, serviceName)
		if err != nil {
			node.publisherStarted.Store(false)

			return err
		}
		if published {
			break
		}

		select {
		case <-ctx.Done():
			node.publisherStarted.Store(false)

			return ctx.Err()
		case <-timeout.C:
			node.publisherStarted.Store(false)

			return fmt.Errorf("Tailscale service %s is unavailable", serviceName)
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
					node,
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

func createServiceNode(name string) (*serviceNode, error) {
	clientSecret := os.Getenv("TS_CLIENT_SECRET")
	if clientSecret == "" {
		return nil, fmt.Errorf("Tailscale OAuth client secret is required")
	}

	return &serviceNode{Server: &tsnet.Server{
		AdvertiseTags: strings.Fields(os.Getenv("TS_ADVERTISE_TAGS")),
		ClientSecret:  clientSecret,
		Dir:           filepath.Join(os.TempDir(), "tsnet-caddy", name),
		Ephemeral:     true,
		Hostname:      name,
	}}, nil
}

func (node *serviceNode) Destruct() error {
	if node.publisherCancel != nil {
		node.publisherCancel()
	}

	return node.Close()
}

type sharedServiceListener struct {
	net.Listener
	key string
}

func (listener *sharedServiceListener) Destruct() error {
	return listener.Close()
}

type serviceListener struct {
	closed atomic.Bool
	*sharedServiceListener
	serviceNodeName string
}

func (listener *serviceListener) Accept() (net.Conn, error) {
	if listener.closed.Load() {
		return nil, net.ErrClosed
	}

	return listener.sharedServiceListener.Accept()
}

func (listener *serviceListener) Close() error {
	if listener.closed.CompareAndSwap(false, true) {
		_, _ = serviceListeners.Delete(listener.key)
		_, _ = serviceNodes.Delete(listener.serviceNodeName)
	}

	return nil
}

func (listener *serviceListener) Unwrap() net.Listener {
	return listener.Listener
}
