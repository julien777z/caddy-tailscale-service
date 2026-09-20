# Caddy Tailscale Service

A Caddy network listener and reverse-proxy transport backed by `tsnet`. The listener publishes a
Caddy server as a [Tailscale Service](https://tailscale.com/kb/1552/tailscale-services), while the
transport reaches HTTPS upstreams over the tailnet.

## Build

```sh
xcaddy build \
  --with github.com/julien777z/caddy-tailscale-service@latest
```

## Configuration

The module uses `TS_CLIENT_SECRET` for Tailscale OAuth authentication and `TS_ADVERTISE_TAGS` for
the tags granted to its ephemeral nodes. Set `TAILSCALE_SERVICE_NAME` and
`TAILSCALE_SERVICE_NODE_NAME` for the listener. Set `TAILSCALE_UPSTREAM_NODE_NAME` when using the
reverse-proxy transport.

```caddyfile
{
	default_bind tailscale-service/example-proxy
}
```

```caddyfile
reverse_proxy service-hostname:443 {
	transport tailscale_service {
		tls_server_name service.example.invalid
	}
}
```
Caddy listener for Tailscale Services
