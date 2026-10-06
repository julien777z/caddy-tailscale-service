# Caddy Tailscale Service

Expose Caddy through Tailscale Services and reach upstreams over the tailnet.

## Features

- TCP service listeners with optional PROXY protocol v2.
- TLS and plaintext reverse-proxy transports, with service discovery and timeout recovery.
- Single-node or paired service/upstream startup under one readiness deadline.
- Direct user identity, capability checks, and trusted proxy identity forwarding.
- Atomic IPv4 service-address publication after primary-route activation.

## Examples

Build Caddy with a released module version:

```sh
xcaddy build v2.11.4 \
  --with github.com/julien777z/caddy-tailscale-service@v0.1.0
```

Publish a listener using the configured service name and node:

```caddyfile
{
    default_bind tailscale-service/example
}
```

Reach an upstream service over the tailnet. Omit `service` to dial the configured upstream node's
hostnames directly; add `plaintext` for HTTP.

```caddyfile
reverse_proxy application:443 {
    transport tailscale_service {
        service svc:example
        tls_server_name application.example.invalid
    }
}
```

Authenticate a user and replace incoming identity headers with verified Tailscale identity:

```caddyfile
route {
    tailscale_identity
    reverse_proxy 127.0.0.1:8080
}
```

Require a capability, optionally also requiring a user. Header names listed in `strip_headers`
are removed before forwarding.

```caddyfile
tailscale_access example.invalid/cap/read user {
    strip_headers X-Private-Token
}
```

Accept forwarded user identity only from a proxy bearing an allowed Tailscale tag:

```caddyfile
tailscale_identity {
    trusted_proxy_tags tag:example-proxy
}
```

Use `tailscale_readiness` on a health endpoint to start the configured nodes when requested
and return HTTP 503 until the service has an active primary route.

## Configuration

| Environment variable | Purpose |
| --- | --- |
| `TS_OAUTH_SECRET` | OAuth client secret; authentication requests preauthorized ephemeral nodes. |
| `TS_ADVERTISE_TAGS` | Space-separated node tags. |
| `TAILSCALE_SERVICE_NAME` | Service name, such as `svc:example`. |
| `TAILSCALE_SERVICE_NODE_NAME` | Service node hostname. |
| `TAILSCALE_UPSTREAM_NODE_NAME` | Optional distinct upstream hostname. Required for hostname transport. |
| `TAILSCALE_PROXY_PROTOCOL_PORTS` | Space-separated listener ports emitting PROXY protocol v2; empty disables it. Configure matching Caddy `proxy_protocol` wrappers. |
| `TAILSCALE_SERVICES_FILE` | Optional path for published IPv4 service addresses, refreshed every minute. |
| `TAILSCALE_IDENTITY_STRIP_HEADERS` | Additional space-separated headers stripped by identity handlers. |

Provisioning allocates nodes without waiting for Tailscale. Service listeners, outbound requests,
and readiness requests start the nodes when used. Service listeners retry failed registration in
the running Caddy process without waiting for route activation, allowing every configured port
to be advertised. The health endpoint
requires an active primary route independently of optional file publication. Optional publication
waits up to 60 seconds for that route and refreshes the file every minute. Explicit-service transports use a dedicated
outbound node named after the
service node with `-outbound` appended and retry once after a dial timeout, unless the request was
cancelled. Direct identity handling clears supplied Tailscale identity and capability headers;
trusted-proxy handling preserves identity only after verifying the caller's tag and a user login.

## Local Development

Use the Go version declared in `go.mod`:

```sh
go test -race ./...
go vet ./...
docker build .
```
