FROM caddy:2.11.4-builder AS builder

WORKDIR /src
COPY . ./module

RUN xcaddy build v2.11.4 \
    --with github.com/julien777z/caddy-tailscale-service=./module

FROM caddy:2.11.4

COPY --from=builder /src/caddy /usr/bin/caddy
