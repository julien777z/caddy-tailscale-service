package caddytailscaleservice

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/tailcfg"
	"tailscale.com/tailcfg/peercap"
)

const (
	userLoginHeader       = "Tailscale-User-Login"
	userNameHeader        = "Tailscale-User-Name"
	userProfilePicHeader  = "Tailscale-User-Profile-Pic"
	appCapabilitiesHeader = "Tailscale-App-Capabilities"
)

func init() {
	caddy.RegisterModule(&IdentityHandler{})
	httpcaddyfile.RegisterHandlerDirective("tailscale_identity", parseIdentityHandler)
	httpcaddyfile.RegisterHandlerDirective("tailscale_access", parseAccessHandler)
	httpcaddyfile.RegisterDirectiveOrder("tailscale_access", httpcaddyfile.Before, "handle")
}

// IdentityHandler authenticates the calling user or a tagged identity-forwarding proxy.
type IdentityHandler struct {
	Capability       peercap.Cap `json:"capability,omitempty"`
	RequireUser      bool        `json:"require_user,omitempty"`
	TrustedProxyTags []string    `json:"trusted_proxy_tags,omitempty"`
	StripHeaders     []string    `json:"strip_headers,omitempty"`

	node             *serviceNode
	serviceNodeName  string
	upstreamNodeName string
	serviceName      tailcfg.ServiceName
}

func (IdentityHandler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.tailscale_identity",
		New: func() caddy.Module { return new(IdentityHandler) },
	}
}

func (handler *IdentityHandler) Provision(ctx caddy.Context) error {
	if handler.Capability != "" && len(handler.TrustedProxyTags) != 0 {
		return fmt.Errorf("Tailscale capabilities apply to direct identities, not forwarded identities")
	}

	serviceNodeName, upstreamNodeName, err := gatewayTailscaleNodeNames()
	if err != nil {
		return err
	}

	serviceName := os.Getenv("TAILSCALE_SERVICE_NAME")
	if serviceName == "" {
		return fmt.Errorf("TAILSCALE_SERVICE_NAME is required")
	}

	nodes, err := loadReadyGatewayTailscaleNodes(ctx, serviceNodeName, upstreamNodeName)
	if err != nil {
		return err
	}

	handler.StripHeaders = append(handler.StripHeaders, strings.Fields(os.Getenv("TAILSCALE_IDENTITY_STRIP_HEADERS"))...)

	handler.node = nodes.service
	handler.serviceNodeName = serviceNodeName
	handler.upstreamNodeName = upstreamNodeName
	handler.serviceName = tailcfg.ServiceName(serviceName)

	return nil
}

func (handler *IdentityHandler) Cleanup() error {
	if handler.node == nil {
		return nil
	}

	return releaseGatewayTailscaleNodes(handler.serviceNodeName, handler.upstreamNodeName)
}

func (handler *IdentityHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request, next caddyhttp.Handler) error {
	client, err := handler.node.LocalClient()
	if err != nil {
		return caddyhttp.Error(http.StatusInternalServerError, err)
	}

	return handler.serveIdentity(writer, request, next, client.WhoIsForService)
}

func (handler *IdentityHandler) serveIdentity(
	writer http.ResponseWriter,
	request *http.Request,
	next caddyhttp.Handler,
	whoIs func(context.Context, string, tailcfg.ServiceName) (*apitype.WhoIsResponse, error),
) error {
	for _, header := range handler.StripHeaders {
		request.Header.Del(header)
	}

	forwarded := len(handler.TrustedProxyTags) != 0
	if !forwarded {
		request.Header.Del(userLoginHeader)
		request.Header.Del(userNameHeader)
		request.Header.Del(userProfilePicHeader)
		request.Header.Del(appCapabilitiesHeader)
	}

	identity, err := whoIs(request.Context(), request.RemoteAddr, handler.serviceName)
	if err != nil {
		return caddyhttp.Error(http.StatusForbidden, fmt.Errorf("Tailscale service identity lookup failed"))
	}

	if forwarded {
		if identity.Node == nil || !slices.ContainsFunc(handler.TrustedProxyTags, func(tag string) bool {
			return slices.Contains(identity.Node.Tags, tag)
		}) {
			return caddyhttp.Error(http.StatusForbidden, fmt.Errorf("request did not come from a trusted identity proxy"))
		}

		if request.Header.Get(userLoginHeader) == "" {
			return caddyhttp.Error(http.StatusForbidden, fmt.Errorf("request has no Tailscale user identity"))
		}

		return next.ServeHTTP(writer, request)
	}

	if handler.Capability != "" && !identity.CapMap.HasCapability(handler.Capability) {
		return caddyhttp.Error(http.StatusForbidden, fmt.Errorf("Tailscale policy does not grant access"))
	}

	if handler.RequireUser && (identity.UserProfile == nil || identity.UserProfile.LoginName == "") {
		return caddyhttp.Error(http.StatusForbidden, fmt.Errorf("request has no Tailscale user identity"))
	}

	capabilities := "{}"
	if identity.CapMap != nil {
		encoded, _ := json.Marshal(identity.CapMap)
		capabilities = string(encoded)
	}

	request.Header.Set(appCapabilitiesHeader, capabilities)

	if identity.UserProfile != nil {
		if identity.UserProfile.LoginName != "" {
			request.Header.Set(userLoginHeader, encodeIdentityHeader(identity.UserProfile.LoginName))
			request.Header.Set(userNameHeader, encodeIdentityHeader(identity.UserProfile.DisplayName))
		}

		if identity.UserProfile.ProfilePicURL != "" {
			request.Header.Set(userProfilePicHeader, encodeIdentityHeader(identity.UserProfile.ProfilePicURL))
		}
	}

	return next.ServeHTTP(writer, request)
}

func encodeIdentityHeader(value string) string {
	for _, character := range value {
		if character > 127 {
			return mime.QEncoding.Encode("utf-8", value)
		}
	}

	return value
}

func parseIdentityHandler(helper httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	handler := &IdentityHandler{RequireUser: true}

	return handler, handler.UnmarshalCaddyfile(helper.Dispenser)
}

func parseAccessHandler(helper httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	handler := new(IdentityHandler)

	return handler, handler.UnmarshalCaddyfile(helper.Dispenser)
}

func (handler *IdentityHandler) UnmarshalCaddyfile(dispenser *caddyfile.Dispenser) error {
	dispenser.Next()
	if dispenser.Val() == "tailscale_access" {
		if !dispenser.NextArg() {
			return dispenser.ArgErr()
		}

		handler.Capability = peercap.Cap(dispenser.Val())
		if dispenser.NextArg() {
			if dispenser.Val() != "user" {
				return dispenser.ArgErr()
			}

			handler.RequireUser = true
		}
	}

	if dispenser.NextArg() {
		return dispenser.ArgErr()
	}

	for dispenser.NextBlock(0) {
		switch dispenser.Val() {
		case "trusted_proxy_tags":
			handler.TrustedProxyTags = dispenser.RemainingArgs()
			if len(handler.TrustedProxyTags) == 0 {
				return dispenser.ArgErr()
			}
		case "strip_headers":
			handler.StripHeaders = dispenser.RemainingArgs()
			if len(handler.StripHeaders) == 0 {
				return dispenser.ArgErr()
			}
		default:
			return dispenser.Errf("unrecognized Tailscale identity option %q", dispenser.Val())
		}
	}

	return nil
}

var (
	_ caddy.Provisioner           = (*IdentityHandler)(nil)
	_ caddy.CleanerUpper          = (*IdentityHandler)(nil)
	_ caddyhttp.MiddlewareHandler = (*IdentityHandler)(nil)
	_ caddyfile.Unmarshaler       = (*IdentityHandler)(nil)
)
