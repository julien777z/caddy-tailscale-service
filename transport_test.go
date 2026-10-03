package caddytailscaleservice

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestRoundTripSuppliesHTTPScheme(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	transport := &Transport{
		transport: server.Client().Transport.(*http.Transport),
	}
	request := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host: serverURL.Host,
			Path: "/",
		},
	}

	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(body) != "ok" {
		t.Fatalf("response body = %q", body)
	}
	if request.URL.Scheme != "" {
		t.Fatalf("request URL scheme = %q", request.URL.Scheme)
	}
}

func TestRoundTripSuppliesHTTPForPlaintextTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	transport := &Transport{
		Plaintext: true,
		transport: server.Client().Transport.(*http.Transport),
	}
	request := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host: serverURL.Host,
			Path: "/",
		},
	}

	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(body) != "ok" {
		t.Fatalf("response body = %q", body)
	}
	if request.URL.Scheme != "" {
		t.Fatalf("request URL scheme = %q", request.URL.Scheme)
	}
}

func TestOutboundReconnect(t *testing.T) {
	t.Setenv("TS_OAUTH_SECRET", "tskey-client-example")
	for _, test := range []struct {
		name        string
		failure     error
		closed      bool
		replacement bool
	}{
		{name: "timeout replaces node", failure: context.DeadlineExceeded, replacement: true},
		{name: "ordinary failure retains node", failure: errors.New("refused")},
		{name: "closed transport cannot reconnect", failure: context.DeadlineExceeded, closed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			node, err := createServiceNode("outbound-example")
			if err != nil {
				t.Fatal(err)
			}
			outbound := &outboundServiceNode{name: node.Hostname, node: node, closed: test.closed}
			t.Cleanup(func() { _ = outbound.node.Close() })

			replacement, err := outbound.reconnect(node, test.failure)
			if test.replacement {
				if err != nil || replacement == node || outbound.node != replacement {
					t.Fatalf("node was not replaced: node=%p replacement=%p error=%v", node, replacement, err)
				}
			} else if err == nil || outbound.node != node {
				t.Fatalf("unexpected reconnection: error=%v", err)
			}
		})
	}
}

func TestTransportTLSHandshakeFailureClosesConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	closed := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			closed <- err
			return
		}
		defer connection.Close()
		_, _ = connection.Write([]byte("not a TLS record"))
		_, err = io.Copy(io.Discard, connection)
		closed <- err
	}()

	transport := &Transport{ServerName: "example.invalid"}
	transport.configureHTTPTransport((&net.Dialer{}).DialContext)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+listener.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = transport.RoundTrip(request)
	if err == nil {
		t.Fatal("TLS handshake accepted plaintext")
	}

	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("failed TLS handshake left the connection open")
	}
}

func TestTLSInfersUpstreamHostname(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("ok"))
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())

	for _, test := range []struct{ name, host, serverName string }{
		{name: "inferred", host: "127.0.0.1:443"},
		{name: "explicit", host: "upstream.example.invalid:443", serverName: "127.0.0.1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &Transport{ServerName: test.serverName}
			transport.configureHTTPTransport(func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			})
			transport.transport.TLSClientConfig.RootCAs = roots
			t.Cleanup(transport.transport.CloseIdleConnections)
			response, err := transport.RoundTrip(&http.Request{
				Method: http.MethodGet,
				Header: make(http.Header),
				URL:    &url.URL{Host: test.host, Path: "/"},
			})
			if err != nil {
				t.Fatalf("TLS hostname verification failed: %v", err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != "ok" {
				t.Fatalf("TLS response: body=%q error=%v", body, err)
			}
		})
	}
}

func TestTLSHandshakeDeadline(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	closed := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, server)
		close(closed)
	}()

	transport := new(Transport)
	transport.configureHTTPTransport(func(context.Context, string, string) (net.Conn, error) {
		return client, nil
	})
	// Scale the configured deadline while preserving a disabled deadline as zero.
	transport.transport.TLSHandshakeTimeout /= 500
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://upstream.example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = transport.RoundTrip(request)
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() || ctx.Err() != nil {
		t.Fatalf("handshake did not enforce its own deadline: %v", err)
	}

	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("timed-out handshake left the connection open")
	}
}
