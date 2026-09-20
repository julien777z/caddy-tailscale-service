package caddytailscaleservice

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
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
