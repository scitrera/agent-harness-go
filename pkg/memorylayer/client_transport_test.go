package memorylayer

import (
	"context"
	"net/http"
	"testing"

	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"
)

type recordingTransport struct {
	request *memorylayersdk.Request
}

func (t *recordingTransport) RoundTrip(_ context.Context, request *memorylayersdk.Request) (*memorylayersdk.Response, error) {
	t.request = request
	return &memorylayersdk.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       []byte(`{"threads":[]}`),
	}, nil
}

func (*recordingTransport) Close() error { return nil }

func TestStoreRoutesLegacyChatSurfaceThroughSDKTransport(t *testing.T) {
	transport := &recordingTransport{}
	store, err := New(Config{
		Transport: transport,
		APIKey:    "test-key",
		Workspace: "project-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if transport.request == nil {
		t.Fatal("transport received no request")
	}
	if transport.request.Method != http.MethodGet || transport.request.Path != "/threads" {
		t.Fatalf("request = %s %s, want GET /threads", transport.request.Method, transport.request.Path)
	}
	if got := transport.request.Query.Get("workspace_id"); got != "project-a" {
		t.Fatalf("workspace_id = %q, want project-a", got)
	}
	if got := transport.request.Header.Get("authorization"); got != "Bearer test-key" {
		t.Fatalf("authorization = %q", got)
	}
}
