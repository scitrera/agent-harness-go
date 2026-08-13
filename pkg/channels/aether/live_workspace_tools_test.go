package aether_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"
	spec "github.com/scitrera/ecosystem-messaging-spec/go"
	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"
	mlaether "github.com/scitrera/memorylayer/memorylayer-sdk-go/aether"

	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/channel"
	aetherchan "github.com/scitrera/agent-harness-go/pkg/channels/aether"
	"github.com/scitrera/agent-harness-go/pkg/localtools"
	"github.com/scitrera/agent-harness-go/pkg/memorylayer"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/refinement"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type liveAetherACLRule struct {
	RuleID        string     `json:"rule_id,omitempty"`
	PrincipalType string     `json:"principal_type"`
	PrincipalID   string     `json:"principal_id"`
	ResourceType  string     `json:"resource_type"`
	ResourceID    string     `json:"resource_id"`
	AccessLevel   int        `json:"access_level"`
	GrantedBy     string     `json:"granted_by"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	Reason        string     `json:"reason,omitempty"`
}

// grantLiveAetherACL temporarily installs one exact rule through AetherLite's
// loopback-only development admin API. The previous rule, if any, is restored
// after the test; otherwise the temporary rule is removed. Production tests
// must provision ACLs out of band and must not expose an insecure admin API.
func grantLiveAetherACL(t *testing.T, ctx context.Context, adminURL string, rule liveAetherACLRule) {
	t.Helper()
	adminURL = strings.TrimRight(adminURL, "/")
	parsedAdminURL, err := url.Parse(adminURL)
	if err != nil || parsedAdminURL.Scheme != "http" || parsedAdminURL.Host == "" ||
		(parsedAdminURL.Hostname() != "127.0.0.1" && parsedAdminURL.Hostname() != "localhost" && parsedAdminURL.Hostname() != "::1") ||
		(parsedAdminURL.Path != "" && parsedAdminURL.Path != "/") || parsedAdminURL.RawQuery != "" || parsedAdminURL.Fragment != "" {
		t.Fatalf("live Aether ACL fixture requires a plain loopback HTTP admin URL, got %q", adminURL)
	}
	query := url.Values{
		"principal_type": {rule.PrincipalType},
		"principal_id":   {rule.PrincipalID},
		"resource_type":  {rule.ResourceType},
		"resource_id":    {rule.ResourceID},
	}
	rulesURL := adminURL + "/api/v1/acl/rules"
	status, raw, err := liveAetherAdminRequest(ctx, http.MethodGet, rulesURL+"?"+query.Encode(), nil)
	if err != nil {
		t.Fatalf("inspect live Aether ACL %s/%s: %v", rule.ResourceType, rule.ResourceID, err)
	}
	if status != http.StatusOK {
		t.Fatalf("inspect live Aether ACL %s/%s: HTTP %d: %s", rule.ResourceType, rule.ResourceID, status, raw)
	}
	var listed struct {
		Rules []liveAetherACLRule `json:"rules"`
	}
	if err := json.Unmarshal(raw, &listed); err != nil {
		t.Fatalf("decode live Aether ACL list: %v", err)
	}
	if len(listed.Rules) > 1 {
		t.Fatalf("exact live Aether ACL lookup returned %d rules", len(listed.Rules))
	}
	var previous *liveAetherACLRule
	if len(listed.Rules) == 1 {
		copy := listed.Rules[0]
		previous = &copy
	}

	encoded, err := json.Marshal(rule)
	if err != nil {
		t.Fatal(err)
	}
	status, raw, err = liveAetherAdminRequest(ctx, http.MethodPost, rulesURL, encoded)
	if err != nil {
		t.Fatalf("grant live Aether ACL %s/%s: %v", rule.ResourceType, rule.ResourceID, err)
	}
	if status != http.StatusCreated {
		t.Fatalf("grant live Aether ACL %s/%s: HTTP %d: %s", rule.ResourceType, rule.ResourceID, status, raw)
	}
	var granted struct {
		Rule liveAetherACLRule `json:"rule"`
	}
	if err := json.Unmarshal(raw, &granted); err != nil || granted.Rule.RuleID == "" {
		t.Fatalf("decode granted live Aether ACL: rule_id missing: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if previous != nil {
			body, marshalErr := json.Marshal(previous)
			if marshalErr != nil {
				t.Logf("restore live Aether ACL: %v", marshalErr)
				return
			}
			cleanupStatus, cleanupRaw, cleanupErr := liveAetherAdminRequest(cleanupCtx, http.MethodPost, rulesURL, body)
			if cleanupErr != nil || cleanupStatus != http.StatusCreated {
				t.Logf("restore live Aether ACL %s/%s: HTTP %d: %s: %v", rule.ResourceType, rule.ResourceID, cleanupStatus, cleanupRaw, cleanupErr)
			}
			return
		}
		cleanupURL := rulesURL + "/" + url.PathEscape(granted.Rule.RuleID) + "?" + query.Encode()
		cleanupStatus, cleanupRaw, cleanupErr := liveAetherAdminRequest(cleanupCtx, http.MethodDelete, cleanupURL, nil)
		if cleanupErr != nil || cleanupStatus != http.StatusOK {
			t.Logf("revoke live Aether ACL %s/%s: HTTP %d: %s: %v", rule.ResourceType, rule.ResourceID, cleanupStatus, cleanupRaw, cleanupErr)
		}
	})
}

func liveAetherAdminRequest(ctx context.Context, method, endpoint string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	return response.StatusCode, raw, err
}

// TestLiveAetherClientWorkspaceToolRouting is an opt-in deterministic E2E. It
// exercises the real gateway and MemoryLayer service without relying on an LLM
// to choose a tool. Run it against oss/e2e with AETHER_E2E_ADDR set.
func TestLiveAetherClientWorkspaceToolRouting(t *testing.T) {
	serverAddr := os.Getenv("AETHER_E2E_ADDR")
	if serverAddr == "" {
		t.Skip("set AETHER_E2E_ADDR to run the live Aether/MemoryLayer tool-routing test")
	}
	aetherWorkspace := os.Getenv("AETHER_E2E_WORKSPACE")
	if aetherWorkspace == "" {
		aetherWorkspace = "default"
	}
	memoryLayerTarget := os.Getenv("MEMORYLAYER_E2E_TARGET")
	if memoryLayerTarget == "" {
		memoryLayerTarget = "sv::memorylayer"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	logicalWorkspace := "e2e-view-" + suffix
	specifier := "view-test-" + suffix
	windowID := "window-" + suffix

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	worker, err := aetherchan.New(aetherchan.Config{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: logicalWorkspace,
		Specifier: specifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = worker.Close() }()
	client, err := aetherchan.NewClient(aetherchan.ClientConfig{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: logicalWorkspace,
		AgentSpecifier: specifier, UserID: "workspace-e2e", WindowID: windowID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	clientMLConfig := memorylayer.Config{
		Transport: mlaether.NewTransport(client, mlaether.WithTarget(memoryLayerTarget)),
		Workspace: logicalWorkspace,
	}
	clientStore, err := memorylayer.New(clientMLConfig)
	if err != nil {
		t.Fatal(err)
	}
	clientPublisher, err := memorylayer.NewWorkspaceViewPublisher(clientMLConfig, clientStore)
	if err != nil {
		t.Fatal(err)
	}
	workerMLConfig := memorylayer.Config{
		Transport: mlaether.NewTransport(worker, mlaether.WithTarget(memoryLayerTarget)),
		Workspace: logicalWorkspace,
	}
	workerStore, err := memorylayer.New(workerMLConfig)
	if err != nil {
		t.Fatal(err)
	}
	workerPublisher, err := memorylayer.NewWorkspaceViewPublisher(workerMLConfig, workerStore)
	if err != nil {
		t.Fatal(err)
	}
	worker.SetExecutionBindingAuthorizer(workerPublisher)

	clientRoot := t.TempDir()
	workerRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(clientRoot, "identity.txt"), []byte("client checkout"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workerRoot, "identity.txt"), []byte("worker checkout"), 0o644); err != nil {
		t.Fatal(err)
	}
	host, err := aetherchan.NewClientToolHost(ctx, aetherchan.ClientToolHostConfig{
		WorkspaceID: logicalWorkspace, WorkspaceRoot: clientRoot, StateDir: t.TempDir(),
		ToolHostID: client.ToolHostID(), AgentTopic: client.AgentTopic(), Publisher: clientPublisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.SetToolHost(host)
	binding, err := host.ExecutionBindingForDirectory(ctx, clientRoot)
	if err != nil {
		t.Fatal(err)
	}
	message := protocol.ChatMessage{
		ID: "user-task-1", Role: protocol.RoleUser,
		Addr: protocol.MessageAddress{WorkspaceID: logicalWorkspace, ThreadID: "thread-1", TaskID: "task-1"},
	}
	scope, err := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{
		WriteAccess: workspacepkg.ViewWriteAccessReadWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := workspacepkg.PutExecutionScope(&message, scope); err != nil {
		t.Fatal(err)
	}
	if err := client.Enqueue(ctx, channel.Inbound{Addr: message.Addr, Message: message}); err != nil {
		t.Fatal(err)
	}
	inbound, err := worker.FetchTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workerWorkspace, err := localtools.NewWorkspace(workerRoot)
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := tools.RegisterLocal(registry, tools.LocalConfig{Workspace: workerWorkspace}); err != nil {
		t.Fatal(err)
	}
	turnCtx := worker.TurnContext(ctx, inbound.Addr)
	result, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "call-live-1", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: inbound.Addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		t.Fatal(err)
	}
	if output.Text != "client checkout" {
		t.Fatalf("live routed read = %q, want client checkout", output.Text)
	}

	// Start a real client-hosted process, wait until it records its PID, then
	// cancel from the worker side. A fast worker return alone would not prove
	// the reverse cancellation crossed Aether; the client process must exit too.
	cancelCtx, cancelTool := context.WithCancel(turnCtx)
	cancelDone := make(chan error, 1)
	pidFile := filepath.Join(clientRoot, "cancel.pid")
	go func() {
		_, invokeErr := registry.Invoke(cancelCtx, tools.Request{
			CallID: "call-live-cancel", Name: "shell",
			Arguments: json.RawMessage(`{"command":"/bin/sh","args":["-c","echo $$ > cancel.pid; exec sleep 20"]}`),
			Addr:      inbound.Addr,
		})
		cancelDone <- invokeErr
	}()
	var pid int
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		contents, readErr := os.ReadFile(pidFile)
		if readErr == nil {
			pid, err = strconv.Atoi(strings.TrimSpace(string(contents)))
			if err != nil {
				t.Fatalf("parse client tool pid: %v", err)
			}
			break
		}
		if !os.IsNotExist(readErr) {
			t.Fatal(readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("live client tool did not start")
	}
	cancelTool()
	select {
	case invokeErr := <-cancelDone:
		if !errors.Is(invokeErr, context.Canceled) {
			t.Fatalf("live cancelled tool error = %v", invokeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live worker tool call did not return after cancellation")
	}
	processPath := filepath.Join("/proc", strconv.Itoa(pid))
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if _, statErr := os.Stat(processPath); os.IsNotExist(statErr) {
			pid = 0
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid != 0 {
		t.Fatalf("client-hosted process %d survived reverse cancellation", pid)
	}

	client.SetToolHost(nil)
	if _, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "call-live-2", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: inbound.Addr,
	}); err == nil {
		t.Fatal("missing live client host silently fell back to worker checkout")
	}
}

// TestLiveAetherCatalogAgentMemoryLayerOBO is an opt-in, model-free proof of
// the arbitrary-agent tool-host path. A user session lends authority to a
// caller agent, the catalog derives an invocation-bound child for a second
// provider agent, and that provider uses the child on a real MemoryLayer
// request before returning a VFS-shaped reference. It requires the OSS E2E
// compose stack's tool-catalog service and reviewed test policy.
func TestLiveAetherCatalogAgentMemoryLayerOBO(t *testing.T) {
	if os.Getenv("AETHER_CATALOG_E2E") != "1" {
		t.Skip("set AETHER_CATALOG_E2E=1 with AETHER_E2E_ADDR to run the live catalog-agent OBO test")
	}
	serverAddr := os.Getenv("AETHER_E2E_ADDR")
	if serverAddr == "" {
		t.Skip("set AETHER_E2E_ADDR to run the live catalog-agent OBO test")
	}
	baseWorkspace := os.Getenv("AETHER_E2E_WORKSPACE")
	if baseWorkspace == "" {
		baseWorkspace = "default"
	}
	memoryLayerTarget := os.Getenv("MEMORYLAYER_E2E_TARGET")
	if memoryLayerTarget == "" {
		memoryLayerTarget = "sv::memorylayer"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	catalogWorkspace := baseWorkspace + "-catalog-" + suffix
	userID := "catalog-e2e-user-" + suffix
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	user, err := sdk.NewUserClient(sdk.UserOptions{
		ClientOptions: sdk.ClientOptions{ServerAddr: serverAddr},
		Workspace:     catalogWorkspace, UserID: userID, WindowID: "catalog-e2e-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer user.Close()
	caller, err := sdk.NewAgentClient(sdk.AgentOptions{
		ClientOptions: sdk.ClientOptions{ServerAddr: serverAddr},
		Workspace:     catalogWorkspace, Implementation: "catalog-e2e-caller", Specifier: suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	provider, err := sdk.NewAgentClient(sdk.AgentOptions{
		ClientOptions: sdk.ClientOptions{ServerAddr: serverAddr},
		Workspace:     catalogWorkspace, Implementation: "catalog-e2e-provider", Specifier: "memorylayer-e2e",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()

	catalogClient, err := aetherchan.NewCatalogClient(aetherchan.CatalogClientConfig{
		Route: caller.Topic(), Sender: caller,
	})
	if err != nil {
		t.Fatal(err)
	}
	caller.OnMessage(func(_ context.Context, message *sdk.Message) error {
		catalogClient.TryHandle(message)
		return nil
	})

	mlClient, err := memorylayersdk.NewClient(
		memorylayersdk.WithTransport(mlaether.NewTransport(provider, mlaether.WithTarget(memoryLayerTarget))),
		memorylayersdk.WithWorkspaceID(catalogWorkspace),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer mlClient.Close()
	registry := tools.NewRegistry()
	seenAuthority := make(chan tools.MemoryAuthority, 1)
	if err := registry.Register("remember_to_vfs", tools.HandlerFunc(func(callCtx context.Context, request tools.Request) (tools.Result, error) {
		if request.Authority.GrantID == "" || request.Authority.SubjectType != "user" || request.Authority.SubjectID != userID {
			return tools.Result{}, fmt.Errorf("provider received invalid forwarded authority: %+v", request.Authority)
		}
		seenAuthority <- request.Authority
		vfsRef := "vfs://memorylayer-e2e/" + suffix
		acting := mlClient.ActingFor(request.Authority.GrantID, &memorylayersdk.PrincipalRef{
			Type: request.Authority.SubjectType, ID: request.Authority.SubjectID,
		})
		stored, err := acting.Remember(callCtx, "catalog provider OBO proof "+suffix, memorylayersdk.RememberOptions{
			WorkspaceID: catalogWorkspace,
			Metadata:    map[string]any{"vfs_ref": vfsRef, "source": "catalog-agent-e2e"},
		})
		if err != nil {
			return tools.Result{}, err
		}
		payload, err := json.Marshal(map[string]any{
			"vfs_ref": vfsRef, "memory_id": stored.ID, "grant_id": request.Authority.GrantID,
		})
		if err != nil {
			return tools.Result{}, err
		}
		return tools.NewJSONResult(request.CallID, request.Name, payload)
	})); err != nil {
		t.Fatal(err)
	}
	registry.Describe(tools.Descriptor{
		Name: "remember_to_vfs", Description: "Store a deterministic authority proof in MemoryLayer and return its VFS reference.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		CatalogKind: "remote", Effect: spec.ToolEffectWrite,
	})
	profile := catalog.InvocationAuthorityProfile{
		Mode: catalog.InvocationAuthorityModeCallerOBO,
		ResourceScope: []catalog.InvocationAuthorityResourceScope{
			{ResourceType: "memories", Patterns: []string{"*"}},
		},
		OperationScope: []string{"write"}, MaxAccessLevel: 20,
	}
	host, err := aetherchan.NewCatalogToolHost(aetherchan.CatalogToolHostConfig{
		Route: provider.Topic(), ProviderID: "memorylayer-e2e-provider", RegistrationID: "primary",
		Generation: "generation-1", GenerationReplacement: true,
		Context: spec.ToolCatalogContext{WorkspaceID: catalogWorkspace}, Registry: registry, Sender: provider,
		Exports: []aetherchan.CatalogToolExport{{
			Name: "remember_to_vfs", Revision: "sha256:remember-to-vfs-v1",
			Effect: spec.ToolEffectWrite, InvocationAuthority: &profile,
		}},
		LeaseDuration: 2 * time.Minute, RenewInterval: time.Minute, RPCTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider.OnMessage(host.Handle)

	for name, client := range map[string]interface {
		Connect(context.Context) error
		Run(context.Context) error
	}{"user": user, "caller": caller, "provider": provider} {
		if err := client.Connect(ctx); err != nil {
			t.Fatalf("connect %s: %v", name, err)
		}
		go func() { _ = client.Run(ctx) }()
	}
	for deadline := time.Now().Add(3 * time.Second); user.SessionID() == "" && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if user.SessionID() == "" {
		t.Fatal("user connection did not receive a gateway session id")
	}

	ref := spec.ToolReference{
		ProviderID: "memorylayer-e2e-provider", RegistrationID: "primary", Generation: "generation-1",
		Name: "remember_to_vfs", Revision: "sha256:remember-to-vfs-v1",
	}
	catalogContext := spec.ToolCatalogContext{WorkspaceID: catalogWorkspace}
	providerResource, err := catalog.ProviderResourceID(catalogContext, ref.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	entryResource, err := catalog.EntryResourceID(catalogContext, ref)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	adminURL := os.Getenv("AETHER_E2E_ADMIN_URL")
	if adminURL == "" {
		adminURL = "http://127.0.0.1:31880"
	}
	expiresAt := now.Add(10 * time.Minute)
	for _, rule := range []liveAetherACLRule{
		{
			PrincipalType: "agent", PrincipalID: provider.Topic(),
			ResourceType: "tool-catalog/provider", ResourceID: providerResource, AccessLevel: 20,
			GrantedBy: "agent-harness-oss-e2e", ExpiresAt: &expiresAt,
			Reason: "temporary model-free catalog provider lifecycle proof",
		},
		{
			PrincipalType: "user", PrincipalID: userID,
			ResourceType: "tool-catalog/entry", ResourceID: entryResource, AccessLevel: 20,
			GrantedBy: "agent-harness-oss-e2e", ExpiresAt: &expiresAt,
			Reason: "temporary model-free catalog discovery and invocation proof",
		},
		{
			PrincipalType: "service", PrincipalID: "sv::tool-catalog::e2e",
			ResourceType: "kv_scope", ResourceID: "workspace", AccessLevel: 20,
			GrantedBy: "agent-harness-oss-e2e", ExpiresAt: &expiresAt,
			Reason: "temporary model-free catalog persistence proof",
		},
		{
			PrincipalType: "service", PrincipalID: "sv::tool-catalog::e2e",
			ResourceType: "workspace", ResourceID: catalogWorkspace, AccessLevel: 20,
			GrantedBy: "agent-harness-oss-e2e", ExpiresAt: &expiresAt,
			Reason: "temporary model-free catalog reply-routing proof",
		},
	} {
		grantLiveAetherACL(t, ctx, adminURL, rule)
	}
	resourceScope := []*pb.ACLAuthorityGrantResourceScopeEntry{
		{ResourceType: "tool-catalog/entry", Patterns: []string{entryResource}},
		{ResourceType: "memories", Patterns: []string{"*"}},
	}
	operationScope := []string{
		catalog.CatalogActionDiscover, catalog.CatalogActionDescribe,
		catalog.CatalogActionInvokeWrite, "write",
	}
	rootResponse, err := user.AuthorityGrants().Exchange(ctx, "", sdk.ExchangeOpts{
		WorkspaceScope: []string{catalogWorkspace},
		ResourceScope:  resourceScope, OperationScope: operationScope,
		MaxAccessLevel: 20, ValidWhileAudienceActive: true, MayDelegate: true, RemainingHops: 3,
		ExpiresAt: now.Add(5 * time.Minute).Unix(), RenewableUntil: now.Add(10 * time.Minute).Unix(),
		Reason: "model-free catalog agent MemoryLayer E2E root",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rootResponse.GetSuccess() || rootResponse.GetGrant().GetGrantId() == "" {
		t.Fatalf("root authority exchange = %+v", rootResponse)
	}
	grantResponse, err := user.AuthorityGrants().Derive(
		ctx, rootResponse.GetGrant().GetGrantId(), "agent", caller.Topic(), sdk.DeriveOpts{
			WorkspaceScope: []string{catalogWorkspace},
			ResourceScope:  resourceScope, OperationScope: operationScope,
			MaxAccessLevel: 20, AudienceType: "agent", AudienceID: caller.Topic(),
			ValidWhileAudienceActive: true, MayDelegate: true, RemainingHops: 2,
			ExpiresAt: now.Add(5 * time.Minute).Unix(), RenewableUntil: now.Add(10 * time.Minute).Unix(),
			Reason: "model-free catalog agent MemoryLayer E2E caller",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !grantResponse.GetSuccess() || grantResponse.GetGrant().GetGrantId() == "" {
		t.Fatalf("caller authority derivation = %+v", grantResponse)
	}
	auth := tools.MemoryAuthority{
		GrantID:     grantResponse.GetGrant().GetGrantId(),
		SubjectType: strings.ToLower(grantResponse.GetGrant().GetSubject().GetPrincipalType()),
		SubjectID:   grantResponse.GetGrant().GetSubject().GetPrincipalId(),
	}
	if auth.SubjectID != userID {
		t.Fatalf("exchanged subject = %+v, want user %q", auth, userID)
	}
	if err := host.Publish(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer revokeCancel()
		_ = host.Revoke(revokeCtx)
	}()
	page, err := catalogClient.QueryCatalog(ctx, spec.ToolCatalogQuery{
		Context: catalogContext, Query: ref.Name, Limit: 10,
	}, auth)
	if err != nil {
		t.Fatal(err)
	}
	var resolved *catalog.ResolvedCatalogEntry
	for i := range page.Records {
		candidate := page.Records[i].Entry.Ref
		if candidate.ProviderID == ref.ProviderID && candidate.RegistrationID == ref.RegistrationID &&
			candidate.Generation == ref.Generation && candidate.Name == ref.Name && candidate.Revision == ref.Revision {
			resolved = &page.Records[i]
			break
		}
	}
	if resolved == nil || resolved.ProviderRoute != provider.Topic() || resolved.InvocationAuthority == nil {
		t.Fatalf("resolved catalog record = %+v", resolved)
	}
	envelope := spec.NewToolInvokeEnvelope("catalog-e2e-call-"+suffix, ref.Name)
	envelope.ToolRef = &ref
	envelope.Addr = spec.MessageAddress{
		WorkspaceID: catalogWorkspace, ThreadID: "catalog-e2e-thread", TaskID: "catalog-e2e-task-" + suffix, UserID: userID,
	}
	resultRaw, err := catalogClient.InvokeCatalogTool(
		ctx, resolved.ProviderRoute, resolved.Context, resolved.Entry.Effect,
		resolved.InvocationAuthority, envelope, auth,
	)
	if err != nil {
		t.Fatal(err)
	}
	var result spec.ToolResultPartBody
	if err := json.Unmarshal(resultRaw, &result); err != nil {
		t.Fatal(err)
	}
	if result.IsError || result.CallID != envelope.CallID {
		t.Fatalf("tool result = %+v", result)
	}
	var output struct {
		VFSRef   string `json:"vfs_ref"`
		MemoryID string `json:"memory_id"`
		GrantID  string `json:"grant_id"`
	}
	if err := json.Unmarshal(result.Output, &output); err != nil {
		t.Fatal(err)
	}
	if output.VFSRef != "vfs://memorylayer-e2e/"+suffix || output.MemoryID == "" ||
		output.GrantID == "" || output.GrantID == auth.GrantID {
		t.Fatalf("provider output = %+v parent=%q", output, auth.GrantID)
	}
	select {
	case forwarded := <-seenAuthority:
		if forwarded.GrantID != output.GrantID || forwarded.SubjectID != userID {
			t.Fatalf("provider authority = %+v output=%+v", forwarded, output)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// TestLiveAetherScheduledWorkerView verifies the WorkflowEngine path without an
// LLM: a one-shot schedule creates an exact-target BACKGROUND task, the worker
// reconstructs its versioned envelope, and ingress retains the published view.
func TestLiveAetherScheduledWorkerView(t *testing.T) {
	serverAddr := os.Getenv("AETHER_E2E_ADDR")
	if serverAddr == "" {
		t.Skip("set AETHER_E2E_ADDR to run the live Aether scheduled-view test")
	}
	aetherWorkspace := os.Getenv("AETHER_E2E_WORKSPACE")
	if aetherWorkspace == "" {
		aetherWorkspace = "default"
	}
	memoryLayerTarget := os.Getenv("MEMORYLAYER_E2E_TARGET")
	if memoryLayerTarget == "" {
		memoryLayerTarget = "sv::memorylayer"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	logicalWorkspace := "e2e-schedule-" + suffix

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	worker, err := aetherchan.New(aetherchan.Config{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: logicalWorkspace,
		Specifier: "schedule-test-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = worker.Close() }()
	mlConfig := memorylayer.Config{
		Transport: mlaether.NewTransport(worker, mlaether.WithTarget(memoryLayerTarget)),
		Workspace: logicalWorkspace,
	}
	mlStore, err := memorylayer.New(mlConfig)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := memorylayer.NewWorkspaceViewPublisher(mlConfig, mlStore)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "identity.txt"), []byte("scheduled worker view"), 0o644); err != nil {
		t.Fatal(err)
	}
	host, err := aetherchan.NewWorkerToolHost(ctx, aetherchan.WorkerToolHostConfig{
		WorkspaceID: logicalWorkspace, WorkspaceRoot: root, StateDir: t.TempDir(),
		ToolHostID: worker.Topic(), Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker.SetWorkerToolHost(host)
	binding, err := host.ScheduledExecutionBindingForDirectory(ctx, root, true, false)
	if err != nil {
		t.Fatal(err)
	}
	registration := aetherchan.ScheduledTurnRegistration{
		ID: "once-" + suffix, Name: "E2E scheduled view", Enabled: true,
		ScheduleType: "once", ScheduleExpression: time.Now().Add(30 * time.Second).UTC().Format(time.RFC3339), MissPolicy: aetherchan.ScheduledMissPolicyFireOnce,
		TargetOfflinePolicy: "queue", ThreadID: "scheduled-e2e",
		Prompt: "This initial declaration must be replaced", Binding: binding,
		ViewPolicy: aetherchan.ScheduledViewPolicy{
			WriteAccess: workspacepkg.ViewWriteAccessReadOnly, AllowMutableView: true,
		},
	}
	if err := worker.EnableScheduledTurns(ctx, []aetherchan.ScheduledTurnRegistration{registration}, nil, 0); err != nil {
		t.Fatal(err)
	}
	registration.ScheduleExpression = time.Now().Add(3 * time.Second).UTC().Format(time.RFC3339)
	registration.Prompt = "Inspect the exact worker view"
	if err := worker.UpdateScheduledTurns(ctx, []aetherchan.ScheduledTurnRegistration{registration}); err != nil {
		t.Fatal(err)
	}
	inbound, err := worker.FetchTask(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inbound.Addr.WorkspaceID != logicalWorkspace || inbound.Addr.ThreadID != "scheduled-e2e" ||
		inbound.Addr.TaskID == "" || !aetherchan.IsScheduledTurnMessage(inbound.Message) {
		t.Fatalf("scheduled inbound = %+v", inbound)
	}
	info, err := worker.GetTaskInfo(ctx, inbound.Addr.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	scheduledFor, scheduledErr := time.Parse(time.RFC3339Nano, info.Metadata["aether.schedule.scheduled_for"])
	dispatchedAt, dispatchedErr := time.Parse(time.RFC3339Nano, info.Metadata["aether.schedule.dispatched_at"])
	if info.Metadata["aether.schedule.id"] == "" ||
		info.Metadata["aether.schedule.miss_policy"] != aetherchan.ScheduledMissPolicyFireOnce ||
		info.Metadata["scitrera.schedule_miss_policy"] != aetherchan.ScheduledMissPolicyFireOnce ||
		info.Metadata["scitrera.thread_id"] != registration.ThreadID ||
		info.Metadata["aether.schedule.disposition"] != aetherchan.ScheduledDispositionOrdinary ||
		info.Metadata["aether.schedule.backlog_count"] != "1" ||
		info.Metadata["aether.schedule.backlog_truncated"] != "false" ||
		info.Metadata["aether.schedule.backlog_index"] != "1" ||
		scheduledErr != nil || dispatchedErr != nil || dispatchedAt.Before(scheduledFor) {
		t.Fatalf("scheduled occurrence metadata = %#v scheduled_err=%v dispatched_err=%v", info.Metadata, scheduledErr, dispatchedErr)
	}
	var scheduleState *aetherchan.ScheduledTurnScheduleState
	stateDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(stateDeadline) {
		states, stateErr := worker.ListScheduledTurnScheduleStates(ctx)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		for i := range states {
			if states[i].DeclarationID == registration.ID && states[i].LastOccurrence != nil {
				scheduleState = &states[i]
				break
			}
		}
		if scheduleState != nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if scheduleState == nil || scheduleState.LastOccurrence.Disposition != aetherchan.ScheduledDispositionOrdinary ||
		scheduleState.LastOccurrence.DispatchedAt == nil || scheduleState.LastOccurrence.BacklogCount != 1 ||
		scheduleState.LastOccurrence.BacklogTruncated || scheduleState.LastOccurrence.BacklogIndex != 1 ||
		scheduleState.LastFiredAt == nil || !scheduleState.LastFiredAt.Equal(*scheduleState.LastOccurrence.DispatchedAt) {
		t.Fatalf("authoritative schedule state = %+v", scheduleState)
	}
	journal, err := turnjournal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	operations, err := aetherchan.NewScheduledOperationsCommands(worker, journal, mlStore, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	schedulesText, err := operations.RunScheduledOperationsCommand(ctx, inbound.Addr, inbound.Message, "schedules", "")
	if err != nil || !strings.Contains(schedulesText, registration.ID) || !strings.Contains(schedulesText, "latest=ordinary") {
		t.Fatalf("live schedules command = %q err=%v", schedulesText, err)
	}
	runsText, err := operations.RunScheduledOperationsCommand(ctx, inbound.Addr, inbound.Message, "runs", "--status queued --limit 1")
	if err != nil || !strings.Contains(runsText, inbound.Addr.TaskID) ||
		!strings.Contains(runsText, "task=queued") || !strings.Contains(runsText, "disposition=ordinary") {
		t.Fatalf("live runs command = %q err=%v", runsText, err)
	}
	gotBinding, err := spec.GetExecutionBinding(inbound.Message)
	if err != nil || gotBinding == nil || gotBinding.ViewID != binding.ViewID ||
		gotBinding.ToolHostID != worker.Topic() || gotBinding.ExecutionSite != spec.ExecutionSiteWorker {
		t.Fatalf("scheduled binding = %+v err=%v", gotBinding, err)
	}
	fallback, err := localtools.NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	registry := tools.NewRegistry()
	if err := tools.RegisterLocal(registry, tools.LocalConfig{Workspace: fallback}); err != nil {
		t.Fatal(err)
	}
	turnCtx := worker.TurnContext(ctx, inbound.Addr)
	readResult, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "scheduled-read", Name: "read_file", Arguments: json.RawMessage(`{"path":"identity.txt"}`), Addr: inbound.Addr,
	})
	if err != nil || !strings.Contains(string(readResult.Payload), "scheduled worker view") {
		t.Fatalf("scheduled exact-view read = %s err=%v", readResult.Payload, err)
	}
	if _, err := registry.Invoke(turnCtx, tools.Request{
		CallID: "scheduled-write", Name: "write_file",
		Arguments: json.RawMessage(`{"path":"forbidden.txt","content":"no"}`), Addr: inbound.Addr,
	}); err == nil || !strings.Contains(err.Error(), "requires write admission") {
		t.Fatalf("scheduled read-only write error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "forbidden.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("scheduled write escaped policy: %v", statErr)
	}
	if err := worker.FailTask(context.Background(), inbound.Addr.TaskID, "E2E inspection complete"); err != nil {
		t.Fatal(err)
	}

	// Exercise the same commands through the separately deployed OSS worker.
	// Keeping this in the test that creates the run makes the live assertion
	// independent of Go test ordering and any state left by previous runs.
	specifier := os.Getenv("AETHER_E2E_SPECIFIER")
	if specifier == "" {
		specifier = "e2e"
	}
	client, err := aetherchan.NewClient(aetherchan.ClientConfig{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: "default",
		AgentSpecifier: specifier, UserID: "operations-e2e", WindowID: "operations-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	schedulesText = runLiveMetaCommand(t, ctx, client, "default", "schedules-"+suffix, "/schedules")
	if !strings.Contains(schedulesText, "Scheduled turns") || strings.Contains(schedulesText, "Thinking") {
		t.Fatalf("deployed /schedules reply = %q", schedulesText)
	}
	runsText = runLiveMetaCommand(t, ctx, client, "default", "runs-"+suffix, "/runs --status failed --limit 1")
	if !strings.Contains(runsText, "Scheduled runs") || !strings.Contains(runsText, "task=failed") ||
		!strings.Contains(runsText, "disposition=ordinary") || strings.Contains(runsText, "Thinking") {
		t.Fatalf("deployed /runs reply = %q", runsText)
	}
}

// TestLiveAetherRefinementAudit proves that the deployed worker reads the
// MemoryLayer-authoritative audit through sv::memorylayer and serves a bounded
// model-free operator command.
func TestLiveAetherRefinementAudit(t *testing.T) {
	serverAddr := os.Getenv("AETHER_E2E_ADDR")
	if serverAddr == "" {
		t.Skip("set AETHER_E2E_ADDR to run the live refinement-audit test")
	}
	aetherWorkspace := os.Getenv("AETHER_E2E_WORKSPACE")
	if aetherWorkspace == "" {
		aetherWorkspace = "default"
	}
	specifier := os.Getenv("AETHER_E2E_SPECIFIER")
	if specifier == "" {
		specifier = "e2e"
	}
	memoryLayerTarget := os.Getenv("MEMORYLAYER_E2E_TARGET")
	if memoryLayerTarget == "" {
		memoryLayerTarget = "sv::memorylayer"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := aetherchan.NewClient(aetherchan.ClientConfig{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: "default",
		AgentSpecifier: specifier, UserID: "refinement-e2e", WindowID: "refinement-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	store, err := memorylayer.NewRefinementRecordStore(memorylayer.Config{
		Transport: mlaether.NewTransport(client, mlaether.WithTarget(memoryLayerTarget)), Workspace: "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	refinementID := "refinement-e2e-" + suffix
	created, err := store.Append(ctx, "default", "refinement-e2e-create-"+suffix, refinement.AppendRequest{
		Key: "refinements/" + refinementID + "/application",
		Plan: refinement.Plan{
			RefinementID: refinementID, Trigger: "deterministic live check", Scope: refinement.ScopeWorkspace,
			Summary: "Inspect a failed MemoryLayer audit record", Rationale: "Prove bounded worker-authoritative browsing",
			ExpectedOutcome: "The deployed worker returns this exact immutable record",
			Evidence:        []refinement.Evidence{{Kind: refinement.EvidenceEvaluation, Reference: "live:" + suffix, Description: "model-free E2E fixture"}},
			Edits:           []refinement.Edit{{Action: refinement.ActionCreate, ResourceKind: refinement.ResourceMemory, ResourceKey: "e2e/" + suffix, Reason: "deterministic failed fixture"}},
		},
		Phase: refinement.PhaseApplication, Outcome: refinement.OutcomeFailed, SchemaVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	reply := runLiveMetaCommand(t, ctx, client, "default", "refinements-"+suffix, "/refinements --attention --refinement "+refinementID+" --limit 1")
	if !strings.Contains(reply, "source: memorylayer authoritative") || !strings.Contains(reply, created.Record.ID) ||
		!strings.Contains(reply, "outcome=failed") || !strings.Contains(reply, "attention=required") || strings.Contains(reply, "Thinking") {
		t.Fatalf("deployed /refinements reply = %q", reply)
	}
}

// TestLiveAetherExecutionLedger proves the deployed worker persists operational
// state in Aether KV: a model pin made by one task is visible through the
// bounded, model-free branch-aware ledger command on the next task.
func TestLiveAetherExecutionLedger(t *testing.T) {
	serverAddr := os.Getenv("AETHER_E2E_ADDR")
	if serverAddr == "" {
		t.Skip("set AETHER_E2E_ADDR to run the live execution-ledger test")
	}
	aetherWorkspace := os.Getenv("AETHER_E2E_WORKSPACE")
	if aetherWorkspace == "" {
		aetherWorkspace = "default"
	}
	specifier := os.Getenv("AETHER_E2E_SPECIFIER")
	if specifier == "" {
		specifier = "e2e"
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := aetherchan.NewClient(aetherchan.ClientConfig{
		ServerAddr: serverAddr, Workspace: aetherWorkspace, SessionWorkspace: "default",
		AgentSpecifier: specifier, UserID: "ledger-e2e", WindowID: "ledger-" + suffix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	list := runLiveMetaCommand(t, ctx, client, "default", "model-list-"+suffix, "/model")
	marker := "\n\nActive: "
	index := strings.Index(list, marker)
	if index < 0 {
		t.Fatalf("deployed /model reply lacks active model: %q", list)
	}
	activeLine := strings.SplitN(list[index+len(marker):], "\n", 2)[0]
	modelName := strings.TrimSpace(strings.TrimSuffix(activeLine, ". Switch with /model MODEL_NAME."))
	if modelName == "" {
		t.Fatalf("deployed /model active model is empty: %q", list)
	}
	pinTask := "model-pin-" + suffix
	pinned := runLiveMetaCommand(t, ctx, client, "default", pinTask, "/model "+modelName)
	if !strings.Contains(pinned, "Switched to model") || strings.Contains(pinned, "Thinking") {
		t.Fatalf("deployed model pin reply = %q", pinned)
	}
	ledger := runLiveMetaCommand(t, ctx, client, "default", "ledger-read-"+suffix, "/ledger --type model_pinned --task "+pinTask+" --limit 1")
	if !strings.Contains(ledger, "source: aether-kv authoritative") || !strings.Contains(ledger, "model_pinned") ||
		!strings.Contains(ledger, "task="+pinTask) || !strings.Contains(ledger, "model="+modelName) || strings.Contains(ledger, "Thinking") {
		t.Fatalf("deployed /ledger reply = %q", ledger)
	}
}

func runLiveMetaCommand(t *testing.T, ctx context.Context, client *aetherchan.Client, workspaceID, taskID, text string) string {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatal(err)
	}
	addr := protocol.MessageAddress{WorkspaceID: workspaceID, ThreadID: "operations-e2e", TaskID: taskID}
	message := protocol.ChatMessage{
		ID: "user-" + taskID, Role: protocol.RoleUser, Addr: addr,
		Content: []protocol.ContentPart{part},
	}
	if err := client.Enqueue(ctx, channel.Inbound{Addr: addr, Message: message}); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %s: %v", text, ctx.Err())
		case event := <-client.Events():
			if event.Type != channel.EventMessageFinal || event.Addr.TaskID != taskID || event.Message == nil {
				continue
			}
			var b strings.Builder
			for _, content := range event.Message.Content {
				if textPart, ok := content.AsText(); ok {
					b.WriteString(textPart.Text)
				}
			}
			return b.String()
		}
	}
}
