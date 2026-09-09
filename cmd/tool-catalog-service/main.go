// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

// Command tool-catalog-service exposes pkg/catalog's deterministic Go live
// catalog as the workspace-less Aether service sv::tool-catalog.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/catalog"
	"github.com/scitrera/agent-harness-go/pkg/catalog/catalogrpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("tool catalog service stopped", "err", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	address := flag.String("aether", envOr("AETHER_ADDR", envOr("AETHER_GATEWAY", "127.0.0.1:50051")), "Aether gateway host:port")
	implementation := flag.String("implementation", envOr("TOOL_CATALOG_IMPLEMENTATION", "tool-catalog"), "Aether service implementation")
	specifier := flag.String("specifier", envOr("AETHER_SERVICE_SPECIFIER", hostname()), "Aether service specifier")
	policyEpoch := flag.String("policy-epoch", envOr("TOOL_CATALOG_POLICY_EPOCH", "aether-entry-v1"), "authorization policy epoch bound into snapshots")
	authorityPolicyFile := flag.String("invocation-authority-policy", os.Getenv("TOOL_CATALOG_INVOCATION_AUTHORITY_POLICY"), "optional JSON file mapping exact tool references to caller-OBO ceilings")
	mutationSources := flag.String("mutation-source-prefixes", envOr("TOOL_CATALOG_MUTATION_SOURCE_PREFIXES", "sv::platform-bridge::"), "comma-separated trusted mutation source prefixes")
	keyValue := flag.String("cursor-key", os.Getenv("TOOL_CATALOG_CURSOR_KEY"), "stable cursor HMAC key (raw or base64, at least 32 bytes)")
	flag.Parse()

	cursorKey, err := parseCursorKey(*keyValue)
	if err != nil {
		return err
	}
	tlsConfig, err := loadTLSConfig()
	if err != nil {
		return err
	}
	credentials := map[string]string{}
	if value := strings.TrimSpace(os.Getenv("AETHER_API_KEY")); value != "" {
		credentials["api_key"] = value
	}
	if value := strings.TrimSpace(envOr("AETHER_TENANT", os.Getenv("SCITRERA_TENANT"))); value != "" {
		credentials["tenant_id"] = value
	}
	client, err := sdk.NewServiceClient(sdk.ServiceOptions{
		ClientOptions: sdk.ClientOptions{
			ServerAddr: strings.TrimSpace(*address), TLS: tlsConfig, Credentials: credentials,
		},
		Implementation: strings.TrimSpace(*implementation), Specifier: strings.TrimSpace(*specifier),
	})
	if err != nil {
		return fmt.Errorf("construct Aether service client: %w", err)
	}
	defer func() {
		runErr = errors.Join(runErr, client.Close())
	}()

	authorizer, err := catalogrpc.NewAetherEntryAuthorizer(client)
	if err != nil {
		return err
	}
	authorityPolicy, authorityPolicyDigest, err := loadInvocationAuthorityPolicy(*authorityPolicyFile)
	if err != nil {
		return err
	}
	resolvedPolicyEpoch := strings.TrimSpace(*policyEpoch) + ":obo-" + authorityPolicyDigest
	pool := &workspacePool{
		kv: client.KV(), cursorKey: cursorKey, authorizer: authorizer,
		invocationAuthority: authorityPolicy,
		services:            make(map[string]*catalog.LiveService),
	}
	service, err := catalogrpc.NewService(pool, catalogrpc.ServiceOptions{
		MutationSourcePrefixes: splitCSV(*mutationSources), PolicyEpoch: resolvedPolicyEpoch,
	})
	if err != nil {
		return err
	}
	serviceTopic := fmt.Sprintf("sv::%s::%s", strings.TrimSpace(*implementation), strings.TrimSpace(*specifier))
	handler, err := catalogrpc.NewAetherHandler(service, client, serviceTopic)
	if err != nil {
		return err
	}
	client.OnMessage(sdk.AsyncMessageHandlerWithTimeout(handler.Handle, 30*time.Second))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect to Aether: %w", err)
	}
	slog.Info("tool catalog service connected", "topic", "sv::"+*implementation+"::"+*specifier)
	if err := client.Run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("run Aether client: %w", err)
	}
	return nil
}

type workspacePool struct {
	mu                  sync.Mutex
	kv                  *sdk.KV
	cursorKey           []byte
	authorizer          catalog.EntryAuthorizer
	invocationAuthority catalog.InvocationAuthorityResolver
	services            map[string]*catalog.LiveService
}

func (p *workspacePool) ResolveCatalogWorkspace(_ context.Context, workspace string) (*catalog.LiveService, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if service := p.services[workspace]; service != nil {
		return service, nil
	}
	backend, err := catalog.NewAetherLiveBackend(p.kv, catalog.AetherLiveBackendOptions{
		Scope: sdk.KVScopeWorkspace, Workspace: workspace,
	})
	if err != nil {
		return nil, err
	}
	service, err := catalog.NewLiveService(backend, catalog.LiveServiceOptions{
		Authorizer: p.authorizer, InvocationAuthority: p.invocationAuthority, CursorKey: p.cursorKey,
		MaxLease: 30 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	p.services[workspace] = service
	return service, nil
}

type invocationAuthorityPolicyFile struct {
	Profiles []catalog.InvocationAuthorityRule `json:"profiles"`
}

func loadInvocationAuthorityPolicy(path string) (catalog.InvocationAuthorityResolver, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, "none", nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read invocation authority policy: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var file invocationAuthorityPolicyFile
	if err := decoder.Decode(&file); err != nil {
		return nil, "", fmt.Errorf("decode invocation authority policy: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, "", fmt.Errorf("decode invocation authority policy: multiple JSON values")
		}
		return nil, "", fmt.Errorf("decode invocation authority policy: %w", err)
	}
	policy, err := catalog.NewStaticInvocationAuthorityPolicy(file.Profiles)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(raw)
	return policy, hex.EncodeToString(digest[:8]), nil
}

func loadTLSConfig() (*sdk.TLSConfig, error) {
	enabled, err := strconv.ParseBool(envOr("AETHER_TLS_ENABLED", "false"))
	if err != nil {
		return nil, fmt.Errorf("parse AETHER_TLS_ENABLED: %w", err)
	}
	if !enabled {
		return nil, nil
	}
	config, err := sdk.LoadTLSConfigFromFiles(
		os.Getenv("AETHER_TLS_CA_CERT"), os.Getenv("AETHER_TLS_CLIENT_CERT"), os.Getenv("AETHER_TLS_CLIENT_KEY"),
	)
	if err != nil {
		return nil, err
	}
	config.ServerName = strings.TrimSpace(os.Getenv("AETHER_TLS_SERVER_NAME"))
	return config, nil
}

func parseCursorKey(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("TOOL_CATALOG_CURSOR_KEY is required and must be stable across replicas")
	}
	for _, encoding := range []*base64.Encoding{base64.RawURLEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.StdEncoding} {
		if decoded, err := encoding.DecodeString(value); err == nil && len(decoded) >= sha256.Size {
			return deriveCursorKey(decoded), nil
		}
	}
	if len(value) < sha256.Size {
		return nil, fmt.Errorf("TOOL_CATALOG_CURSOR_KEY must contain at least 32 bytes raw or base64-decoded")
	}
	return deriveCursorKey([]byte(value)), nil
}

func deriveCursorKey(material []byte) []byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("scitrera/tool-catalog/cursor/v1\x00"))
	_, _ = digest.Write(material)
	return digest.Sum(nil)
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}

func hostname() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "default"
	}
	return name
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
