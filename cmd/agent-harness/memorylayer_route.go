package main

import (
	"errors"
	"strings"

	aethersdk "github.com/scitrera/aether/sdk/go/aether"
	"github.com/scitrera/agent-harness-go/pkg/memorylayer"
	mlaether "github.com/scitrera/memorylayer/memorylayer-sdk-go/aether"
)

// withMemoryLayerAetherTransport binds MemoryLayer to the already-authenticated
// Aether connection owned by the current mode. An explicit HTTP route never
// reaches this path and therefore remains the highest-priority override.
func withMemoryLayerAetherTransport(cfg appConfig, proxy mlaether.ProxyClient) appConfig {
	if cfg.memorylayerMode != memoryLayerModeAuto && cfg.memorylayerMode != memoryLayerModeAether {
		return cfg
	}
	cfg.memorylayerTransport = mlaether.NewTransport(proxy, mlaether.WithTarget(cfg.memorylayerTarget))
	return cfg
}

func memoryLayerClientConfig(cfg appConfig) memorylayer.Config {
	return memorylayer.Config{
		BaseURL:   cfg.memorylayerURL,
		Transport: cfg.memorylayerTransport,
		APIKey:    cfg.memorylayerKey,
		Workspace: cfg.memorylayerWorkspace,
	}
}

func memoryLayerLocation(cfg appConfig) string {
	if cfg.memorylayerMode == memoryLayerModeHTTP {
		return cfg.memorylayerURL
	}
	return cfg.memorylayerTarget + " via aether"
}

// isAbsentMemoryLayerService is deliberately narrow. Auto mode may fall back
// only when wildcard discovery proves no healthy MemoryLayer service exists.
// ACL failures, timeouts, malformed responses, and server errors are surfaced:
// treating those as absence would hide configuration and production outages.
func isAbsentMemoryLayerService(err error) bool {
	var proxyErr *aethersdk.ProxyTransportError
	if !errors.As(err, &proxyErr) || proxyErr.Kind != "SIDECAR_UNAVAILABLE" {
		return false
	}
	message := strings.ToLower(proxyErr.Message)
	return strings.HasPrefix(message, "no healthy sv::") && strings.HasSuffix(message, " instances available")
}
