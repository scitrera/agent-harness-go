// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"context"
	"errors"
	"testing"

	aethersdk "github.com/scitrera/aether/sdk/go/aether"
	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"
)

type absentMemoryLayerTransport struct{}

func (absentMemoryLayerTransport) RoundTrip(context.Context, *memorylayersdk.Request) (*memorylayersdk.Response, error) {
	return nil, &aethersdk.ProxyTransportError{
		Kind:    "SIDECAR_UNAVAILABLE",
		Message: "no healthy sv::memorylayer instances available",
	}
}

func (absentMemoryLayerTransport) Close() error { return nil }

func TestAbsentMemoryLayerServiceClassificationIsNarrow(t *testing.T) {
	absent := errors.New("outer: " + (&aethersdk.ProxyTransportError{
		Kind:    "SIDECAR_UNAVAILABLE",
		Message: "no healthy sv::memorylayer instances available",
	}).Error())
	if isAbsentMemoryLayerService(absent) {
		t.Fatal("string-equivalent errors must not trigger fallback")
	}
	if !isAbsentMemoryLayerService(&aethersdk.ProxyTransportError{
		Kind:    "SIDECAR_UNAVAILABLE",
		Message: "no healthy sv::custom-memory instances available",
	}) {
		t.Fatal("healthy-instance discovery miss must trigger auto fallback")
	}
	if isAbsentMemoryLayerService(&aethersdk.ProxyTransportError{Kind: "ACL_DENIED", Message: "denied"}) {
		t.Fatal("authorization errors must never trigger fallback")
	}
	if isAbsentMemoryLayerService(&aethersdk.ProxyTransportError{Kind: "SIDECAR_UNAVAILABLE", Message: "connected terminator failed"}) {
		t.Fatal("a connected service outage must never be treated as absence")
	}
}

func TestOpenStoresAutoFallsBackOnlyWhenMemoryLayerIsOptionalAndAbsent(t *testing.T) {
	cfg := appConfig{
		workspaceRoot:        t.TempDir(),
		stateDir:             t.TempDir(),
		memorylayerMode:      memoryLayerModeAuto,
		memorylayerTarget:    defaultMemoryLayerTarget,
		memorylayerTransport: absentMemoryLayerTransport{},
		memorylayerWorkspace: "project",
	}
	st, err := openStores(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if st.remote || historyLabel(st) != "local files" {
		t.Fatalf("fallback store = remote %t, history %q", st.remote, historyLabel(st))
	}

	cfg.memorylayerRequired = true
	if _, err := openStores(context.Background(), cfg); err == nil || !isAbsentMemoryLayerService(err) {
		t.Fatalf("required MemoryLayer error = %v, want surfaced service absence", err)
	}
}
