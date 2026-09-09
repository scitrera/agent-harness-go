// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"errors"
	"fmt"
	"time"

	sdk "github.com/scitrera/aether/sdk/go/aether"
)

// KVOperations is the atomic subset of the Aether SDK KV client required by
// the session event adapter. The interface keeps the adapter unit-testable
// without a gateway while retaining the SDK's exact scope and timeout options.
type KVOperations interface {
	GetSync(ctx context.Context, opts sdk.KVGetOptions) (*sdk.KVResponse, error)
	SetNXSync(ctx context.Context, key string, value []byte, scope sdk.KVScope, userID, workspace string, ttl, timeout time.Duration) (bool, error)
	CompareAndSetSync(ctx context.Context, key string, expected, value []byte, scope sdk.KVScope, userID, workspace string, ttl, timeout time.Duration) (bool, error)
}

// KVBlobStore adapts Aether's workspace-exclusive KV namespace to the neutral
// compare-and-swap blob contract used by distributed session and lifecycle
// stores. The Aether agent implementation+specifier provides host identity;
// logical workspace and session identities remain encoded in each store key.
type KVBlobStore struct {
	kv        KVOperations
	workspace string
	timeout   time.Duration
}

func NewKVBlobStore(kv KVOperations, workspace string, timeout time.Duration) (*KVBlobStore, error) {
	if kv == nil {
		return nil, errors.New("aether: KV blob store requires KV operations")
	}
	if workspace == "" {
		return nil, errors.New("aether: KV blob store requires a transport workspace")
	}
	if timeout <= 0 {
		timeout = sdk.DefaultKVTimeout
	}
	return &KVBlobStore{kv: kv, workspace: workspace, timeout: timeout}, nil
}

// BlobStore returns a store bound to this agent's workspace-exclusive KV
// namespace. It is safe to construct before Start; operations require the
// channel's SDK connection to be running.
func (c *Channel) BlobStore(timeout time.Duration) (*KVBlobStore, error) {
	return NewKVBlobStore(c.client.KV(), c.workspace, timeout)
}

// SessionBlobStore is retained for source compatibility with the first event
// log integration. New callers should use BlobStore because lifecycle and other
// distributed state share the same neutral contract.
func (c *Channel) SessionBlobStore(timeout time.Duration) (*KVBlobStore, error) {
	return c.BlobStore(timeout)
}

func (s *KVBlobStore) Read(ctx context.Context, key string) ([]byte, bool, error) {
	if key == "" {
		return nil, false, errors.New("aether: KV blob key is required")
	}
	response, err := s.kv.GetSync(ctx, sdk.KVGetOptions{
		Key:       key,
		Scope:     sdk.KVScopeWorkspaceExclusive,
		Workspace: s.workspace,
		Timeout:   s.timeout,
	})
	if err != nil {
		return nil, false, fmt.Errorf("aether: KV blob read: %w", err)
	}
	if response == nil || !response.Success {
		return nil, false, errors.New("aether: KV blob read was rejected")
	}
	// Aether represents an ordinary GET miss as success with a nil value.
	// Session event states are always non-empty JSON, so nil is unambiguous.
	if response.Value == nil {
		return nil, false, nil
	}
	return append([]byte(nil), response.Value...), true, nil
}

func (s *KVBlobStore) Create(ctx context.Context, key string, value []byte) (bool, error) {
	if key == "" || len(value) == 0 {
		return false, errors.New("aether: KV blob create requires a key and non-empty value")
	}
	created, err := s.kv.SetNXSync(
		ctx, key, value, sdk.KVScopeWorkspaceExclusive, "", s.workspace, 0, s.timeout,
	)
	if err != nil {
		return false, fmt.Errorf("aether: KV blob create: %w", err)
	}
	return created, nil
}

func (s *KVBlobStore) CompareAndSwap(ctx context.Context, key string, expected, value []byte) (bool, error) {
	if key == "" || len(expected) == 0 || len(value) == 0 {
		return false, errors.New("aether: KV blob compare-and-swap requires a key and non-empty values")
	}
	swapped, err := s.kv.CompareAndSetSync(
		ctx, key, expected, value, sdk.KVScopeWorkspaceExclusive, "", s.workspace, 0, s.timeout,
	)
	if err != nil {
		return false, fmt.Errorf("aether: KV blob compare-and-swap: %w", err)
	}
	return swapped, nil
}
