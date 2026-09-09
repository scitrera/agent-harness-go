// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

const ExecutionViewPolicyMetaKey = "scitrera.execution_view_policy"

type ViewWriteAccess string

const (
	ViewWriteAccessReadOnly  ViewWriteAccess = "read_only"
	ViewWriteAccessReadWrite ViewWriteAccess = "read_write"
)

// ExecutionViewPolicy is the authority carried with an exact view selector.
// WriteAccess is always explicit on newly created scopes. The mutable/dirty
// flags retain the admission constraints used by durable worker views.
type ExecutionViewPolicy struct {
	WriteAccess      ViewWriteAccess `json:"write_access"`
	AllowMutableView bool            `json:"allow_mutable_view,omitempty"`
	AllowDirtyView   bool            `json:"allow_dirty_view,omitempty"`
}

func (p ExecutionViewPolicy) Validate() error {
	switch p.WriteAccess {
	case ViewWriteAccessReadOnly, ViewWriteAccessReadWrite:
		return nil
	default:
		return fmt.Errorf("workspace execution policy: unsupported write access %q", p.WriteAccess)
	}
}

// ExecutionScope binds filesystem authority and its write ceiling. A binding is
// a resource selector, not a grant; transport and host authorizers still decide
// whether the current principal may use it.
type ExecutionScope struct {
	Binding spec.ExecutionBinding `json:"binding"`
	Policy  ExecutionViewPolicy   `json:"policy"`
}

func NewExecutionScope(binding spec.ExecutionBinding, policy ExecutionViewPolicy) (ExecutionScope, error) {
	scope := ExecutionScope{Binding: binding, Policy: policy}
	if err := scope.Validate(); err != nil {
		return ExecutionScope{}, err
	}
	return scope, nil
}

func (s ExecutionScope) Validate() error {
	if err := s.Binding.Validate(); err != nil {
		return fmt.Errorf("workspace execution scope: %w", err)
	}
	return s.Policy.Validate()
}

func (s ExecutionScope) ReadOnly() ExecutionScope {
	s.Policy.WriteAccess = ViewWriteAccessReadOnly
	return s
}

func ExecutionScopesEqual(left, right *ExecutionScope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return reflect.DeepEqual(*left, *right)
}

// IsMonotonicScopeNarrowing permits only an exact scope or a read-write to
// read-only reduction on the same binding. Every binding change or write-access
// expansion requires a composition-provided authorizer.
func IsMonotonicScopeNarrowing(parent, child *ExecutionScope) bool {
	if ExecutionScopesEqual(parent, child) {
		return true
	}
	if parent == nil || child == nil || !reflect.DeepEqual(parent.Binding, child.Binding) {
		return false
	}
	want := *parent
	want.Policy.WriteAccess = ViewWriteAccessReadOnly
	return parent.Policy.WriteAccess == ViewWriteAccessReadWrite && reflect.DeepEqual(want, *child)
}

type executionScopeKey struct{}

func WithExecutionScope(ctx context.Context, scope ExecutionScope) context.Context {
	if err := scope.Validate(); err != nil {
		return ctx
	}
	return context.WithValue(ctx, executionScopeKey{}, scope)
}

func ExecutionScopeFrom(ctx context.Context) (ExecutionScope, bool) {
	scope, ok := ctx.Value(executionScopeKey{}).(ExecutionScope)
	return scope, ok
}

// PutExecutionScope stores the portable binding plus Sahara's private policy
// extension on a message.
func PutExecutionScope(message *spec.ChatMessage, scope ExecutionScope) error {
	if message == nil {
		return errors.New("workspace execution scope: message is required")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := spec.PutExecutionBinding(message, scope.Binding); err != nil {
		return err
	}
	data, err := json.Marshal(scope.Policy)
	if err != nil {
		return fmt.Errorf("workspace execution scope: encode policy: %w", err)
	}
	if message.Meta == nil {
		message.Meta = map[string]json.RawMessage{}
	}
	message.Meta[ExecutionViewPolicyMetaKey] = data
	return nil
}

// GetExecutionScope requires a policy whenever a binding is present. The scope
// contract is unreleased, so missing authority is rejected rather than mapped
// through a transitional default.
func GetExecutionScope(message spec.ChatMessage) (*ExecutionScope, error) {
	binding, err := spec.GetExecutionBinding(message)
	if err != nil {
		return nil, err
	}
	raw := message.Meta[ExecutionViewPolicyMetaKey]
	if binding == nil {
		if len(raw) != 0 {
			return nil, errors.New("workspace execution scope: policy has no binding")
		}
		return nil, nil
	}
	if len(raw) == 0 {
		return nil, errors.New("workspace execution scope: binding has no policy")
	}
	var policy ExecutionViewPolicy
	if err := decodeExecutionViewPolicy(raw, &policy); err != nil {
		return nil, err
	}
	scope := &ExecutionScope{Binding: *binding, Policy: policy}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return scope, nil
}

func EncodeExecutionViewPolicy(policy ExecutionViewPolicy) (json.RawMessage, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(policy)
	return json.RawMessage(data), err
}

func DecodeExecutionViewPolicy(data []byte) (ExecutionViewPolicy, error) {
	var policy ExecutionViewPolicy
	if err := decodeExecutionViewPolicy(data, &policy); err != nil {
		return ExecutionViewPolicy{}, err
	}
	return policy, nil
}

func decodeExecutionViewPolicy(data []byte, policy *ExecutionViewPolicy) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(policy); err != nil {
		return fmt.Errorf("workspace execution scope: decode policy: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("workspace execution scope: policy has trailing JSON")
	}
	return policy.Validate()
}
