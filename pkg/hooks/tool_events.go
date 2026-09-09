// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package hooks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

func (r *Runtime) EmitToolEvent(ctx context.Context, event tools.ToolEvent) error {
	if r == nil || recursionGuarded(ctx) {
		return nil
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal tool hook event: %w", err)
	}
	_, err = r.Dispatch(ctx, Invocation{Event: EventToolLifecycle, Input: raw})
	if err != nil {
		return fmt.Errorf("dispatch tool hook event: %w", err)
	}
	return nil
}
