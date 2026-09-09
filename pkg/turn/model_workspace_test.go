// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package turn

import (
	"testing"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

func TestStickyModelIsScopedByWorkspace(t *testing.T) {
	runner := &Runner{threadModels: map[string]string{}}
	first := protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "shared"}
	second := protocol.MessageAddress{WorkspaceID: "project-b", ThreadID: "shared"}

	runner.setStickyModel(first, "model-a")
	runner.setStickyModel(second, "model-b")

	if got := runner.stickyModel(first); got != "model-a" {
		t.Fatalf("project-a model = %q", got)
	}
	if got := runner.stickyModel(second); got != "model-b" {
		t.Fatalf("project-b model = %q", got)
	}
}
