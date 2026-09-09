// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package aether

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/scitrera/aether/sdk/go/aether"
)

func TestTaskAssignmentRouterDispatchesAllHandlersAndJoinsErrors(t *testing.T) {
	router := NewTaskAssignmentRouter()
	var calls []string
	router.Register(func(_ context.Context, assignment *sdk.TaskAssignment) error {
		calls = append(calls, "first:"+assignment.TaskType)
		return errors.New("first failed")
	})
	router.Register(func(_ context.Context, assignment *sdk.TaskAssignment) error {
		calls = append(calls, "second:"+assignment.TaskType)
		return nil
	})
	err := router.HandleAssignment(context.Background(), &sdk.TaskAssignment{TaskType: "goal"})
	if err == nil || len(calls) != 2 || calls[0] != "first:goal" || calls[1] != "second:goal" {
		t.Fatalf("calls=%v err=%v", calls, err)
	}
}
