package aether

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/tools"
)

// LookupTaskInfo returns authoritative task state while preserving the
// not-found/unauthorized ambiguity as found=false. Transport failures remain
// errors so startup recovery does not mistake an outage for absence.
func (c *Channel) LookupTaskInfo(ctx context.Context, taskID string) (*sdk.TaskInfo, bool, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, false, errors.New("aether: task id is required")
	}
	response, err := c.client.GetTask(ctx, taskID, defaultTaskTimeout)
	if err != nil {
		return nil, false, err
	}
	if response == nil {
		return nil, false, errors.New("aether: task query returned no response")
	}
	if !response.Success || response.Task == nil {
		return nil, false, nil
	}
	return response.Task, true, nil
}

func (c *Channel) GetTaskInfo(ctx context.Context, taskID string) (*sdk.TaskInfo, error) {
	info, found, err := c.LookupTaskInfo(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("aether: get task %q: task not found or not authorized", taskID)
	}
	return info, nil
}

func (c *Channel) ClaimTask(ctx context.Context, taskID string) error {
	return c.confirmLifecycleOperation(ctx, taskID, pb.TaskStatus_TASK_STATUS_RUNNING.String(), func() (*sdk.TaskOperationResponse, error) {
		return c.client.ClaimTask(ctx, taskID, defaultTaskTimeout)
	})
}

func (c *Channel) CompleteTask(ctx context.Context, taskID string) error {
	return c.confirmLifecycleOperation(ctx, taskID, pb.TaskStatus_TASK_STATUS_COMPLETED.String(), func() (*sdk.TaskOperationResponse, error) {
		return c.client.CompleteTask(ctx, taskID, defaultTaskTimeout)
	})
}

func (c *Channel) FailTask(ctx context.Context, taskID, reason string) error {
	return c.confirmLifecycleOperation(ctx, taskID, pb.TaskStatus_TASK_STATUS_FAILED.String(), func() (*sdk.TaskOperationResponse, error) {
		return c.client.FailTask(ctx, taskID, boundedTaskReason(reason), defaultTaskTimeout)
	})
}

func (c *Channel) confirmLifecycleOperation(ctx context.Context, taskID, desired string, operation func() (*sdk.TaskOperationResponse, error)) error {
	if strings.TrimSpace(taskID) == "" {
		return nil
	}
	response, opErr := operation()
	if opErr == nil && response != nil && response.Success {
		return nil
	}
	info, found, queryErr := c.LookupTaskInfo(context.WithoutCancel(ctx), taskID)
	if queryErr == nil && found && info.Status == desired {
		return nil
	}
	if opErr != nil {
		return errors.Join(fmt.Errorf("aether: task operation: %w", opErr), queryErr)
	}
	message := "task operation returned no response"
	if response != nil {
		message = strings.TrimSpace(response.Error)
		if message == "" {
			message = strings.TrimSpace(response.Message)
		}
	}
	if queryErr != nil {
		return errors.Join(errors.New("aether: "+message), queryErr)
	}
	status := ""
	if found {
		status = info.Status
	}
	return fmt.Errorf("aether: %s (confirmed status %q, wanted %q)", message, status, desired)
}

// TaskInfoAuthority validates and projects task-scoped authority for recovery.
func TaskInfoAuthority(info *sdk.TaskInfo) (tools.MemoryAuthority, error) {
	if info == nil {
		return tools.MemoryAuthority{}, errors.New("aether: task info is required")
	}
	switch strings.TrimSpace(info.AuthorityMode) {
	case "", "direct":
		return tools.MemoryAuthority{}, nil
	case "on_behalf_of":
		authority := tools.MemoryAuthority{
			GrantID:     strings.TrimSpace(info.AuthorityGrantID),
			SubjectType: normalizeAuthoritySubjectType(info.SubjectType), SubjectID: strings.TrimSpace(info.SubjectID),
		}
		if authority.GrantID == "" || authority.SubjectType == "" || authority.SubjectID == "" {
			return tools.MemoryAuthority{}, errors.New("aether: task has incomplete on-behalf-of authority")
		}
		return authority, nil
	default:
		return tools.MemoryAuthority{}, fmt.Errorf("aether: task has unsupported authority mode %q", info.AuthorityMode)
	}
}

// normalizeAuthoritySubjectType projects Aether's authenticated principal
// vocabulary onto the lowercase form used by MemoryLayer and the harness.
// Aether's model-facing surfaces use title-case values such as "User", while
// ACL/grant and ecosystem authority records use lowercase canonical values.
func normalizeAuthoritySubjectType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "workflowengine":
		return "workflow_engine"
	case "metricsbridge":
		return "metrics_bridge"
	default:
		return value
	}
}
