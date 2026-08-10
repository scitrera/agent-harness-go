package subagent

import (
	"context"
	"errors"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// AssignedRunner consumes an already-admitted execution. The caller owns the
// durable task claim and terminal transition; implementations must not admit a
// second task for the same envelope.
type AssignedRunner interface {
	ExecuteAssignedSubagent(ctx context.Context, taskID string, envelope ExecutionEnvelope, req Request) (Result, error)
}

// AssignedExecutionResources are resolved after the executor has validated an
// envelope's workspace identity and typed task authority, but before it claims
// the task. A multi-workspace host uses this seam to select a runner and catalog
// built exclusively for that logical workspace.
type AssignedExecutionResources struct {
	Runner  AssignedRunner
	Catalog Catalog
}

// AssignedExecutionResolver selects workspace-scoped execution resources for
// an externally assigned child. Implementations must not fall back across
// workspace boundaries: a resolution failure prevents model/tool execution and
// is recorded as a terminal rejection before the task is claimed.
type AssignedExecutionResolver interface {
	ResolveAssignedExecution(ctx context.Context, workspaceID string) (AssignedExecutionResources, error)
}

// AssignedExecutionResolverFunc adapts a function to AssignedExecutionResolver.
type AssignedExecutionResolverFunc func(context.Context, string) (AssignedExecutionResources, error)

func (f AssignedExecutionResolverFunc) ResolveAssignedExecution(ctx context.Context, workspaceID string) (AssignedExecutionResources, error) {
	return f(ctx, workspaceID)
}

// ReconstructExecutionRequest rebuilds the private Request policy an assigned
// executor must use. Named agent policies are loaded from the executor's local
// catalog and verified against the envelope digest; generic policies can be
// reconstructed only when they carried no private instructions. The delegated
// task text is intentionally left empty and must be resolved from Input by the
// execution runner after the durable task has been claimed.
func ReconstructExecutionRequest(
	ctx context.Context,
	envelope ExecutionEnvelope,
	catalog Catalog,
	grantID, subjectType, subjectID string,
) (Request, error) {
	if err := envelope.Validate(); err != nil {
		return Request{}, err
	}
	req := Request{
		Depth: envelope.Depth,
		Parent: protocol.MessageAddress{
			WorkspaceID: envelope.WorkspaceID,
			ThreadID:    envelope.ParentSessionID,
			TaskID:      envelope.ParentTaskID,
		},
		InvocationID:    envelope.InvocationID,
		ParentMessageID: envelope.ParentMessageID,
		GrantID:         grantID,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		Background:      envelope.Background,
		ExecutionScope:  cloneExecutionScope(envelope.ExecutionScope),
	}

	if envelope.Policy.AgentType == "" {
		req.AgentName = AgentName(envelope.Policy.AgentName)
		req.Model = envelope.Policy.Model
		req.MaxTurns = envelope.Policy.MaxTurns
		req.AllowedTools = append([]string(nil), envelope.Policy.AllowedTools...)
		req.DeniedTools = append([]string(nil), envelope.Policy.DeniedTools...)
		req.Skills = append([]string(nil), envelope.Policy.Skills...)
		req.MCPServers = append([]string(nil), envelope.Policy.MCPServers...)
		req.PermissionMode = PermissionMode(envelope.Policy.PermissionMode)
		req.ExecPolicyHint = envelope.Policy.ExecPolicyHint
	} else {
		if catalog == nil {
			return Request{}, errors.New("subagent: assigned execution requires a shared agent catalog")
		}
		def, err := catalog.Get(ctx, AgentType(envelope.Policy.AgentType))
		if err != nil {
			return Request{}, fmt.Errorf("subagent: resolve assigned agent policy: %w", err)
		}
		req.AgentName = def.Name
		req.AgentType = def.Type
		req.Instructions = def.Prompt
		req.Model = def.Model
		req.MaxTurns = def.MaxTurns
		req.AllowedTools = append([]string(nil), def.AllowedTools...)
		req.DeniedTools = append([]string(nil), def.DeniedTools...)
		req.Skills = append([]string(nil), def.Skills...)
		req.MCPServers = append([]string(nil), def.MCPServers...)
		req.PermissionMode = def.PermissionMode
		req.ExecPolicyHint = def.ExecPolicyHint
		req.Background = envelope.Background
	}
	if err := envelope.VerifyPolicy(req); err != nil {
		return Request{}, fmt.Errorf("subagent: verify assigned execution policy: %w", err)
	}
	return req, nil
}
