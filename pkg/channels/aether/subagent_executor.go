package aether

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/scitrera/aether/api/proto"
	sdk "github.com/scitrera/aether/sdk/go/aether"

	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const defaultSubagentExecutorConcurrency = 4

// SubagentExecutorConfig configures the opt-in consumer for externally
// assigned subagent tasks. Catalog may be nil when only generic (non-catalog)
// subagents are accepted.
type SubagentExecutorConfig struct {
	Runner  subagent.AssignedRunner
	Catalog subagent.Catalog
	// Resolver replaces the static Runner/Catalog pair for multi-workspace
	// hosts. It is invoked with the validated logical workspace and the typed
	// assignment authority installed on ctx.
	Resolver             subagent.AssignedExecutionResolver
	ExecutionScopeBinder AssignedExecutionScopeBinder
	MaxConcurrency       int
	Timeout              time.Duration
}

// AssignedExecutionScopeBinder validates an exact inherited view before task
// claim, decorates execution with the corresponding tool delegate, and returns
// a release hook. A bound envelope without this seam is rejected fail-closed.
type AssignedExecutionScopeBinder interface {
	BindAssignedExecutionScope(
		ctx context.Context,
		taskID string,
		scope workspacepkg.ExecutionScope,
	) (context.Context, func(), error)
}

// AssignedSubagentExecutor claims and executes targeted child tasks delivered
// to one Aether agent. It is deliberately at-most-once within a process and
// never replays a task already observed as running after a delivery gap.
type AssignedSubagentExecutor struct {
	backend     *SubagentTaskBackend
	runner      subagent.AssignedRunner
	catalog     subagent.Catalog
	resolver    subagent.AssignedExecutionResolver
	scopeBinder AssignedExecutionScopeBinder
	assignedTo  string
	slots       chan struct{}

	mu       sync.Mutex
	inflight map[string]struct{}
}

// NewAssignedSubagentExecutor constructs a handler over the narrow task API so
// distributions can reuse it without constructing an OSS Channel.
func NewAssignedSubagentExecutor(tasks TaskOperations, assignedTo string, cfg SubagentExecutorConfig) (*AssignedSubagentExecutor, error) {
	if tasks == nil {
		return nil, errors.New("aether: assigned subagent task operations are required")
	}
	if cfg.Resolver != nil && (cfg.Runner != nil || cfg.Catalog != nil) {
		return nil, errors.New("aether: assigned subagent resolver cannot be combined with a static runner or catalog")
	}
	if cfg.Resolver == nil && cfg.Runner == nil {
		return nil, errors.New("aether: assigned subagent runner is required")
	}
	assignedTo = strings.TrimSpace(assignedTo)
	if assignedTo == "" {
		return nil, errors.New("aether: assigned subagent executor identity is required")
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = defaultSubagentExecutorConcurrency
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTaskTimeout
	}
	return &AssignedSubagentExecutor{
		backend:     &SubagentTaskBackend{tasks: tasks, timeout: cfg.Timeout},
		runner:      cfg.Runner,
		catalog:     cfg.Catalog,
		resolver:    cfg.Resolver,
		scopeBinder: cfg.ExecutionScopeBinder,
		assignedTo:  assignedTo,
		slots:       make(chan struct{}, cfg.MaxConcurrency),
		inflight:    make(map[string]struct{}),
	}, nil
}

// EnableSubagentExecutor registers the executor on this channel. Call it before
// Start so no assignment can arrive before the handler is installed.
func (c *Channel) EnableSubagentExecutor(cfg SubagentExecutorConfig) (*AssignedSubagentExecutor, error) {
	executor, err := NewAssignedSubagentExecutor(c.client, c.client.Topic(), cfg)
	if err != nil {
		return nil, err
	}
	c.assignmentRouter.Register(executor.HandleAssignment)
	return executor, nil
}

// HandleAssignment consumes only agent-harness.subagent.v1 tasks. Concurrent
// duplicate delivery is coalesced; terminal redelivery is a no-op.
func (e *AssignedSubagentExecutor) HandleAssignment(ctx context.Context, assignment *sdk.TaskAssignment) error {
	if assignment == nil || assignment.TaskType != subagentTaskType {
		return nil
	}
	taskID := strings.TrimSpace(assignment.TaskID)
	if taskID == "" {
		return errors.New("aether: assigned subagent task id is required")
	}
	if strings.TrimSpace(assignment.AssignedTo) != e.assignedTo {
		// A misrouted delivery is not ours to mutate. In particular, do not fail a
		// task that is authoritatively assigned to another connected worker.
		return fmt.Errorf("aether: subagent task assigned to %q, executor is %q", assignment.AssignedTo, e.assignedTo)
	}
	if !e.markInflight(taskID) {
		return nil
	}
	defer e.clearInflight(taskID)

	select {
	case e.slots <- struct{}{}:
		defer func() { <-e.slots }()
	case <-ctx.Done():
		return e.finish(ctx, taskID, subagent.TaskOutcomeCancelled, ctx.Err())
	}

	recovery, err := e.backend.inspect(ctx, taskID)
	if err != nil {
		return fmt.Errorf("aether: inspect assigned subagent task: %w", err)
	}
	switch recovery {
	case subagent.TaskRecoveryCompleted, subagent.TaskRecoveryFailed, subagent.TaskRecoveryCancelled:
		return nil
	case subagent.TaskRecoveryRunning:
		return e.reject(ctx, taskID, errors.New("aether: assigned subagent task was redelivered while running; refusing uncertain replay"))
	case subagent.TaskRecoveryAdmitted:
	default:
		return e.reject(ctx, taskID, fmt.Errorf("aether: unsupported assigned subagent task state %q", recovery))
	}

	envelope, err := subagent.ParseExecutionEnvelope(assignment.Payload)
	if err != nil {
		return e.reject(ctx, taskID, fmt.Errorf("aether: invalid assigned subagent execution: %w", err))
	}
	if err := validateAssignedExecutionMetadata(assignment.Metadata, envelope); err != nil {
		return e.reject(ctx, taskID, err)
	}
	grantID, subjectType, subjectID, err := assignedAuthority(assignment.Authorization)
	if err != nil {
		return e.reject(ctx, taskID, err)
	}
	authCtx := tools.WithMemoryAuthority(ctx, tools.MemoryAuthority{
		GrantID: grantID, SubjectType: subjectType, SubjectID: subjectID,
	})
	runner, catalog, err := e.resolveExecution(authCtx, envelope.WorkspaceID)
	if err != nil {
		return e.reject(ctx, taskID, err)
	}
	req, err := subagent.ReconstructExecutionRequest(authCtx, envelope, catalog, grantID, subjectType, subjectID)
	if err != nil {
		return e.reject(ctx, taskID, err)
	}
	releaseScope := func() {}
	if envelope.ExecutionScope != nil {
		if e.scopeBinder == nil {
			return e.reject(ctx, taskID, errors.New("aether: assigned subagent execution scope binder is not configured"))
		}
		authCtx, releaseScope, err = e.scopeBinder.BindAssignedExecutionScope(authCtx, taskID, *envelope.ExecutionScope)
		if err != nil {
			return e.reject(ctx, taskID, fmt.Errorf("aether: bind assigned subagent execution scope: %w", err))
		}
		if releaseScope == nil {
			releaseScope = func() {}
		}
	}
	defer releaseScope()

	if err := e.backend.Start(ctx, taskID); err != nil {
		return fmt.Errorf("aether: claim assigned subagent task: %w", err)
	}
	_, runErr := runner.ExecuteAssignedSubagent(authCtx, taskID, envelope, req)
	if runErr != nil {
		outcome := subagent.TaskOutcomeFailed
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			outcome = subagent.TaskOutcomeCancelled
		}
		return e.finish(ctx, taskID, outcome, runErr)
	}
	return e.finish(ctx, taskID, subagent.TaskOutcomeCompleted, nil)
}

func (e *AssignedSubagentExecutor) resolveExecution(ctx context.Context, workspaceID string) (subagent.AssignedRunner, subagent.Catalog, error) {
	if e.resolver == nil {
		return e.runner, e.catalog, nil
	}
	resources, err := e.resolver.ResolveAssignedExecution(ctx, workspaceID)
	if err != nil {
		return nil, nil, fmt.Errorf("aether: resolve assigned subagent workspace %q: %w", workspaceID, err)
	}
	if resources.Runner == nil {
		return nil, nil, fmt.Errorf("aether: resolve assigned subagent workspace %q: resolver returned no runner", workspaceID)
	}
	return resources.Runner, resources.Catalog, nil
}

func (e *AssignedSubagentExecutor) markInflight(taskID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.inflight[taskID]; ok {
		return false
	}
	e.inflight[taskID] = struct{}{}
	return true
}

func (e *AssignedSubagentExecutor) clearInflight(taskID string) {
	e.mu.Lock()
	delete(e.inflight, taskID)
	e.mu.Unlock()
}

func (e *AssignedSubagentExecutor) reject(ctx context.Context, taskID string, cause error) error {
	return e.finish(ctx, taskID, subagent.TaskOutcomeFailed, cause)
}

func (e *AssignedSubagentExecutor) finish(ctx context.Context, taskID string, outcome subagent.TaskOutcome, cause error) error {
	// Assignment delivery may be cancelled during disconnect/shutdown. The task
	// transition still gets its own bounded SDK timeout so it is not abandoned
	// merely because model execution ended with cancellation.
	finishErr := e.backend.Finish(context.WithoutCancel(ctx), taskID, outcome, errorString(cause))
	if finishErr != nil {
		finishErr = fmt.Errorf("aether: finish assigned subagent task: %w", finishErr)
	}
	return errors.Join(cause, finishErr)
}

func validateAssignedExecutionMetadata(metadata map[string]string, envelope subagent.ExecutionEnvelope) error {
	agentName := envelope.Policy.AgentName
	if agentName == "" {
		agentName = envelope.Policy.AgentType
	}
	if agentName == "" {
		agentName = "subagent"
	}
	want := map[string]string{
		"scitrera.component":         taskMetadataComponent,
		"scitrera.kind":              taskMetadataKind,
		"scitrera.execution_mode":    "external",
		"scitrera.logical_workspace": envelope.WorkspaceID,
		"scitrera.parent_session_id": envelope.ParentSessionID,
		"scitrera.child_session_id":  envelope.ChildSessionID,
		"scitrera.parent_task_id":    envelope.ParentTaskID,
		"scitrera.parent_message_id": envelope.ParentMessageID,
		"scitrera.invocation_id":     envelope.InvocationID,
		"scitrera.depth":             strconv.Itoa(envelope.Depth),
		"scitrera.background":        strconv.FormatBool(envelope.Background),
		"scitrera.execution_schema":  envelope.Schema,
		"scitrera.execution_id":      envelope.ExecutionID,
		"scitrera.policy_digest":     envelope.Policy.SnapshotDigest,
		"scitrera.agent_name":        agentName,
		"scitrera.agent_kind":        envelope.Policy.AgentType,
		"scitrera.model":             envelope.Policy.Model,
	}
	if scope := envelope.ExecutionScope; scope != nil {
		want["scitrera.execution_scope_digest"] = envelope.ExecutionScopeDigest()
		want["scitrera.view_id"] = scope.Binding.ViewID
		want["scitrera.tool_host_id"] = scope.Binding.ToolHostID
		want["scitrera.execution_site"] = string(scope.Binding.ExecutionSite)
		want["scitrera.view_write_access"] = string(scope.Policy.WriteAccess)
		want["scitrera.view_revision"] = scope.Binding.Revision
	}
	for key, expected := range want {
		if actual := metadata[key]; actual != expected {
			return fmt.Errorf("aether: assigned subagent metadata %q mismatch", key)
		}
	}
	return nil
}

func assignedAuthority(auth *pb.AuthorizationContext) (grantID, subjectType, subjectID string, err error) {
	if auth == nil {
		return "", "", "", nil
	}
	mode := strings.TrimSpace(auth.GetAuthorityMode())
	grantID = strings.TrimSpace(auth.GetGrantId())
	subjectType = strings.TrimSpace(auth.GetSubject().GetPrincipalType())
	subjectID = strings.TrimSpace(auth.GetSubject().GetPrincipalId())
	switch mode {
	case "direct":
		if grantID != "" || subjectType != "" || subjectID != "" {
			return "", "", "", errors.New("aether: direct assigned subagent authority must not carry a grant or subject")
		}
		return "", "", "", nil
	case "on_behalf_of":
		if grantID == "" || subjectType == "" || subjectID == "" {
			return "", "", "", errors.New("aether: assigned subagent authority requires grant, subject type, and subject id")
		}
		return grantID, subjectType, subjectID, nil
	default:
		return "", "", "", fmt.Errorf("aether: unsupported assigned subagent authority mode %q", mode)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
