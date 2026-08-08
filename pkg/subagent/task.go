package subagent

import (
	"context"
	"errors"
)

// ErrTaskOutcomeUncertain means child execution ended locally but the durable
// task backend could not confirm its terminal transition. Callers must not
// automatically replay the child: its transcript or tool side effects may have
// committed even though the task still appears non-terminal.
var ErrTaskOutcomeUncertain = errors.New("subagent: execution task outcome is uncertain")

// TaskAdmission is the non-secret identity and policy projection used to admit
// one subagent invocation to an optional durable task backend. Task is
// deliberately absent: prompts and OBO credentials do not belong in task
// metadata. InvocationID should be stable for a retried tool invocation (the
// spawn tool uses its tool-call ID), allowing a backend to deduplicate admission.
type TaskAdmission struct {
	WorkspaceID     string
	ParentSessionID string
	ChildSessionID  string
	ParentTaskID    string
	ParentMessageID string
	InvocationID    string
	Name            string
	Kind            string
	Model           string
	Depth           int
	Background      bool
	GrantID         string
	SubjectType     string
	SubjectID       string
}

// TaskOutcome is the requested durable terminal state for one execution task.
type TaskOutcome string

const (
	TaskOutcomeCompleted TaskOutcome = "completed"
	TaskOutcomeFailed    TaskOutcome = "failed"
	TaskOutcomeCancelled TaskOutcome = "cancelled"
)

// TaskRecovery is the authoritative task state projected during owner restart.
// Interrupted means the backend found non-terminal work whose in-process
// execution was lost and durably terminated it; it must not be replayed.
type TaskRecovery string

const (
	TaskRecoveryCompleted   TaskRecovery = "completed"
	TaskRecoveryFailed      TaskRecovery = "failed"
	TaskRecoveryCancelled   TaskRecovery = "cancelled"
	TaskRecoveryInterrupted TaskRecovery = "interrupted"
)

// TaskBackend makes a durable task system the execution authority while the
// SubagentRegistry remains a parent-session snapshot projection. Implementations
// must make Admit idempotent for a non-empty InvocationID. Finish must return nil
// only when the requested terminal state is confirmed; after an ambiguous
// mutation it should query, not blindly repeat the mutation. Recover must return
// a terminal projection, terminating orphaned non-terminal work before returning
// TaskRecoveryInterrupted.
type TaskBackend interface {
	Admit(ctx context.Context, admission TaskAdmission) (taskID string, err error)
	Start(ctx context.Context, taskID string) error
	Finish(ctx context.Context, taskID string, outcome TaskOutcome, reason string) error
	Recover(ctx context.Context, taskID string) (TaskRecovery, error)
}
