package turn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

func (r *Runner) resolveRecoveredExternalTool(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, streamer *turnStreamer, execution *turnExecution) error {
	if execution == nil || execution.record.Phase == turnjournal.PhaseChildResolved {
		return nil
	}
	record := execution.record
	if record.Phase != turnjournal.PhaseWaitingExternalChild || record.Tool == nil || record.Tool.External == nil {
		return fmt.Errorf("%w: execution lacks an admitted external child", ErrRecoveryUnsafe)
	}
	external := record.Tool.External
	envelope, err := subagent.ParseExecutionEnvelope(external.Descriptor)
	if err != nil {
		return fmt.Errorf("%w: invalid child execution descriptor: %v", ErrRecoveryUnsafe, err)
	}
	if err := validateRecoveredEnvelope(record, envelope); err != nil {
		return err
	}
	awaiter, ok := r.subagentTasks.(subagent.TaskAwaiter)
	if !ok || !awaiter.ExecutesExternally() {
		return fmt.Errorf("%w: external task awaiter is unavailable", ErrRecoveryUnsafe)
	}
	recovery, err := awaiter.Await(ctx, external.TaskID, nil)
	if err != nil {
		return fmt.Errorf("turn: await recovered child task %q: %w", external.TaskID, err)
	}
	result, err := r.recoveredSubagentToolResult(ctx, addr, envelope, recovery, external.TaskID, record.Tool.InvocationID)
	if err != nil {
		return err
	}
	part, err := result.ContentPart()
	if err != nil {
		return fmt.Errorf("turn: build recovered child tool result: %w", err)
	}
	parts := append([]protocol.ContentPart{part}, result.Parts...)
	if err := appendRecoveredToolResultOnce(ctx, session, addr, record.Tool.InvocationID, parts); err != nil {
		return err
	}
	assistant, err := journalMessageByRef(session.History(), record.Tool.Assistant)
	if err != nil {
		return err
	}
	for _, assistantPart := range assistant.Content {
		if _, err := streamer.appendUnstreamed(ctx, assistantPart); err != nil {
			return fmt.Errorf("turn: stream recovered assistant checkpoint: %w", err)
		}
	}
	for _, resultPart := range parts {
		if _, err := streamer.appendPart(ctx, resultPart); err != nil {
			return fmt.Errorf("turn: stream recovered child result: %w", err)
		}
	}
	if err := execution.toolConfirmed(ctx, session, record.Tool.InvocationID, int(record.Iteration)); err != nil {
		return err
	}
	return nil
}

func validateRecoveredEnvelope(record turnjournal.Record, envelope subagent.ExecutionEnvelope) error {
	external := record.Tool.External
	if envelope.ExecutionID != external.ExecutionID || envelope.WorkspaceID != record.WorkspaceID ||
		envelope.ParentSessionID != record.SessionID || envelope.ChildSessionID != external.ChildSessionID ||
		envelope.ParentTaskID != record.TaskID || envelope.ParentMessageID != record.Tool.Assistant.MessageID ||
		envelope.InvocationID != record.Tool.InvocationID || envelope.Background {
		return fmt.Errorf("%w: child execution descriptor does not match parent checkpoint", ErrRecoveryUnsafe)
	}
	return nil
}

func (r *Runner) recoveredSubagentToolResult(ctx context.Context, parent protocol.MessageAddress, envelope subagent.ExecutionEnvelope, recovery subagent.TaskRecovery, taskID, callID string) (tools.Result, error) {
	switch recovery {
	case subagent.TaskRecoveryCompleted:
		auth, _ := tools.MemoryAuthorityFrom(ctx)
		childAddr := parent
		childAddr.WorkspaceID = envelope.WorkspaceID
		childAddr.ThreadID = envelope.ChildSessionID
		childAddr.TaskID = taskID
		child, err := harness.NewSession(ctx, childAddr, r.store, r.registry, auth)
		if err != nil {
			return tools.Result{}, fmt.Errorf("turn: load recovered child session: %w", err)
		}
		assistant, err := envelope.ResolveResult(child.History())
		if err != nil {
			return tools.Result{}, fmt.Errorf("turn: resolve recovered child result: %w", err)
		}
		text := textOf(assistant)
		summary := summarizeSubagent(text)
		payload, err := json.Marshal(map[string]string{"result": text, "thread_id": envelope.ChildSessionID, "summary": summary})
		if err != nil {
			return tools.Result{}, err
		}
		result, err := tools.NewJSONResult(callID, tools.SubagentToolName, payload)
		if err != nil {
			return tools.Result{}, err
		}
		name := strings.TrimSpace(envelope.Policy.AgentName)
		if name == "" {
			name = strings.TrimSpace(envelope.Policy.AgentType)
		}
		if name == "" {
			name = "subagent"
		}
		if reference, refErr := protocol.NewSubagentPart(protocol.SubagentPart{
			ID: callID, Name: name, ThreadID: envelope.ChildSessionID,
			Status: protocol.SubagentCompleted, Summary: summary,
		}); refErr == nil {
			result.Parts = []protocol.ContentPart{reference}
		}
		return result, nil
	case subagent.TaskRecoveryFailed:
		return recoveredSubagentError(callID, fmt.Sprintf("sub-agent failed: subagent: external task %q failed", taskID))
	case subagent.TaskRecoveryCancelled:
		return recoveredSubagentError(callID, fmt.Sprintf("sub-agent failed: subagent: external task %q was cancelled", taskID))
	default:
		return tools.Result{}, fmt.Errorf("%w: child task %q returned non-terminal state %q", ErrRecoveryUnsafe, taskID, recovery)
	}
}

func recoveredSubagentError(callID, message string) (tools.Result, error) {
	payload, err := json.Marshal(map[string]string{"error": message})
	if err != nil {
		return tools.Result{}, err
	}
	return tools.Result{CallID: callID, Name: tools.SubagentToolName, Payload: payload, IsError: true}, nil
}

func appendRecoveredToolResultOnce(ctx context.Context, session *harness.Session, addr protocol.MessageAddress, callID string, parts []protocol.ContentPart) error {
	expected := protocol.ChatMessage{ID: callID + "-result", Role: protocol.RoleToolResult, Addr: addr, Content: parts}
	expectedRef, err := journalMessageRef(expected, addr.WorkspaceID, addr.ThreadID)
	if err != nil {
		return err
	}
	found := 0
	for _, message := range session.History() {
		if message.ID != expected.ID {
			continue
		}
		found++
		actual, err := journalMessageRef(message, message.Addr.WorkspaceID, message.Addr.ThreadID)
		if err != nil {
			return err
		}
		if actual != expectedRef {
			return errors.New("turn: existing recovered tool result does not match the admitted child outcome")
		}
	}
	if found > 1 {
		return errors.New("turn: recovered tool result is duplicated")
	}
	if found == 1 {
		return nil
	}
	if err := session.AppendToolResultParts(ctx, callID, parts...); err != nil {
		return fmt.Errorf("turn: persist recovered child result: %w", err)
	}
	return nil
}
