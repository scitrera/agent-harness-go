package subagent

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

type executionCatalog struct {
	definition Definition
}

func (c executionCatalog) List(context.Context) ([]Definition, error) {
	return []Definition{c.definition}, nil
}

func (c executionCatalog) Get(_ context.Context, typ AgentType) (Definition, error) {
	if c.definition.Type != typ {
		return Definition{}, ErrUnknownAgent
	}
	return c.definition, nil
}

func executionTestRequest() Request {
	binding := spec.NewExecutionBinding("project-a", "view-a", "worker-a", spec.ExecutionSiteWorker)
	binding.Revision = "0123456789abcdef"
	scope, _ := workspacepkg.NewExecutionScope(binding, workspacepkg.ExecutionViewPolicy{
		WriteAccess: workspacepkg.ViewWriteAccessReadOnly,
	})
	return Request{
		Task:  "inspect the private design and summarize it",
		Depth: 2,
		Parent: protocol.MessageAddress{
			WorkspaceID: "project-a",
			ThreadID:    "parent-session",
			TaskID:      "parent-task",
		},
		InvocationID:    "tool-call-7",
		ParentMessageID: "parent-message",
		GrantID:         "secret-grant",
		SubjectType:     "user",
		SubjectID:       "secret-user",
		ExecutionScope:  &scope,
		AgentName:       "reviewer",
		AgentType:       "review",
		Instructions:    "PRIVATE CATALOG INSTRUCTIONS",
		Model:           "model-a",
		MaxTurns:        4,
		AllowedTools:    []string{"read_file", "inspect", "read_file"},
		DeniedTools:     []string{"shell"},
		Skills:          []string{"code-review", "reasoning"},
		MCPServers:      []string{"project-tools"},
		PermissionMode:  PermissionModeAsk,
		ExecPolicyHint:  "read-only",
	}
}

func TestExecutionEnvelopeRoundTripOmitsPromptInstructionsAndCredentials(t *testing.T) {
	req := executionTestRequest()
	envelope, err := NewExecutionEnvelope(req, "project-a", "child-session", true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := MarshalExecutionEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{req.Task, req.Instructions, req.GrantID, req.SubjectID} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("execution envelope leaked %q: %s", secret, data)
		}
	}
	decoded, err := ParseExecutionEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ExecutionID != envelope.ExecutionID || decoded.Input.RecordID != envelope.ExecutionID+"-input" {
		t.Fatalf("decoded envelope = %+v", decoded)
	}
	if decoded.SchemaRevision != ExecutionEnvelopeSchemaRevision ||
		!workspacepkg.ExecutionScopesEqual(decoded.ExecutionScope, req.ExecutionScope) {
		t.Fatalf("decoded execution scope = %+v", decoded.ExecutionScope)
	}
	if strings.Join(decoded.Policy.AllowedTools, ",") != "inspect,read_file" {
		t.Fatalf("normalized allowed tools = %#v", decoded.Policy.AllowedTools)
	}
	if decoded.Ownership.Authority != ExecutionAuthorityDurableTask || decoded.Ownership.MaxAttempts != 1 || !decoded.Ownership.ClaimRequired {
		t.Fatalf("ownership = %+v", decoded.Ownership)
	}
}

func TestExecutionEnvelopeResolvesOnlyMatchingWorkspaceInput(t *testing.T) {
	req := executionTestRequest()
	envelope, err := NewExecutionEnvelope(req, "project-a", "child-session", false)
	if err != nil {
		t.Fatal(err)
	}
	part, _ := protocol.NewTextPart(req.Task)
	message := protocol.ChatMessage{
		ID: envelope.Input.RecordID, Role: protocol.RoleUser,
		Addr:    protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "child-session"},
		Content: []protocol.ContentPart{part},
	}
	got, task, err := envelope.ResolveInput([]protocol.ChatMessage{message})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != message.ID || task != req.Task {
		t.Fatalf("resolved message=%+v task=%q", got, task)
	}

	t.Run("digest mismatch", func(t *testing.T) {
		changed := message
		changed.Content = []protocol.ContentPart{mustExecutionTextPart(t, "different task")}
		if _, _, err := envelope.ResolveInput([]protocol.ChatMessage{changed}); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("workspace mismatch", func(t *testing.T) {
		changed := message
		changed.Addr.WorkspaceID = "project-b"
		if _, _, err := envelope.ResolveInput([]protocol.ChatMessage{changed}); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("duplicate id", func(t *testing.T) {
		if _, _, err := envelope.ResolveInput([]protocol.ChatMessage{message, message}); err == nil || !strings.Contains(err.Error(), "duplicated") {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestExecutionEnvelopeResolvesBoundedTerminalResult(t *testing.T) {
	envelope, err := NewExecutionEnvelope(executionTestRequest(), "project-a", "child-session", false)
	if err != nil {
		t.Fatal(err)
	}
	input := protocol.ChatMessage{
		ID: envelope.Input.RecordID, Role: protocol.RoleUser,
		Addr:    protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "child-session"},
		Content: []protocol.ContentPart{mustExecutionTextPart(t, executionTestRequest().Task)},
	}
	intermediateCall, _ := protocol.NewToolCallPart(protocol.ToolInvokeEnvelope{CallID: "call-1", Name: "inspect"})
	intermediate := protocol.ChatMessage{
		ID: "assistant-tool", Role: protocol.RoleAssistant, Addr: input.Addr,
		Content: []protocol.ContentPart{intermediateCall},
	}
	final := protocol.ChatMessage{
		ID: "assistant-final", Role: protocol.RoleAssistant, Addr: input.Addr,
		Content: []protocol.ContentPart{mustExecutionTextPart(t, "done")},
	}
	got, err := envelope.ResolveResult([]protocol.ChatMessage{input, intermediate, final})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != final.ID {
		t.Fatalf("result = %+v", got)
	}

	laterInput := protocol.ChatMessage{ID: "ahx-v1-" + strings.Repeat("a", 64) + "-input", Role: protocol.RoleUser, Addr: input.Addr}
	if _, err := envelope.ResolveResult([]protocol.ChatMessage{input, intermediate, laterInput, final}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("bounded result error = %v", err)
	}
}

func TestExecutionEnvelopePolicySnapshotFailsClosedOnCatalogDrift(t *testing.T) {
	req := executionTestRequest()
	first, err := NewExecutionEnvelope(req, "project-a", "child-session", false)
	if err != nil {
		t.Fatal(err)
	}
	reordered := executionTestRequest()
	reordered.AllowedTools = []string{"read_file", "inspect"}
	second, err := NewExecutionEnvelope(reordered, "project-a", "child-session", false)
	if err != nil {
		t.Fatal(err)
	}
	if first.Policy.SnapshotDigest != second.Policy.SnapshotDigest || first.ExecutionID != second.ExecutionID {
		t.Fatalf("semantically identical request drifted: first=%+v second=%+v", first, second)
	}
	changed := req
	changed.Instructions = "changed instructions"
	if err := first.VerifyPolicy(changed); err == nil || !strings.Contains(err.Error(), "snapshot mismatch") {
		t.Fatalf("error = %v", err)
	}
	tampered := first
	tampered.ExecutionScope = cloneExecutionScope(first.ExecutionScope)
	tampered.ExecutionScope.Policy.WriteAccess = workspacepkg.ViewWriteAccessReadWrite
	if err := tampered.Validate(); err == nil || !strings.Contains(err.Error(), "execution id") {
		t.Fatalf("scope tamper error = %v", err)
	}
}

func TestExecutionEnvelopeRejectsUnreleasedAlternateRevision(t *testing.T) {
	envelope, err := NewExecutionEnvelope(executionTestRequest(), "project-a", "child-session", false)
	if err != nil {
		t.Fatal(err)
	}
	envelope.SchemaRevision = 2
	if err := envelope.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported execution schema revision") {
		t.Fatalf("alternate revision error = %v", err)
	}
}

func TestReconstructExecutionRequestUsesCatalogAndTaskAuthority(t *testing.T) {
	req := executionTestRequest()
	envelope, err := NewExecutionEnvelope(req, "project-a", "child-session", true)
	if err != nil {
		t.Fatal(err)
	}
	reconstructed, err := ReconstructExecutionRequest(context.Background(), envelope, executionCatalog{definition: Definition{
		Name: req.AgentName, Type: req.AgentType, Description: "review code", Prompt: req.Instructions,
		Model: req.Model, MaxTurns: req.MaxTurns, AllowedTools: req.AllowedTools, DeniedTools: req.DeniedTools,
		Skills: req.Skills, MCPServers: req.MCPServers, PermissionMode: req.PermissionMode, ExecPolicyHint: req.ExecPolicyHint,
	}}, "task-grant", "user", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if reconstructed.Task != "" || reconstructed.Instructions != req.Instructions || reconstructed.Depth != req.Depth || !reconstructed.Background {
		t.Fatalf("reconstructed request = %+v", reconstructed)
	}
	if reconstructed.GrantID != "task-grant" || reconstructed.SubjectType != "user" || reconstructed.SubjectID != "alice" {
		t.Fatalf("reconstructed authority = %+v", reconstructed)
	}
	if !workspacepkg.ExecutionScopesEqual(reconstructed.ExecutionScope, req.ExecutionScope) {
		t.Fatalf("reconstructed scope = %+v", reconstructed.ExecutionScope)
	}

	drifted := executionCatalog{definition: Definition{
		Name: req.AgentName, Type: req.AgentType, Description: "review code", Prompt: "changed prompt",
		Model: req.Model, MaxTurns: req.MaxTurns, AllowedTools: req.AllowedTools, DeniedTools: req.DeniedTools,
		Skills: req.Skills, MCPServers: req.MCPServers, PermissionMode: req.PermissionMode, ExecPolicyHint: req.ExecPolicyHint,
	}}
	if _, err := ReconstructExecutionRequest(context.Background(), envelope, drifted, "task-grant", "user", "alice"); err == nil || !strings.Contains(err.Error(), "snapshot mismatch") {
		t.Fatalf("catalog drift error = %v", err)
	}
}

func TestReconstructExecutionRequestRejectsPrivateGenericPolicy(t *testing.T) {
	req := Request{
		Task: "inspect", Instructions: "private generic instructions",
		Parent: protocol.MessageAddress{WorkspaceID: "project-a", ThreadID: "parent-session"},
	}
	envelope, err := NewExecutionEnvelope(req, "project-a", "child-session", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReconstructExecutionRequest(context.Background(), envelope, nil, "", "", ""); err == nil || !strings.Contains(err.Error(), "snapshot mismatch") {
		t.Fatalf("private generic policy error = %v", err)
	}
	envelope.Depth = -1
	if err := envelope.Validate(); err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("negative depth error = %v", err)
	}
}

func TestParseExecutionEnvelopeRejectsUnknownTrailingAndCrossWorkspace(t *testing.T) {
	envelope, err := NewExecutionEnvelope(executionTestRequest(), "project-a", "child-session", false)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := MarshalExecutionEnvelope(envelope)

	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	object["future_field"] = true
	unknown, _ := json.Marshal(object)
	if _, err := ParseExecutionEnvelope(unknown); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown-field error = %v", err)
	}
	if _, err := ParseExecutionEnvelope(append(data, []byte(` {}`)...)); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing error = %v", err)
	}

	envelope.Input.WorkspaceID = "project-b"
	if err := envelope.Validate(); err == nil || !strings.Contains(err.Error(), "cross-workspace") {
		t.Fatalf("cross-workspace error = %v", err)
	}
}

func mustExecutionTextPart(t *testing.T, text string) protocol.ContentPart {
	t.Helper()
	part, err := protocol.NewTextPart(text)
	if err != nil {
		t.Fatal(err)
	}
	return part
}
