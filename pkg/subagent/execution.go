package subagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	workspacepkg "github.com/scitrera/agent-harness-go/pkg/workspace"
)

const (
	// ExecutionEnvelopeSchema identifies the OSS child-execution descriptor
	// carried by a durable task backend. It is an execution-plane contract, not
	// part of the ecosystem session-message protocol.
	ExecutionEnvelopeSchema = "agent-harness.subagent.execution"
	// Revision 2 adds an exact workspace execution scope. Revision 1 remains
	// readable for already-admitted unbound tasks during rolling upgrades.
	ExecutionEnvelopeSchemaRevision       = 2
	executionEnvelopeLegacySchemaRevision = 1

	ExecutionBackendHistory        = "history"
	ExecutionBackendTaskCheckpoint = "task_checkpoint"

	ExecutionArtifactMessage        = "message"
	ExecutionArtifactAssistantAfter = "assistant_after_input"
	ExecutionArtifactCheckpoint     = "checkpoint"

	ExecutionAuthorityDurableTask = "durable_task"
	ExecutionUncertainInterrupt   = "interrupt_without_replay"

	maxExecutionEnvelopeBytes   = 64 << 10
	maxExecutionIdentifierRunes = 1024
)

// ExecutionArtifactRef names durable child input, output, or checkpoint state
// without embedding model-visible text in the task payload. History references
// are resolved within the explicit workspace/session pair. A checkpoint key is
// task-scoped by the durable backend.
type ExecutionArtifactRef struct {
	Backend     string `json:"backend"`
	Kind        string `json:"kind"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	RecordID    string `json:"record_id"`
	Digest      string `json:"digest,omitempty"`
}

// ExecutionPolicy is the non-secret, deterministic execution surface an
// assignee must reproduce. SnapshotDigest also covers catalog instructions, so
// an external executor can fail closed if its local catalog has changed without
// putting those instructions in Aether task metadata or payload.
type ExecutionPolicy struct {
	AgentName      string   `json:"agent_name,omitempty"`
	AgentType      string   `json:"agent_type,omitempty"`
	Model          string   `json:"model,omitempty"`
	MaxTurns       int      `json:"max_turns,omitempty"`
	AllowedTools   []string `json:"allowed_tools,omitempty"`
	DeniedTools    []string `json:"denied_tools,omitempty"`
	Skills         []string `json:"skills,omitempty"`
	MCPServers     []string `json:"mcp_servers,omitempty"`
	PermissionMode string   `json:"permission_mode,omitempty"`
	ExecPolicyHint string   `json:"exec_policy_hint,omitempty"`
	SnapshotDigest string   `json:"snapshot_digest"`
}

// ExecutionOwnership makes the task/executor boundary explicit. The first
// successful durable-task claim owns execution. One attempt is allowed; an
// uncertain outcome is interrupted and inspected rather than replayed.
type ExecutionOwnership struct {
	Authority       string `json:"authority"`
	ClaimRequired   bool   `json:"claim_required"`
	MaxAttempts     uint32 `json:"max_attempts"`
	UncertainPolicy string `json:"uncertain_policy"`
}

// ExecutionEnvelope is the immutable descriptor for one child invocation. It
// contains references and hashes, never the delegated task text, catalog
// instructions, or authority credentials. A backend may carry the encoded
// envelope as its task payload while retaining ordinary metadata for indexing.
type ExecutionEnvelope struct {
	SchemaRevision  uint32                       `json:"schema_revision"`
	Schema          string                       `json:"schema"`
	ExecutionID     string                       `json:"execution_id"`
	WorkspaceID     string                       `json:"workspace_id"`
	ParentSessionID string                       `json:"parent_session_id"`
	ChildSessionID  string                       `json:"child_session_id"`
	ParentTaskID    string                       `json:"parent_task_id,omitempty"`
	ParentMessageID string                       `json:"parent_message_id,omitempty"`
	InvocationID    string                       `json:"invocation_id,omitempty"`
	Depth           int                          `json:"depth"`
	Background      bool                         `json:"background,omitempty"`
	ExecutionScope  *workspacepkg.ExecutionScope `json:"execution_scope,omitempty"`
	Input           ExecutionArtifactRef         `json:"input"`
	Result          ExecutionArtifactRef         `json:"result"`
	Checkpoint      ExecutionArtifactRef         `json:"checkpoint"`
	Policy          ExecutionPolicy              `json:"policy"`
	Ownership       ExecutionOwnership           `json:"ownership"`
}

// NewExecutionEnvelope builds the immutable descriptor used by both the local
// in-process executor and optional durable task backends.
func NewExecutionEnvelope(req Request, workspaceID, childSessionID string, background bool) (ExecutionEnvelope, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	childSessionID = strings.TrimSpace(childSessionID)
	executionID := "ahx-v2-" + hashExecutionIdentity(
		workspaceID,
		req.Parent.ThreadID,
		childSessionID,
		req.Parent.TaskID,
		req.ParentMessageID,
		req.InvocationID,
		strconv.Itoa(req.Depth),
		strconv.FormatBool(background),
		digestExecutionScope(req.ExecutionScope),
	)
	policy := executionPolicy(req)
	envelope := ExecutionEnvelope{
		SchemaRevision:  ExecutionEnvelopeSchemaRevision,
		Schema:          ExecutionEnvelopeSchema,
		ExecutionID:     executionID,
		WorkspaceID:     workspaceID,
		ParentSessionID: strings.TrimSpace(req.Parent.ThreadID),
		ChildSessionID:  childSessionID,
		ParentTaskID:    strings.TrimSpace(req.Parent.TaskID),
		ParentMessageID: strings.TrimSpace(req.ParentMessageID),
		InvocationID:    strings.TrimSpace(req.InvocationID),
		Depth:           req.Depth,
		Background:      background,
		ExecutionScope:  cloneExecutionScope(req.ExecutionScope),
		Input: ExecutionArtifactRef{
			Backend:     ExecutionBackendHistory,
			Kind:        ExecutionArtifactMessage,
			WorkspaceID: workspaceID,
			SessionID:   childSessionID,
			RecordID:    executionID + "-input",
			Digest:      digestExecutionText(req.Task),
		},
		Result: ExecutionArtifactRef{
			Backend:     ExecutionBackendHistory,
			Kind:        ExecutionArtifactAssistantAfter,
			WorkspaceID: workspaceID,
			SessionID:   childSessionID,
			RecordID:    executionID,
		},
		Checkpoint: ExecutionArtifactRef{
			Backend:  ExecutionBackendTaskCheckpoint,
			Kind:     ExecutionArtifactCheckpoint,
			RecordID: "agent-harness/subagent/" + executionID + "/v2",
		},
		Policy: policy,
		Ownership: ExecutionOwnership{
			Authority:       ExecutionAuthorityDurableTask,
			ClaimRequired:   true,
			MaxAttempts:     1,
			UncertainPolicy: ExecutionUncertainInterrupt,
		},
	}
	if err := envelope.Validate(); err != nil {
		return ExecutionEnvelope{}, err
	}
	if err := envelope.VerifyPolicy(req); err != nil {
		return ExecutionEnvelope{}, err
	}
	return envelope, nil
}

// Validate checks identity, workspace isolation, artifact shape, and ownership
// invariants without resolving any referenced content.
func (e ExecutionEnvelope) Validate() error {
	switch {
	case e.Schema != ExecutionEnvelopeSchema:
		return fmt.Errorf("subagent: unsupported execution schema %q", e.Schema)
	case e.SchemaRevision != executionEnvelopeLegacySchemaRevision && e.SchemaRevision != ExecutionEnvelopeSchemaRevision:
		return fmt.Errorf("subagent: unsupported execution schema revision %d", e.SchemaRevision)
	case e.Depth < 0:
		return errors.New("subagent: execution depth must not be negative")
	}
	prefix := "ahx-v1-"
	identity := []string{
		e.WorkspaceID,
		e.ParentSessionID,
		e.ChildSessionID,
		e.ParentTaskID,
		e.ParentMessageID,
		e.InvocationID,
		strconv.Itoa(e.Depth),
		strconv.FormatBool(e.Background),
	}
	if e.SchemaRevision == ExecutionEnvelopeSchemaRevision {
		prefix = "ahx-v2-"
		identity = append(identity, digestExecutionScope(e.ExecutionScope))
	} else if e.ExecutionScope != nil {
		return errors.New("subagent: revision 1 execution cannot carry an execution scope")
	}
	wantExecutionID := prefix + hashExecutionIdentity(identity...)
	if e.ExecutionID != wantExecutionID {
		return errors.New("subagent: invalid execution id")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "execution id", value: e.ExecutionID},
		{name: "workspace id", value: e.WorkspaceID},
		{name: "parent session id", value: e.ParentSessionID},
		{name: "child session id", value: e.ChildSessionID},
	} {
		if err := validateExecutionIdentifier(field.name, field.value); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "parent task id", value: e.ParentTaskID},
		{name: "parent message id", value: e.ParentMessageID},
		{name: "invocation id", value: e.InvocationID},
	} {
		name, value := field.name, field.value
		if value != "" {
			if err := validateExecutionIdentifier(name, value); err != nil {
				return err
			}
		}
	}
	if err := validateHistoryArtifact(e.Input, ExecutionArtifactMessage, e.WorkspaceID, e.ChildSessionID, true); err != nil {
		return fmt.Errorf("subagent: invalid execution input: %w", err)
	}
	if e.Input.RecordID != e.ExecutionID+"-input" {
		return errors.New("subagent: execution input does not match execution id")
	}
	if err := validateHistoryArtifact(e.Result, ExecutionArtifactAssistantAfter, e.WorkspaceID, e.ChildSessionID, false); err != nil {
		return fmt.Errorf("subagent: invalid execution result: %w", err)
	}
	if e.Result.RecordID != e.ExecutionID {
		return errors.New("subagent: execution result does not match execution id")
	}
	if e.Checkpoint.Backend != ExecutionBackendTaskCheckpoint || e.Checkpoint.Kind != ExecutionArtifactCheckpoint {
		return errors.New("subagent: invalid execution checkpoint backend or kind")
	}
	if e.Checkpoint.RecordID != fmt.Sprintf("agent-harness/subagent/%s/v%d", e.ExecutionID, e.SchemaRevision) {
		return errors.New("subagent: execution checkpoint does not match execution id")
	}
	if err := validateExecutionIdentifier("checkpoint record id", e.Checkpoint.RecordID); err != nil {
		return err
	}
	if e.Checkpoint.WorkspaceID != "" || e.Checkpoint.SessionID != "" || e.Checkpoint.Digest != "" {
		return errors.New("subagent: task checkpoint must not carry history identity")
	}
	if e.ExecutionScope != nil {
		if err := e.ExecutionScope.Validate(); err != nil {
			return fmt.Errorf("subagent: invalid workspace execution scope: %w", err)
		}
		if e.ExecutionScope.Binding.WorkspaceID != e.WorkspaceID {
			return errors.New("subagent: execution scope workspace mismatch")
		}
	}
	if e.Policy.MaxTurns < 0 {
		return errors.New("subagent: execution max turns must not be negative")
	}
	if err := validateDigest(e.Policy.SnapshotDigest); err != nil {
		return fmt.Errorf("subagent: invalid execution policy digest: %w", err)
	}
	if !normalizedExecutionList(e.Policy.AllowedTools) || !normalizedExecutionList(e.Policy.DeniedTools) ||
		!normalizedExecutionList(e.Policy.Skills) || !normalizedExecutionList(e.Policy.MCPServers) {
		return errors.New("subagent: execution policy lists must be sorted, unique, and non-empty")
	}
	if e.Ownership.Authority != ExecutionAuthorityDurableTask || !e.Ownership.ClaimRequired ||
		e.Ownership.MaxAttempts != 1 || e.Ownership.UncertainPolicy != ExecutionUncertainInterrupt {
		return errors.New("subagent: unsupported execution ownership policy")
	}
	return nil
}

// VerifyPolicy proves that req is the same immutable policy snapshot described
// by the envelope. It includes catalog instructions in the digest even though
// those instructions are not serialized into the envelope.
func (e ExecutionEnvelope) VerifyPolicy(req Request) error {
	if err := e.Validate(); err != nil {
		return err
	}
	want := executionPolicy(req)
	gotJSON, err := json.Marshal(e.Policy)
	if err != nil {
		return err
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		return err
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		return errors.New("subagent: execution policy snapshot mismatch")
	}
	if !workspacepkg.ExecutionScopesEqual(e.ExecutionScope, req.ExecutionScope) {
		return errors.New("subagent: workspace execution scope snapshot mismatch")
	}
	return nil
}

// ResolveInput finds and verifies the referenced user message, returning the
// exact task text an executor should run. Duplicate record IDs fail closed.
func (e ExecutionEnvelope) ResolveInput(messages []protocol.ChatMessage) (protocol.ChatMessage, string, error) {
	if err := e.Validate(); err != nil {
		return protocol.ChatMessage{}, "", err
	}
	var found *protocol.ChatMessage
	for i := range messages {
		if messages[i].ID != e.Input.RecordID {
			continue
		}
		if found != nil {
			return protocol.ChatMessage{}, "", errors.New("subagent: execution input record is duplicated")
		}
		message := messages[i]
		found = &message
	}
	if found == nil {
		return protocol.ChatMessage{}, "", errors.New("subagent: execution input record was not found")
	}
	if found.Role != protocol.RoleUser || found.Addr.WorkspaceID != e.Input.WorkspaceID || found.Addr.ThreadID != e.Input.SessionID {
		return protocol.ChatMessage{}, "", errors.New("subagent: execution input identity mismatch")
	}
	if len(found.Content) != 1 {
		return protocol.ChatMessage{}, "", errors.New("subagent: execution input must contain exactly one text part")
	}
	text, ok := found.Content[0].AsText()
	if !ok {
		return protocol.ChatMessage{}, "", errors.New("subagent: execution input must contain exactly one text part")
	}
	if digestExecutionText(text.Text) != e.Input.Digest {
		return protocol.ChatMessage{}, "", errors.New("subagent: execution input digest mismatch")
	}
	return *found, text.Text, nil
}

// ResolveResult returns the first terminal assistant message after the exact
// referenced input. Assistant messages that contain tool calls are intermediate
// execution state and are skipped. A later execution input bounds the search so
// resuming the same child session cannot replace an earlier result.
func (e ExecutionEnvelope) ResolveResult(messages []protocol.ChatMessage) (protocol.ChatMessage, error) {
	if err := e.Validate(); err != nil {
		return protocol.ChatMessage{}, err
	}
	inputIndex := -1
	for i := range messages {
		if messages[i].ID != e.Input.RecordID {
			continue
		}
		if inputIndex >= 0 {
			return protocol.ChatMessage{}, errors.New("subagent: execution input record is duplicated")
		}
		inputIndex = i
	}
	if inputIndex < 0 {
		return protocol.ChatMessage{}, errors.New("subagent: execution input record was not found")
	}
	for i := inputIndex + 1; i < len(messages); i++ {
		message := messages[i]
		if message.Role == protocol.RoleUser && isExecutionInputID(message.ID) {
			break
		}
		if message.Role != protocol.RoleAssistant || message.Addr.WorkspaceID != e.Result.WorkspaceID || message.Addr.ThreadID != e.Result.SessionID {
			continue
		}
		terminal := true
		for _, part := range message.Content {
			if _, ok := part.AsToolCall(); ok {
				terminal = false
				break
			}
		}
		if terminal {
			return message, nil
		}
	}
	return protocol.ChatMessage{}, errors.New("subagent: execution result was not found")
}

// MarshalExecutionEnvelope validates and deterministically encodes a descriptor
// for a durable task payload.
func MarshalExecutionEnvelope(envelope ExecutionEnvelope) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

// ExecutionScopeDigest is a stable, credential-free correlation value for task
// metadata. The complete scope remains in the versioned payload.
func (e ExecutionEnvelope) ExecutionScopeDigest() string {
	return digestExecutionScope(e.ExecutionScope)
}

// ParseExecutionEnvelope strictly decodes one descriptor. Unknown fields and
// trailing JSON are rejected so executors cannot silently disagree on policy.
func ParseExecutionEnvelope(data []byte) (ExecutionEnvelope, error) {
	if len(data) == 0 || len(data) > maxExecutionEnvelopeBytes {
		return ExecutionEnvelope{}, errors.New("subagent: execution envelope size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope ExecutionEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return ExecutionEnvelope{}, fmt.Errorf("subagent: decode execution envelope: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ExecutionEnvelope{}, errors.New("subagent: execution envelope has trailing JSON")
	}
	if err := envelope.Validate(); err != nil {
		return ExecutionEnvelope{}, err
	}
	return envelope, nil
}

func executionPolicy(req Request) ExecutionPolicy {
	return ExecutionPolicy{
		AgentName:      strings.TrimSpace(string(req.AgentName)),
		AgentType:      strings.TrimSpace(string(req.AgentType)),
		Model:          strings.TrimSpace(req.Model),
		MaxTurns:       req.MaxTurns,
		AllowedTools:   normalizeExecutionList(req.AllowedTools),
		DeniedTools:    normalizeExecutionList(req.DeniedTools),
		Skills:         normalizeExecutionList(req.Skills),
		MCPServers:     normalizeExecutionList(req.MCPServers),
		PermissionMode: strings.TrimSpace(string(req.PermissionMode)),
		ExecPolicyHint: strings.TrimSpace(req.ExecPolicyHint),
		SnapshotDigest: digestExecutionPolicy(req),
	}
}

func digestExecutionPolicy(req Request) string {
	type privatePolicy struct {
		AgentName      string   `json:"agent_name,omitempty"`
		AgentType      string   `json:"agent_type,omitempty"`
		Instructions   string   `json:"instructions,omitempty"`
		Model          string   `json:"model,omitempty"`
		MaxTurns       int      `json:"max_turns,omitempty"`
		AllowedTools   []string `json:"allowed_tools,omitempty"`
		DeniedTools    []string `json:"denied_tools,omitempty"`
		Skills         []string `json:"skills,omitempty"`
		MCPServers     []string `json:"mcp_servers,omitempty"`
		PermissionMode string   `json:"permission_mode,omitempty"`
		ExecPolicyHint string   `json:"exec_policy_hint,omitempty"`
	}
	data, _ := json.Marshal(privatePolicy{
		AgentName: strings.TrimSpace(string(req.AgentName)), AgentType: strings.TrimSpace(string(req.AgentType)),
		Instructions: req.Instructions, Model: strings.TrimSpace(req.Model), MaxTurns: req.MaxTurns,
		AllowedTools: normalizeExecutionList(req.AllowedTools), DeniedTools: normalizeExecutionList(req.DeniedTools),
		Skills: normalizeExecutionList(req.Skills), MCPServers: normalizeExecutionList(req.MCPServers),
		PermissionMode: strings.TrimSpace(string(req.PermissionMode)), ExecPolicyHint: strings.TrimSpace(req.ExecPolicyHint),
	})
	return digestExecutionText(string(data))
}

func digestExecutionText(text string) string {
	sum := sha256.Sum256([]byte(text))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func digestExecutionScope(scope *workspacepkg.ExecutionScope) string {
	if scope == nil {
		return ""
	}
	data, err := json.Marshal(scope)
	if err != nil {
		return "invalid"
	}
	return digestExecutionText(string(data))
}

func cloneExecutionScope(scope *workspacepkg.ExecutionScope) *workspacepkg.ExecutionScope {
	if scope == nil {
		return nil
	}
	copyScope := *scope
	if scope.Binding.Extra != nil {
		copyScope.Binding.Extra = make(map[string]json.RawMessage, len(scope.Binding.Extra))
		for key, value := range scope.Binding.Extra {
			copyScope.Binding.Extra[key] = append(json.RawMessage(nil), value...)
		}
	}
	return &copyScope
}

func isExecutionInputID(id string) bool {
	return strings.HasSuffix(id, "-input") &&
		(strings.HasPrefix(id, "ahx-v1-") || strings.HasPrefix(id, "ahx-v2-"))
}

func hashExecutionIdentity(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func normalizeExecutionList(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizedExecutionList(values []string) bool {
	if len(values) == 0 {
		return true
	}
	for i, value := range values {
		if strings.TrimSpace(value) != value || value == "" {
			return false
		}
		if i > 0 && values[i-1] >= value {
			return false
		}
	}
	return true
}

func validateHistoryArtifact(ref ExecutionArtifactRef, kind, workspaceID, sessionID string, digest bool) error {
	if ref.Backend != ExecutionBackendHistory || ref.Kind != kind {
		return errors.New("unsupported history backend or kind")
	}
	if ref.WorkspaceID != workspaceID || ref.SessionID != sessionID {
		return errors.New("cross-workspace or cross-session reference")
	}
	if err := validateExecutionIdentifier("record id", ref.RecordID); err != nil {
		return err
	}
	if digest {
		return validateDigest(ref.Digest)
	}
	if ref.Digest != "" {
		return errors.New("result selector must not carry an unresolved digest")
	}
	return nil
}

func validateDigest(value string) error {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return errors.New("expected a sha256 digest")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(decoded) != sha256.Size {
		return errors.New("expected a lowercase sha256 digest")
	}
	if value != strings.ToLower(value) {
		return errors.New("expected a lowercase sha256 digest")
	}
	return nil
}

func validateExecutionIdentifier(name, value string) error {
	if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("subagent: %s is required and must be trimmed", name)
	}
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maxExecutionIdentifierRunes {
		return fmt.Errorf("subagent: %s is invalid or too long", name)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("subagent: %s contains control characters", name)
		}
	}
	return nil
}
