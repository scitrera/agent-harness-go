package acp

import (
	"encoding/json"
	"strings"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// This file carries hand-written Go structs for the subset of the ACP v1 wire
// protocol the channel speaks, transcribed from schema/v1/schema.json. Only the
// messages this input channel handles or emits are modeled; unknown fields are
// tolerated on decode and omitted on encode.

// protocolVersionV1 is the ACP v1 wire version (an integer per schema.json).
const protocolVersionV1 = 1

// ACP method names (agent-side handlers + the client-side session/update we
// emit). See schema/v1/meta.json.
const (
	methodInitialize    = "initialize"
	methodSessionNew    = "session/new"
	methodSessionPrompt = "session/prompt"
	methodSessionCancel = "session/cancel"
	methodSessionUpdate = "session/update"
)

// sessionUpdate discriminator values emitted by this channel.
const (
	updateAgentMessageChunk = "agent_message_chunk"
	updateToolCall          = "tool_call"
	updateToolCallUpdate    = "tool_call_update"
	updatePlan              = "plan"
)

// StopReason values (schema.json StopReason).
const (
	stopEndTurn   = "end_turn"
	stopCancelled = "cancelled"
	stopRefusal   = "refusal"
	stopMaxTokens = "max_tokens"
)

// ACP ToolKind values (schema.json ToolKind).
const (
	toolKindRead    = "read"
	toolKindEdit    = "edit"
	toolKindDelete  = "delete"
	toolKindMove    = "move"
	toolKindSearch  = "search"
	toolKindExecute = "execute"
	toolKindThink   = "think"
	toolKindFetch   = "fetch"
	toolKindOther   = "other"
)

// ACP ToolCallStatus values (schema.json ToolCallStatus).
const (
	toolStatusPending    = "pending"
	toolStatusInProgress = "in_progress"
	toolStatusCompleted  = "completed"
	toolStatusFailed     = "failed"
)

// ─── initialize ──────────────────────────────────────────────────────────

type initializeParams struct {
	ProtocolVersion    int             `json:"protocolVersion"`
	ClientCapabilities json.RawMessage `json:"clientCapabilities,omitempty"`
	ClientInfo         *implementation `json:"clientInfo,omitempty"`
}

type initializeResult struct {
	ProtocolVersion   int               `json:"protocolVersion"`
	AgentCapabilities agentCapabilities `json:"agentCapabilities"`
	AgentInfo         *implementation   `json:"agentInfo,omitempty"`
}

type implementation struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type agentCapabilities struct {
	LoadSession        bool               `json:"loadSession"`
	PromptCapabilities promptCapabilities `json:"promptCapabilities"`
}

type promptCapabilities struct {
	Image           bool `json:"image"`
	Audio           bool `json:"audio"`
	EmbeddedContext bool `json:"embeddedContext"`
}

// ─── session/new ─────────────────────────────────────────────────────────

type newSessionParams struct {
	Cwd                   string          `json:"cwd"`
	MCPServers            json.RawMessage `json:"mcpServers,omitempty"`
	AdditionalDirectories []string        `json:"additionalDirectories,omitempty"`
}

type newSessionResult struct {
	SessionID string `json:"sessionId"`
}

// ─── session/prompt ──────────────────────────────────────────────────────

type promptParams struct {
	SessionID string         `json:"sessionId"`
	Prompt    []contentBlock `json:"prompt"`
}

type promptResult struct {
	StopReason string `json:"stopReason"`
}

// ─── session/cancel ──────────────────────────────────────────────────────

type cancelParams struct {
	SessionID string `json:"sessionId"`
}

// ─── session/update (client-side notification we emit) ───────────────────

// sessionNotification is the session/update params: {sessionId, update} where
// update is one of the sessionUpdate variants below (marshaled into Update).
type sessionNotification struct {
	SessionID string          `json:"sessionId"`
	Update    json.RawMessage `json:"update"`
}

// contentBlock is the ACP ContentBlock union flattened to the fields this
// channel reads (prompts) and writes (chunks). Text and image are modeled;
// resource_link carries name/uri.
type contentBlock struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	Data     string          `json:"data,omitempty"`     // image: base64 payload
	MimeType string          `json:"mimeType,omitempty"` // image / resource_link
	URI      string          `json:"uri,omitempty"`
	Name     string          `json:"name,omitempty"`     // resource_link
	Resource json.RawMessage `json:"resource,omitempty"` // embedded resource
}

// agentMessageChunk is the flattened SessionUpdate + ContentChunk for an
// agent_message_chunk (streamed assistant text/image).
type agentMessageChunk struct {
	SessionUpdate string       `json:"sessionUpdate"`
	Content       contentBlock `json:"content"`
	MessageID     string       `json:"messageId,omitempty"`
}

// toolCall is the flattened SessionUpdate + ToolCall for a tool_call (a new
// tool invocation the model requested).
type toolCall struct {
	SessionUpdate string            `json:"sessionUpdate"`
	ToolCallID    string            `json:"toolCallId"`
	Title         string            `json:"title"`
	Kind          string            `json:"kind,omitempty"`
	Status        string            `json:"status,omitempty"`
	RawInput      json.RawMessage   `json:"rawInput,omitempty"`
	Content       []toolCallContent `json:"content,omitempty"`
}

// toolCallUpdate is the flattened SessionUpdate + ToolCallUpdate for a
// tool_call_update (progress/result of an existing tool call).
type toolCallUpdate struct {
	SessionUpdate string            `json:"sessionUpdate"`
	ToolCallID    string            `json:"toolCallId"`
	Status        string            `json:"status,omitempty"`
	Title         string            `json:"title,omitempty"`
	Content       []toolCallContent `json:"content,omitempty"`
}

// toolCallContent is a ToolCallContent of the standard "content" variant.
type toolCallContent struct {
	Type    string       `json:"type"` // "content"
	Content contentBlock `json:"content"`
}

// planUpdate is the flattened SessionUpdate + Plan for a plan notification.
type planUpdate struct {
	SessionUpdate string      `json:"sessionUpdate"`
	Entries       []planEntry `json:"entries"`
}

type planEntry struct {
	Content  string `json:"content"`
	Priority string `json:"priority"`
	Status   string `json:"status"`
}

// ─── content mapping (ACP ContentBlock ⇄ protocol.ContentPart) ───────────

// contentBlockToPart maps an inbound ACP prompt content block to a protocol
// content part. text/image/resource_link are supported; any other block with a
// text field degrades to a text part. Returns ok=false when nothing is
// representable.
func contentBlockToPart(b contentBlock) (protocol.ContentPart, bool) {
	switch b.Type {
	case "text":
		p, _ := protocol.NewTextPart(b.Text)
		return p, true
	case "image":
		img := protocol.ImagePart{Mime: b.MimeType, URI: b.URI}
		if b.Data != "" {
			mime := b.MimeType
			if mime == "" {
				mime = "application/octet-stream"
			}
			img.DataURI = "data:" + mime + ";base64," + b.Data
		}
		p, _ := protocol.NewImagePart(img)
		return p, true
	case "resource_link":
		p, _ := protocol.NewFilePart(protocol.FilePart{URI: b.URI, FileName: b.Name, Mime: b.MimeType})
		return p, true
	default:
		if b.Text != "" {
			p, _ := protocol.NewTextPart(b.Text)
			return p, true
		}
	}
	return protocol.ContentPart{}, false
}

// partToContentBlock maps an outbound protocol content part to an ACP content
// block. Only text and image are representable as chunk content.
func partToContentBlock(p protocol.ContentPart) (contentBlock, bool) {
	switch p.Type() {
	case protocol.ContentText:
		if body, ok := p.AsText(); ok {
			return contentBlock{Type: "text", Text: body.Text}, true
		}
	case protocol.ContentImage:
		if body, ok := p.AsImage(); ok {
			cb := contentBlock{Type: "image", MimeType: body.Mime, URI: body.URI}
			if body.DataURI != "" {
				cb.Data = stripDataURI(body.DataURI)
			}
			return cb, true
		}
	}
	return contentBlock{}, false
}

// stripDataURI returns the raw base64 payload of a "data:<mime>;base64,<data>"
// URI, or the input unchanged when it is not a base64 data URI.
func stripDataURI(s string) string {
	const marker = ";base64,"
	if i := strings.Index(s, marker); i >= 0 {
		return s[i+len(marker):]
	}
	return s
}

// toolKindFor maps a tool name to an ACP ToolKind so clients pick an icon/UI
// treatment. Unknown tools fall back to "other".
func toolKindFor(name string) string {
	switch strings.ToLower(name) {
	case "read_file", "read", "cat", "view":
		return toolKindRead
	case "edit_file", "write_file", "edit", "write", "apply_patch":
		return toolKindEdit
	case "ls", "glob", "grep", "search", "find":
		return toolKindSearch
	case "execute", "bash", "shell", "run", "exec":
		return toolKindExecute
	case "fetch", "web_search", "http", "curl":
		return toolKindFetch
	default:
		return toolKindOther
	}
}

// mapToolStatus maps a spec tool_call status to an ACP ToolCallStatus (which
// has no "cancelled"; cancelled maps to failed).
func mapToolStatus(s spec.ToolCallStatus) string {
	switch s {
	case spec.ToolCallRunning:
		return toolStatusInProgress
	case spec.ToolCallCompleted:
		return toolStatusCompleted
	case spec.ToolCallFailed, spec.ToolCallCancelled:
		return toolStatusFailed
	default:
		return toolStatusPending
	}
}

// mapPlanStatus maps a spec todo status to an ACP PlanEntryStatus (pending /
// in_progress / completed; cancelled degrades to pending).
func mapPlanStatus(s spec.TodoStatus) string {
	switch s {
	case spec.TodoInProgress:
		return "in_progress"
	case spec.TodoCompleted:
		return "completed"
	default:
		return "pending"
	}
}
