package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

var (
	ErrInvalidTool     = errors.New("tools: invalid tool")
	ErrToolExists      = errors.New("tools: already registered")
	ErrUnknownTool     = errors.New("tools: unknown tool")
	ErrInvalidArgument = errors.New("tools: invalid argument")
)

type Request struct {
	CallID string
	// MessageID is the id of the assistant message that carries this tool call.
	// Tools that create cross-thread back-refs (spawn_subagent) record it as the
	// parent MESSAGE id; the tool CallID alone doesn't identify the enclosing
	// message. Empty when the caller doesn't supply it.
	MessageID string
	Name      string
	Arguments json.RawMessage
	Addr      protocol.MessageAddress
	// Authority is the turn's OBO grant, injected by the session so tools (e.g.
	// memory_search) act on behalf of the turn's user with a fresh grant.
	Authority MemoryAuthority
	// Approved marks a call the user just authorized via the approval flow, so
	// the registry bypasses the policy gate for this single invocation. (Durable
	// session/always grants are recorded separately in the policy.)
	Approved bool
	// ToolRef is the exact provider-qualified catalog entry selected for a
	// dynamically surfaced tool. It is nil for the standalone static-registry
	// path and older bare-name callers.
	ToolRef *protocol.ToolReference
}

type Result struct {
	CallID   string          `json:"call_id"`
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload"`
	IsError  bool            `json:"is_error"`
	Metadata ResultMetadata  `json:"metadata,omitempty"`
	// Parts holds optional extra content parts the tool loop records alongside
	// the tool_result part (e.g. a subagent reference part). Runtime-only (not a
	// wire field); nil = today's single tool_result behavior.
	Parts []protocol.ContentPart `json:"-"`
	// InjectUserParts holds content parts the tool loop records as a NEW
	// user-role message (distinct from Parts, which co-locate on the tool-result
	// message). Used by inject_image to put a locally-produced image into the
	// model's OWN context: an image must ride on a user-role message (a tool-role
	// message is text-only), so it cannot go on the tool_result. Runtime-only;
	// nil = no-op. An image part here also triggers mid-loop vision escalation.
	InjectUserParts []protocol.ContentPart `json:"-"`
}

type Handler interface {
	Invoke(ctx context.Context, req Request) (Result, error)
}

type HandlerFunc func(ctx context.Context, req Request) (Result, error)

func (f HandlerFunc) Invoke(ctx context.Context, req Request) (Result, error) {
	return f(ctx, req)
}

func RequestFromEnvelope(env protocol.ToolInvokeEnvelope) Request {
	var ref *protocol.ToolReference
	if env.ToolRef != nil {
		cloned := *env.ToolRef
		ref = &cloned
	}
	return Request{CallID: env.CallID, Name: env.Name, Arguments: protocol.ArgsToRaw(env.Args), Addr: env.Addr, ToolRef: ref}
}

func NewJSONResult(callID string, name string, payload json.RawMessage) (Result, error) {
	if callID == "" || name == "" {
		return Result{}, fmt.Errorf("%w: call id and name required", ErrInvalidTool)
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	return Result{CallID: callID, Name: name, Payload: payload}, nil
}

func (r Result) ContentPart() (protocol.ContentPart, error) {
	return protocol.NewToolResultPart(r.CallID, r.Name, r.Payload, r.IsError)
}

type messageIDKey struct{}

// WithMessageID carries the enclosing assistant message id on ctx so the
// Request builders (session + dynamic invocation) can stamp Request.MessageID.
// Mirrors how MemoryAuthority flows on ctx. Empty id is a no-op.
func WithMessageID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, messageIDKey{}, id)
}

// MessageIDFrom returns the assistant message id carried by WithMessageID.
func MessageIDFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(messageIDKey{}).(string)
	return id, ok && id != ""
}
