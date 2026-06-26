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
	CallID    string
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
}

type Result struct {
	CallID  string          `json:"call_id"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
	IsError bool            `json:"is_error"`
}

type Handler interface {
	Invoke(ctx context.Context, req Request) (Result, error)
}

type HandlerFunc func(ctx context.Context, req Request) (Result, error)

func (f HandlerFunc) Invoke(ctx context.Context, req Request) (Result, error) {
	return f(ctx, req)
}

func RequestFromEnvelope(env protocol.ToolInvokeEnvelope) Request {
	return Request{CallID: env.CallID, Name: env.Name, Arguments: protocol.ArgsToRaw(env.Args), Addr: env.Addr}
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
