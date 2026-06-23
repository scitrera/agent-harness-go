// Package protocol is a thin compatibility shim over the authoritative
// github.com/scitrera/ecosystem-messaging-spec/go module. The harness consumes the spec types
// through these aliases so there is a single wire model across the platform;
// historically this package carried its own (drifted) copy.
package protocol

import (
	"encoding/json"

	spec "github.com/scitrera/ecosystem-messaging-spec/go"
)

// Authoritative spec types, re-exported.
type (
	ChatMessage        = spec.ChatMessage
	MessageAddress     = spec.MessageAddress
	MessageRef         = spec.MessageRef
	ContentPart        = spec.ContentPart
	ToolInvokeEnvelope = spec.ToolInvokeEnvelope
	Role               = spec.Role
	ContentPartType    = spec.PartType
	ToolCallPartBody   = spec.ToolCallPartBody
	ToolResultPartBody = spec.ToolResultPartBody
	ToolError          = spec.ToolError
	ImagePart          = spec.ImagePart
	FilePart           = spec.FilePart
)

// NewImagePart builds an image content part (keeps the harness's historical
// (ContentPart, error) signature; the spec constructor cannot fail).
func NewImagePart(body ImagePart) (ContentPart, error) {
	return spec.NewImagePart(body), nil
}

// NewFilePart builds a file content part.
func NewFilePart(body FilePart) (ContentPart, error) {
	return spec.NewFilePart(body), nil
}

// ErrInvalidContentPart is re-exported from the spec.
var ErrInvalidContentPart = spec.ErrInvalidContentPart

const (
	RoleSystem    = spec.RoleSystem
	RoleUser      = spec.RoleUser
	RoleAssistant = spec.RoleAssistant
	RoleTool      = spec.RoleTool
	// RoleToolResult is a harness-internal marker for persisted tool-result
	// messages. It is NOT a spec role (the spec models tool results as
	// tool_result content parts). Retained to preserve existing turn behavior;
	// migrating the turn engine off it is tracked separately.
	RoleToolResult Role = "tool_result"
)

const (
	ContentText       = spec.PartText
	ContentReasoning  = spec.PartReasoning
	ContentImage      = spec.PartImage
	ContentFile       = spec.PartFile
	ContentCitation   = spec.PartCitation
	ContentDynamic    = spec.PartDynamic
	ContentToolCall   = spec.PartToolCall
	ContentToolResult = spec.PartToolResult
	ContentSubagent   = spec.PartSubagent
	ContentFeedback   = spec.PartFeedback
	ContentControl    = spec.PartControl
)

// NewTextPart builds a text content part. It keeps the harness's historical
// (ContentPart, error) signature; the spec constructor cannot fail.
func NewTextPart(text string) (ContentPart, error) {
	return spec.NewTextPart(text), nil
}

// NewToolResultPart builds a spec tool_result part; the payload is carried in
// the spec "output" field.
func NewToolResultPart(callID, name string, output json.RawMessage, isError bool) (ContentPart, error) {
	return spec.NewToolResultPart(spec.ToolResultPartBody{
		CallID:  callID,
		Name:    name,
		Output:  output,
		IsError: isError,
	}), nil
}

// NewToolCallPart builds a spec tool_call part from an invoke envelope.
func NewToolCallPart(env ToolInvokeEnvelope) (ContentPart, error) {
	return spec.NewToolCallPart(spec.ToolCallPartBody{
		ID:   env.CallID,
		Name: env.Name,
		Args: env.Args,
		Meta: env.Meta,
	}), nil
}

// ToolCallFromPart extracts an invoke envelope from a tool_call content part.
// The spec tool_call part uses "id" for the call id; it is mapped to
// envelope.CallID.
func ToolCallFromPart(p ContentPart) (ToolInvokeEnvelope, bool) {
	body, ok := p.AsToolCall()
	if !ok {
		return ToolInvokeEnvelope{}, false
	}
	return ToolInvokeEnvelope{
		SchemaVersion: spec.ToolsSchemaVersion,
		CallID:        body.ID,
		Name:          body.Name,
		Args:          body.Args,
		Meta:          body.Meta,
	}, true
}

// ArgsToRaw encodes a spec args map as a single JSON object (the harness's
// internal argument representation).
func ArgsToRaw(args map[string]json.RawMessage) json.RawMessage {
	if len(args) == 0 {
		return json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(args)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

// RawToArgs decodes a JSON object into a spec args map.
func RawToArgs(raw json.RawMessage) map[string]json.RawMessage {
	if len(raw) == 0 {
		return map[string]json.RawMessage{}
	}
	var args map[string]json.RawMessage
	if err := json.Unmarshal(raw, &args); err != nil || args == nil {
		return map[string]json.RawMessage{}
	}
	return args
}
