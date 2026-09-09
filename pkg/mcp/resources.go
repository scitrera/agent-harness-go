// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

const codeMethodNotFound = -32601

// ResourceClient is the stable seam callers use for MCP resource discovery and reads.
type ResourceClient interface {
	ListResources(ctx context.Context, server string, req ListResourcesRequest) (ListResourcesResult, error)
	ReadResource(ctx context.Context, server string, req ReadResourceRequest) (ReadResourceResult, error)
	ListResourceTemplates(ctx context.Context, server string, req ListResourceTemplatesRequest) (ListResourceTemplatesResult, error)
}

type ListResourcesRequest struct {
	Cursor string `json:"cursor,omitempty"`
}

type ListResourcesResult struct {
	Resources  []ResourceDescriptor `json:"resources"`
	NextCursor string               `json:"nextCursor,omitempty"`
	Raw        json.RawMessage      `json:"-"`
}

type ReadResourceRequest struct {
	URI string `json:"uri"`
}

type ReadResourceResult struct {
	Contents []ResourceContent `json:"contents"`
	Raw      json.RawMessage   `json:"-"`
}

type ListResourceTemplatesRequest struct {
	Cursor string `json:"cursor,omitempty"`
}

type ListResourceTemplatesResult struct {
	ResourceTemplates []ResourceTemplateDescriptor `json:"resourceTemplates"`
	NextCursor        string                       `json:"nextCursor,omitempty"`
	Raw               json.RawMessage              `json:"-"`
}

type ResourceDescriptor struct {
	URI         string          `json:"uri"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	MimeType    string          `json:"mimeType,omitempty"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
}

type ResourceTemplateDescriptor struct {
	URITemplate string          `json:"uriTemplate"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	MimeType    string          `json:"mimeType,omitempty"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
}

type ResourceContent struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     string `json:"blob,omitempty"`
}

func (m *Manager) ListResources(ctx context.Context, server string, req ListResourcesRequest) (ListResourcesResult, error) {
	state, err := m.stateFor(ctx, server)
	if err != nil {
		return ListResourcesResult{}, err
	}
	if err := state.ensureInitialized(ctx); err != nil {
		return ListResourcesResult{}, err
	}
	params, err := paginatedParams(req.Cursor)
	if err != nil {
		return ListResourcesResult{}, err
	}
	raw, err := state.request(ctx, "resources/list", params)
	if err != nil {
		return ListResourcesResult{}, err
	}
	var decoded ListResourcesResult
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return ListResourcesResult{}, fmt.Errorf("decode mcp resources/list: %w", err)
	}
	if decoded.Resources == nil {
		decoded.Resources = []ResourceDescriptor{}
	}
	decoded.Raw = append(json.RawMessage(nil), raw...)
	return decoded, nil
}

func (m *Manager) ReadResource(ctx context.Context, server string, req ReadResourceRequest) (ReadResourceResult, error) {
	if req.URI == "" {
		return ReadResourceResult{}, fmt.Errorf("%w: uri required", ErrInvalidResource)
	}
	state, err := m.stateFor(ctx, server)
	if err != nil {
		return ReadResourceResult{}, err
	}
	if err := state.ensureInitialized(ctx); err != nil {
		return ReadResourceResult{}, err
	}
	params, err := json.Marshal(req)
	if err != nil {
		return ReadResourceResult{}, fmt.Errorf("marshal resources/read params: %w", err)
	}
	raw, err := state.request(ctx, "resources/read", params)
	if err != nil {
		return ReadResourceResult{}, err
	}
	var decoded ReadResourceResult
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return ReadResourceResult{}, fmt.Errorf("decode mcp resources/read: %w", err)
	}
	if decoded.Contents == nil {
		decoded.Contents = []ResourceContent{}
	}
	decoded.Raw = append(json.RawMessage(nil), raw...)
	return decoded, nil
}

func (m *Manager) ListResourceTemplates(ctx context.Context, server string, req ListResourceTemplatesRequest) (ListResourceTemplatesResult, error) {
	state, err := m.stateFor(ctx, server)
	if err != nil {
		return ListResourceTemplatesResult{}, err
	}
	if err := state.ensureInitialized(ctx); err != nil {
		return ListResourceTemplatesResult{}, err
	}
	params, err := paginatedParams(req.Cursor)
	if err != nil {
		return ListResourceTemplatesResult{}, err
	}
	raw, err := state.request(ctx, "resources/templates/list", params)
	if err != nil {
		return ListResourceTemplatesResult{}, err
	}
	var decoded ListResourceTemplatesResult
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return ListResourceTemplatesResult{}, fmt.Errorf("decode mcp resources/templates/list: %w", err)
	}
	if decoded.ResourceTemplates == nil {
		decoded.ResourceTemplates = []ResourceTemplateDescriptor{}
	}
	decoded.Raw = append(json.RawMessage(nil), raw...)
	return decoded, nil
}

func paginatedParams(cursor string) (json.RawMessage, error) {
	if cursor == "" {
		return nil, nil
	}
	params, err := json.Marshal(struct {
		Cursor string `json:"cursor,omitempty"`
	}{Cursor: cursor})
	if err != nil {
		return nil, fmt.Errorf("marshal paginated mcp params: %w", err)
	}
	return params, nil
}

func IsUnsupported(err error) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == codeMethodNotFound
}
