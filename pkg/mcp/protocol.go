// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const mcpProtocolVersion = "2024-11-05"

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
	Annotations ToolAnnotations `json:"annotations,omitempty"`
}

// ToolAnnotations are the server's self-reported behavioral hints. They are
// UNTRUSTED — a server declaring a destructive tool read-only would otherwise
// talk its way past a read-only approval tier — so consumers may only act on
// them in the restrictive direction: honor readOnlyHint:true as a downgrade to
// read, and treat everything else (absent, false, or unparseable) as the
// stricter class.
type ToolAnnotations struct {
	ReadOnlyHint *bool `json:"readOnlyHint,omitempty"`
}

// IsReadOnly reports an explicit readOnlyHint:true. Absent or false is not
// read-only.
func (a ToolAnnotations) IsReadOnly() bool {
	return a.ReadOnlyHint != nil && *a.ReadOnlyHint
}

type Content struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	Data     string          `json:"data,omitempty"`
	MimeType string          `json:"mimeType,omitempty"`
	Raw      json.RawMessage `json:"-"`
}

type CallToolResult struct {
	Content []Content       `json:"content"`
	IsError bool            `json:"isError,omitempty"`
	Raw     json.RawMessage `json:"-"`
}

func (m *Manager) ListTools(ctx context.Context, name string) ([]Tool, error) {
	raw, state, err := m.callWithRetry(ctx, name, "tools/list", nil)
	if err != nil {
		return nil, err
	}
	var decoded struct {
		Tools []Tool `json:"tools"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decode mcp tools/list: %w", err)
	}
	state.cacheToolSchemas(decoded.Tools)
	return decoded.Tools, nil
}

func (m *Manager) CallTool(ctx context.Context, server string, tool string, args json.RawMessage) (CallToolResult, error) {
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	args = m.normalizeToolArgs(ctx, server, tool, args)
	params, err := json.Marshal(struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}{Name: tool, Arguments: args})
	if err != nil {
		return CallToolResult{}, fmt.Errorf("marshal tools/call params: %w", err)
	}
	raw, _, err := m.callWithRetry(ctx, server, "tools/call", params)
	if err != nil {
		return CallToolResult{}, err
	}
	var decoded CallToolResult
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return CallToolResult{}, fmt.Errorf("decode mcp tools/call: %w", err)
	}
	decoded.Raw = append(json.RawMessage(nil), raw...)
	return decoded, nil
}

// peekServerState looks up a registered server's state without starting it.
func (m *Manager) peekServerState(name string) (*serverState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.servers[name]
	return state, ok
}

// normalizeToolArgs drops "" values for optional (non-required) string
// fields before a tools/call. LLMs frequently fill unset optional string
// parameters with "", and some MCP servers reject that. Required fields and
// non-string fields are always left untouched. Schemas are cached from
// tools/list; if the schema for this tool isn't known yet it is fetched
// once (best effort — on failure args are sent through unmodified).
func (m *Manager) normalizeToolArgs(ctx context.Context, server, tool string, args json.RawMessage) json.RawMessage {
	state, ok := m.peekServerState(server)
	if !ok {
		return args
	}
	schema, ok := state.lookupToolSchema(tool)
	if !ok {
		if _, err := m.ListTools(ctx, server); err != nil {
			return args
		}
		schema, ok = state.lookupToolSchema(tool)
		if !ok {
			return args
		}
	}
	return schema.dropEmptyOptionalStrings(args)
}

// callWithRetry issues a JSON-RPC request against the named server. If the
// underlying session is dead (broken pipe, closed stdout, or process exit),
// it invalidates the cached session, restarts the server, and retries the
// full initialize+request sequence exactly once before giving up.
func (m *Manager) callWithRetry(ctx context.Context, name string, method string, params json.RawMessage) (json.RawMessage, *serverState, error) {
	state, err := m.stateFor(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	raw, err := doRequest(ctx, state, method, params)
	if err == nil || !errors.Is(err, ErrProcessExit) {
		return raw, state, err
	}
	if restartErr := m.restartServer(ctx, state); restartErr != nil {
		return nil, state, err
	}
	raw, err = doRequest(ctx, state, method, params)
	return raw, state, err
}

func doRequest(ctx context.Context, state *serverState, method string, params json.RawMessage) (json.RawMessage, error) {
	if err := state.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	return state.request(ctx, method, params)
}

// toolArgSchema captures the subset of a tool's JSON input schema needed to
// normalize call arguments: which fields are required, and which are typed
// as strings.
type toolArgSchema struct {
	required map[string]bool
	strings  map[string]bool
}

func parseToolArgSchema(raw json.RawMessage) toolArgSchema {
	schema := toolArgSchema{required: map[string]bool{}, strings: map[string]bool{}}
	if len(raw) == 0 {
		return schema
	}
	var decoded struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return schema
	}
	for _, name := range decoded.Required {
		schema.required[name] = true
	}
	for name, prop := range decoded.Properties {
		if prop.Type == "string" {
			schema.strings[name] = true
		}
	}
	return schema
}

// dropEmptyOptionalStrings removes "" values for non-required string fields.
// Required fields and non-string fields are always left untouched; if args
// isn't a JSON object it is returned unmodified.
func (schema toolArgSchema) dropEmptyOptionalStrings(args json.RawMessage) json.RawMessage {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(args, &fields); err != nil {
		return args
	}
	changed := false
	for name, raw := range fields {
		if schema.required[name] || !schema.strings[name] {
			continue
		}
		var str string
		if err := json.Unmarshal(raw, &str); err != nil || str != "" {
			continue
		}
		delete(fields, name)
		changed = true
	}
	if !changed {
		return args
	}
	normalized, err := json.Marshal(fields)
	if err != nil {
		return args
	}
	return normalized
}

func (s *serverState) cacheToolSchemas(tools []Tool) {
	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()
	if s.toolSchemas == nil {
		s.toolSchemas = map[string]toolArgSchema{}
	}
	for _, tool := range tools {
		s.toolSchemas[tool.Name] = parseToolArgSchema(tool.InputSchema)
	}
}

func (s *serverState) lookupToolSchema(tool string) (toolArgSchema, bool) {
	s.schemaMu.Lock()
	defer s.schemaMu.Unlock()
	schema, ok := s.toolSchemas[tool]
	return schema, ok
}

func (s *serverState) ensureInitialized(ctx context.Context) error {
	s.rpcMu.Lock()
	defer s.rpcMu.Unlock()
	if s.initialized {
		return nil
	}
	params, err := json.Marshal(struct {
		ProtocolVersion string                     `json:"protocolVersion"`
		Capabilities    map[string]json.RawMessage `json:"capabilities"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}{ProtocolVersion: mcpProtocolVersion, Capabilities: map[string]json.RawMessage{}})
	if err != nil {
		return fmt.Errorf("marshal initialize params: %w", err)
	}
	if _, err := s.requestLocked(ctx, "initialize", params); err != nil {
		return err
	}
	if err := s.notifyLocked("notifications/initialized", nil); err != nil {
		return err
	}
	s.initialized = true
	return nil
}

func (s *serverState) request(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	s.rpcMu.Lock()
	defer s.rpcMu.Unlock()
	return s.requestLocked(ctx, method, params)
}

func (s *serverState) requestLocked(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	if s.stdin == nil {
		return nil, fmt.Errorf("%w: %s", ErrProcessExit, s.cfg.Name)
	}
	s.nextID++
	id := s.nextID
	ch := make(chan rpcIncoming, 1)
	s.pendingMu.Lock()
	s.pending[id] = ch
	s.pendingMu.Unlock()
	payload, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		s.removePending(id)
		return nil, fmt.Errorf("marshal mcp request: %w", err)
	}
	if _, err := s.stdin.Write(append(payload, '\n')); err != nil {
		s.removePending(id)
		// A failed write to the child's stdin means the session is dead
		// (broken pipe / closed pipe) regardless of the exact cause;
		// classify it as ErrProcessExit so callers can detect and retry.
		return nil, fmt.Errorf("%w: write mcp request %s: %w", ErrProcessExit, s.cfg.Name, err)
	}
	select {
	case msg := <-ch:
		if msg.Err != nil {
			return nil, msg.Err
		}
		if msg.Error != nil {
			return nil, msg.Error
		}
		return msg.Result, nil
	case <-ctx.Done():
		s.removePending(id)
		return nil, ctx.Err()
	}
}

func (s *serverState) notifyLocked(method string, params json.RawMessage) error {
	if s.stdin == nil {
		return fmt.Errorf("%w: %s", ErrProcessExit, s.cfg.Name)
	}
	payload, err := json.Marshal(rpcRequest{JSONRPC: "2.0", Method: method, Params: params})
	if err != nil {
		return fmt.Errorf("marshal mcp notification: %w", err)
	}
	if _, err := s.stdin.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("%w: write mcp notification %s: %w", ErrProcessExit, s.cfg.Name, err)
	}
	return nil
}

func (s *serverState) readLoop(stdout io.Reader, generation int64) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for scanner.Scan() {
		var msg rpcIncoming
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		if msg.ID == 0 {
			continue
		}
		s.deliver(msg)
	}
	s.failPending(generation, fmt.Errorf("%w: %s stdout closed", ErrProcessExit, s.cfg.Name))
}

func (s *serverState) deliver(msg rpcIncoming) {
	s.pendingMu.Lock()
	ch := s.pending[msg.ID]
	delete(s.pending, msg.ID)
	s.pendingMu.Unlock()
	if ch != nil {
		ch <- msg
	}
}

func (s *serverState) failPending(generation int64, err error) {
	s.pendingMu.Lock()
	if generation != s.generation {
		s.pendingMu.Unlock()
		return
	}
	pending := s.pending
	s.pending = map[int64]chan rpcIncoming{}
	s.pendingMu.Unlock()
	for _, ch := range pending {
		ch <- rpcIncoming{Err: err}
	}
}

func (s *serverState) removePending(id int64) {
	s.pendingMu.Lock()
	delete(s.pending, id)
	s.pendingMu.Unlock()
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcIncoming struct {
	ID     int64           `json:"id,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
	Err    error           `json:"-"`
}
