package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

const mcpProtocolVersion = "2024-11-05"

type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
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
	state, err := m.stateFor(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := state.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	raw, err := state.request(ctx, "tools/list", nil)
	if err != nil {
		return nil, err
	}
	var decoded struct {
		Tools []Tool `json:"tools"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decode mcp tools/list: %w", err)
	}
	return decoded.Tools, nil
}

func (m *Manager) CallTool(ctx context.Context, server string, tool string, args json.RawMessage) (CallToolResult, error) {
	state, err := m.stateFor(ctx, server)
	if err != nil {
		return CallToolResult{}, err
	}
	if err := state.ensureInitialized(ctx); err != nil {
		return CallToolResult{}, err
	}
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	params, err := json.Marshal(struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}{Name: tool, Arguments: args})
	if err != nil {
		return CallToolResult{}, fmt.Errorf("marshal tools/call params: %w", err)
	}
	raw, err := state.request(ctx, "tools/call", params)
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
		return nil, fmt.Errorf("write mcp request: %w", err)
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
		return fmt.Errorf("write mcp notification: %w", err)
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
