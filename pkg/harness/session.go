package harness

import (
	"context"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/tools"
)

type HistoryStore interface {
	// LoadHistory returns the thread's messages. When the turn carries an OBO
	// authority it is available on ctx via tools.MemoryAuthorityFrom — an
	// authority-aware store (e.g. a MemoryLayer-backed loader) should apply it;
	// local stores ignore it.
	LoadHistory(ctx context.Context, threadID string) ([]protocol.ChatMessage, error)
	SaveHistory(ctx context.Context, threadID string, messages []protocol.ChatMessage) error
}

type Session struct {
	addr      protocol.MessageAddress
	store     HistoryStore
	tools     *tools.Registry
	authority tools.MemoryAuthority
	history   []protocol.ChatMessage
}

func NewSession(ctx context.Context, addr protocol.MessageAddress, store HistoryStore, registry *tools.Registry, authority tools.MemoryAuthority) (*Session, error) {
	if addr.ThreadID == "" {
		return nil, ErrMissingThreadID
	}
	if store == nil {
		return nil, ErrMissingHistoryStore
	}
	if registry == nil {
		registry = tools.NewRegistry()
	}
	// Carry the per-turn OBO authority on ctx so an authority-aware history store
	// can apply the user's grant on the load; local stores ignore it.
	history, err := store.LoadHistory(tools.WithMemoryAuthority(ctx, authority), addr.ThreadID)
	if err != nil {
		return nil, fmt.Errorf("load history: %w", err)
	}
	return &Session{addr: addr, store: store, tools: registry, authority: authority, history: history}, nil
}

func (s *Session) Append(ctx context.Context, msg protocol.ChatMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	msg.Addr = mergeAddress(s.addr, msg.Addr)
	next := append(s.History(), msg)
	if err := s.store.SaveHistory(ctx, s.addr.ThreadID, next); err != nil {
		return fmt.Errorf("save history: %w", err)
	}
	s.history = next
	return nil
}

func (s *Session) History() []protocol.ChatMessage {
	out := make([]protocol.ChatMessage, len(s.history))
	copy(out, s.history)
	return out
}

func (s *Session) InvokeTool(ctx context.Context, env protocol.ToolInvokeEnvelope) (tools.Result, error) {
	req := tools.RequestFromEnvelope(env)
	req.Authority = s.authority
	result, err := s.tools.Invoke(ctx, req)
	if err != nil {
		return tools.Result{}, err
	}
	part, err := result.ContentPart()
	if err != nil {
		return tools.Result{}, err
	}
	if err := s.AppendToolResult(ctx, env.CallID, part); err != nil {
		return tools.Result{}, err
	}
	return result, nil
}

// AppendToolResult persists a tool_result content part as a tool-result message
// (used both by InvokeTool and by the turn loop to record a denial without
// executing the tool).
func (s *Session) AppendToolResult(ctx context.Context, callID string, part protocol.ContentPart) error {
	msg := protocol.ChatMessage{ID: callID + "-result", Role: protocol.RoleToolResult, Addr: s.addr, Content: []protocol.ContentPart{part}}
	return s.Append(ctx, msg)
}

func mergeAddress(base protocol.MessageAddress, override protocol.MessageAddress) protocol.MessageAddress {
	merged := base
	if override.TenantID != "" {
		merged.TenantID = override.TenantID
	}
	if override.WorkspaceID != "" {
		merged.WorkspaceID = override.WorkspaceID
	}
	if override.UserID != "" {
		merged.UserID = override.UserID
	}
	if override.ThreadID != "" {
		merged.ThreadID = override.ThreadID
	}
	if override.AppID != "" {
		merged.AppID = override.AppID
	}
	if override.AgentID != "" {
		merged.AgentID = override.AgentID
	}
	if override.TaskID != "" {
		merged.TaskID = override.TaskID
	}
	if override.RequestID != "" {
		merged.RequestID = override.RequestID
	}
	return merged
}
