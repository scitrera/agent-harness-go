package mcp

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/procgroup"
)

type Clock func() time.Time

type ServerConfig struct {
	Name    string
	Command string
	Args    []string
	Env     []string
	IdleTTL time.Duration
}

type ServerStatus struct {
	Name    string
	Running bool
	PID     int
}

type Manager struct {
	mu      sync.Mutex
	clock   Clock
	servers map[string]*serverState
}

type serverState struct {
	cfg         ServerConfig
	cmd         *exec.Cmd
	done        chan error
	lastUsed    time.Time
	stdin       io.WriteCloser
	pending     map[int64]chan rpcIncoming
	nextID      int64
	initialized bool
	generation  int64
	rpcMu       sync.Mutex
	pendingMu   sync.Mutex
	schemaMu    sync.Mutex
	toolSchemas map[string]toolArgSchema
}

func NewManager(clock Clock) *Manager {
	if clock == nil {
		clock = time.Now
	}
	return &Manager{clock: clock, servers: map[string]*serverState{}}
}

func (m *Manager) Register(cfg ServerConfig) error {
	if cfg.Name == "" || cfg.Command == "" {
		return fmt.Errorf("%w: name and command required", ErrInvalidServer)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := cfg
	copied.Args = append([]string(nil), cfg.Args...)
	copied.Env = append([]string(nil), cfg.Env...)
	m.servers[cfg.Name] = &serverState{cfg: copied}
	return nil
}

func (m *Manager) List() []ServerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.servers))
	for name := range m.servers {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]ServerStatus, 0, len(names))
	for _, name := range names {
		state := m.servers[name]
		m.refreshLocked(state)
		out = append(out, statusOf(state))
	}
	return out
}

func (m *Manager) EnsureStarted(ctx context.Context, name string) (ServerStatus, error) {
	m.mu.Lock()
	state, ok := m.servers[name]
	if !ok {
		m.mu.Unlock()
		return ServerStatus{}, fmt.Errorf("%w: %s", ErrUnknownServer, name)
	}
	m.refreshLocked(state)
	if state.cmd != nil {
		state.lastUsed = m.clock()
		status := statusOf(state)
		m.mu.Unlock()
		return status, nil
	}
	status, err := m.startLocked(ctx, state)
	m.mu.Unlock()
	return status, err
}

func (m *Manager) Stop(ctx context.Context, name string) error {
	m.mu.Lock()
	state, ok := m.servers[name]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrUnknownServer, name)
	}
	err := m.stopLocked(ctx, state)
	m.mu.Unlock()
	return err
}

func (m *Manager) StopIdle(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock()
	for _, state := range m.servers {
		if state.cmd == nil || state.cfg.IdleTTL <= 0 {
			continue
		}
		if now.Sub(state.lastUsed) >= state.cfg.IdleTTL {
			if err := m.stopLocked(ctx, state); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) stateFor(ctx context.Context, name string) (*serverState, error) {
	if _, err := m.EnsureStarted(ctx, name); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.servers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownServer, name)
	}
	state.lastUsed = m.clock()
	return state, nil
}

func (m *Manager) startLocked(ctx context.Context, state *serverState) (ServerStatus, error) {
	cmd := exec.CommandContext(ctx, state.cfg.Command, state.cfg.Args...)
	cmd.SysProcAttr = procgroup.Attr()
	if len(state.cfg.Env) > 0 {
		cmd.Env = append(os.Environ(), state.cfg.Env...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return ServerStatus{}, fmt.Errorf("mcp stdin %s: %w", state.cfg.Name, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return ServerStatus{}, fmt.Errorf("mcp stdout %s: %w", state.cfg.Name, err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return ServerStatus{}, fmt.Errorf("start mcp server %s: %w", state.cfg.Name, err)
	}
	state.rpcMu.Lock()
	state.cmd = cmd
	state.stdin = stdin
	state.done = make(chan error, 1)
	state.pendingMu.Lock()
	state.generation++
	generation := state.generation
	state.pending = map[int64]chan rpcIncoming{}
	state.pendingMu.Unlock()
	state.initialized = false
	state.lastUsed = m.clock()
	go state.readLoop(stdout, generation)
	go func() {
		state.done <- cmd.Wait()
		state.failPending(generation, fmt.Errorf("%w: %s", ErrProcessExit, state.cfg.Name))
	}()
	status := statusOf(state)
	state.rpcMu.Unlock()
	return status, nil
}

func (m *Manager) stopLocked(ctx context.Context, state *serverState) error {
	state.rpcMu.Lock()
	defer state.rpcMu.Unlock()
	if state.cmd == nil || state.cmd.Process == nil {
		state.clearProcessLocked()
		return nil
	}
	proc := state.cmd.Process
	_ = procgroup.Terminate(proc)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-state.done:
		state.clearProcessLocked()
		return nil
	case <-timer.C:
		_ = procgroup.Kill(proc)
		<-state.done
		state.clearProcessLocked()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// restartServer stops (if still alive) and restarts the given server state in
// place. Used to recover from a dead session (broken pipe, closed stdout, or
// process exit) detected mid-request; state is reused so callers holding the
// pointer see the refreshed process without re-resolving it by name.
func (m *Manager) restartServer(ctx context.Context, state *serverState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.stopLocked(ctx, state)
	_, err := m.startLocked(ctx, state)
	return err
}

func (m *Manager) refreshLocked(state *serverState) {
	if state.done == nil {
		return
	}
	select {
	case <-state.done:
		state.rpcMu.Lock()
		state.clearProcessLocked()
		state.rpcMu.Unlock()
	default:
	}
}

func (s *serverState) clearProcessLocked() {
	s.cmd = nil
	s.done = nil
	s.stdin = nil
	s.pendingMu.Lock()
	s.pending = nil
	s.pendingMu.Unlock()
	s.initialized = false
}

func statusOf(state *serverState) ServerStatus {
	status := ServerStatus{Name: state.cfg.Name}
	if state.cmd != nil && state.cmd.Process != nil {
		status.Running = true
		status.PID = state.cmd.Process.Pid
	}
	return status
}
