package runtime

import (
	"context"
	"fmt"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
)

// TaskSource is the ingress half of the transport seam (channel.Receiver).
type TaskSource = channel.Receiver

type TurnExecutor interface {
	Run(ctx context.Context, addr protocol.MessageAddress, user protocol.ChatMessage) (protocol.ChatMessage, error)
}

type Runner struct {
	source    TaskSource
	executor  TurnExecutor
	canceller *turncancel.Canceller
}

func NewRunner(source TaskSource, executor TurnExecutor) (*Runner, error) {
	if source == nil {
		return nil, ErrMissingTaskSource
	}
	if executor == nil {
		return nil, ErrMissingTurnExecutor
	}
	return &Runner{source: source, executor: executor}, nil
}

// SetCanceller wires a turn canceller so an out-of-band cancel control can abort
// the in-flight turn (keyed by task id).
func (r *Runner) SetCanceller(c *turncancel.Canceller) {
	r.canceller = c
}

func (r *Runner) RunOnce(ctx context.Context) (protocol.ChatMessage, error) {
	envelope, err := r.source.FetchTask(ctx)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("fetch task: %w", err)
	}
	return r.runTask(ctx, envelope)
}

// runTask executes one fetched task: resolves the address, derives a
// cancellable per-turn context (keyed by task id), and runs the turn.
func (r *Runner) runTask(ctx context.Context, envelope channel.Inbound) (protocol.ChatMessage, error) {
	addr := turnAddress(envelope)
	// A missing thread id is NOT an error here: the turn executor resolves it — a
	// new chat from a "dumb" client mints one (canonical via the backend thread
	// registrar, else a local id) and the resolved id rides back on the egress
	// stream + the returned message. The runtime loop delegates thread-id policy to
	// the executor rather than hard-requiring the caller to supply one.
	turnCtx, done := r.canceller.Begin(ctx, addr.TaskID)
	defer done()
	assistant, err := r.executor.Run(turnCtx, addr, envelope.Message)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("run turn: %w", err)
	}
	return assistant, nil
}

// turnAddress resolves the effective address for a task (envelope address, then
// the message address).
func turnAddress(envelope channel.Inbound) protocol.MessageAddress {
	addr := envelope.Addr
	if addr.ThreadID == "" {
		addr = envelope.Message.Addr
	}
	return addr
}
