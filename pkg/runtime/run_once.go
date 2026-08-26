package runtime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/scitrera/agent-harness-go/pkg/channel"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/shellcontext"
	"github.com/scitrera/agent-harness-go/pkg/steering"
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
	steering  *steering.Inbox
	rejector  channel.SteeringRejector
}

// Reasons a steering message never reached a turn. Both are surfaced to the
// sender verbatim, so they are phrased for a person.
const (
	reasonNoActiveTurn   = "there is no longer a turn in progress to add this to"
	reasonTurnEndedFirst = "the turn in progress finished before this could be added to it"
)

// steeringOutcome classifies an inbound message against the steering lane.
type steeringOutcome int

const (
	// notSteering: an ordinary message that runs as its own turn.
	notSteering steeringOutcome = iota
	// steeringParked: delivered into the running turn; no turn is started.
	steeringParked
	// steeringMissed: declared as an interjection, but no turn was running to
	// receive it.
	steeringMissed
)

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

// SetSteering wires the inbox that delivers a user's mid-turn message into the
// turn already running on its thread. Without one, such a message simply waits
// its turn as before.
//
// The rejector is taken from the task source when it implements
// channel.SteeringRejector, so a transport that can talk back to the sender
// tells it when an interjection missed its turn. SetSteeringRejector overrides.
func (r *Runner) SetSteering(inbox *steering.Inbox) {
	r.steering = inbox
	if r.rejector == nil {
		if rejector, ok := r.source.(channel.SteeringRejector); ok {
			r.rejector = rejector
		}
	}
}

// SetSteeringRejector overrides how a missed steering message is reported.
func (r *Runner) SetSteeringRejector(rejector channel.SteeringRejector) {
	r.rejector = rejector
}

func (r *Runner) RunOnce(ctx context.Context) (protocol.ChatMessage, error) {
	for {
		envelope, err := r.source.FetchTask(ctx)
		if err != nil {
			return protocol.ChatMessage{}, fmt.Errorf("fetch task: %w", err)
		}
		// A steering send never becomes a turn of its own: it is either
		// delivered into one already running or rejected. In the serial loop
		// nothing can be running when FetchTask returns, so it is always the
		// latter — correct, and honest about it.
		switch r.classifySteering(envelope) {
		case steeringParked:
			continue
		case steeringMissed:
			if fallback, ok := shellSteeringFallback(envelope); ok {
				return r.runTask(ctx, fallback)
			}
			r.rejectSteering(ctx, envelope, reasonNoActiveTurn)
			continue
		}
		return r.runTask(ctx, envelope)
	}
}

// classifySteering decides what an inbound message is with respect to the
// steering lane, parking it as a side effect when a turn is running to take it.
func (r *Runner) classifySteering(envelope channel.Inbound) steeringOutcome {
	if r.steering == nil || !steering.IsRequested(envelope.Message) {
		return notSteering
	}
	addr := turnAddress(envelope)
	if r.steering.Park(steering.Key(addr.WorkspaceID, addr.ThreadID), envelope.Message) {
		return steeringParked
	}
	return steeringMissed
}

// rejectSteering tells the sender their interjection never landed. With no
// rejector wired the message is dropped, which is logged at WARN because the
// user's words are gone and nothing else would say so.
func (r *Runner) rejectSteering(ctx context.Context, envelope channel.Inbound, reason string) {
	addr := turnAddress(envelope)
	if r.rejector == nil {
		slog.WarnContext(ctx, "steering message dropped with no way to tell the sender",
			slog.String("thread", addr.ThreadID),
			slog.String("message", envelope.Message.ID),
			slog.String("reason", reason),
		)
		return
	}
	slog.InfoContext(ctx, "steering message rejected",
		slog.String("thread", addr.ThreadID),
		slog.String("message", envelope.Message.ID),
		slog.String("reason", reason),
	)
	r.rejector.RejectSteering(ctx, envelope, reason)
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
	// Open the steering lane for the whole turn. Anything still parked when it
	// closes was aimed at a turn that has now ended. Ordinary steering is
	// rejected; structured shell context takes its explicitly configured idle
	// fallback so command output is never lost. Closing BEFORE the error check
	// matters: a failed turn must not strand a user's message in the inbox.
	closeSteering := r.steering.Begin(steering.Key(addr.WorkspaceID, addr.ThreadID))
	assistant, err := r.executor.Run(turnCtx, addr, envelope.Message)
	for _, missed := range closeSteering() {
		missedEnvelope := channel.Inbound{Addr: missed.Addr, Message: missed}
		if fallback, ok := shellSteeringFallback(missedEnvelope); ok {
			if _, fallbackErr := r.runTask(ctx, fallback); fallbackErr != nil && err == nil {
				err = fallbackErr
			}
			continue
		}
		r.rejectSteering(ctx, missedEnvelope, reasonTurnEndedFirst)
	}
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("run turn: %w", err)
	}
	return assistant, nil
}

// Shell results have a stronger persistence promise than ordinary steering: a
// turn-ending race must still add them to context. Their structured idle policy
// tells the runner whether the fallback invokes the model or only commits.
func shellSteeringFallback(envelope channel.Inbound) (channel.Inbound, bool) {
	if _, ok := shellcontext.FromMessage(envelope.Message); !ok {
		return channel.Inbound{}, false
	}
	envelope.Message = steering.Unmark(envelope.Message)
	envelope.Addr = envelope.Message.Addr
	return envelope, true
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
