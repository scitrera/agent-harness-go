package turn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/scitrera/agent-harness-go/pkg/harness"
	"github.com/scitrera/agent-harness-go/pkg/protocol"
	"github.com/scitrera/agent-harness-go/pkg/subagent"
	"github.com/scitrera/agent-harness-go/pkg/tasklifecycle"
	"github.com/scitrera/agent-harness-go/pkg/tools"
	"github.com/scitrera/agent-harness-go/pkg/turncancel"
	"github.com/scitrera/agent-harness-go/pkg/turnjournal"
)

// turnExecution advances the durable record for one live parent model/tool
// loop. It is intentionally private to Runner: callers configure a neutral
// turnjournal.Store, while the runner owns the exact persistence boundaries.
type turnExecution struct {
	store  turnjournal.Store
	record turnjournal.Record
}

func normalizeJournalInput(addr protocol.MessageAddress, user protocol.ChatMessage) protocol.ChatMessage {
	if strings.TrimSpace(user.ID) == "" {
		user.ID = addr.TaskID + "-input"
	}
	// Session.Append merges this same resolved address. Stamping it before the
	// journal create makes the future history reference hash exact even if the
	// process exits between journal creation and transcript persistence.
	user.Addr = addr
	return user
}

func beginTurnExecution(ctx context.Context, store turnjournal.Store, owner string, addr protocol.MessageAddress, user protocol.ChatMessage) (*turnExecution, error) {
	if store == nil || strings.TrimSpace(addr.TaskID) == "" {
		return nil, nil
	}
	input, err := journalMessageRef(user, addr.WorkspaceID, addr.ThreadID)
	if err != nil {
		return nil, err
	}
	record, err := store.Create(ctx, turnjournal.Record{
		WorkspaceID:   addr.WorkspaceID,
		SessionID:     addr.ThreadID,
		TaskID:        addr.TaskID,
		OwnerIdentity: owner,
		Phase:         turnjournal.PhasePrepared,
		Input:         input,
		LastMessageID: user.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("turn: create execution journal: %w", err)
	}
	return &turnExecution{store: store, record: record}, nil
}

func (e *turnExecution) update(ctx context.Context, mutate func(*turnjournal.Record)) error {
	if e == nil {
		return nil
	}
	next := cloneTurnExecutionRecord(e.record)
	mutate(&next)
	updated, err := e.store.Update(ctx, next, e.record.Revision)
	if err != nil {
		return fmt.Errorf("turn: update execution journal: %w", err)
	}
	e.record = updated
	return nil
}

func (e *turnExecution) providerPending(ctx context.Context, iteration int) error {
	if e == nil {
		return nil
	}
	return e.update(ctx, func(record *turnjournal.Record) {
		record.Phase = turnjournal.PhaseProviderPending
		record.Iteration = uint32(max(iteration, 0))
	})
}

func (e *turnExecution) assistantPersisted(ctx context.Context, session *harness.Session, assistant protocol.ChatMessage, iteration int) (turnjournal.HistoryMessageRef, error) {
	if e == nil {
		return turnjournal.HistoryMessageRef{}, nil
	}
	ref, err := journalPersistedMessageRef(session, assistant.ID)
	if err != nil {
		return turnjournal.HistoryMessageRef{}, err
	}
	err = e.update(ctx, func(record *turnjournal.Record) {
		record.Iteration = uint32(max(iteration, 0))
		record.LastMessageID = ref.MessageID
	})
	return ref, err
}

func (e *turnExecution) toolPending(ctx context.Context, call protocol.ToolInvokeEnvelope, assistant turnjournal.HistoryMessageRef, iteration int) error {
	return e.toolsPending(ctx, []protocol.ToolInvokeEnvelope{call}, assistant, iteration)
}

// toolsPending records a complete assistant tool batch in source order before
// any call body is exposed. Sequential execution uses a one-call batch per
// mutation; explicitly parallel-safe execution checkpoints every sibling call
// together so a crash cannot hide an unconfirmed invocation.
func (e *turnExecution) toolsPending(ctx context.Context, calls []protocol.ToolInvokeEnvelope, assistant turnjournal.HistoryMessageRef, iteration int) error {
	if e == nil {
		return nil
	}
	if len(calls) == 0 {
		return errors.New("turn: cannot checkpoint an empty tool batch")
	}
	// ChildResolved is a deliberately recoverable boundary. Once the live owner
	// is about to expose another mutation, leave it before recording the next
	// requested call so a crash cannot replay that mutation as child recovery.
	if e.record.Phase == turnjournal.PhaseChildResolved {
		if err := e.providerPending(ctx, iteration); err != nil {
			return err
		}
	}
	return e.update(ctx, func(record *turnjournal.Record) {
		record.Phase = turnjournal.PhaseToolPending
		record.Iteration = uint32(max(iteration, 0))
		record.ToolBatch = &turnjournal.ToolBatchCheckpoint{
			Assistant: assistant,
			Calls:     make([]turnjournal.ToolCheckpoint, len(calls)),
		}
		for i := range calls {
			record.ToolBatch.Calls[i] = turnjournal.ToolCheckpoint{
				InvocationID: calls[i].CallID,
				Name:         calls[i].Name,
				ArgsDigest:   digestJournalBytes(protocol.ArgsToRaw(calls[i].Args)),
				Outcome:      turnjournal.ToolOutcomeRequested,
			}
		}
	})
}

func (e *turnExecution) externalAdmitted(ctx context.Context, taskID string, envelope subagent.ExecutionEnvelope) error {
	if e == nil || envelope.Background {
		return nil
	}
	pending := singleJournalTool(e.record.ToolBatch)
	if pending == nil || e.record.Phase != turnjournal.PhaseToolPending || pending.Outcome != turnjournal.ToolOutcomeRequested || pending.InvocationID != envelope.InvocationID {
		return errors.New("turn: external child admission does not match the pending tool invocation")
	}
	descriptor, err := subagent.MarshalExecutionEnvelope(envelope)
	if err != nil {
		return fmt.Errorf("turn: encode admitted child descriptor: %w", err)
	}
	external, err := turnjournal.NewExternalChildRef(taskID, envelope.ExecutionID, envelope.ChildSessionID, descriptor)
	if err != nil {
		return fmt.Errorf("turn: checkpoint admitted child descriptor: %w", err)
	}
	return e.update(ctx, func(record *turnjournal.Record) {
		record.Phase = turnjournal.PhaseWaitingExternalChild
		record.ToolBatch.Calls[0].Outcome = turnjournal.ToolOutcomeAdmitted
		record.ToolBatch.Calls[0].External = &external
	})
}

func (e *turnExecution) toolConfirmed(ctx context.Context, session *harness.Session, callID string, iteration int) error {
	if e == nil {
		return nil
	}
	index := journalToolIndex(e.record.ToolBatch, callID)
	if index < 0 {
		return errors.New("turn: confirmed tool result does not match the pending journal invocation")
	}
	result, err := journalPersistedMessageRef(session, callID+"-result")
	if err != nil {
		return err
	}
	return e.update(ctx, func(record *turnjournal.Record) {
		call := &record.ToolBatch.Calls[index]
		if call.External != nil {
			record.Phase = turnjournal.PhaseChildResolved
		} else if journalBatchConfirmedExcept(record.ToolBatch, index) {
			record.Phase = turnjournal.PhaseProviderPending
		} else {
			record.Phase = turnjournal.PhaseToolPending
		}
		record.Iteration = uint32(max(iteration, 0))
		record.LastMessageID = result.MessageID
		call.Outcome = turnjournal.ToolOutcomeConfirmed
		call.Result = &result
	})
}

func singleJournalTool(batch *turnjournal.ToolBatchCheckpoint) *turnjournal.ToolCheckpoint {
	if batch == nil || len(batch.Calls) != 1 {
		return nil
	}
	return &batch.Calls[0]
}

func journalToolIndex(batch *turnjournal.ToolBatchCheckpoint, callID string) int {
	if batch == nil {
		return -1
	}
	for i := range batch.Calls {
		if batch.Calls[i].InvocationID == callID {
			return i
		}
	}
	return -1
}

func journalBatchConfirmedExcept(batch *turnjournal.ToolBatchCheckpoint, confirming int) bool {
	if batch == nil || confirming < 0 || confirming >= len(batch.Calls) {
		return false
	}
	for i := range batch.Calls {
		if i != confirming && batch.Calls[i].Outcome != turnjournal.ToolOutcomeConfirmed {
			return false
		}
	}
	return true
}

func journalBatchHasRequested(batch *turnjournal.ToolBatchCheckpoint) bool {
	if batch == nil {
		return false
	}
	for i := range batch.Calls {
		if batch.Calls[i].Outcome == turnjournal.ToolOutcomeRequested {
			return true
		}
	}
	return false
}

func markJournalBatchUncertain(batch *turnjournal.ToolBatchCheckpoint) {
	if batch == nil {
		return
	}
	for i := range batch.Calls {
		if batch.Calls[i].Outcome == turnjournal.ToolOutcomeRequested {
			batch.Calls[i].Outcome = turnjournal.ToolOutcomeUncertain
		}
	}
}

func (e *turnExecution) finish(ctx context.Context, runErr error) error {
	if e == nil || e.record.Terminal() {
		return nil
	}
	managed := tasklifecycle.IsManagedTask(ctx)
	phase := turnjournal.PhaseCompleted
	if managed {
		phase = turnjournal.PhaseCompleting
	}
	reason := ""
	if runErr != nil {
		phase = turnjournal.PhaseFailed
		if managed {
			phase = turnjournal.PhaseFailing
		}
		reason = boundedJournalReason(runErr.Error())
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) || errors.Is(runErr, turncancel.ErrTurnCancelled) || errors.Is(runErr, subagent.ErrParentCheckpointUncertain) || errors.Is(runErr, ErrRecoveryUnsafe) ||
			journalBatchHasRequested(e.record.ToolBatch) {
			phase = turnjournal.PhaseInterrupted
			if managed && !errors.Is(runErr, turncancel.ErrTurnCancelled) {
				phase = turnjournal.PhaseInterrupting
			}
		}
	}
	return e.update(ctx, func(record *turnjournal.Record) {
		record.Phase = phase
		record.FailureReason = reason
		if phase == turnjournal.PhaseInterrupted || phase == turnjournal.PhaseInterrupting {
			markJournalBatchUncertain(record.ToolBatch)
		}
	})
}

func (e *turnExecution) interrupt(ctx context.Context, reason string) error {
	if e == nil || e.record.Terminal() {
		return nil
	}
	return e.update(ctx, func(record *turnjournal.Record) {
		record.Phase = turnjournal.PhaseInterrupted
		if tasklifecycle.IsManagedTask(ctx) {
			record.Phase = turnjournal.PhaseInterrupting
		}
		record.FailureReason = boundedJournalReason(reason)
		markJournalBatchUncertain(record.ToolBatch)
	})
}

// AcknowledgeTaskTerminal closes the journal-side outbox record only after the
// external task lifecycle decorator (or startup reconciler) has confirmed the
// authoritative task terminal state. It is idempotent for a concurrently
// acknowledged record and a no-op when this runner did not journal the turn.
func (r *Runner) AcknowledgeTaskTerminal(ctx context.Context, workspaceID, taskID string) error {
	if r.turnJournal == nil {
		return nil
	}
	record, err := r.turnJournal.Get(ctx, workspaceID, taskID)
	if errors.Is(err, turnjournal.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("turn: load execution for terminal acknowledgment: %w", err)
	}
	if record.OwnerIdentity != r.turnOwnerIdentity {
		return fmt.Errorf("%w: execution owner does not match this runner", ErrRecoveryUnsafe)
	}
	if record.Terminal() {
		return nil
	}
	phase, ok := record.AcknowledgedPhase()
	if !ok {
		return fmt.Errorf("%w: execution phase %q has no pending task terminal intent", ErrRecoveryUnsafe, record.Phase)
	}
	record.Phase = phase
	if _, err := r.turnJournal.Update(ctx, record, record.Revision); err != nil {
		if errors.Is(err, turnjournal.ErrConflict) {
			current, getErr := r.turnJournal.Get(ctx, workspaceID, taskID)
			if getErr == nil && current.Terminal() {
				return nil
			}
		}
		return fmt.Errorf("turn: acknowledge execution terminal state: %w", err)
	}
	return nil
}

type recoveredTurnContextKey struct{}

func withRecoveredTurn(ctx context.Context, record turnjournal.Record) context.Context {
	return context.WithValue(ctx, recoveredTurnContextKey{}, record)
}

func recoveredTurnFrom(ctx context.Context) (turnjournal.Record, bool) {
	record, ok := ctx.Value(recoveredTurnContextKey{}).(turnjournal.Record)
	return record, ok
}

// ActiveTurnExecutions returns the deterministic owner-scoped startup scan.
// Hosts with workspace routing can use each record's WorkspaceID to select the
// same runner/catalog that owned the original turn.
func (r *Runner) ActiveTurnExecutions(ctx context.Context) ([]turnjournal.Record, error) {
	if r.turnJournal == nil {
		return []turnjournal.Record{}, nil
	}
	return r.turnJournal.ListActive(ctx, r.turnOwnerIdentity)
}

// ResumeTurn resumes one journaled parent turn. Only an admitted external child
// (or its already-persisted result) is recoverable. Every other active phase is
// durably interrupted and returned as ErrRecoveryUnsafe.
func (r *Runner) ResumeTurn(ctx context.Context, workspaceID, taskID string) (protocol.ChatMessage, error) {
	if r.turnJournal == nil {
		return protocol.ChatMessage{}, fmt.Errorf("%w: no execution journal is configured", ErrRecoveryUnsafe)
	}
	record, err := r.turnJournal.Get(ctx, workspaceID, taskID)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("turn: load execution for recovery: %w", err)
	}
	if record.OwnerIdentity != r.turnOwnerIdentity {
		return protocol.ChatMessage{}, fmt.Errorf("%w: execution owner does not match this runner", ErrRecoveryUnsafe)
	}
	if record.Terminal() {
		return protocol.ChatMessage{}, fmt.Errorf("%w: execution is already terminal", ErrRecoveryUnsafe)
	}
	if record.Phase != turnjournal.PhaseWaitingExternalChild && record.Phase != turnjournal.PhaseChildResolved {
		execution := &turnExecution{store: r.turnJournal, record: record}
		reason := fmt.Sprintf("startup recovery cannot safely replay phase %q", record.Phase)
		if err := execution.interrupt(ctx, reason); err != nil {
			return protocol.ChatMessage{}, errors.Join(fmt.Errorf("%w: %s", ErrRecoveryUnsafe, reason), err)
		}
		return protocol.ChatMessage{}, fmt.Errorf("%w: %s", ErrRecoveryUnsafe, reason)
	}
	auth, _ := tools.MemoryAuthorityFrom(ctx)
	addr := protocol.MessageAddress{WorkspaceID: record.WorkspaceID, ThreadID: record.SessionID, TaskID: record.TaskID}
	session, err := harness.NewSession(ctx, addr, r.store, r.registry, auth)
	if err != nil {
		return protocol.ChatMessage{}, fmt.Errorf("turn: load recovery session: %w", err)
	}
	user, err := journalMessageByRef(session.History(), record.Input)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	addr = user.Addr
	addr.WorkspaceID = record.WorkspaceID
	addr.ThreadID = record.SessionID
	addr.TaskID = record.TaskID
	user.Addr = addr
	return r.Run(withRecoveredTurn(ctx, record), addr, user)
}

// InterruptTurn durably closes an active execution that its host has proven
// cannot be resumed (for example, the authoritative parent task is already
// terminal). It never changes a terminal record.
func (r *Runner) InterruptTurn(ctx context.Context, workspaceID, taskID, reason string) error {
	if r.turnJournal == nil {
		return fmt.Errorf("%w: no execution journal is configured", ErrRecoveryUnsafe)
	}
	record, err := r.turnJournal.Get(ctx, workspaceID, taskID)
	if err != nil {
		return fmt.Errorf("turn: load execution for interruption: %w", err)
	}
	if record.OwnerIdentity != r.turnOwnerIdentity {
		return fmt.Errorf("%w: execution owner does not match this runner", ErrRecoveryUnsafe)
	}
	return (&turnExecution{store: r.turnJournal, record: record}).interrupt(ctx, reason)
}

func cloneTurnExecutionRecord(record turnjournal.Record) turnjournal.Record {
	if record.CompletedAt != nil {
		completed := *record.CompletedAt
		record.CompletedAt = &completed
	}
	if record.ToolBatch != nil {
		batch := *record.ToolBatch
		batch.Calls = append([]turnjournal.ToolCheckpoint(nil), batch.Calls...)
		for i := range batch.Calls {
			if batch.Calls[i].External != nil {
				external := *batch.Calls[i].External
				external.Descriptor = append(json.RawMessage(nil), external.Descriptor...)
				batch.Calls[i].External = &external
			}
			if batch.Calls[i].Result != nil {
				result := *batch.Calls[i].Result
				batch.Calls[i].Result = &result
			}
		}
		record.ToolBatch = &batch
	}
	return record
}

func journalPersistedMessageRef(session *harness.Session, messageID string) (turnjournal.HistoryMessageRef, error) {
	var found *protocol.ChatMessage
	for _, message := range session.History() {
		if message.ID != messageID {
			continue
		}
		if found != nil {
			return turnjournal.HistoryMessageRef{}, fmt.Errorf("turn: journal message %q is duplicated", messageID)
		}
		copy := message
		found = &copy
	}
	if found == nil {
		return turnjournal.HistoryMessageRef{}, fmt.Errorf("turn: journal message %q was not persisted", messageID)
	}
	return journalMessageRef(*found, found.Addr.WorkspaceID, found.Addr.ThreadID)
}

func journalMessageByRef(messages []protocol.ChatMessage, ref turnjournal.HistoryMessageRef) (protocol.ChatMessage, error) {
	var found *protocol.ChatMessage
	for i := range messages {
		if messages[i].ID != ref.MessageID {
			continue
		}
		if found != nil {
			return protocol.ChatMessage{}, fmt.Errorf("turn: journal message %q is duplicated", ref.MessageID)
		}
		message := messages[i]
		found = &message
	}
	if found == nil {
		return protocol.ChatMessage{}, fmt.Errorf("turn: journal message %q was not found", ref.MessageID)
	}
	actual, err := journalMessageRef(*found, found.Addr.WorkspaceID, found.Addr.ThreadID)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	if actual != ref {
		return protocol.ChatMessage{}, fmt.Errorf("turn: journal message %q identity or digest mismatch", ref.MessageID)
	}
	return *found, nil
}

func journalMessageRef(message protocol.ChatMessage, workspaceID, sessionID string) (turnjournal.HistoryMessageRef, error) {
	if strings.TrimSpace(message.ID) == "" || strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(sessionID) == "" {
		return turnjournal.HistoryMessageRef{}, errors.New("turn: journal message requires workspace, session, and message identity")
	}
	encoded, err := canonicalJournalMessage(message)
	if err != nil {
		return turnjournal.HistoryMessageRef{}, fmt.Errorf("turn: encode journal message: %w", err)
	}
	return turnjournal.HistoryMessageRef{
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		MessageID:   message.ID,
		Digest:      digestJournalBytes(encoded),
	}, nil
}

// canonicalJournalMessage hashes the durable protocol projection rather than
// byte-for-byte storage output. MemoryLayer assigns created_at and exposes the
// address workspace through its native app_workspace metadata on reload; both
// are store-owned projections of fields already represented elsewhere and are
// not part of the message mutation boundary. Decoding with UseNumber also
// canonicalizes nested RawMessage key order without losing large JSON numbers.
func canonicalJournalMessage(message protocol.ChatMessage) ([]byte, error) {
	encoded, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var canonical map[string]any
	if err := decoder.Decode(&canonical); err != nil {
		return nil, err
	}
	delete(canonical, "created_at")
	if meta, ok := canonical["meta"].(map[string]any); ok {
		delete(meta, "app_workspace")
	}
	return json.Marshal(canonical)
}

func digestJournalBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func boundedJournalReason(reason string) string {
	const maxRunes = 8192
	runes := []rune(strings.TrimSpace(reason))
	if len(runes) > maxRunes {
		runes = runes[:maxRunes]
	}
	if len(runes) == 0 {
		return "turn execution failed"
	}
	return string(runes)
}
