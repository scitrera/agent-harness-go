// Package steering delivers a user's mid-turn chat message INTO the turn that
// is already running, instead of making it wait for that turn to finish.
//
// It is deliberately not a control signal. A steering message is an ordinary
// user chat message — it is persisted to history, it enters the model's context,
// and the model answers it. The only thing that distinguishes it is WHEN it is
// delivered: at the next input-assembly boundary of a turn already in flight,
// rather than as the opening message of a turn of its own. Overloading the
// control channel (cancel/approve/deny) would have relabelled user content as
// machinery; a message that the model reads and replies to is not a control.
//
// Steering never interrupts work. Tool calls already issued run to completion
// and their results are delivered alongside the steering message; a user who
// wants the current work abandoned has the existing cancel control for that.
//
// This package is a dependency-free leaf (peer of turncancel) so both the
// transport/runtime side, which parks messages, and the turn side, which drains
// them, can use it without an import cycle.
package steering

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/scitrera/agent-harness-go/pkg/protocol"
)

// MetaKey marks a chat message as a steering send: the sender is declaring that
// this message is an interjection into whatever turn is already running on the
// thread, not the start of a new one.
//
// The declaration is explicit rather than inferred from "a turn happens to be
// active", because the sender is also the party that decides not to open a task
// for it. A message that merely arrives during a busy period — a scheduled turn,
// a subagent completion notice, a send from another surface — is still its own
// turn and must not be swallowed into an unrelated one.
const MetaKey = "steering"

// OpenTag and CloseTag delimit the steering body in the delivered message. The
// block must be the WHOLE text of the message: a model that sees ordinary user
// text arriving mid-turn reads it as a new task and abandons what it was doing,
// which is the opposite of steering.
const (
	OpenTag  = "[user_steering]"
	CloseTag = "[/user_steering]"
)

// Mark flags a message as a steering send. Returns the message unchanged when
// it already carries the marker.
func Mark(message protocol.ChatMessage) protocol.ChatMessage {
	if IsRequested(message) {
		return message
	}
	if message.Meta == nil {
		message.Meta = map[string]json.RawMessage{}
	}
	message.Meta[MetaKey] = json.RawMessage(`true`)
	return message
}

// IsRequested reports whether the sender marked this message as a steering send.
func IsRequested(message protocol.ChatMessage) bool {
	raw, ok := message.Meta[MetaKey]
	if !ok {
		return false
	}
	var requested bool
	if err := json.Unmarshal(raw, &requested); err != nil {
		// A malformed marker is not a licence to swallow the message into an
		// unrelated turn: fail toward "ordinary message", which at worst costs
		// the user a wait rather than misattributing their words.
		return false
	}
	return requested
}

// Unmark returns a copy that is no longer classified as steering. It is used
// when a delivery with an explicit safe fallback must become its own turn.
func Unmark(message protocol.ChatMessage) protocol.ChatMessage {
	if message.Meta == nil {
		return message
	}
	cloned := make(map[string]json.RawMessage, len(message.Meta))
	for key, value := range message.Meta {
		if key != MetaKey {
			cloned[key] = value
		}
	}
	message.Meta = cloned
	return message
}

// Key is the steering lane identity: one in-flight turn per (workspace, thread).
// It MUST agree with the runtime dispatcher's key — a message parked under one
// spelling and drained under another is silently never delivered — so the
// dispatcher derives its key from this function rather than repeating the
// scheme. The workspace is length-prefixed so two opaque (workspace, thread)
// pairs cannot concatenate into the same lane.
func Key(workspaceID, threadID string) string {
	return strconv.Itoa(len(workspaceID)) + ":" + workspaceID + threadID
}

// entry is one lane's parked messages. The Inbox keys by lane but compares
// identity by POINTER on close, so a turn that has already been replaced cannot
// clear its successor's queue.
type entry struct {
	parked []protocol.ChatMessage
}

// Inbox holds messages parked for turns that are currently running. The zero
// value is not usable; call New. A nil *Inbox is safe for every method, so a
// host that has not wired steering behaves exactly as before.
type Inbox struct {
	mu     sync.Mutex
	active map[string]*entry
}

func New() *Inbox {
	return &Inbox{active: map[string]*entry{}}
}

// Begin marks a lane as running and returns the close func for that turn. The
// close func returns any messages that were parked but never drained — the
// caller MUST re-submit them as an ordinary turn.
//
// That handoff is the whole reason this type exists. Without it there is a race
// with no safe side: a message can be parked microseconds before the turn
// reaches its last drain point, and would then sit in the inbox with nobody left
// to deliver it. Returning the remainder converts a lost message into a slightly
// late one.
//
// A nil Inbox or empty key yields a close func that returns nothing.
func (i *Inbox) Begin(key string) func() []protocol.ChatMessage {
	if i == nil || key == "" {
		return func() []protocol.ChatMessage { return nil }
	}
	mine := &entry{}
	i.mu.Lock()
	i.active[key] = mine
	i.mu.Unlock()
	return func() []protocol.ChatMessage {
		i.mu.Lock()
		defer i.mu.Unlock()
		// Only clear the lane if it is still ours: a later turn may already have
		// claimed it, and dropping its queue would lose that turn's steering.
		if current, ok := i.active[key]; ok && current == mine {
			delete(i.active, key)
		}
		parked := mine.parked
		mine.parked = nil
		return parked
	}
}

// Park queues a steering message for the turn running on key, reporting whether
// it was accepted. False means no turn is running and the caller must dispatch
// the message as an ordinary turn.
//
// The active check and the append happen under one lock precisely so that
// "accepted" is a promise: once Park returns true the message is in a queue that
// either a drain or the close handoff will deliver.
func (i *Inbox) Park(key string, message protocol.ChatMessage) bool {
	if i == nil || key == "" {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	current, ok := i.active[key]
	if !ok {
		return false
	}
	current.parked = append(current.parked, message)
	return true
}

// Drain removes and returns the messages parked for key since the last drain.
func (i *Inbox) Drain(key string) []protocol.ChatMessage {
	if i == nil || key == "" {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	current, ok := i.active[key]
	if !ok || len(current.parked) == 0 {
		return nil
	}
	parked := current.parked
	current.parked = nil
	return parked
}

// Deliver renders a parked message as the user-role message the turn appends to
// its history.
//
// The text is wrapped whole in the steering tags, and any non-text parts
// (images, files) follow it unchanged — an attachment cannot carry the block
// without breaking the "the block is the entire text" rule that gives the
// message its steering identity. A message whose text is empty still gets an
// empty block rather than being delivered bare, so an image-only interjection is
// still recognizable as one.
func Deliver(addr protocol.MessageAddress, message protocol.ChatMessage) (protocol.ChatMessage, error) {
	var text strings.Builder
	var extra []protocol.ContentPart
	for _, part := range message.Content {
		body, ok := part.AsText()
		if !ok {
			extra = append(extra, part)
			continue
		}
		if text.Len() > 0 {
			text.WriteString("\n")
		}
		text.WriteString(body.Text)
	}
	block, err := protocol.NewTextPart(OpenTag + "\n" + text.String() + "\n" + CloseTag)
	if err != nil {
		return protocol.ChatMessage{}, err
	}
	delivered := message
	delivered.Role = protocol.RoleUser
	delivered.Addr = addr
	delivered.Content = append([]protocol.ContentPart{block}, extra...)
	// The marker has done its job at ingress; carrying it into history would
	// invite a replay to re-park a message that was already delivered.
	if delivered.Meta != nil {
		cloned := make(map[string]json.RawMessage, len(delivered.Meta))
		for k, v := range delivered.Meta {
			if k == MetaKey {
				continue
			}
			cloned[k] = v
		}
		delivered.Meta = cloned
	}
	return delivered, nil
}
