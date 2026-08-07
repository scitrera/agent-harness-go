package main

import (
	"fmt"
	"os"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/ids"
)

const shutdownTimeout = 5 * time.Second

// aetherStreamFlush coalesces streamed token deltas into at most one message per
// interval on the Aether transport. Every delta published as its own message
// would burn the gateway's per-identity message-rate quota on a fast reply (and
// the drop is invisible to the sender — the rejection arrives asynchronously),
// so the egress is bounded here rather than relying on the gateway's limit being
// generous. In-process channels stream every delta and set this to 0.
const aetherStreamFlush = 50 * time.Millisecond

type appConfig struct {
	workspaceRoot string
	stateDir      string
	thread        string
	baseURL       string
	model         string
	seed          bool
	record        string // --record path: opt-in per-LLM-call JSONL trace log ("" = off)

	// Aether transport. Empty aetherAddr means no Aether (the in-process
	// channels).
	aetherAddr        string
	aetherWorkspace   string
	aetherSpecifier   string
	aetherTLS         bool
	aetherTLSInsecure bool
	// Client-session identity. WindowID distinguishes two frontends run by the
	// same user; it is what the agent addresses its replies to.
	aetherUser   string
	aetherWindow string

	// streamFlush is the token-delta coalescing interval handed to the turn
	// runner; 0 streams every delta.
	streamFlush time.Duration
}

// defaultAetherUser names the human driving a client session. It is a routing
// label, not a credential — the gateway decides what the connection may do.
func defaultAetherUser() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "user"
}

// resolveWindowID keeps an explicitly configured window id, otherwise mints a
// fresh one per process. Two frontends run by the same user must not share a
// window id: it is the address the agent replies to, so a collision sends one
// session's stream to the other.
func resolveWindowID(configured string) string {
	if configured != "" {
		return configured
	}
	id, err := ids.New("win-")
	if err != nil {
		return fmt.Sprintf("win-%d", os.Getpid())
	}
	return id
}

// sourceAgent labels this process on outbound Aether envelopes. Falls back to
// the hostname so two workers on one gateway are distinguishable in a log.
func (c appConfig) sourceAgent() string {
	if c.aetherSpecifier != "" {
		return "agent-harness:" + c.aetherSpecifier
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "agent-harness"
	}
	return fmt.Sprintf("agent-harness:%s", host)
}
