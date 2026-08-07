package main

import (
	"fmt"
	"os"
	"time"
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

	// streamFlush is the token-delta coalescing interval handed to the turn
	// runner; 0 streams every delta.
	streamFlush time.Duration
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
