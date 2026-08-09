package main

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
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
	// workspaceID is empty in legacy single-workspace mode. A non-empty value
	// activates composite workspace/thread history keys for local turns.
	workspaceID string
	// visibleWorkspaces are additional logical workspaces this host permits an
	// explicit client to address. The selected/default workspace is implicit.
	visibleWorkspaces []string
	stateDir          string
	thread            string
	baseURL           string
	model             string
	seed              bool
	record            string // --record path: opt-in per-LLM-call JSONL trace log ("" = off)

	// Aether transport. Empty aetherAddr means no Aether (the in-process
	// channels).
	aetherAddr        string
	aetherWorkspace   string
	aetherSpecifier   string
	aetherTLS         bool
	aetherTLSInsecure bool
	// aetherTaskMessageLanes routes turn events onto a real Aether task's
	// per-task message lane. It is opt-in because task-less OSS clients depend
	// on direct replies to their user-session topic.
	aetherTaskMessageLanes bool
	// subagentTarget opts parent turns into targeted Aether child execution.
	// subagentExecutor enables consumption of those targeted tasks on this agent.
	subagentTarget              string
	subagentExecutor            bool
	subagentExecutorConcurrency int
	// Client-session identity. WindowID distinguishes two frontends run by the
	// same user; it is what the agent addresses its replies to.
	aetherUser   string
	aetherWindow string

	// MemoryLayer. Empty memorylayerURL keeps transcripts on local disk.
	memorylayerURL       string
	memorylayerKey       string
	memorylayerWorkspace string
	// memoryRecall injects memories relevant to the user's message into the
	// turn; memoryRecallLimit caps how many.
	memoryRecall      bool
	memoryRecallLimit int

	// goalMaxContinuations bounds host-created follow-up turns for one durable
	// goal. Zero keeps goal tools/state enabled but disables automatic follow-ups.
	goalMaxContinuations uint32

	// streamFlush is the token-delta coalescing interval handed to the turn
	// runner; 0 streams every delta.
	streamFlush time.Duration
}

func validateExternalSubagentConfig(mode appMode, target string, executor bool, memorylayerURL string) error {
	target = strings.TrimSpace(target)
	if target == "" && !executor {
		return nil
	}
	if mode != appModeServe && mode != appModeStandalone {
		return errors.New("external subagents require --serve or --aether-standalone")
	}
	if strings.TrimSpace(memorylayerURL) == "" {
		return errors.New("external subagents require --memorylayer so parent and executor share history")
	}
	if target != "" && !strings.HasPrefix(target, "ag::") {
		return errors.New("--subagent-target must be a full Aether agent topic (ag::<workspace>::<implementation>::<specifier>)")
	}
	return nil
}

func parseVisibleWorkspaces(value string) []string {
	seen := map[string]struct{}{}
	for _, workspaceID := range strings.Split(value, ",") {
		if workspaceID = strings.TrimSpace(workspaceID); workspaceID != "" {
			seen[workspaceID] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for workspaceID := range seen {
		out = append(out, workspaceID)
	}
	sort.Strings(out)
	return out
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
