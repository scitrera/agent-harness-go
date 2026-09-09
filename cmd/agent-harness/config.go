// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Scitrera LLC

package main

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/scitrera/agent-harness-go/pkg/ids"
	modelpkg "github.com/scitrera/agent-harness-go/pkg/model"
	memorylayersdk "github.com/scitrera/memorylayer/memorylayer-sdk-go"
)

const shutdownTimeout = 5 * time.Second

const (
	promptNotesAuthorityOff                 = "off"
	promptNotesAuthorityLocal               = "local"
	promptNotesAuthorityMemoryLayer         = "memorylayer"
	agentSpecificationsAuthorityOff         = "off"
	agentSpecificationsAuthorityLocal       = "local"
	agentSpecificationsAuthorityMemoryLayer = "memorylayer"
	refinementAuthorityOff                  = "off"
	refinementAuthorityLocal                = "local"
	refinementAuthorityMemoryLayer          = "memorylayer"
	memoryLayerModeAuto                     = "auto"
	memoryLayerModeOff                      = "off"
	memoryLayerModeHTTP                     = "http"
	memoryLayerModeAether                   = "aether"
	defaultMemoryLayerTarget                = "sv::memorylayer"
)

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
	// workspaceIndexDir is the shared canonical project-to-workspace index used
	// when a client registers another local coding project with /cd.
	workspaceIndexDir string
	// dynamicWorkspaces is true only for an automatically resolved project-mode
	// client. A pinned workspace intentionally remains a single logical scope.
	dynamicWorkspaces bool
	// visibleWorkspaces are additional logical workspaces this host permits an
	// explicit client to address. The selected/default workspace is implicit.
	visibleWorkspaces       []string
	stateDir                string
	thread                  string
	baseURL                 string
	model                   string
	modelRegistry           *modelpkg.Registry
	modelsFile              string
	llmFormat               string
	reasoningEffort         string
	tuiRetainReasoning      bool
	tuiShellTriggerAgent    bool
	tuiShellPreferencesFile string
	seed                    bool
	record                  string // --record path: opt-in per-LLM-call JSONL trace log ("" = off)
	// Workspace roots are ordered before absolute system/operator roots, so a
	// project can shadow host-contributed skills and commands by name.
	skillsDirs         []string
	systemSkillsDirs   []string
	commandsDirs       []string
	systemCommandsDirs []string

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

	// MemoryLayer. mode=auto selects an explicit URL first, then probes the
	// default Aether service target when this process already uses Aether.
	memorylayerMode      string
	memorylayerURL       string
	memorylayerTarget    string
	memorylayerTransport memorylayersdk.Transport
	// memorylayerRequired disables auto-mode's service-absent fallback because
	// another requested feature (for example external subagents) needs the
	// shared authority rather than merely preferring it.
	memorylayerRequired  bool
	memorylayerKey       string
	memorylayerWorkspace string
	// promptNotesAuthority explicitly selects the sole prompt-note source. It is
	// separate from transcript placement so enabling MemoryLayer history does not
	// silently activate remote prompt notes or introduce a hidden fallback.
	promptNotesAuthority string
	// agentSpecificationsAuthority explicitly selects the sole reusable
	// subagent-definition source. local means the workspace's agents directory;
	// memorylayer means typed, workspace-scoped agent specification resources.
	agentSpecificationsAuthority string
	// refinementAuthority selects the sole append-only proposal/audit store.
	// Target resources still follow their prompt-note/agent-spec authorities.
	refinementAuthority        string
	refinementSessionAutoApply bool
	// memoryRecall injects memories relevant to the user's message into the
	// turn; memoryRecallLimit caps how many.
	memoryRecall      bool
	memoryRecallLimit int

	// goalMaxContinuations bounds host-created follow-up turns for one durable
	// goal. Zero keeps goal tools/state enabled but disables automatic follow-ups.
	goalMaxContinuations uint32
	// scheduleConfig declares Aether WorkflowEngine schedules whose task payload
	// is pinned to a MemoryLayer-authoritative worker view.
	scheduleConfig         string
	scheduleReloadInterval time.Duration

	// streamFlush is the token-delta coalescing interval handed to the turn
	// runner; 0 streams every delta.
	streamFlush time.Duration
}

func normalizeMemoryLayerMode(value, baseURL, target string, aetherAvailable bool) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		value = memoryLayerModeAuto
	}
	baseURL = strings.TrimSpace(baseURL)
	target = strings.TrimSpace(target)
	switch value {
	case memoryLayerModeOff:
		return value, nil
	case memoryLayerModeHTTP:
		if baseURL == "" {
			return "", errors.New("memorylayer mode http requires --memorylayer")
		}
		return value, nil
	case memoryLayerModeAether:
		if !aetherAvailable {
			return "", errors.New("memorylayer mode aether requires an Aether-connected mode")
		}
		if target == "" {
			return "", errors.New("memorylayer mode aether requires --memorylayer-target")
		}
		return value, nil
	case memoryLayerModeAuto:
		if baseURL != "" {
			return memoryLayerModeHTTP, nil
		}
		if aetherAvailable {
			if target == "" {
				return "", errors.New("memorylayer auto mode requires --memorylayer-target when Aether is configured")
			}
			return value, nil
		}
		return memoryLayerModeOff, nil
	default:
		return "", fmt.Errorf("invalid memorylayer mode %q (want auto, off, http, or aether)", value)
	}
}

func memoryLayerConfigured(mode string) bool {
	return mode != "" && mode != memoryLayerModeOff
}

func normalizePromptNotesAuthority(value string, memorylayerConfigured bool) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		value = promptNotesAuthorityLocal
	}
	switch value {
	case promptNotesAuthorityOff, promptNotesAuthorityLocal:
		return value, nil
	case promptNotesAuthorityMemoryLayer:
		if !memorylayerConfigured {
			return "", errors.New("prompt-note authority memorylayer requires MemoryLayer")
		}
		return value, nil
	default:
		return "", fmt.Errorf("invalid prompt-note authority %q (want off, local, or memorylayer)", value)
	}
}

func normalizeAgentSpecificationsAuthority(value string, memorylayerConfigured bool) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		value = agentSpecificationsAuthorityLocal
	}
	switch value {
	case agentSpecificationsAuthorityOff, agentSpecificationsAuthorityLocal:
		return value, nil
	case agentSpecificationsAuthorityMemoryLayer:
		if !memorylayerConfigured {
			return "", errors.New("agent-specification authority memorylayer requires MemoryLayer")
		}
		return value, nil
	default:
		return "", fmt.Errorf("invalid agent-specification authority %q (want off, local, or memorylayer)", value)
	}
}

func normalizeRefinementAuthority(value string, memorylayerConfigured bool) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		value = refinementAuthorityLocal
	}
	switch value {
	case refinementAuthorityOff, refinementAuthorityLocal:
		return value, nil
	case refinementAuthorityMemoryLayer:
		if !memorylayerConfigured {
			return "", errors.New("refinement authority memorylayer requires MemoryLayer")
		}
		return value, nil
	default:
		return "", fmt.Errorf("invalid refinement authority %q (want off, local, or memorylayer)", value)
	}
}

func validateExternalSubagentConfig(mode appMode, target string, executor bool, memorylayerConfigured bool) error {
	target = strings.TrimSpace(target)
	if target == "" && !executor {
		return nil
	}
	if mode != appModeServe && mode != appModeStandalone {
		return errors.New("external subagents require --serve or --aether-standalone")
	}
	if !memorylayerConfigured {
		return errors.New("external subagents require MemoryLayer so parent and executor share history")
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
