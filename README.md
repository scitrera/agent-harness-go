# agent-harness-go

A generic, embeddable Go agent runtime: a native tool-calling turn loop with
pluggable seams for transport, memory, tool catalog, and tool gating. It speaks
the [ecosystem messaging spec](https://github.com/scitrera/ecosystem-messaging-spec)
and is the open-source core that the Scitrera distribution ("sahara") builds on.

> **Note on env vars:** configuration env vars are prefixed `SAHARA_` — the
> project's internal codename. The prefix is intentional (low collision risk);
> the OSS core itself takes configuration as struct fields, and the reference
> CLI / distribution maps `SAHARA_*` env onto them.

## Seams (interfaces with simple core defaults)

| Seam | Package | Core default | Swap in |
|------|---------|--------------|---------|
| Transport | `channel` | web UI / CLI (stdin/stdout) | any `channel.Channel` (e.g. a message bus) |
| Memory | `harness` (history) + `turn.MemoryService` + `bootstrap.Loader` | in-memory / files | durable / remote stores |
| Catalog | `catalog` | fixed + filesystem | service-backed `catalog.Provider` |
| Tool approval | `hooks` | allow-all | allow-lists, ACL/human approvers |
| Tool observer | `hooks` | none | OTel/audit observers |
| Session attach/replay | `sessionlog` | bounded memory/file event logs | Aether/MemoryLayer/distributed implementations |
| Subagent execution authority | `subagent.TaskBackend` | in-process execution | durable task systems such as Aether |
| Goals and continuation | `goal.Service`, `goal.ContinuationPolicy` | atomic files + bounded host follow-ups | CAS stores, custom policies/verifiers |

## Quick start

The reference CLI (`cmd/agent-harness`) runs a chat against any OpenAI-compatible
endpoint using only core packages + reference impls (file store, filesystem
skills/commands, web, TUI, and CLI channels) — no external transport or memory backend:

```sh
export SAHARA_LLM_BASE_URL=http://localhost:11434/v1   # e.g. ollama / vLLM / OpenAI
export SAHARA_LLM_API_KEY=...                          # if the endpoint needs one
export SAHARA_LLM_MODEL=llama3.1                        # a model id the endpoint serves
go run ./cmd/agent-harness --workspace ./workspace
```

By default it opens the terminal UI; pass `--web` to serve the browser chat UI
on `127.0.0.1:8787`, or `--cli` to run the stdin REPL instead. `--base-url` and
`--model` can also be passed as flags
(overriding the env). It seeds default
workspace files (SOUL/AGENTS/…) on first run, persists history under
`<workspace>/.agent-harness`, and streams replies to the active channel (SSE to
the browser, the terminal UI, or stdout in `--cli` mode).

### Interfaces

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `--addr` | `SAHARA_WEB_ADDR` | `127.0.0.1:8787` | web UI listen address |
| `--tui` | — | default | run the Bubble Tea terminal UI |
| `--cli` | — | `false` | run the stdin REPL |
| `--acp` | — | `false` | run as an ACP agent over stdio |
| `--web` | — | `false` | run the localhost web UI |
| `--no-browser` | — | `false` | don't auto-open a browser (web mode) |
| `--workspace-mode` | `SAHARA_WORKSPACE_MODE` | `single` | `single` preserves the legacy layout; `project` derives a stable logical workspace from cwd/Git |
| `--workspace-id` | `SAHARA_WORKSPACE_ID` | — | pin a logical workspace ID and enable composite workspace/thread state |
| `--visible-workspaces` | `SAHARA_VISIBLE_WORKSPACES` | — | comma-separated additional logical workspaces clients may explicitly address |
| `--workspace-index-dir` | `SAHARA_WORKSPACE_INDEX_DIR` | user config dir | shared canonical project-path to workspace-ID index used by project mode |
| `--serve` | — | `false` | run as a headless agent worker over Aether |
| `--aether` | `AETHER_ADDR` | — | Aether gateway address, e.g. `127.0.0.1:50051` |
| `--aether-standalone` | — | `false` | run the worker and the terminal UI in one process |
| `--aether-task-message-lanes` | — | `false` | route turns carrying real Aether task IDs to their subscribed per-task message lanes |
| `--subagent-target` | `SAHARA_SUBAGENT_TARGET` | — | target spawned subagents at a full Aether agent topic; requires MemoryLayer |
| `--subagent-executor` | — | `false` | consume targeted agent-harness subagent tasks on this worker; requires MemoryLayer |
| `--subagent-executor-concurrency` | — | `4` | bound concurrent externally assigned subagents |
| `--memorylayer` | `MEMORYLAYER_BASE_URL` | — | store threads + transcripts in MemoryLayer instead of on disk |
| `--memory-recall` | — | `true` | inject MemoryLayer memories relevant to each message |
| `--goal-max-continuations` | — | `3` | maximum automatic follow-up turns for one durable goal; `0` disables follow-ups |

### Project workspaces

Multi-workspace state is optional. The default `single` mode retains the
existing unscoped local history layout. For programming use, start the harness
from a repository with project mode enabled:

```bash
cd /path/to/project
agent-harness --workspace-mode project --workspace .
```

Project mode canonicalizes the current directory, prefers its Git worktree
root, and persists a stable path-to-workspace assignment in the shared workspace
index. Repositories with the same directory name receive distinct stable IDs.
The logical workspace ID scopes transcripts, thread indexes, task/team state,
model pins, and local runtime lanes. `--workspace` still selects the filesystem
sandbox; it is deliberately separate from logical identity. Use
`--workspace-id <id>` for containers or deployments where the host path is not
stable.

When Aether or MemoryLayer is enabled, the resolved logical ID is their default
workspace too. Explicit `--aether-workspace` / `AETHER_WORKSPACE` and
`--memorylayer-workspace` / `MEMORYLAYER_WORKSPACE` values take precedence.
Hosts that intentionally serve more than one project can list additional IDs in
`--visible-workspaces`; omitted request workspace still selects the configured
default, and unlisted explicit workspaces fail closed.

### Session attachment library

`pkg/sessionlog` is the transport-independent OSS reference for resumable
clients. It provides bounded in-memory, atomically persisted file, and
compare-and-swap event logs keyed by `(workspace_id, session_id)`,
generation-aware complete/partial/unavailable replay, an atomic attach capture,
and a publisher wrapper that records the shared chat-stream vocabulary. Its
attach coordinator combines workspace-scoped durable history with any live
stream projection so a client does not miss a finalized message during the
short interval before transcript persistence.

The wire types, version, capabilities, and validation rules come from the
ecosystem messaging spec rather than a Sahara-specific duplicate. The reference
web mode exposes the local integration at `POST /api/session/attach`. Its
`GET /api/session/stream?session_id=<id>&client_id=<id>` SSE endpoint first emits
`session_attached`, then cursor-bearing `session_event` records; reconnectors can
also pass `generation` and `sequence`. A detected reset or delivery gap closes
the stream after a `session_reset` or `session_gap` marker so the client can
attach again. Legacy single-workspace history storage is preserved on disk while
the wire API resolves it as workspace `default`. Web mode persists its session
cursor, retained event suffix, and live message projection below
`<state-dir>/sessionlog/workspaces`; each local state directory supports one
writing process at a time. Aether worker and standalone modes instead keep the
same bounded state in the agent's workspace-exclusive Aether KV namespace.
Full-value compare-and-swap prevents two replicas from assigning the same cursor
or overwriting one another while preserving the same `EventStore` behavior.

Schema-revision-3 clients may additionally negotiate
`session.state.subagents.v1` and `session.state.goals.v1`. The reference host
projects workspace/session-isolated lifecycle stores into those shared snapshot
namespaces. Local web/CLI modes use atomic files below
`<state-dir>/session-state/workspaces`; Aether worker/standalone modes store the
same typed states in workspace-exclusive KV with atomic create and full-value
compare-and-swap. The in-process subagent runner records admitted, running, and
terminal child lifecycle plus child token usage. An optional
`subagent.TaskBackend` makes a durable task system authoritative for admission,
start, terminal state, and restart reconciliation while the registry remains the
parent-session snapshot projection. Local startup marks children left running
by a prior single writer as `interrupted`; Aether defers that scan until its
stable agent identity has connected successfully, proving a duplicate worker is
not still active, then reconciles every indexed parent projection against its
task. Goal and subagent CAS stores preserve deterministic ordering, deletion
tombstones, workspace isolation, and the same strict corruption checks as their
file counterparts. Clients that do not request these capabilities do not load
or receive the optional state.

Aether and other channels can adapt the same library without changing the
protocol or local storage behavior.

### Durable goals and bounded continuation

The reference host registers `create_goal`, `get_goal`, and `update_goal` over
the exported `goal.Service`. Goal creation is intended only for explicitly
requested persistent multi-turn work. A workspace/session retains terminal goal
history but may have at most one pending, active, or blocked goal. The service
atomically validates lifecycle transitions under both the file and Aether-CAS
stores; `update_goal` is the explicit completion/block/cancel/resume surface and
accepts stable message, artifact, task, or verifier evidence references.

An active goal receives up to `--goal-max-continuations` host-created follow-up
turns after its initial turn. Provider-reported usage is charged exactly once per
finalized assistant message, including the turn that completes the goal. A goal
whose cumulative token budget is reached, or whose automatic follow-up limit is
exhausted, becomes blocked before another turn is admitted. Missing provider
usage remains zero, so the independent continuation count still bounds the run.
Set the flag to `0` to retain explicit goal tools and state without automatic
follow-ups.

TUI, web, and ACP deliver follow-ups through their existing ingress queue. The
direct stdin CLI has no ingress queue, so it records why automatic delivery was
unavailable and leaves the goal active for explicit user turns. Every decision
is appended below the workspace/session state directory; Aether modes use the
equivalent CAS ledger and admit each follow-up as a deterministic Aether task
targeted to the worker's stable identity. Its versioned payload is
credential-free; typed task authority is converted to the same opaque,
single-use handoff only when assignment reaches the worker. Live assignments
use that payload directly. After restart, the worker scans its private Aether
task type and reconstructs queued ingress from the credential-free envelope
stored on the planned decision; the normal task lifecycle/turn journal recovers
running work. Local delivery retains the conservative plan-before-enqueue
boundary and never blindly replays an ambiguous plan.

Embedders can replace `goal.BoundedPolicy` and supply a `goal.Verifier`. When a
`turn.RubricVerifier` and goal runtime are both configured on `turn.Runner`, the
rubric becomes that verifier: satisfied evidence completes the goal, revision
feedback informs the one ledgered continuation path, and grader errors block
automatic progress. The standalone rubric retry loop runs only on turns that
are not associated with a goal.

No additional wire schema is needed. Revision-3 clients already negotiate the
portable `session.state.goals.v1` projection containing lifecycle, budget, usage,
evidence, and blocked reason. Continuation decisions are private execution
records rather than a second client-visible source of truth.

### Over Aether

The harness can put an Aether gateway between the UI and the agent, so several
frontends — on several machines — drive one agent:

```bash
# the agent worker (needs a provider endpoint)
agent-harness --serve --aether 127.0.0.1:50051 --base-url $SAHARA_LLM_BASE_URL

# a terminal UI attached to it (needs no provider of its own)
agent-harness --tui --aether 127.0.0.1:50051
```

`--aether-standalone` runs both halves in one process — still talking over the
gateway — when you want the real transport without two terminals. A local
`aetherlite --dev` gateway accepts unauthenticated connections, so no token
setup is needed to try it.

The Aether worker accepts the same versioned attach/snapshot/replay protocol as
the web reference, carried in the messaging spec's multiplexed session frame.
Go clients can call `AttachSession` and consume `SessionEvents` for
cursor-bearing reconnects. Attach responses and session events return on the
attaching client lane; per-turn `tk::<workspace>::<task>::msg` routing is an
explicit library option reserved for real Aether tasks with subscribed
recipients. Worker and standalone modes expose that option as
`--aether-task-message-lanes`; leave it disabled for task-less clients. Aether
routing workspace and logical session workspace are kept separate, so a
transport-specific `--aether-workspace` does not change project storage identity.
The routing workspace plus agent implementation/specifier identify the Aether KV
namespace; use a stable `--aether-specifier` for replay across worker restarts.
The standalone mode's generated specifier intentionally makes its default KV
state ephemeral to that run. Checkpoints remain reserved for task hibernation
and hand-off snapshots; the active multi-writer event ledger uses KV because it
requires atomic create and compare-and-swap.

Aether worker and standalone modes also account every subagent invocation as a
real Aether task before child model or tool work starts. The default task is
self-assigned and executes in-process. The task
uses a stable admission idempotency key, one execution attempt, a session context
ID, bounded non-secret correlation metadata, and the parent turn's OBO authority
when one is available. Its ID is used on the child stream and in the
revision-3 `session.state.subagents.v1` record. A task transition is confirmed
before the corresponding lifecycle projection; an ambiguous terminal result is
projected as `interrupted` and is never automatically replayed. Restart recovery
queries terminal tasks and terminates abandoned non-terminal in-process work.

When the triggering turn is an active Aether task assigned to the worker, child
creation also requests native Aether `parent_task_id`. Aether validates the
request-scoped parent against the caller, workspace, and active lifecycle before
persisting the hierarchy; this works even though the long-lived worker remains
connected as its stable agent identity. The metadata copy remains useful for
inspection and non-Aether backends. The shared session protocol needs no extra
field for this: revision 3 already carries the child `task_id`, while Aether owns
task identity, idempotency, authorization, and parentage semantics.

Each child also carries the strict OSS
[`agent-harness.subagent.execution` v1 descriptor](docs/subagent-execution-v1.md)
in its Aether task payload. The descriptor contains workspace-isolated durable
input/result/checkpoint references, hashes, policy identity, and claim/recovery
rules—not prompt text or credentials. The runner persists the referenced child
input before task admission and resolves that same input for in-process
execution.

External execution is an explicit two-worker deployment. Both processes must
use the same MemoryLayer workspace, and named agent definitions must match (the
executor verifies their digest before claiming):

```bash
# Assignee. Its full topic is ag::dev::agent-harness::executor.
agent-harness --serve --aether 127.0.0.1:50051 --aether-workspace dev \
  --aether-specifier executor --memorylayer http://127.0.0.1:61001 \
  --subagent-executor --base-url "$SAHARA_LLM_BASE_URL"

# Parent. Ordinary spawn_subagent calls become targeted Aether tasks.
agent-harness --serve --aether 127.0.0.1:50051 --aether-workspace dev \
  --aether-specifier parent --memorylayer http://127.0.0.1:61001 \
  --subagent-target ag::dev::agent-harness::executor \
  --base-url "$SAHARA_LLM_BASE_URL"
```

The parent only observes Aether task state and reads the terminal result from
shared history. The assignee receives task-derived OBO authority on the typed
assignment field, claims exactly once, and never re-enters the parent's admission
path. A running task redelivered after an executor gap is failed for inspection,
not replayed. The descriptor, creator-supplied task metadata, and logs remain
prompt- and credential-free.

By default each client keeps a local copy of the conversation it witnessed, so a
second client attaching mid-conversation sees only what arrives after it
connects. Point both the worker and its clients at a shared MemoryLayer to give
them one conversation instead:

```bash
agent-harness --serve --aether 127.0.0.1:50051 --memorylayer http://127.0.0.1:61001 --base-url $SAHARA_LLM_BASE_URL
agent-harness --tui   --aether 127.0.0.1:50051 --memorylayer http://127.0.0.1:61001
```

The workspace (`--memorylayer-workspace`, defaulting to the resolved logical
workspace or `default`) is created on first use if MemoryLayer does not have it.
The OSS MemoryLayer adapter also implements the additive workspace-aware history
and thread-index interfaces: one process can address another visible workspace
per operation, and identical thread IDs remain isolated by the composite
workspace/thread key. The configured workspace remains the default for existing
single-workspace callers. New subagents use MemoryLayer's native
`parent_thread` hierarchy: the server mints the child ID before execution and
the harness adopts that canonical ID for lifecycle events, transcript storage,
kernel isolation, and resume handles. Without MemoryLayer, the same runner keeps
the local `<parent>::sub::<sequence>` fallback.

When MemoryLayer is configured, the reference process uses
`memorylayer.NewCatalogProvider` on the first turn in each resolved logical
workspace. It merges enabled MemoryLayer skills (including accepted addenda,
allowed tools, prerequisites, and preferred-model metadata) with filesystem
skills, then exposes enabled stdio MCP servers through the normal dynamic MCP
tool path. Filesystem skills win a same-name conflict. MCP processes start lazily
when their tools are discovered or called.

Each successful catalog is cached independently for the process lifetime; restart
the reference process to force a refresh after changing a remote catalog. A cold
catalog error is explicitly fail-open to filesystem skills for that turn and is
retried on the next turn. It is never satisfied from another workspace's cached
entry. An explicit `--memorylayer-workspace` maps only the selected logical
default to that backend workspace; other visible logical workspaces remain
independently addressable. Library users can apply the same mapping through
`catalog.BindWorkspaceProvider` or call `LoadWorkspace` directly.

MemoryLayer MCP entries are executable host configuration: anyone allowed to
change an enabled stdio server can choose a command and environment inherited by
the harness process. Give catalog-management authority only to principals that
are already trusted to configure code execution on that host.

With MemoryLayer wired, each turn also gets the memories it has distilled from
past conversations that are relevant to the current message (`--memory-recall`,
on by default). The harness does not write memories itself — it stores the
conversation, and MemoryLayer extracts from it.

In the TUI, type `@` followed by a path and use Tab/arrow keys to complete files
or directories. Paths resolve from `/pwd`; use `/cd <path>` to change that
virtual working directory without changing the process directory. An explicit
`/cd` may leave the original workspace: the selected directory becomes an
additional read/inspect/shell root for the live agent, while persistence,
skills, and `write_file`/`edit_file` remain rooted in the original workspace.
Referenced images are sent inline; other references are normalized to
workspace-relative or absolute granted paths. Press `Ctrl+C` or `Ctrl+D` twice
within one second to quit. `Ctrl+Left`/`Ctrl+Right` move the composer by words;
mouse reporting stays disabled so the terminal can perform native text
selection and copying. The mouse wheel scrolls transcript history in compatible
terminals without changing that selection behavior; Up/Down provide the same
three-line scrolling while the composer is empty.

The web server has **no authentication** and is intended for **localhost use
only** — do not bind it beyond loopback. State-changing endpoints enforce a
same-origin check (a request whose `Origin` doesn't match the host is rejected),
and shutdown drains any in-flight turn.

## Capabilities

Native tool-calling turn loop with bounded tool iterations, streaming egress,
cancellation, context compaction + token-aware overflow recovery, transcript
hygiene, provider error classification, in-process sub-agents, OpenClaw-style
slash commands, filesystem skills, MCP client, OpenTelemetry spans, and an
optional in-process scheduler for proactive work.

## Releases

`versions.yaml` is the single source of truth for this repo's version, its CI,
and its release artifacts; the files under `.github/workflows/` are generated
from it by [scitrera-repo-tools](https://github.com/scitrera/repo-tools) and
carry a "do not edit by hand" header for that reason.

```bash
python scripts/update-versions.py --check    # versions.yaml vs. the tree
python scripts/generate-ci-gha.py            # workflows vs. versions.yaml
python scripts/generate-ci-gha.py --force    # apply after editing versions.yaml
```

Pushing a `vX.Y.Z` tag runs the Go tests, cross-compiles `agent-harness` for
linux/amd64, linux/arm64, windows/amd64, windows/arm64 and darwin/arm64, and
attaches the archives plus a `checksums.txt` to the GitHub release. Each archive
carries the binary, `LICENSE`, `NOTICE` and this file; Windows ships as `.zip`
and everything else as `.tar.gz`. `agent-harness --version` reports the release
version and the commit it was built from.

Bump the version in `versions.yaml`, run `python scripts/update-versions.py` to
propagate it into `pkg/version/version.go`, then tag.

## Status

Extracted from a working internal runtime. APIs may shift before a tagged
release. Apache-2.0 licensed.
