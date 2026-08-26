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

For the full OSS local stack—AetherLite transport, MemoryLayer persistence, a
containerized agent worker, a host TUI, and a capability-aware multi-model
`config/models.yaml`—follow the [OSS E2E getting-started guide](e2e/README.md).
The reference CLI can also use an OpenAI Plus/Pro ChatGPT subscription through
the OSS credential broker; see [OpenAI subscription authentication](docs/openai-subscription-auth.md).
Operators deploying the Aether live tool catalog can configure reviewed,
per-tool downstream caller authority using the
[tool catalog service guide](docs/tool-catalog-service.md).

### Interfaces

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `--addr` | `SAHARA_WEB_ADDR` | `127.0.0.1:8787` | web UI listen address |
| `--tui` | — | default | run the Bubble Tea terminal UI |
| `--cli` | — | `false` | run the stdin REPL |
| `--acp` | — | `false` | run as an ACP agent over stdio |
| `--web` | — | `false` | run the localhost web UI |
| `--no-browser` | — | `false` | don't auto-open a browser (web mode) |
| `--workspace-mode` | `SAHARA_WORKSPACE_MODE` | `single` | `single` preserves the legacy layout; `project` derives a stable logical workspace from the configured workspace root/Git and enables TUI project switching |
| `--workspace-id` | `SAHARA_WORKSPACE_ID` | — | pin a logical workspace ID and enable composite workspace/thread state |
| `--visible-workspaces` | `SAHARA_VISIBLE_WORKSPACES` | — | comma-separated additional logical workspaces clients may explicitly address |
| `--workspace-index-dir` | `SAHARA_WORKSPACE_INDEX_DIR` | user config dir | shared canonical project-path to workspace-ID index used by project mode |
| `--skills-dirs` | `SAHARA_SKILLS_DIRS` | `skills,.agent-harness-skills` | comma-separated replacement list of workspace-relative skill roots |
| `--system-skills-dirs` | `SAHARA_SYSTEM_SKILLS_DIRS` | — | comma-separated absolute operator skill roots, appended after workspace roots with read-only access |
| `--commands-dirs` | `SAHARA_COMMANDS_DIRS` | `commands,.agent-harness-commands` | comma-separated replacement list of workspace-relative slash-command roots |
| `--system-commands-dirs` | `SAHARA_SYSTEM_COMMANDS_DIRS` | — | comma-separated absolute operator command roots appended after workspace roots |
| `--models-file` | `SAHARA_MODELS_FILE` | `config/models.yaml` | explicit absolute or workspace-relative model registry; an explicitly selected missing file is an error |
| `--reasoning-effort` | `SAHARA_REASONING_EFFORT` | provider/model default | process-wide reasoning preference; model allowlists can narrow it and `/reasoning` can override it per thread and model |
| `--tui-retain-reasoning` | `SAHARA_TUI_RETAIN_REASONING` | `false` | retain `reasoning>` rows after the response or tool action they led to; by default they are visible only while current |
| `--tui-shell-trigger-agent` | `SAHARA_TUI_SHELL_TRIGGER_AGENT` | `false` | process-wide default for whether an idle `!command` starts an agent response after recording its result |
| `--tui-shell-preferences-file` | `SAHARA_TUI_SHELL_PREFERENCES_FILE` | user config dir | persistent per-user and per-workspace/thread overrides for idle shell responses |
| `--serve` | — | `false` | run as a headless agent worker over Aether |
| `--aether` | `AETHER_ADDR` | — | Aether gateway address, e.g. `127.0.0.1:50051` |
| `--aether-standalone` | — | `false` | run the worker and the terminal UI in one process |
| `--aether-task-message-lanes` | — | `false` | route turns carrying real Aether task IDs to their subscribed per-task message lanes |
| `--subagent-target` | `SAHARA_SUBAGENT_TARGET` | — | target spawned subagents at a full Aether agent topic; requires MemoryLayer |
| `--subagent-executor` | — | `false` | consume targeted agent-harness subagent tasks on this worker; requires MemoryLayer |
| `--subagent-executor-concurrency` | — | `4` | bound concurrent externally assigned subagents |
| `--memorylayer-mode` | `MEMORYLAYER_MODE` | `auto` | prefer an explicit HTTP URL, otherwise discover MemoryLayer over Aether; `off`, `http`, and `aether` are explicit policies |
| `--memorylayer` | `MEMORYLAYER_BASE_URL` | — | explicit direct-HTTP URL; takes priority in `auto` mode |
| `--memorylayer-target` | `MEMORYLAYER_TARGET_TOPIC` | `sv::memorylayer` | bare or instance-pinned Aether service topic |
| `--prompt-notes-authority` | `SAHARA_PROMPT_NOTES_AUTHORITY` | `local` | reusable prompt-note authority: `off`, `local`, or `memorylayer`; never dual-writes or falls back |
| `--agent-specifications-authority` | `SAHARA_AGENT_SPECIFICATIONS_AUTHORITY` | `local` | reusable subagent-definition authority: `off`, local workspace files, or typed MemoryLayer resources |
| `--memory-recall` | — | `true` | inject MemoryLayer memories relevant to each message |
| `--goal-max-continuations` | — | `3` | maximum automatic follow-up turns for one durable goal; `0` disables follow-ups |

### Operator-provided skills, commands, and model metadata

Workspace and operator roots are intentionally separate. `SAHARA_SKILLS_DIRS`
and `SAHARA_COMMANDS_DIRS` are ordered, comma-separated replacement lists whose
entries must remain relative to the selected workspace. Their `SYSTEM`
counterparts accept only absolute paths and are appended after every workspace
root, so a project-local skill or slash command wins a same-name collision.
Missing system roots are skipped. System skill roots are registered as
read-only file-tool roots; system command bodies are eagerly loaded and do not
receive a filesystem grant.

For example, a launcher can contribute versioned assets without writing into a
user's checkout:

```bash
export SAHARA_SYSTEM_SKILLS_DIRS="$XDG_CACHE_HOME/my-launcher/skills/v1"
export SAHARA_SYSTEM_COMMANDS_DIRS="$XDG_CACHE_HOME/my-launcher/commands/v1"
export SAHARA_MODELS_FILE="$XDG_CACHE_HOME/my-launcher/models/intent.yaml"
agent-harness --workspace-mode project --workspace /path/to/project
```

An absolute `SAHARA_MODELS_FILE` is read in place; a relative value resolves
against the workspace. The conventional `config/models.yaml` remains optional,
but an explicit flag/env path fails fast when missing so an operator typo cannot
silently restore single-model behavior. These inputs are process-local
configuration and do not add ecosystem messaging protocol fields.

Reasoning effort is request-scoped, so variants such as `high` and `xhigh` do
not require duplicate model entries. A registry entry can declare
`reasoning.default_effort` and `reasoning.allowed_efforts`; the runner resolves
the effective value from a `/reasoning` thread override, the process preference,
the model default, and finally the provider default. `/reasoning default` clears
the current thread/model override. See the
[OpenAI subscription guide](docs/openai-subscription-auth.md#configure-reasoning-effort)
for an example.

### Project workspaces

Multi-workspace state is optional. The default `single` mode retains the
existing unscoped local history layout. For programming use, start the harness
from a repository with project mode enabled:

```bash
cd /path/to/project
agent-harness --workspace-mode project --workspace .
```

Project mode canonicalizes the configured workspace directory, prefers its Git worktree
root, and persists a stable path-to-workspace assignment in the shared workspace
index. Repositories with the same directory name receive distinct stable IDs.
In the TUI, `/cd` into another project resolves that ID before changing state,
refreshes only that workspace's thread registry, and loads only its composite
workspace/thread history. Separate worktrees of one repository share the
logical workspace and retain distinct execution views. A pinned
`--workspace-id` intentionally disables automatic project switching.
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

An Aether-connected TUI registers its local checkout as a workspace view and
binds each turn to `(workspace_id, view_id, tool_host_id,
relative_directory)`. File, directory, shell, and Python tools then execute on
that exact client window, not in the worker container. When MemoryLayer is
available it stores the durable view plus the client's current Git observation
and authorizes dynamically discovered project workspaces. Without MemoryLayer,
the configured default and `--visible-workspaces` policy remains authoritative.
Client and worker tool hosts refresh those observations every two minutes before
their five-minute leases expire. Cancelling an in-flight turn also sends the
spec-defined `tool_cancel` envelope to the exact client host; cross-host calls
reuse the same narrow Aether OBO authorization, and the client matches source,
address, call ID, and OBO subject before stopping local execution.
The ordinary in-process TUI/CLI/web/ACP modes do not require either service.
The filesystem reference store provides the same workspace-isolated TUI thread
discovery when MemoryLayer is absent. In project mode, an in-process TUI grants
structured writes only after the user explicitly selects the external project;
single mode retains the narrower read/inspect/shell external-directory grant.

The OSS remote-tool policy is deliberately single-user and exact-window: the
turn must originate from the `tool_host_id` named by its binding, and that host
accepts calls only from its configured Sahara agent. An execution binding is
not an access grant. Enterprise compositions can replace both authorization
seams to apply Aether ACLs and gateway-validated on-behalf-of identity for
same-user, named-principal/group, or workspace-member sharing while retaining
the same binding and MemoryLayer view model. Message-address `user_id` is never
used as authenticated identity.

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
execution. The descriptor pins the parent's exact workspace execution view and
write ceiling. Synchronous and detached children inherit it by default;
`permission_mode: read_only` may narrow access, while a different view or a
write-access expansion requires the host's explicit authorization seam. The
scope is also stamped on background completion turns so the parent cannot
silently resume against the worker's default checkout. The contract remains
unreleased, so replaced local shapes are not retained as compatibility formats.

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
prompt- and credential-free. A bound external child is validated and decorated
with its exact tool host before claim; missing binders, mismatched worker hosts,
or client-hosted views without binding/OBO providers fail closed.

By default each client keeps a local copy of the conversation it witnessed, so a
second client attaching mid-conversation sees only what arrives after it
connects. When MemoryLayer is registered as `sv::memorylayer`, pointing both
processes at Aether is enough to give them one conversation:

```bash
agent-harness --serve --aether 127.0.0.1:50051 --base-url $SAHARA_LLM_BASE_URL
agent-harness --tui   --aether 127.0.0.1:50051
```

The default `--memorylayer-mode auto` prefers an explicit
`--memorylayer http://...`, then probes `--memorylayer-target` over the existing
Aether connection. It falls back to local files only when wildcard discovery
proves that no healthy MemoryLayer service is registered. Authentication,
authorization, timeout, protocol, and server failures remain startup errors.
Select `--memorylayer-mode aether` to require the service, or
`--memorylayer-mode off` to preserve local history while still using Aether.
With no Aether and no explicit URL, `auto` resolves directly to `off`; the OSS
TUI/CLI/web/ACP standalone paths therefore open neither dependency.

The workspace (`--memorylayer-workspace`, defaulting to the resolved logical
workspace or `default`) is created on first use if MemoryLayer does not have it.
The OSS MemoryLayer adapter also implements the workspace-aware history
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

### Reusable prompt notes

Prompt notes are workspace-scoped supplemental instructions resolved once per
turn and reused by every model call/retry in that turn. They are ordered by
stable `key`, bounded to 128 enabled notes and
64 KiB total content, and rendered before the current request's instructions.
An invalid or unavailable selected authority fails prompt assembly; Sahara does
not silently omit authoritative guidance or read a different backend.

The default `local` authority reads `prompt-notes.json` below the workspace's
harness state directory. In legacy single-workspace mode that is
`<state-dir>/prompt-notes.json`; scoped workspaces use the same encoded
`<state-dir>/workspaces/<workspace>/` layout as other local state. A missing file
means no notes. The file is intentionally simple and can be managed directly:

```json
{
  "schema_version": 1,
  "workspace_id": "project-a",
  "notes": [
    {
      "key": "project-conventions",
      "title": "Project conventions",
      "content": "Run the focused tests after changing the parser.",
      "enabled": true
    }
  ]
}
```

Select `memorylayer` to read current heads from MemoryLayer's typed,
revisioned `/v1/prompt-notes` API through its Go SDK and opaque cursors:

```bash
agent-harness --memorylayer http://127.0.0.1:61001 \
  --prompt-notes-authority memorylayer \
  --base-url "$SAHARA_LLM_BASE_URL"
```

`memorylayer` requires an active MemoryLayer route—an explicit `--memorylayer`
URL or the discovered/required Aether service. `off` disables prompt notes
explicitly.
The resolved turn workspace is forwarded on every read, including additional
visible workspaces, and an explicit `--memorylayer-workspace` remaps only the
selected logical default. This resource protocol remains a MemoryLayer API and
does not add a session-message type to the ecosystem messaging spec.

Local refinement mutations keep their accepted idempotency results in the same
atomically replaced `prompt-notes.json` document as the note heads. Exact replay
is count-bounded: when 512 receipts are retained, the next successful mutation
compacts the oldest receipts down to 384 and records the policy version,
generation, and total compacted count in `refinement_operation_journal` before
adding its result. A retained operation ID returns its original result and a
conflicting reuse is rejected. Once a receipt has expired, exact replay and
operation-ID conflict detection are no longer promised; current ETags, stable
keys, and retained delete tombstones still fail closed against stale mutations.
Operation IDs must never be intentionally reused. This file authority remains
single-writer; use MemoryLayer for durable multi-process mutation authority.

### Reusable agent specifications

Named `spawn_subagent(agent=...)` definitions use one explicit authority. The
default `local` authority retains the independently useful `agents/` or
`.agent-harness-agents/` JSON catalog below the selected project workspace.
`off` leaves only generic unnamed subagents. Select `memorylayer` to resolve
enabled, revisioned `/v1/agent-specifications` heads for the request's logical
workspace:

```bash
agent-harness --memorylayer http://127.0.0.1:61001 \
  --agent-specifications-authority memorylayer
```

The typed resource key is the immutable agent type. Revisions carry display
name, purpose, instructions, invocation guidance, model/turn limits, tool
policy, skills, MCP servers, permission mode, and background preference. The
adapter follows opaque cursors within explicit bounds, forwards per-turn OBO
authority, validates every enabled definition, sorts types deterministically,
and never falls back to local files after remote selection. Multi-workspace
turns resolve the catalog from the address on the actual `spawn_subagent` call.

### Refinement audit records

`pkg/refinement` defines evidence-backed plans and an append-only,
workspace-aware `Store`. `refinement.NewFileStore` persists immutable JSONL
records below the same encoded workspace state layout, with exact operation-ID
replay, stable hashes/ETags, restart validation, and conflict detection. The
MemoryLayer adapter uses the typed `/v1/refinement-records` API through the Go
SDK. Proposal, decision, application, correction, and rollback are separate
linked records; rollback never rewrites history.

Both authorities expose the same bounded, newest-first query contract. Filters
for phase, outcome, scope, resource kind, exact refinement lineage, and bounded
text search are ANDed across fields and use authority-owned opaque cursors. The
model can call `query_refinement_records`; operators can inspect the same source
without a model turn:

```text
/refinements --attention --limit 20
/refinements --refinement refine-123
/refinements --resource skill --search evidence
```

`--attention` selects immutable `failed` and `partially_applied` application
records and points the operator at the exact record and current resource heads
before any retry or rollback. Use `/refinements --help` for all filters. The OSS
executable defaults to its local audit so standalone TUI/CLI/web/ACP operation
remains dependency-free. The E2E stack explicitly selects MemoryLayer, making
`sv::memorylayer` the authoritative multi-process audit without dual writes or
fallback after selection.

The store intentionally does not execute edits or represent a task's live
lifecycle. Hosts keep approval, proposal application, recovery, and operational
CAS with their execution authority (Aether in distributed deployments).

### Branch-aware execution ledger

`pkg/executionledger` keeps a bounded operational record separate from streamed
chat events and transcript history. Events have stable IDs, optional parent IDs,
and a per-turn branch ID. Concurrent turns therefore share the last terminal
ancestor while each model-call, compaction, authority reference, recovery
marker, and terminal outcome remains on its own immutable chain. Token deltas do
not consume this retention budget.

Standalone OSS modes persist the ledger atomically under
`<state-dir>/execution-ledger/`. Aether worker modes use the same contract over
Aether KV CAS. MemoryLayer continues to own chat history and the authoritative
goal/refinement records; ledger events carry only stable references such as a
goal ID, refinement record ID, or exact turn-journal revision. There is no dual
write of those records.

Operators can inspect one newest-first bounded page without invoking a model:

```text
/ledger --limit 20
/ledger --type recovery_marker,context_compacted
/ledger --task task-123
/ledger --reference ref-123
```

Use `/ledger --help` for the complete filter and opaque-cursor shape. `/model
MODEL_NAME` pins are stored in the same operational authority and survive a
runner restart. Removing that model from the configured registry makes the
runner ignore the stale pin and use normal selection; the immutable event
remains available for audit.

This ledger is a private harness execution-plane contract. It does not change
the ecosystem session protocol: a future spec revision is warranted only if
branch attach/replay becomes a portable, client-negotiated capability.

With MemoryLayer wired, each turn also gets the memories it has distilled from
past conversations that are relevant to the current message (`--memory-recall`,
on by default). The harness does not write memories itself — it stores the
conversation, and MemoryLayer extracts from it.

In the TUI, type `@` followed by a path and use Tab/arrow keys to complete files
or directories. Paths resolve from `/pwd`; use `/cd <path>` to change that
virtual working directory without changing the process directory. An explicit
`/cd` may leave the original workspace. In default single mode, an in-process
TUI treats that directory as an additional read/inspect/shell root while
structured writes remain rooted in the launch workspace. In project mode,
`/cd` atomically selects the containing logical workspace, its thread/history
partition, and an explicitly writable local project root; switching to an
external project waits until active turns finish or are cancelled. In an
Aether-connected TUI, the client registers the containing project/worktree as a
separate logical view; all
built-in workspace tools execute against that client-owned view, and no client
absolute path is sent to or dereferenced by the worker. If the bound client
disconnects, the tool call fails rather than silently falling back to the
worker's checkout.
Referenced images are sent inline; other references are normalized to
workspace-relative or absolute granted paths. Press `Ctrl+C` or `Ctrl+D` twice
within one second to quit. `Ctrl+Left`/`Ctrl+Right` move the composer by words;
mouse reporting is enabled so the wheel scrolls transcript history independently
of composer message recall. Hold Shift while dragging to use terminal-native
text selection and copying. Up/Down continue to move through composer and sent
message history.

Prefix input with `!` to execute it explicitly through `/bin/bash -lc` in the
current `/pwd`, for example `!git status`. The TUI shows a `shell>` row and
records the command, working directory, exit status, and combined output as a
user-context message (transcript output is capped at 1 MiB). If a model turn is
active when the command completes, the result is delivered as steering at the
next model-action boundary. Otherwise the resolved shell-response preference
commits the context without calling the provider by default, or starts a normal
agent turn when enabled. Resolution is thread override, then user override,
then the process flag/environment default. Use `/shell-response status`, `/shell-response
on|off|default` for the current thread, or `/shell-response user
on|off|default` for the current user. Preferences are shared by local and
Aether-client TUI modes. Because `!` is an explicit user shell action, it does
not use the model-tool approval policy.

The web server has **no authentication** and is intended for **localhost use
only** — do not bind it beyond loopback. State-changing endpoints enforce a
same-origin check (a request whose `Origin` doesn't match the host is rejected),
and shutdown drains any in-flight turn.

An Aether-backed worker also serves model-free `/schedules` and `/runs`
inspection commands. `/schedules` reads definitions and the latest occurrence
decision from Aether WorkflowEngine; `/runs` joins Aether task state with the
Aether KV-backed turn journal and MemoryLayer thread metadata when present. Use
`/runs --help` for status filters, bounded page sizes, and opaque cursor
continuation. The TUI only renders the worker response—it does not query or
cache private scheduler state. Aether-free modes remain supported and report
that distributed scheduled operations are unavailable.

Embedded distributions can inject a `ScheduledTurnAuthorityProvider` into the
Aether channel. It keeps workflow CRUD authorization and bounded scheduled-task
authority on Aether's transport envelope; schedule YAML, action JSON, task
payloads, metadata, and ecosystem messages remain credential-free. Direct
schedules stay available without a provider, while declarations that set
`require_task_authority` fail closed unless one is configured.

## Capabilities

Native tool-calling turn loop with bounded tool iterations, streaming egress,
cancellation, context compaction + token-aware overflow recovery, transcript
hygiene, provider error classification, in-process sub-agents, OpenClaw-style
slash commands, filesystem skills, MCP client, OpenTelemetry spans, and optional
Aether WorkflowEngine schedules pinned to MemoryLayer workspace views. The
older in-process scheduler remains an embedding seam; durable server operation
uses Aether tasks and their lifecycle instead.

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
