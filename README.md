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
| `--workspace-index-dir` | `SAHARA_WORKSPACE_INDEX_DIR` | user config dir | shared canonical project-path to workspace-ID index used by project mode |
| `--serve` | — | `false` | run as a headless agent worker over Aether |
| `--aether` | `AETHER_ADDR` | — | Aether gateway address, e.g. `127.0.0.1:50051` |
| `--aether-standalone` | — | `false` | run the worker and the terminal UI in one process |
| `--memorylayer` | `MEMORYLAYER_BASE_URL` | — | store threads + transcripts in MemoryLayer instead of on disk |
| `--memory-recall` | — | `true` | inject MemoryLayer memories relevant to each message |

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
