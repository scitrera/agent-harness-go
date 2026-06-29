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

By default it serves a browser chat UI (the **web** channel) on `127.0.0.1:8787`
and opens a browser; pass `--tui` for the terminal UI or `--cli` to run the
stdin REPL instead. `--base-url` and `--model` can also be passed as flags
(overriding the env). It seeds default
workspace files (SOUL/AGENTS/…) on first run, persists history under
`<workspace>/.agent-harness`, and streams replies to the active channel (SSE to
the browser, the terminal UI, or stdout in `--cli` mode).

### Web UI

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `--addr` | `SAHARA_WEB_ADDR` | `127.0.0.1:8787` | web UI listen address |
| `--cli` | — | `false` | run the stdin REPL instead of the web UI |
| `--tui` | — | `false` | run the Bubble Tea terminal UI instead of the web UI |
| `--no-browser` | — | `false` | don't auto-open a browser (web mode) |

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

## Status

Extracted from a working internal runtime. APIs may shift before a tagged
release. Apache-2.0 licensed.
