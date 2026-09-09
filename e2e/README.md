# OSS E2E getting started

An Aether gateway, the harness serving on it, and MemoryLayer holding the
conversations — enough to run the harness the way it is meant to be deployed
(UI and agent in different processes, sharing durable history) without any
platform.

The stack uses only OSS components. By default, it pulls the latest published
multi-architecture agent-harness, AetherLite, MemoryLayer, and CPU embed-server
images from GHCR. This exercises the artifacts an outside user receives and
requires no sibling source checkouts. Developers changing these projects can
instead pass `--local` to rebuild the complete stack from local source; the
local harness build still resolves its Go dependencies from the public module
proxy.

## 1. Prerequisites

- Docker with Compose v2 and BuildKit support.
- This agent-harness checkout. Local-source mode additionally needs the Aether
  OSS and MemoryLayer OSS checkouts. Point the ignored `.local-deps/aether` and
  `.local-deps/memorylayer` symlinks at them, or override `AETHER_REPO` and
  `MEMORYLAYER_REPO`.
- An OpenAI-compatible model endpoint with a tool-capable model.
- Go only for the host TUI. Use the version required by `go.mod`; the Go
  toolchain may automatically select a newer compatible patch release.

## 2. Configure the provider and models

From this directory:

```bash
cp .env.example .env
$EDITOR .env
```

At minimum, set the fallback provider:

```dotenv
SAHARA_LLM_BASE_URL=http://host.docker.internal:11434/v1
SAHARA_LLM_API_KEY=
SAHARA_LLM_MODEL=replace-with-one-real-model-id
SAHARA_LLM_FORMAT=openai
```

`SAHARA_LLM_FORMAT` and each registry provider's `format` accept:

- `openai` for `/v1/chat/completions`;
- `responses` for `/v1/responses`, including typed Responses SSE; and
- `native` for the legacy Scitrera sidecar ChatMessages contract.

The `openai` and `responses` paths share the Apache-2.0
`github.com/scitrera/go-llm/client` transport and
`github.com/scitrera/go-llm/protocol` codecs. That keeps authentication,
attribution headers, tool-call decoding, usage normalization, stream liveness,
and backpressure behavior identical across the two public OpenAI protocols.
Use `responses` only against an endpoint that implements the Responses API (or
FoxSci Route, which can strictly translate representable stateless calls to a
Chat target). The native path remains for existing sidecar deployments.

`SAHARA_LLM_BASE_URL` is resolved inside the agent container. For a model server
on the host, keep `host.docker.internal` and make sure the server listens on an
address reachable from Docker, not only `127.0.0.1`.
Both host-only bases and versioned bases ending in `/v1` are accepted; the
harness normalizes the default chat-completions path without duplicating the
version segment.

Single-model operation needs nothing else. To exercise model selection, install
the example registry and replace all three placeholder IDs with model IDs the
endpoint actually serves:

```bash
mkdir -p workspace/config
cp models.example.yaml workspace/config/models.yaml
$EDITOR workspace/config/models.yaml
```

The smallest useful registry looks like this:

```yaml
default: fast-model-id
models:
  - name: fast-model-id
    tier: fast
    capabilities: {tools: true, vision: false}
  - name: strong-model-id
    tier: strong
    capabilities: {tools: true, vision: false}
    reasoning:
      default_effort: high
      allowed_efforts: [medium, high, xhigh, max]
  - name: vision-model-id
    tier: vision
    capabilities: {tools: true, vision: true}
```

All three entries above use the fallback `SAHARA_LLM_*` provider. When
`models.yaml` is present, its valid `default` wins over `SAHARA_LLM_MODEL`.
Automatic OSS policy is intentionally simple: keep the default when it satisfies
the turn, otherwise choose the first declared model satisfying the required
capabilities. `tier` is descriptive metadata; OSS does not apply cost routing.

Reasoning levels are request settings, not separate model entries. The optional
`reasoning.default_effort` supplies the model default and `allowed_efforts`
constrains process or `/reasoning` overrides. Use `/reasoning` to inspect the
effective setting, `/reasoning xhigh` to set it for the current thread and model,
and `/reasoning default` to clear the override.

Capabilities are operator assertions, not provider discovery. Set `tools: true`
only for models that support the OpenAI-compatible tool-call loop. Set
`vision: true` only when the model and provider accept image inputs. `context`
is optional; if supplied, use the provider's real context-window value because
the harness derives its compaction budget from it. Although `audio` is reserved
in the schema, the current reference UI's automatic capability routing is for
tools and images.

### Optional: different providers per model

Declare named providers and reference one from each model. Use `api_key_env` so
secrets remain in `.env`, not in the workspace YAML:

```yaml
default: fast-model-id
providers:
  - name: openai
    base_url: https://api.openai.com/v1
    api_key_env: OPENAI_API_KEY
    format: openai
  - name: responses
    base_url: https://api.openai.com/v1
    api_key_env: OPENAI_API_KEY
    format: responses
  - name: openrouter
    base_url: https://openrouter.ai/api/v1
    api_key_env: OPENROUTER_API_KEY
    format: openai
models:
  - name: fast-model-id
    provider: openrouter
    capabilities: {tools: true}
  - name: strong-model-id
    provider: openai
    capabilities: {tools: true}
  - name: vision-model-id
    provider: responses
    capabilities: {tools: true, vision: true}
```

Put the corresponding `OPENAI_API_KEY` and `OPENROUTER_API_KEY` values in
`.env`; Compose passes those variables only to the agent. Model names are sent
to the selected provider verbatim. A model without `provider` uses the fallback
`SAHARA_LLM_*` endpoint.

For a provider with `kind: openai_subscription`, start the stack and log in
through the repository wrapper:

```bash
./e2e/run.sh up
./e2e/run.sh auth login --profile personal
./e2e/run.sh auth status --profile personal
```

Run those commands from the repository root. The wrapper executes the auth
broker in the agent container, where it shares the worker's `sahara-auth` named
volume. The volume survives ordinary `down`/`up` and agent recreation; using
`docker compose down -v` removes it along with the other E2E data volumes.

## 3. Start the published stack

```bash
./e2e/run.sh up
docker compose -f e2e/docker-compose.yml ps
docker compose -f e2e/docker-compose.yml logs -f agent
```

The wrapper refreshes the published images before replacing the previous stack,
then enables the CPU embed server and semantic embeddings. The first start can
take several minutes while Docker pulls images and the embed server downloads
its model. Use `E2E_SOURCE_MODE=local` or `./e2e/run.sh up --local` to build the
same stack from the current harness checkout and the configured sibling
checkouts. `--published` explicitly overrides a persisted local mode.

Changing `workspace/config/models.yaml` or `.env` does not require an image
build, but the worker reads them at startup, so recreate it:

```bash
docker compose up -d --force-recreate agent
```

### Optional: scheduled worker turns

Aether's WorkflowEngine is enabled in this development stack. To declare a
scheduled turn, copy the example, explicitly enable the entry, and tell the
worker where the container-visible file lives:

```bash
cp schedules.example.yaml workspace/config/schedules.yaml
$EDITOR workspace/config/schedules.yaml
printf '\nSAHARA_SCHEDULE_CONFIG=/workspace/config/schedules.yaml\n' >> .env
docker compose up -d --force-recreate agent
```

Each declaration is upserted idempotently into Aether and targets the concrete
worker topic. Its payload carries a versioned logical binding to a
MemoryLayer-authoritative workspace view; it never carries the host's absolute
path. Clean Git views pin the current commit by default. Mutable directories
and dirty worktrees are rejected unless the declaration opts in with
`allow_mutable_view` or `allow_dirty_view` respectively. `miss_policy` defaults
to `fire_once`, which coalesces an outage backlog into one task. `skip` discards
a backlog containing multiple due occurrences and resumes at the next future
time, while `fire_all` emits one task per occurrence in durable batches of at
most 100 without discarding a larger backlog. Every emitted Aether task records
its scheduled occurrence and dispatch time and receives a deterministic
per-occurrence idempotency key. Supported schedule types are `cron`, `interval`,
and `once`.
`offline_policy` defaults to `queue`, which persists a due exact-target task
until this static worker reconnects without asking an orchestrator to launch
it. Use `reject` to fail creation while absent or `orchestrate` only when the
worker implementation is registered with Aether orchestration.

That workspace policy is rechecked when the task is admitted and before every
tool call. `allow_dirty_view: false` also makes the execution scope read-only,
so `write_file`, `edit_file`, `shell`, and `python` are denied before their first
mutation. A schedule that intentionally modifies its checkout must explicitly
set `allow_dirty_view: true`. The registration remains pinned to the revision
selected when the declaration is reconciled. If the checkout moves to another
revision, later fires fail closed until the worker is restarted or the schedule
file content changes and publishes a new view.

Editing a declaration changes its digest. Already queued work with an older
digest fails closed instead of running the new prompt or a replacement view.
The worker polls the file every two seconds by default; set
`SAHARA_SCHEDULE_RELOAD_INTERVAL` to another positive duration. Valid edits are
hot-reconciled, invalid edits leave the last valid declaration set in place,
and removed entries delete only schedules demonstrably owned by this exact
worker. `schedules: []` removes every owned schedule. `enabled: false` removes
one declaration's deterministic Aether schedule.

Schedule credentials never belong in this file. The OSS Aether channel exposes
an operation-aware `ScheduledTurnAuthorityProvider` for distributions that need
user/OBO execution: set `require_task_authority: true` and optionally
`required_downstream_authority_hops: 1`, then inject a provider that supplies
the transport-only authorization and bounded Aether scope. List, upsert, and
delete each ask the provider independently. A required-authority declaration
fails before reconciliation when no provider is configured, while the stock OSS
CLI continues to support direct schedules without one. The one-hop option is
for a scheduled turn that must delegate its task authority once more, such as
to an authorized catalog or remote tool service.

## 4. Attach the host TUI

In a second terminal, from the OSS repository root (`oss/`):

```bash
go run ./cmd/agent-harness \
  --tui \
  --workspace ./e2e/workspace \
  --aether 127.0.0.1:50051 \
  --aether-specifier e2e
```

The host process is only the client; the containerized worker performs turns.
MemoryLayer registers its REST API at the canonical Aether service topic
`sv::memorylayer`; the worker and host client auto-detect it over their existing
Aether connections. No separate MemoryLayer address is required. An explicit
`--memorylayer http://...` remains the direct-HTTP override, and
`--memorylayer-mode off` keeps history local even when Aether is present.
The specifier must match `AETHER_SPECIFIER` in `.env` (Compose defaults it to
`e2e`); otherwise the client targets a different Aether agent and waits for a
worker that is not running. The TUI's startup target should end in `::e2e`.
Starting a second TUI with the same command demonstrates that multiple clients
can attach to the same Aether session and read the MemoryLayer-backed history.
The host TUI also acts as an exact-window workspace tool host: the worker keeps
model execution and policy, while file/shell/Python calls return over Aether to
the client checkout named by the turn's logical execution binding.

To verify this transport deterministically without depending on a model to
choose a tool, run from the repository root while the stack is up:

```bash
./e2e/run.sh check
```

The check creates conflicting `identity.txt` files on a temporary client root
and worker root, registers/authorizes the client view through
`sv::memorylayer`, and asserts that the live Aether tool result contains the
client value. It then removes the client host and asserts an error instead of a
worker fallback.
It also creates a one-shot Aether schedule, publishes a temporary worker view
through MemoryLayer, and asserts that the resulting targeted background task
arrives with the exact worker/view binding. Neither scenario invokes a model.
Finally, it appends a failed refinement record through `sv::memorylayer` and
asserts that the deployed worker returns it through `/refinements --attention`
without entering the model path.
The catalog scenario uses a fresh logical workspace on every run and installs
only short-lived exact provider, entry, catalog-KV, and reply-routing ACL rules
through AetherLite's loopback-only dev admin API. It restores every prior rule
afterward, then verifies that the provider receives a newly derived user-OBO
child and uses it for a real MemoryLayer write.

## 5. Exercise model capabilities

In the TUI:

1. Enter `/model` to list the registry and show the active model.
2. Enter `/model strong-model-id` (using your real configured name), then send a
   text request to test a manual thread pin.
3. Put an image under `e2e/workspace/`, enter `/attach image-name.png`, and send
   “Describe this image.” A text-only default is automatically replaced for that
   turn by the first `vision: true, tools: true` model.
4. Enter `/model fast-model-id` to return the thread to the fast model. A pinned
   model that cannot satisfy an image turn is bypassed for that turn.
5. Enter `/cd /absolute/path/to/another/local/project`, then ask the model to
   list or read a file there. That directory is not mounted into the agent
   container: Sahara registers a MemoryLayer workspace view and the worker
   routes the tool call to this exact TUI. Stop the TUI while a bound tool is
   needed to verify that the worker fails the call instead of reading its own
   checkout.
6. Enter `/refinements --attention` to inspect failed or partially applied
   immutable refinement records from the MemoryLayer-authoritative audit. This
   is a model-free worker command; use its emitted opaque cursor command to
   continue a bounded page.

Useful observation commands:

```bash
docker compose logs -f agent
curl -fsS http://127.0.0.1:61001/health
```

If `/model` reports that switching is unavailable, the worker did not find
`/workspace/config/models.yaml`; check the host path and recreate `agent`. If a
named provider fails to construct, the worker logs a warning and uses the
fallback provider, so verify both the provider reference and its key env var
when testing provider separation.

## 6. What this E2E proves

- the OSS worker and host client communicate through Aether rather than an
  in-process channel;
- transcripts and recall use MemoryLayer, surviving an agent restart;
- both clients discover MemoryLayer through `sv::memorylayer` over Aether rather
  than requiring a separately configured HTTP endpoint;
- session state is shared across attached clients;
- an unmounted client checkout can service bound file/shell/Python tools over
  Aether without exposing its absolute path or falling back to a worker checkout;
- Aether's WorkflowEngine can create an exact-worker scheduled turn whose
  MemoryLayer view, declaration digest, and revision remain pinned at ingress;
- the worker can browse the bounded MemoryLayer-authoritative refinement audit
  through `/refinements` without relying on a model response or client cache;
- the opt-in model-free catalog test can publish an arbitrary provider agent,
  derive a per-invocation caller-OBO child, and use it on a real MemoryLayer
  request without routing the invocation through Sahara or Platform Bridge;
- the OSS executable loads `config/models.yaml`, supports `/model` pins, routes
  images by declared capability, and can resolve a provider per model;
- the default path interoperates across the latest published agent-harness,
  AetherLite, MemoryLayer, and embed-server containers;
- the explicit local path builds the agent against the same published Go
  dependency graph used by a clean OSS checkout while selecting the configured
  Aether and MemoryLayer source checkouts.

It does not prove production authentication, semantic retrieval quality with the
default lexical embedder, model execution for a scheduled prompt, or recovery
across a deliberately interrupted scheduled turn. The catalog test returns a
VFS-shaped reference stored as MemoryLayer metadata; it does not replace the
enterprise data-connector/materialization E2E.

Run that focused live test after `up` has started the stack:

```bash
AETHER_CATALOG_E2E=1 AETHER_E2E_ADDR=127.0.0.1:50051 \
  AETHER_E2E_ADMIN_URL=http://127.0.0.1:31880 \
  go test ./pkg/channels/aether \
  -run TestLiveAetherCatalogAgentMemoryLayerOBO -count=1 -v
```

## Services

| Service | Port (loopback) | What it is |
|---|---|---|
| `aether` | 50051, 31880 | AetherLite in dev mode — the broker between UI(s) and agent |
| `memorylayer` | 61001 | threads, transcripts, and the memories distilled from them |
| `tool-catalog` | — | deterministic catalog service with one reviewed model-free E2E authority profile |
| `agent` | — | the harness, `--serve`; dials out, exposes nothing |
| `embed-server` | — | real embeddings; enabled by `run.sh up`, opt-in with raw Compose |

## Published images and local source mode

The Compose defaults track these published GHCR tags:

```dotenv
AETHERLITE_IMAGE=ghcr.io/scitrera/aetherlite:dev-latest
MEMORYLAYER_IMAGE=ghcr.io/scitrera/memorylayer-server:latest
MEMORYLAYER_EMBED_IMAGE=ghcr.io/scitrera/memorylayer-embed-server:latest
AGENT_HARNESS_IMAGE=ghcr.io/scitrera/agent-harness:latest
```

`run.sh up` explicitly pulls the resolved references before startup. Set any of
the image variables in `.env` to pin a version or digest while retaining the
published-image path.

For coordinated development across repositories, use:

```bash
./e2e/run.sh up --local
```

That invokes `build.sh`, layering Aether locally as `aether:local` →
`aetherlite:local` → `aetherlite:dev-local`, and builds MemoryLayer, the CPU
embed server, and agent-harness with `:local` tags. Its default source layout is:

```
<this-repo>/.local-deps/aether       <- $AETHER_REPO
<this-repo>/.local-deps/memorylayer  <- $MEMORYLAYER_REPO
```

The AetherLite development tag owns the `aetherlite` entrypoint and enables
`AETHER_DEV`, `AETHER_INSECURE_ADMIN`, and their explicit safety opt-in. Keep it
on loopback; use the normal AetherLite tag with production configuration for a
real deployment.

## Embeddings

`run.sh up` enables the published CPU embed server and configures MemoryLayer's
384-dimensional semantic provider. Raw `docker compose up` retains
MemoryLayer's lightweight `hash` provider unless the embed profile and matching
environment values are selected explicitly; `hash` is lexical and is useful
for fast lifecycle/API checks, not retrieval-quality evaluation.

For real semantic embeddings (all-MiniLM-L6-v2, 384-d, CPU):

```bash
cd e2e
docker compose --profile embed pull
# in .env:
#   MEMORYLAYER_EMBEDDING_PROVIDER=embed_server
docker compose --profile embed up -d
```

The first request downloads model weights, so give it a few minutes. The
embedding width is **written into every stored vector**, so switching providers
against an existing store is not a config change — start from a fresh
`memorylayer-data` volume (`docker compose down -v`). The compose pins 384-d on
both providers precisely so this switch stays possible.

## Security

Dev mode: the gateway accepts unauthenticated connections, MemoryLayer runs with
REST auth disabled, and its Aether service connection uses `AETHER_AUTH=none`.
That is what makes the stack zero-setup — no token minting before the first turn
— and exactly why every port is bound to `127.0.0.1`. This is a development
stack, not a deployment template.

## Reset and troubleshooting

- `workspace/` is bind-mounted into the agent, so you can drop in skills, edit
  `AGENTS.md`, and read what the agent writes.
- Stop while preserving Aether/MemoryLayer data with `docker compose down`.
- Wipe Aether/MemoryLayer data with `docker compose down -v`. This is destructive
  to this E2E stack's named volumes but does not remove `workspace/`.
- Validate the rendered Compose configuration with `docker compose config`.
- If every turn waits indefinitely, confirm the startup target ends in the same
  specifier as the agent's `--aether-specifier` setting (default `::e2e`).
- Provider failures are rendered as `Turn failed: ...` in the TUI and remain in
  history. Inspect `docker compose logs agent` for the corresponding worker log.
