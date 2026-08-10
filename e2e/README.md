# OSS E2E getting started

An Aether gateway, the harness serving on it, and MemoryLayer holding the
conversations — enough to run the harness the way it is meant to be deployed
(UI and agent in different processes, sharing durable history) without any
platform.

The stack uses only the OSS components. The local agent image is deliberately
built with the sibling ecosystem-spec, Aether, and MemoryLayer source trees, so
it exercises the current `replace` graph before coordinated releases exist.

## 1. Prerequisites

- Docker with Compose v2 and BuildKit support.
- The agent-harness, ecosystem messaging spec, Aether OSS, and MemoryLayer OSS
  checkouts. `build.sh` defaults to the layout used by this monorepo; override
  `ECOSYSTEM_SPEC_REPO`, `AETHER_REPO`, or `MEMORYLAYER_REPO` if yours differs.
- An OpenAI-compatible model endpoint with a tool-capable model.
- Go only for the host TUI. In this checkout, the known toolchain is
  `/home/drew/sdk/go1.25.10/bin/go`; Go may auto-select the newer patch version
  required by `go.mod`.

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

`SAHARA_LLM_BASE_URL` is resolved inside the agent container. For a model server
on the host, keep `host.docker.internal` and make sure the server listens on an
address reachable from Docker, not only `127.0.0.1`.

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
  - name: vision-model-id
    tier: vision
    capabilities: {tools: true, vision: true}
```

All three entries above use the fallback `SAHARA_LLM_*` provider. When
`models.yaml` is present, its valid `default` wins over `SAHARA_LLM_MODEL`.
Automatic OSS policy is intentionally simple: keep the default when it satisfies
the turn, otherwise choose the first declared model satisfying the required
capabilities. `tier` is descriptive metadata; OSS does not apply cost routing.

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
    provider: openai
    capabilities: {tools: true, vision: true}
```

Put the corresponding `OPENAI_API_KEY` and `OPENROUTER_API_KEY` values in
`.env`; Compose passes those variables only to the agent. Model names are sent
to the selected provider verbatim. A model without `provider` uses the fallback
`SAHARA_LLM_*` endpoint.

## 3. Build and start the local replacement graph

```bash
./build.sh
docker compose up -d
docker compose ps
docker compose logs -f agent
```

The first build can take several minutes. The default MemoryLayer `hash`
embedding provider avoids a model download; it is suitable for lifecycle and
API testing, not semantic-retrieval quality.

Changing `workspace/config/models.yaml` or `.env` does not require an image
build, but the worker reads them at startup, so recreate it:

```bash
docker compose up -d --force-recreate agent
```

## 4. Attach the host TUI

In a second terminal, from the OSS repository root (`oss/`):

```bash
/home/drew/sdk/go1.25.10/bin/go run ./cmd/agent-harness \
  --tui \
  --workspace ./e2e/workspace \
  --aether 127.0.0.1:50051 \
  --memorylayer http://127.0.0.1:61001
```

The host process is only the client; the containerized worker performs turns.
Starting a second TUI with the same command demonstrates that multiple clients
can attach to the same Aether session and read the MemoryLayer-backed history.

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
- session state is shared across attached clients;
- the OSS executable loads `config/models.yaml`, supports `/model` pins, routes
  images by declared capability, and can resolve a provider per model;
- the build consumes the untagged sibling source graph used during development.

It does not prove production authentication, semantic retrieval quality with the
default lexical embedder, or scheduled workflow behavior. The compose file keeps
Aether's WorkflowEngine disabled; the planned scheduled/cron refinement and
reconciliation slice needs a dedicated scenario when implemented.

## Services

| Service | Port (loopback) | What it is |
|---|---|---|
| `aether` | 50051, 31880 | AetherLite in dev mode — the broker between UI(s) and agent |
| `memorylayer` | 61001 | threads, transcripts, and the memories distilled from them |
| `agent` | — | the harness, `--serve`; dials out, exposes nothing |
| `embed-server` | — | opt-in, real embeddings (`--profile embed`) |

## Local images and future releases

None of the OSS images are published yet, so `build.sh` builds all of them from
sibling checkouts and tags them `:local`:

```
<root>/scitrera-app-monorepo2/agent-harness/oss   <- this repo
<root>/scitrera-app-monorepo2/scitrera-ecosystem-messaging-spec
                                                   <- $ECOSYSTEM_SPEC_REPO
<root>/scitrera-app-monorepo2/backend/scitrera-aether3-go/oss-repo
                                                   <- $AETHER_REPO
<root>/scitrera-memorylayer-ai-cc/oss             <- $MEMORYLAYER_REPO
```

Every image reference in `docker-compose.yml` is a variable, so when the images
ship this stops being a build step and becomes an `.env` edit:

```bash
AETHERLITE_IMAGE=scitrera/aetherlite:0.2.3
MEMORYLAYER_IMAGE=scitrera/memorylayer-server:0.2.0
AGENT_HARNESS_IMAGE=scitrera/agent-harness:0.1.0
```

## Embeddings

The default is MemoryLayer's `hash` provider, which is **lexical** — it matches
shared words, not meaning. That is fine for exercising the API and seeing recall
wired end to end, and it keeps first-run to a couple of minutes.

For real semantic embeddings (all-MiniLM-L6-v2, 384-d, CPU):

```bash
./build.sh --embed
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

Dev mode: the gateway accepts unauthenticated connections and MemoryLayer runs
with auth disabled. That is what makes the stack zero-setup — no token minting
before the first turn — and exactly why every port is bound to `127.0.0.1`. This
is a development stack, not a deployment template.

## Reset and troubleshooting

- `workspace/` is bind-mounted into the agent, so you can drop in skills, edit
  `AGENTS.md`, and read what the agent writes.
- Stop while preserving Aether/MemoryLayer data with `docker compose down`.
- Wipe Aether/MemoryLayer data with `docker compose down -v`. This is destructive
  to this E2E stack's named volumes but does not remove `workspace/`.
- Validate the rendered Compose configuration with `docker compose config`.
