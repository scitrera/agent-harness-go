# Local agent stack

An Aether gateway, the harness serving on it, and MemoryLayer holding the
conversations — enough to run the harness the way it is meant to be deployed
(UI and agent in different processes, sharing durable history) without any
platform.

```bash
./build.sh                              # build the images
cp .env.example .env && $EDITOR .env    # point at your LLM endpoint
docker compose up -d

# attach from the host
agent-harness --tui --aether 127.0.0.1:50051 --memorylayer http://127.0.0.1:61001
```

The terminal UI runs on the **host** and attaches over Aether. That is the whole
point of the split: the agent is a service, and a UI is a client of it. Attach
several at once — with MemoryLayer wired they share one conversation rather than
each seeing only what arrived after it connected.

| Service | Port (loopback) | What it is |
|---|---|---|
| `aether` | 50051, 31880 | AetherLite in dev mode — the broker between UI(s) and agent |
| `memorylayer` | 61001 | threads, transcripts, and the memories distilled from them |
| `agent` | — | the harness, `--serve`; dials out, exposes nothing |
| `embed-server` | — | opt-in, real embeddings (`--profile embed`) |

## Images are built locally, for now

None of the OSS images are published yet, so `build.sh` builds all of them from
sibling checkouts and tags them `:local`:

```
<root>/scitrera-app-monorepo2/agent-harness/oss   <- this repo
<root>/scitrera-aether3-go/oss-repo               <- $AETHER_REPO
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

## Notes

- `workspace/` is bind-mounted into the agent, so you can drop in skills, edit
  `AGENTS.md`, and read what the agent writes.
- `SAHARA_LLM_BASE_URL` may name a server on the host — `host.docker.internal`
  is wired up for Linux too. If you use it, make sure that server listens on
  more than `127.0.0.1`, or the container cannot reach it.
- Wipe everything with `docker compose down -v`.
