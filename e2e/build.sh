#!/usr/bin/env bash
#
# Build the stack's images from local source.
#
# TEMPORARY. The OSS images are not published yet, so everything is built from
# the sibling checkouts and tagged `:local`. When they ship, delete nothing —
# just point the *_IMAGE variables in .env at the published tags and stop running
# this. The compose file already reads them.
#
# Layout assumed (override with the env vars below):
#
#   <root>/scitrera-app-monorepo2/agent-harness/oss   <- this repo
#   <root>/scitrera-app-monorepo2/scitrera-ecosystem-messaging-spec
#                                                    <- ECOSYSTEM_SPEC_REPO
#   <root>/scitrera-app-monorepo2/backend/scitrera-aether3-go/oss-repo
#                                                    <- AETHER_REPO
#   <root>/scitrera-app-monorepo2/llm-gateway/go-llm <- GO_LLM_REPO
#   <root>/scitrera-memorylayer-ai-cc/oss             <- MEMORYLAYER_REPO
#
# Usage:
#   ./build.sh              # aetherlite-dev + memorylayer + agent-harness
#   ./build.sh --embed      # also the CPU embed server (large; slow first build)
#   ./build.sh --only agent # just one of: aether | memorylayer | agent | embed

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oss_repo="$(cd "$here/.." && pwd)"
monorepo_root="$(cd "$oss_repo/../.." && pwd)"

ECOSYSTEM_SPEC_REPO="${ECOSYSTEM_SPEC_REPO:-$monorepo_root/scitrera-ecosystem-messaging-spec}"
AETHER_REPO="${AETHER_REPO:-$monorepo_root/backend/scitrera-aether3-go/oss-repo}"
MEMORYLAYER_REPO="${MEMORYLAYER_REPO:-$HOME/scitrera-memorylayer-ai-cc/oss}"
GO_LLM_REPO="${GO_LLM_REPO:-$monorepo_root/llm-gateway/go-llm}"

want_embed=0
only=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --embed) want_embed=1; shift ;;
    --only)  only="${2:-}"; shift 2 ;;
    -h|--help) sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

build() {
  local name="$1" want="$2"
  [[ -z "$only" || "$only" == "$name" ]] && [[ "$want" == "yes" ]]
}

fail_missing() {
  echo "error: $1 not found at $2" >&2
  echo "       set $3=<path> if your checkout lives elsewhere" >&2
  exit 1
}

verify_aether_image_contract() {
  local normal_image="scitrera/aetherlite:local"
  local dev_image="scitrera/aetherlite:dev-local"
  local normal_env dev_env expected

  [[ "$(docker image inspect "$normal_image" --format '{{json .Config.Entrypoint}}')" == '["aetherlite"]' ]] || {
    echo "error: $normal_image must use the aetherlite entrypoint" >&2
    exit 1
  }
  [[ "$(docker image inspect "$normal_image" --format '{{json .Config.Cmd}}')" == 'null' ]] || {
    echo "error: $normal_image must clear the general gateway command" >&2
    exit 1
  }
  [[ "$(docker image inspect "$dev_image" --format '{{json .Config.Entrypoint}}')" == '["aetherlite"]' ]] || {
    echo "error: $dev_image must inherit the aetherlite entrypoint" >&2
    exit 1
  }

  normal_env="$(docker image inspect "$normal_image" --format '{{range .Config.Env}}{{println .}}{{end}}')"
  for expected in AETHER_ALLOW_DEV_MODE= AETHER_DEV= AETHER_INSECURE_ADMIN=; do
    [[ "$normal_env" != *"$expected"* ]] || {
      echo "error: $normal_image unexpectedly enables ${expected%=}" >&2
      exit 1
    }
  done

  dev_env="$(docker image inspect "$dev_image" --format '{{range .Config.Env}}{{println .}}{{end}}')"
  for expected in AETHER_ALLOW_DEV_MODE=true AETHER_DEV=true AETHER_INSECURE_ADMIN=true; do
    [[ "$dev_env" == *"$expected"* ]] || {
      echo "error: $dev_image is missing $expected" >&2
      exit 1
    }
  done

  echo "==> verified aetherlite normal/dev image contract"
}

if build aether yes; then
  [[ -d "$AETHER_REPO/server" ]] || fail_missing "the aether repo" "$AETHER_REPO" AETHER_REPO
  echo "==> aether base"
  # Context is the repo ROOT: the general Dockerfile copies server/, api/ and
  # sdk/go/. AetherLite then selects the already-built embedded binary, and its
  # dev wrapper owns the intentionally insecure local defaults.
  docker build -t scitrera/aether:local -f "$AETHER_REPO/server/Dockerfile" "$AETHER_REPO"
  echo "==> aetherlite"
  docker build \
    --build-arg BASE_IMAGE=scitrera/aether:local \
    -t scitrera/aetherlite:local \
    -f "$AETHER_REPO/server/Dockerfile.aetherlite" \
    "$AETHER_REPO/server"
  echo "==> aetherlite-dev"
  docker build \
    --build-arg BASE_IMAGE=scitrera/aetherlite:local \
    -t scitrera/aetherlite:dev-local \
    -f "$AETHER_REPO/server/Dockerfile.aetherlite-dev" \
    "$AETHER_REPO/server"
  verify_aether_image_contract
fi

if build memorylayer yes; then
  [[ -d "$MEMORYLAYER_REPO/memorylayer-core-python" ]] || fail_missing "the memorylayer oss repo" "$MEMORYLAYER_REPO" MEMORYLAYER_REPO
  echo "==> memorylayer-server"
  docker build -t scitrera/memorylayer-server:local -f "$MEMORYLAYER_REPO/Dockerfile" "$MEMORYLAYER_REPO"
fi

if build agent yes; then
  [[ -d "$ECOSYSTEM_SPEC_REPO/go" ]] || fail_missing "the ecosystem messaging spec repo" "$ECOSYSTEM_SPEC_REPO" ECOSYSTEM_SPEC_REPO
  [[ -d "$AETHER_REPO/sdk/go" ]] || fail_missing "the aether repo" "$AETHER_REPO" AETHER_REPO
  [[ -d "$MEMORYLAYER_REPO/memorylayer-sdk-go" ]] || fail_missing "the memorylayer oss repo" "$MEMORYLAYER_REPO" MEMORYLAYER_REPO
  [[ -f "$GO_LLM_REPO/go.mod" ]] || fail_missing "the go-llm repo" "$GO_LLM_REPO" GO_LLM_REPO
  echo "==> agent-harness"
  docker build \
    --build-context ecosystem_spec="$ECOSYSTEM_SPEC_REPO" \
    --build-context aether_oss="$AETHER_REPO" \
    --build-context memorylayer_sdk="$MEMORYLAYER_REPO/memorylayer-sdk-go" \
    --build-context go_llm="$GO_LLM_REPO" \
    -f "$here/Dockerfile.agent-local" \
    -t agent-harness:local \
    "$oss_repo"
fi

if build embed "$([[ $want_embed == 1 || "$only" == embed ]] && echo yes || echo no)"; then
  echo "==> memorylayer-embed-server (CPU) — this one is large"
  docker build -t scitrera/memorylayer-embed-server:local \
    -f "$MEMORYLAYER_REPO/memorylayer-embed-server/Dockerfile.cpu" "$MEMORYLAYER_REPO"
fi

echo
docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' \
  | grep -E '(aetherlite:(dev-)?local|memorylayer-server:local|memorylayer-embed-server:local|agent-harness:local)' || true
