#!/usr/bin/env bash
#
# Explicit developer path for building every stack image from local source.
# The normal `run.sh up` path pulls published images; use `run.sh up --local`
# (or invoke this script directly) while changing the participating projects.
# The harness itself still consumes published Go modules.
#
# Default ignored dependency layout (override with the env vars below):
#
#   <this-repo>/.local-deps/aether       <- AETHER_REPO
#   <this-repo>/.local-deps/memorylayer  <- MEMORYLAYER_REPO
#
# Usage:
#   ./build.sh              # aetherlite-dev + memorylayer + agent-harness
#   ./build.sh --embed      # also the CPU embed server (large; slow first build)
#   ./build.sh --only agent # just one of: aether | memorylayer | agent | embed

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oss_repo="$(cd "$here/.." && pwd)"
local_deps_root="$oss_repo/.local-deps"

AETHER_REPO="${AETHER_REPO:-$local_deps_root/aether}"
MEMORYLAYER_REPO="${MEMORYLAYER_REPO:-$local_deps_root/memorylayer}"

usage() {
  cat <<'EOF'
Usage: ./e2e/build.sh [--embed] [--only COMPONENT]

Build local OSS E2E images. Aether and MemoryLayer source default to the
ignored .local-deps layout and can be overridden with AETHER_REPO and
MEMORYLAYER_REPO.

Options:
  --embed             Also build the CPU embedding server.
  --only COMPONENT    Build only aether, memorylayer, agent, or embed.
  -h, --help          Show this help.
EOF
}

want_embed=0
only=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --embed) want_embed=1; shift ;;
    --only)  only="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
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
  echo "==> agent-harness"
  docker build \
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
