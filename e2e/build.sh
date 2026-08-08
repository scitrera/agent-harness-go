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
#   <root>/scitrera-aether3-go/oss-repo               <- AETHER_REPO
#   <root>/scitrera-memorylayer-ai-cc/oss             <- MEMORYLAYER_REPO
#
# Usage:
#   ./build.sh              # aetherlite + memorylayer + agent-harness
#   ./build.sh --embed      # also the CPU embed server (large; slow first build)
#   ./build.sh --only agent # just one of: aether | memorylayer | agent | embed

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oss_repo="$(cd "$here/.." && pwd)"

AETHER_REPO="${AETHER_REPO:-$HOME/scitrera-aether3-go/oss-repo}"
MEMORYLAYER_REPO="${MEMORYLAYER_REPO:-$HOME/scitrera-memorylayer-ai-cc/oss}"

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

if build aether yes; then
  [[ -d "$AETHER_REPO/server" ]] || fail_missing "the aether repo" "$AETHER_REPO" AETHER_REPO
  echo "==> aetherlite"
  # Context is the repo ROOT: the Dockerfile copies server/, api/ and sdk/go/.
  docker build -t scitrera/aetherlite:local -f "$AETHER_REPO/server/Dockerfile" "$AETHER_REPO"
fi

if build memorylayer yes; then
  [[ -d "$MEMORYLAYER_REPO/memorylayer-core-python" ]] || fail_missing "the memorylayer oss repo" "$MEMORYLAYER_REPO" MEMORYLAYER_REPO
  echo "==> memorylayer-server"
  docker build -t scitrera/memorylayer-server:local -f "$MEMORYLAYER_REPO/Dockerfile" "$MEMORYLAYER_REPO"
fi

if build agent yes; then
  echo "==> agent-harness"
  docker build -t agent-harness:local "$oss_repo"
fi

if build embed "$([[ $want_embed == 1 || "$only" == embed ]] && echo yes || echo no)"; then
  echo "==> memorylayer-embed-server (CPU) — this one is large"
  docker build -t scitrera/memorylayer-embed-server:local \
    -f "$MEMORYLAYER_REPO/memorylayer-embed-server/Dockerfile.cpu" "$MEMORYLAYER_REPO"
fi

echo
docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' \
  | grep -E '(aetherlite|memorylayer-server|memorylayer-embed-server|agent-harness):local' || true
