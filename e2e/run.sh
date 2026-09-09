#!/usr/bin/env bash
# Run and attach to the OSS end-to-end stack.
#
# Usage:
#   ./e2e/run.sh up             # latest published images
#   ./e2e/run.sh up --local     # rebuild from local checkouts
#   ./e2e/run.sh tui [agent-harness flags...]
#   ./e2e/run.sh auth login [--profile personal]
#   ./e2e/run.sh check
#   ./e2e/run.sh down

set -euo pipefail

e2e_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oss_dir="$(cd "$e2e_dir/.." && pwd)"
env_file="$e2e_dir/.env"

usage() {
  cat <<'EOF'
Usage: ./e2e/run.sh <command> [arguments]

Commands:
  up       Pull the latest published images, then start the stack with semantic
           embeddings enabled. Pass --local to rebuild from local checkouts;
           pass --published to override E2E_SOURCE_MODE=local.
  tui      Launch the host TUI and attach it to the deployed E2E worker.
           Additional arguments are passed to agent-harness.
  auth     Manage the worker's persisted ChatGPT subscription login.
           Usage: auth <login|status|logout> [--profile name]
  check    Run deterministic safe-parallel runner coverage plus live client-tool
           routing/cancellation, catalog-agent OBO, scheduled worker-view,
           refinement-audit, and execution-ledger tests (no model request).
  down     Stop and remove the E2E containers and network. Volumes are kept.
  help     Show this help.

Environment overrides:
  GO_BIN             Go executable used by `tui` and `check`.
  E2E_WAIT_TIMEOUT   Seconds `up` waits for stack health (default: 600).
  E2E_SOURCE_MODE    Image source for `up`: published (default) or local.
  AETHER_WORKSPACE   Aether workspace used by `tui` (otherwise read from .env).
  AETHER_SPECIFIER   Agent specifier used by `tui` (otherwise read from .env).
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_e2e_env() {
  [[ -f "$env_file" ]] || die "$env_file is missing; copy .env.example to .env and configure the model endpoint"
}

# Read one simple dotenv assignment without sourcing the file. This lets the
# TUI share routing configuration with Compose without executing or printing
# the secret-bearing .env file.
dotenv_value() {
  local key="$1"
  local value

  value="$(awk -v wanted="$key" '
    /^[[:space:]]*(#|$)/ { next }
    {
      equals = index($0, "=")
      if (equals == 0) next
      name = substr($0, 1, equals - 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
      if (name != wanted) next
      result = substr($0, equals + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", result)
      print result
    }
  ' "$env_file" | tail -n 1)"

  if [[ "$value" == \"*\" && "$value" == *\" ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s' "$value"
}

# Shell environment wins over .env, and both win over the published default.
# Reading only these non-secret keys keeps the wrapper from sourcing arbitrary
# or credential-bearing shell content.
image_value() {
  local key="$1"
  local fallback="$2"
  local value="${!key:-}"

  if [[ -z "$value" ]]; then
    value="$(dotenv_value "$key")"
  fi
  printf '%s' "${value:-$fallback}"
}

configure_stack_source() {
  local source_mode="$1"

  case "$source_mode" in
    published)
      export AETHERLITE_IMAGE="$(image_value AETHERLITE_IMAGE ghcr.io/scitrera/aetherlite:dev-latest)"
      export MEMORYLAYER_IMAGE="$(image_value MEMORYLAYER_IMAGE ghcr.io/scitrera/memorylayer-server:latest)"
      export MEMORYLAYER_EMBED_IMAGE="$(image_value MEMORYLAYER_EMBED_IMAGE ghcr.io/scitrera/memorylayer-embed-server:latest)"
      export AGENT_HARNESS_IMAGE="$(image_value AGENT_HARNESS_IMAGE ghcr.io/scitrera/agent-harness:latest)"
      export E2E_PULL_POLICY=always
      ;;
    local)
      export AETHERLITE_IMAGE=scitrera/aetherlite:dev-local
      export MEMORYLAYER_IMAGE=scitrera/memorylayer-server:local
      export MEMORYLAYER_EMBED_IMAGE=scitrera/memorylayer-embed-server:local
      export AGENT_HARNESS_IMAGE=agent-harness:local
      export E2E_PULL_POLICY=never
      ;;
    *)
      die "unknown E2E source mode: $source_mode (expected published or local)"
      ;;
  esac
}

compose() {
  (
    cd "$e2e_dir"
    docker compose --profile embed "$@"
  )
}

find_go() {
  if [[ -n "${GO_BIN:-}" ]]; then
    [[ -x "$GO_BIN" ]] || die "GO_BIN is not executable: $GO_BIN"
    printf '%s' "$GO_BIN"
    return
  fi
  command -v go >/dev/null 2>&1 || die "go is not on PATH; set GO_BIN=/path/to/go"
  command -v go
}

action="${1:-help}"
[[ $# -eq 0 ]] || shift

case "$action" in
  up)
    require_e2e_env
    command -v docker >/dev/null 2>&1 || die "docker is not installed or not on PATH"

    source_mode="${E2E_SOURCE_MODE:-$(dotenv_value E2E_SOURCE_MODE)}"
    source_mode="${source_mode:-published}"
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --local) source_mode=local ;;
        --published) source_mode=published ;;
        *) die "unknown up argument: $1 (expected --local or --published)" ;;
      esac
      shift
    done
    configure_stack_source "$source_mode"

    if [[ "$source_mode" == "local" ]]; then
      echo "==> Building the local OSS E2E images (embed profile included)"
      "$e2e_dir/build.sh" --embed
    else
      echo "==> Pulling the latest published OSS E2E images"
      compose pull
      # Keep startup on the manifests just resolved by the explicit pull.
      export E2E_PULL_POLICY=never
    fi

    echo "==> Stopping the previous OSS E2E stack (volumes are preserved)"
    compose stop --timeout 30 agent tool-catalog memorylayer
    compose down --remove-orphans

    echo "==> Recreating the OSS E2E stack with semantic embeddings enabled"
    export MEMORYLAYER_EMBEDDING_PROVIDER=embed_server
    export MEMORYLAYER_EMBED_SERVER_URL=http://embed-server:61051
    export MEMORYLAYER_EMBEDDING_DIMENSIONS=384
    compose up -d --force-recreate --remove-orphans --wait \
      --wait-timeout "${E2E_WAIT_TIMEOUT:-600}"
    compose ps
    ;;

  down)
    [[ $# -eq 0 ]] || die "down does not accept arguments"
    require_e2e_env
    command -v docker >/dev/null 2>&1 || die "docker is not installed or not on PATH"
    compose down --remove-orphans
    ;;

  tui)
    require_e2e_env
    go_bin="$(find_go)"
    aether_workspace="${AETHER_WORKSPACE:-$(dotenv_value AETHER_WORKSPACE)}"
    aether_specifier="${AETHER_SPECIFIER:-$(dotenv_value AETHER_SPECIFIER)}"
    aether_workspace="${aether_workspace:-default}"
    aether_specifier="${aether_specifier:-e2e}"

    echo "==> Attaching TUI to ag::${aether_workspace}::agent-harness::${aether_specifier}" >&2
    cd "$oss_dir"
    exec "$go_bin" run ./cmd/agent-harness \
      --tui \
      --workspace ./e2e/workspace \
      --workspace-mode project \
      --aether 127.0.0.1:50051 \
      --aether-workspace "$aether_workspace" \
      --aether-specifier "$aether_specifier" \
      "$@"
    ;;

  auth)
    require_e2e_env
    command -v docker >/dev/null 2>&1 || die "docker is not installed or not on PATH"
    [[ $# -gt 0 ]] || die "auth requires login, status, or logout"
    case "$1" in
      login|status|logout) ;;
      *) die "unknown auth action: $1 (expected login, status, or logout)" ;;
    esac
    [[ "$(compose ps --status running --services agent)" == "agent" ]] ||
      die "the E2E agent is not running; start it with ./e2e/run.sh up"
    compose exec -T agent agent-harness auth "$@"
    ;;

  check)
    [[ $# -eq 0 ]] || die "check does not accept arguments"
    require_e2e_env
    go_bin="$(find_go)"
    aether_workspace="${AETHER_WORKSPACE:-$(dotenv_value AETHER_WORKSPACE)}"
    aether_workspace="${aether_workspace:-default}"
    aether_specifier="${AETHER_SPECIFIER:-$(dotenv_value AETHER_SPECIFIER)}"
    aether_specifier="${aether_specifier:-e2e}"

    echo "==> Testing deterministic safe-parallel tool batches" >&2
    cd "$oss_dir"
    "$go_bin" test ./pkg/turn \
      -run '^TestRunnerParallelTools_' -count=1 -v

    echo "==> Testing exact-host tools, catalog-agent OBO, scheduled operations, refinement audit, and the Aether-KV execution ledger" >&2
    AETHER_E2E_ADDR=127.0.0.1:50051 \
      AETHER_E2E_ADMIN_URL=http://127.0.0.1:31880 \
      AETHER_CATALOG_E2E=1 \
      AETHER_E2E_WORKSPACE="$aether_workspace" \
      AETHER_E2E_SPECIFIER="$aether_specifier" \
      "$go_bin" test ./pkg/channels/aether \
        -run '^TestLiveAether(ClientWorkspaceToolRouting|CatalogAgentMemoryLayerOBO|ScheduledWorkerView|RefinementAudit|ExecutionLedger)$' -count=1 -v
    ;;

  help|-h|--help)
    usage
    ;;

  *)
    usage >&2
    die "unknown command: $action"
    ;;
esac
