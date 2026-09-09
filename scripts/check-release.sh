#!/usr/bin/env bash
# Strict checks that apply to a public source/binary release, not to ordinary
# local development against sibling module checkouts.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

failures=0
fail() {
  echo "release check: $*" >&2
  failures=$((failures + 1))
}

if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  tag_name="${GITHUB_REF_NAME:-}"
  tag_version="${tag_name#v}"
  declared_version="$(awk '$1 == "agent-harness-go:" { print $2; exit }' versions.yaml)"
  source_version="$(sed -n 's/^const Version = "\([^"]*\)"$/\1/p' pkg/version/version.go)"
  if [[ "$tag_name" != v* || -z "$tag_version" || "$tag_version" != "$declared_version" || "$tag_version" != "$source_version" ]]; then
    fail "tag $tag_name, versions.yaml ($declared_version), and pkg/version ($source_version) must agree"
  fi
fi

if grep -Eq '^[[:space:]]*replace[[:space:]]' go.mod; then
  fail "go.mod contains local replace directives; publish the pinned modules and remove every replacement"
fi

while IFS= read -r path; do
  spdx_line="$(sed -n '1p' "$path")"
  copyright_line="$(sed -n '2p' "$path")"
  if [[ "$spdx_line" != "// SPDX-License-Identifier: Apache-2.0" ||
        "$copyright_line" != "// Copyright 2026 Scitrera LLC" ]]; then
    fail "Go source is missing the required SPDX/copyright header: $path"
  fi
done < <(git ls-files '*.go')

while IFS= read -r path; do
  case "$path" in
    *.env.example) ;;
    .env|*/.env|*.env.*|*/.env.*|*.pem|*.key|*.p12|*.pfx|*.jks|*.keystore|id_rsa|*/id_rsa|id_dsa|*/id_dsa|id_ecdsa|*/id_ecdsa|id_ed25519|*/id_ed25519|.netrc|*/.netrc|.npmrc|*/.npmrc|.pypirc|*/.pypirc|.aws/*|*/.aws/*|.azure/*|*/.azure/*|.kube/*|*/.kube/*|.config/gcloud/*|*/.config/gcloud/*|.terraform/*|*/.terraform/*|*.tfstate|*.tfstate.*|*.log|*.db|*.sqlite|*.sqlite3|auth/*|*/auth/*|credentials/*|*/credentials/*|credentials.json|*/credentials.json|service-account*.json|*/service-account*.json|auth.json|*/auth.json|tokens.json|*/tokens.json)
      fail "tracked sensitive or runtime-shaped path: $path"
      ;;
  esac
done < <(git ls-files)

if git grep -In -E '(/home/[^/]+/(code|oss|projects|sdk|src|work|workspace)/|/Users/[^/]+/(code|oss|projects|sdk|src|work|workspace)/)' -- . ':!scripts/check-release.sh'; then
  fail "tracked source contains a developer-specific or private-monorepo path"
fi

gitleaks_bin="${GITLEAKS_BIN:-$(command -v gitleaks || true)}"
if [[ -z "$gitleaks_bin" ]]; then
  fail "gitleaks is required (CI pins v8.30.1)"
elif ! "$gitleaks_bin" git --redact --no-banner .; then
  fail "gitleaks found a possible credential in Git history"
fi

licenses_bin="${GO_LICENSES_BIN:-$(command -v go-licenses || true)}"
if [[ -z "$licenses_bin" ]]; then
  fail "go-licenses is required (CI pins v2.0.1)"
else
  report_file="$(mktemp)"
  log_file="$(mktemp)"
  if ! "$licenses_bin" report ./cmd/agent-harness \
    --ignore github.com/scitrera/agent-harness-go >"$report_file" 2>"$log_file"; then
    sed -n '1,120p' "$log_file" >&2
    fail "third-party license discovery failed"
  elif grep -Eq ',Unknown$' "$report_file"; then
    grep -E ',Unknown$' "$report_file" >&2
    fail "one or more linked dependencies have no discoverable license"
  fi
fi

if ! go mod verify; then
  fail "Go module verification failed"
fi

if ! go mod tidy -diff; then
  fail "go.mod or go.sum is not tidy"
fi

empty_tree="$(git hash-object -t tree /dev/null)"
if ! git diff --check "$empty_tree"; then
  fail "the source tree contains whitespace errors"
fi

if (( failures > 0 )); then
  echo "release check failed with $failures blocker(s)" >&2
  exit 1
fi

echo "release check passed"
