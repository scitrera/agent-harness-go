# Contributing

Contributions should be narrowly scoped, tested, and safe to publish.

Before opening a pull request:

1. Keep the Apache-2.0 SPDX and Scitrera copyright header at the top of every
   Go source file, then run `gofmt` on changed Go files.
2. Run `go vet ./...`, `go test -race -count=1 ./...`, and
   `golangci-lint run --timeout=5m`.
3. Run `actionlint` and resolve all workflow findings.
4. Run `govulncheck ./...` and resolve reachable findings.
5. Run `gitleaks git --redact .` against the full local history.
6. Confirm that no credentials, customer material, local paths, generated
   artifacts, or runtime transcripts are staged.

Use environment variables or ignored workspace configuration for provider
credentials. Examples must contain unmistakable placeholders rather than values
that could be mistaken for live secrets.

All contributions are licensed under the Apache License, Version 2.0, as stated
in `LICENSE`.
