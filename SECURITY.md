# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities through the repository's
[private vulnerability reporting form](https://github.com/scitrera/agent-harness-go/security/advisories/new).
Do not open a public issue for an unpatched vulnerability or include live
credentials, tokens, customer data, or exploit details in a public discussion.

Include the affected version or commit, the observed impact, reproduction steps,
and any suggested mitigation. Maintainers will acknowledge the report through
the private advisory and coordinate disclosure after a fix is available.

## Credential handling

The repository must contain placeholders only. Provider keys belong in
environment variables; ChatGPT subscription credentials belong in the protected
credential store described in `docs/openai-subscription-auth.md`. Runtime
workspaces, `.env` files, OAuth profiles, logs, databases, and common private-key
formats are excluded from both Git and Docker build contexts.

If a real credential is committed, revoke or rotate it immediately. Removing it
from the latest revision is not sufficient because Git history and build caches
may retain the value.
