# OpenAI subscription authentication

Sahara OSS can use a ChatGPT Plus or Pro subscription through an OAuth
credential profile. This is separate from OpenAI API-key billing: changing only
`models.yaml` is not sufficient because subscription access needs interactive
login, renewable credentials, account routing, and the ChatGPT Responses request
profile.

This integration is intended for the OSS direct-provider runtime. Sahara's
platform/proprietary composition continues to obtain credentials at its sidecar
and does not instantiate this broker.

## Log in

Run the device authorization flow from the OSS CLI:

```sh
agent-harness auth login --profile personal
```

Open the printed OpenAI URL, enter the one-time code, and complete sign-in. The
device flow also works over SSH and in headless containers. Inspect or remove a
profile with:

```sh
agent-harness auth status --profile personal
agent-harness auth logout --profile personal
```

For the repository's containerized OSS E2E stack, use the wrapper so the
command runs in the worker container and reads or writes the same credential
store as the serving process:

```sh
./e2e/run.sh auth login --profile personal
./e2e/run.sh auth status --profile personal
./e2e/run.sh auth logout --profile personal
```

The wrapper requires the E2E stack to be running. Its `sahara-auth` named
volume survives agent recreation and ordinary `down`/`up` cycles; explicitly
removing Compose volumes deletes the stored profiles.

Profiles are stored as mode `0600` files below the mode `0700` directory
`$SAHARA_AUTH_DIR`. When that variable is unset, the default is
`<user-config-dir>/sahara/auth`. These files contain access, refresh, and ID
tokens and must be treated as secrets. A host can replace the broker's
`llmauth.Store` with an OS keyring implementation without changing go-llm's
provider authentication code.

## Configure a model

Reference the credential profile from `config/models.yaml`:

```yaml
default: gpt-5.6-sol

providers:
  - name: chatgpt
    kind: openai_subscription
    auth_profile: personal
    format: responses

models:
  - name: gpt-5.6-sol
    provider: chatgpt
    capabilities: {tools: true, vision: true}
    reasoning:
      default_effort: high
      allowed_efforts: [none, low, medium, high, xhigh, max]
```

No `base_url`, `api_key`, or `api_key_env` is accepted for this provider kind.
Sahara fixes the destination to the ChatGPT Codex Responses backend before it
attaches credentials. A `401` triggers one synchronized refresh and one replay;
expiring tokens are refreshed proactively. A refresh that changes the account
or user identity is rejected without replacing the stored profile.

The OAuth issuer and public client identifier default to the values used by the
OpenAI Codex authorization flow. Development or an OpenAI-issued application
registration can override them with `SAHARA_OPENAI_OAUTH_ISSUER` and
`SAHARA_OPENAI_OAUTH_CLIENT_ID`.

## Compatibility boundary

OpenAI documents subscription sign-in for Codex, but does not currently
document a general third-party ChatGPT subscription API contract. Sahara's
profile is therefore a compatibility integration and may need updates when the
Codex authorization or ChatGPT backend contract changes. API-key providers
remain the stable option for general OpenAI API usage.

Sahara does not copy its workspace, thread, task, or installation identifiers
into OpenAI client metadata. The request profile sends only the OAuth/account
headers, a stable `originator`, `store: false`, and the encrypted-reasoning
include directive.

## Configure reasoning effort

Reasoning levels are request settings, not separate upstream models. Keep one
`gpt-5.6-sol` entry and configure its default and optional allowlist under
`reasoning`. The allowlist is an operator assertion that prevents an invalid or
unwanted thread override from reaching that model; omit it when the registry
should allow the full provider-neutral vocabulary.

The effective value is resolved for every provider call in this order:

1. the current thread and model's `/reasoning` override;
2. `--reasoning-effort` or `SAHARA_REASONING_EFFORT`;
3. the model's `reasoning.default_effort`;
4. the provider default (the request field is omitted).

Use `/reasoning` to inspect the active value and allowlist, `/reasoning xhigh`
to override it for the current model and thread, and `/reasoning default` to
clear that override. Overrides are runner-lifetime session state and are kept
separately per model; switching away and back restores the model's prior thread
override. If provider recovery switches models, Sahara recomputes the effort
against the fallback model's policy.

Sahara passes the neutral setting to go-llm. The shared Responses codec sends
`reasoning: {effort: ...}` and the Chat Completions codec sends
`reasoning_effort`. For `gpt-5.6-sol`, OpenAI currently documents `none`, `low`,
`medium` (default), `high`, `xhigh`, and `max`; configure the allowlist from the
capabilities of the exact upstream model you deploy.
