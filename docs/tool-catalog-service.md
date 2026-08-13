# Tool catalog service

`cmd/tool-catalog-service` exposes the deterministic OSS live catalog as the
workspace-less Aether service `sv::tool-catalog`. Catalog entries remain
portable ecosystem messages; provider routes, caller identity, authorization
lineage, and invocation-authority policy are private trusted service state.

## Caller OBO for remote tools

Forwarding downstream caller authority is disabled unless an operator supplies
`TOOL_CATALOG_INVOCATION_AUTHORITY_POLICY` (or
`--invocation-authority-policy`) naming a JSON policy file. Every rule matches
one complete immutable `ToolReference`. A generation or revision change
therefore requires an explicit review and policy update.

```json
{
  "profiles": [
    {
      "ref": {
        "provider_id": "data-agent",
        "registration_id": "primary",
        "generation": "generation-2026-08-12",
        "name": "research_to_vfs",
        "revision": "sha256:reviewed-tool-revision"
      },
      "profile": {
        "mode": "caller_obo",
        "resource_scope": [
          {
            "resource_type": "memorylayer/thread",
            "patterns": ["project-a/*"]
          },
          {
            "resource_type": "vfs",
            "patterns": ["workspaces/project-a/*"]
          }
        ],
        "operation_scope": ["read", "write"],
        "max_access_level": 20
      }
    }
  ]
}
```

The selected catalog workspace is always the continuation's sole workspace
scope; it is not configurable by the provider. The gateway also intersects the
requested profile with the Sahara task grant, so the task/root grant must
already include every approved downstream resource and operation. Missing or
broader authority fails the invocation without delivering it.

The catalog removes any provider-authored value using the reserved
`provider_invocation_authority` metadata key and substitutes only its resolved
server policy. The profile is carried in the service-private resolved record,
not in the portable descriptor or `ToolInvokeEnvelope`. Sahara sends it to
Aether as an invocation-bound attenuation request whose binding ID matches the
checked `tool-catalog/entry` receipt. Aether mints a fresh short-lived,
non-delegable child for the exact service or agent recipient.

The policy file's SHA-256 prefix is included in the effective catalog policy
epoch. Changing the policy invalidates retained cursors after service restart,
even if `TOOL_CATALOG_POLICY_EPOCH` itself is unchanged.

## Hosting tools from an Aether agent

`pkg/channels/aether.CatalogToolHost` implements the receiving boundary. It
publishes only the registry names explicitly listed in `Exports`; registering a
tool locally does not expose it remotely. Each export requires an immutable
revision and effect classification. Shell, credentials, control tools, and
other capabilities therefore remain private unless an operator deliberately
adds them.

The host's `Context` contains availability selectors only. A workspace-wide
tool can publish `ToolCatalogContext{WorkspaceID: "project-a"}` while its exact
`ag::project-a::<implementation>::<specifier>` provider route is retained as
private catalog state. A route can no longer take over another route's live
registration merely because both agents have broad catalog ACLs.

Typical wiring is:

```go
host, err := aetherchan.NewCatalogToolHost(aetherchan.CatalogToolHostConfig{
    Route:          "ag::project-a::documents::primary",
    ProviderID:     "documents",
    RegistrationID: "primary",
    Generation:     "generation-2026-08-13",
    Context:        spec.ToolCatalogContext{WorkspaceID: "project-a"},
    Registry:       registry,
    Sender:         agentClient,
    Exports: []aetherchan.CatalogToolExport{{
        Name:     "research_to_vfs",
        Revision: "sha256:reviewed-tool-revision",
        Effect:   spec.ToolEffectWrite,
        InvocationAuthority: &reviewedProfile,
    }},
})
if err != nil { /* fail startup */ }

agentClient.OnMessage(host.Handle)
go func() { _ = host.RunLease(ctx) }()
```

The agent must have direct checked-send access to the canonical
`tool-catalog/provider` resource for `catalog.publish`. `RunLease` publishes,
renews before expiry, and performs a bounded revoke during graceful shutdown;
the lease is the crash fallback. Direct agent mutations are bound to the exact
gateway source route and receipt correlation. Browser and Office publishers
continue through their authenticated Platform Bridge edge but populate the
same private route field.

At invocation, the host compares the delivery target, exact entry resource,
effect operation, call correlation, source agent, caller subject, binding ID,
root lineage, and effective continuation scope. If the local export has no
reviewed profile, any forwarded grant is rejected. If it has one, a missing or
mismatched grant is rejected. Only the validated child grant is installed as
`tools.MemoryAuthority` on that invocation's context and request. MemoryLayer,
data-connectors, and other downstream services still perform their normal
independent ACL checks.

Cancellation is another checked send for the same exact entry/call. It carries
the parent caller authorization for the check but requests no second authority
continuation. The host cancels only a matching source, subject, address, call,
and receipt, then returns the terminal `tool_cancelled` result.

## Consuming the catalog from an Aether agent

`pkg/channels/aether.CatalogClient` is the matching reusable consumer. Sahara
uses this package rather than carrying a distribution-private copy, and another
agent can use the same query/describe/invoke/cancel behavior:

```go
catalogClient, err := aetherchan.NewCatalogClient(aetherchan.CatalogClientConfig{
    Route:  agentClient.Topic(),
    Sender: agentClient,
})
if err != nil { /* fail startup */ }

agentClient.OnMessage(func(_ context.Context, message *aethersdk.Message) error {
    catalogClient.TryHandle(message)
    return nil
})

page, err := catalogClient.QueryCatalog(ctx, query, callerAuthority)
```

The client requires a typed user OBO grant for the current path. It binds every
waiter to both the request ID and gateway-authenticated expected source, derives
the exact catalog-entry access request from the admitted reference, and asks
Aether for an invocation-bound attenuated child only when the catalog's private
record contains a reviewed authority profile. Fully autonomous direct-agent
authority remains a separate policy mode; this client does not manufacture a
user subject or fall back to the agent's direct identity.

The OSS E2E compose stack deploys `tool-catalog-service` with a single immutable
test-only authority rule. `TestLiveAetherCatalogAgentMemoryLayerOBO` is opt-in
and model-free: it connects a caller agent and provider agent to the real
gateway, publishes the provider, discovers and invokes it, then verifies the
fresh provider child grant is used on a real `sv::memorylayer` request.
Each run uses a fresh workspace so replay-protecting generation tombstones are
not weakened for test convenience. It temporarily provisions only the exact
required ACL rules through the loopback AetherLite dev admin API and restores
the previous state. This fixture is not a production ACL provisioning model.
