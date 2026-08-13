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

The receiving tool host must still compare the trusted delivery target,
binding ID, checked-access receipt, effective scope, caller subject, and its own
local export policy before installing the forwarded authorization into the
single invocation context. MemoryLayer, data-connectors, and other downstream
services then perform their normal independent ACL checks.

