# Subagent Execution Envelope v1

Status: implemented for in-process, detached, and opt-in targeted Aether
execution. The contract is unreleased; iterative local shapes are folded into
this revision rather than retained as compatibility formats.

## Authority split

`agent-harness.subagent.execution` is Sahara OSS's private execution-plane
contract. The ecosystem messaging spec owns portable execution-binding and
session-state shapes; Aether owns task identity, assignment, claiming, typed
authority, hierarchy, and terminal state; MemoryLayer or the configured history
store owns child transcripts and logical workspace/view observations.

An execution binding is a resource selector, never an access grant. Aether ACL
and OBO policy plus the Sahara server and selected tool host must still authorize
each use.

## Revision 1 contents

The strict JSON payload contains:

- a deterministic `ahx-v1-*` execution ID and schema revision;
- parent and child workspace/session/task identities;
- immutable input, result, and task-checkpoint references;
- the selected agent/model/tool policy and its private-catalog digest;
- an optional exact `execution_scope` containing the portable workspace binding
  and an explicit `read_only` or `read_write` ceiling;
- mutable/dirty admission flags for a durable worker view;
- claim-before-execute, single-attempt, and uncertain-outcome rules.

The execution ID covers the complete scope digest. Aether metadata repeats only
bounded correlation fields such as view ID, tool-host ID, site, revision, write
ceiling, and the scope digest. The task payload and metadata never contain prompt
text, catalog instructions, absolute paths, grants, tokens, or credentials.
`root_ref` is an opaque value interpretable only by the exact named tool host.

## Inheritance and override rules

1. A child with no requested scope inherits its parent turn's exact scope.
2. `permission_mode: read_only` may monotonically narrow `read_write` to
   `read_only` on the same binding without another authorization decision.
3. Changing workspace, view, tool host, execution site, root reference,
   relative directory, revision, mutable/dirty flags, or broadening write access
   invokes `ExecutionScopeAuthorizer`. With no adapter, the request fails closed.
4. Resuming a child thread compares the new scope with the latest persisted
   child execution input. Bound-to-unbound, unbound-to-bound, view changes, and
   write expansion require the same explicit authorization.
5. Detached children retain their captured delegate after the parent task's
   routing maps are cleaned up. Their completion notice carries the same scope,
   and internal enqueue restores it for the follow-up parent turn.
6. An external assignee must bind the scope before claiming the task. A worker
   view must target that exact assignee; a client view requires authoritative
   binding validation and typed OBO routing. Missing composition seams reject the
   task before model or tool execution.

Read-only is a hard execution boundary. `write_file`, `edit_file`, `shell`, and
`python` are hidden from the child tool surface and denied if requested anyway.
A user approval may admit an otherwise gated tool but cannot broaden view write
authority. The remote client/worker host independently receives and enforces the
same policy before dereferencing its private root.

## Required ordering

```text
resolve inherited/requested scope and authorize any transition
        |
        v
persist input message + exact scope in shared history
        |
        v
admit idempotent task with v1 envelope + scope digest metadata
        |
        v
assignee validates metadata, typed authority, catalog, and exact scope
        |
        v
bind tool host and write ceiling before claim
        |
        v
claim once -> resolve input -> execute -> append result
        |
        v
confirm terminal task state -> project session lifecycle
```

Any ambiguity before claim is a terminal rejection. Any uncertain mutation or
terminal transition is interrupted for inspection and is not automatically
replayed.
