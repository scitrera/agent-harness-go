# Subagent Execution Envelope v1

Status: implemented for the default in-process path and the opt-in targeted
Aether executor. The schema remains pre-release while sibling modules are tested
through local replacements.

## Purpose

`agent-harness.subagent.execution` revision 1 is the handoff boundary between a
parent that admits child work and an executor that may eventually run in another
process. It is owned by Sahara OSS because it describes harness execution, not a
portable client/session message.

The ecosystem messaging spec continues to own the revision-3 lifecycle
projection (`SessionSubagentRecord`). Aether owns task identity, assignment,
claiming, authorization, hierarchy, and terminal state. MemoryLayer or the OSS
history store owns child transcript content.

## Envelope contents

The JSON descriptor carried in an Aether task payload contains:

- a deterministic execution ID and schema revision;
- explicit parent and child workspace/session/task identities;
- an immutable history-message input reference and SHA-256 digest;
- a result selector for the first final assistant message after that input;
- a task-scoped checkpoint key reserved for resumable execution;
- the selected agent/model/tool policy surface plus a digest of the complete
  policy snapshot;
- ownership rules requiring claim-before-execute, one attempt, and interruption
  without automatic replay when the outcome is uncertain.

It deliberately does not contain:

- task/prompt text;
- catalog instructions;
- OBO grants, user IDs, tokens, or other credentials;
- filesystem paths, Git details, or MemoryLayer/Aether connection settings.

Catalog instructions participate in the policy digest. An external executor
must resolve the named catalog definition in the same workspace and reject it if
the reconstructed policy does not match. The task text is resolved from the
referenced child-session message and checked against the input digest.

## Required ordering

```text
resolve/mint child session
        |
        v
persist referenced input message in shared history
        |
        v
admit targeted durable task with envelope payload
        |
        v
assignee validates envelope, metadata, authority, and catalog digest
        |
        v
claim task exactly once
        |
        v
resolve + verify referenced input
        |
        v
execute / checkpoint / append result
        |
        v
confirm task terminal state
        |
        v
project revision-3 terminal lifecycle
```

Persisting input before admission means an assignee cannot receive a task whose
input reference is not yet resolvable. If admission fails, the prepared input
may remain as an auditable orphan, but no model or tool work has started and no
admitted lifecycle is projected.

## Workspace and storage rules

Input and result references repeat both workspace and child session. Parsers
reject cross-workspace or cross-session references, unknown fields, duplicate
input IDs, non-canonical policy lists, and trailing JSON.

Local execution works with the ordinary OSS history store. An external worker
requires a history implementation visible to both creator and executor. The
reference CLI currently requires MemoryLayer for this mode; library hosts may
provide another genuinely shared `HistoryStore`. Merely placing prompt text in
Aether metadata is not an acceptable substitute.

Named-agent catalog definitions remain local deployment inputs in revision 1.
The executor loads its workspace catalog and verifies the complete policy digest
before claiming. Parent and executor deployments therefore need matching agent
definitions. Generic policies can be reconstructed only when they contain no
private instructions that were intentionally omitted from the descriptor.

The checkpoint reference is task-scoped. Revision 1 reserves its stable key but
does not claim that the current one-turn provider/tool loop is resumable. A
worker may only advertise resumption after it can atomically record a boundary
that excludes uncertain tool mutations.

## Activation

The reference runner creates and verifies this envelope in both modes and
persists the referenced input before task admission. Self-assigned in-process
execution remains the default.

With `--subagent-target`, the parent creates a targeted Aether task and becomes
a read-only waiter: it does not claim, execute, fail, cancel, or complete the
child. A worker started with `--subagent-executor` consumes only
`agent-harness.subagent.v1` assignments, validates the descriptor against its
non-secret metadata and local catalog, claims once, resolves the shared-history
input, executes without re-entering admission, appends the result, and performs
the terminal task transition. Both flags are restricted to Aether worker modes
with `--memorylayer`; local-only history fails closed during configuration.

The executor receives any OBO authority on Aether's typed
`TaskAssignment.Authorization` field. That value is projected by the gateway
from persisted, task-scoped, assignee-bound authority—not reconstructed from
creator metadata. The envelope and harness-supplied task metadata still contain
no grant, principal, token, prompt text, or catalog instructions.

## Recovery ownership

- A successful task claim selects the sole executor.
- The durable task is authoritative for running and terminal execution state.
- The child transcript is authoritative for input and model/tool output.
- The session subagent registry is a client-facing projection, not a second
  execution authority.
- A lost response is followed by an authoritative query; the mutation is not
  blindly repeated.
- A running task redelivered after its executor is no longer known in-process is
  durably failed rather than replayed. Concurrent duplicate delivery to the
  same live executor is coalesced.
- Parent restart recovery preserves externally admitted/running task state and
  reconciles its lifecycle projection against Aether instead of terminating the
  remotely owned work.
- A terminal task is never automatically rerun, even when its result reference
  is missing; that inconsistency requires inspection or an explicit new
  invocation.
