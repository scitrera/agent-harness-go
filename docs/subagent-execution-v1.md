# Subagent Execution Envelope v1

Status: implemented as the OSS in-process/Aether task payload contract; external
task assignment is not enabled yet.

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
persist referenced input message
        |
        v
admit durable task with envelope payload
        |
        v
claim task exactly once
        |
        v
resolve + verify input and policy
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
requires a history implementation visible to both creator and executor, such as
MemoryLayer OSS or a deliberately shared filesystem deployment. Merely placing
prompt text in Aether metadata is not an acceptable substitute.

The checkpoint reference is task-scoped. Revision 1 reserves its stable key but
does not claim that the current one-turn provider/tool loop is resumable. A
worker may only advertise resumption after it can atomically record a boundary
that excludes uncertain tool mutations.

## Current activation boundary

The reference runner now creates and verifies this envelope even in local mode,
persists the referenced input before task admission, and consumes that same
input during in-process execution. The Aether adapter serializes the descriptor
as the task payload while keeping only non-secret IDs and hashes in metadata.

Aether tasks remain self-assigned because execution still happens inside the
parent worker. The next activation step is an optional pool/targeted executor
that consumes `TaskAssignment.Payload`, resolves shared history and catalog
policy, claims the task, and runs the already-prepared child without calling the
parent's admission path again.

## Recovery ownership

- A successful task claim selects the sole executor.
- The durable task is authoritative for running and terminal execution state.
- The child transcript is authoritative for input and model/tool output.
- The session subagent registry is a client-facing projection, not a second
  execution authority.
- A lost response is followed by an authoritative query; the mutation is not
  blindly repeated.
- A non-terminal task whose executor is known lost is durably failed or
  cancelled before the lifecycle is projected as `interrupted`.
- A terminal task is never automatically rerun, even when its result reference
  is missing; that inconsistency requires inspection or an explicit new
  invocation.
