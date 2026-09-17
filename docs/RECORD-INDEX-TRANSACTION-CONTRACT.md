# Record, Index, and transaction contract

## Authority

Record is canonical durable truth. Index is derived, disposable, and rebuildable from Record. A query projection must never become the sole authority for a fact.

## Commit semantics

A mutation batch validates before commit and applies atomically to Record. If any operation fails, the batch does not partially issue IDs, write Properties, or create Relationships.

After canonical commit, Index and recovery follow-up are best effort for ordinary mutations. Status exposes lag and reconciliation repairs it. Purge is stricter: canonical purge may commit before its required recovery barrier completes, so failure reports the committed revision explicitly.

## History

Record history is permanent evidence of what occurred. It does not mean that every historical Property or Relationship remains current after removal. Current state is derived from valid canonical history and lifecycle rules.

## Rebuild

Deleting \`index.sqlite\` must not remove canonical information. Opening Core and rebuilding the Index must reproduce current query behavior from Record truth.

## Protected storage

Canonical and recovery files live inside the protected Core store boundary. Symlink/path substitution is rejected before trusted store access.
