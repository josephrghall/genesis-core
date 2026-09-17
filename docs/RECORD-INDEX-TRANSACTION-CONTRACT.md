# Record, Index, and transaction contract

## Authority

Record is canonical durable truth. Index is derived, disposable, and rebuildable from Record. A query projection must never become the sole authority for a fact.

## Commit semantics

A mutation batch validates before commit and applies atomically to Record. If any operation fails, the batch does not partially issue IDs, write Properties, or create Relationships.

After canonical commit, Index and recovery follow-up are best effort for ordinary mutations. Status exposes lag and reconciliation repairs it. Purge is stricter: canonical purge may commit before its required recovery barrier completes, so failure reports the committed revision explicitly.

## History

Canonical permanence applies to identity, fact ownership, and lifecycle semantics. Issued Node IDs are never reused; retired identity remains distinguishable from never-issued identity; canonical Record state remains authoritative; and surviving facts continue according to their ownership and lifecycle contracts.

This is not an unlimited append-only event-log guarantee. The `record_changes` table is a bounded operational journal used to reconcile derived Index and recovery state. The current policy retains changes for the most recent 1,024 Record revisions and may prune older journal entries. If the retained journal cannot bridge a checkpoint, reconciliation rebuilds from canonical Record truth instead of inventing missing events. Not every operational mutation or reconciliation entry is retained forever.

## Rebuild

Deleting `index.sqlite` must not remove canonical information. Opening Core and rebuilding the Index must reproduce current query behavior from Record truth.

## Protected storage

Protected mode is optional and provides configured-root identity validation. Configuration requires an existing root, canonicalizes it, and records a root fingerprint plus a unique persistent sentinel. On Unix the fingerprint includes canonical path, device ID, and inode; on Windows it includes canonical path and the sentinel remains mandatory. Protected open and write access fail when the selected root, recorded fingerprint, or sentinel no longer matches.

This baseline does not validate host path ownership, guarantee rejection of every symlink or reparse-point substitution, provide encryption, or constitute a complete filesystem or process sandbox. Stricter path, ownership/permission, service-account/container, encryption, and OS sandbox controls are external deployment layers.
