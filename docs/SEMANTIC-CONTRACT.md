# Core API semantic contract

## Boundary

Genesis Core exposes generic identity, facts, relationships, canonical mutation, lifecycle, indexed retrieval, migration, recovery, and traversal. The public entry point is \`CoreState\`.

The primitive set is exactly Node, ID, Property, Relationship, Record, and Index. Application entities are modeled by consumers through Properties and Relationships; they are not new Core primitives.

## Authority

Record is canonical. Index is a derived projection and may be deleted and rebuilt. Recovery files are validated redundant material used to reconstruct Record when supported conditions are met.

## Mutation

- batches are atomic in the canonical Record Store;
- failed validation rolls back the entire batch;
- a successful ordinary mutation reports canonical success;
- derived follow-up can lag and is observable through status;
- purge requires a recovery barrier and may report post-commit failure with the committed revision.

## Identity

IDs are UUIDv7 values issued once and never reused. Resolution distinguishes active, tombstoned, retired, and never-issued identity. Titles, paths, content hashes, storage locations, and consumer semantics cannot redefine identity.

## Lifecycle

Tombstone hides a Node from ordinary active reads while preserving reversible state. Restore reactivates it. Purge removes Node-owned reconstructable state, permanently retires the ID, and preserves independently surviving Relationships or facts according to their own ownership rules.

## Reads

Bounded reads cap returned page size. Ordinary cursors provide current-state keyset traversal with watermark semantics. Stable scans hold one canonical Record snapshot and are the required contract for exact multi-page traversal.

## Portability

Portable reconstruction consists of issued identity state, active/tombstoned Nodes, Properties, Relationships, definitions, and the canonical ordering needed to reproduce their state. Derived Index tables are excluded.
