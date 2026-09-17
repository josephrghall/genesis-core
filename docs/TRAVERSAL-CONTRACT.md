# Traversal contract

\`ReadPage::MAX_LIMIT\` is a per-call return bound, not a collection-capacity limit.

## Ordinary current-state traversal

\`list_nodes_cursor\` uses an opaque keyset cursor containing continuation and revision-watermark information.

- later inserts are excluded from the traversal;
- an unvisited Node changed beyond the watermark or purged during traversal may disappear;
- callers must treat the cursor as opaque;
- malformed, zero, out-of-range, or watermark-inconsistent cursor values are rejected.

This contract is suitable for interactive paging and bounded current-state iteration.

## Exact stable traversal

\`with_stable_read_snapshot\` holds one canonical Record read transaction. All issued-ID, Node, Property, Relationship, and definition pages observed inside it share one snapshot.

Use stable snapshots for export, migration, backup preparation, portability, and verification. Keep the callback bounded because a long-lived SQLite read transaction retains a historical view and can delay cleanup.

## Compatibility reads

OFFSET and whole-collection APIs remain compatibility surfaces. OFFSET is not stable under concurrent writes, and whole-collection helpers can allocate proportionally to the result set.
