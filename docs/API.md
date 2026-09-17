# Genesis Core public API guide

`CoreState` is the public engine entry point. Callers never receive raw SQLite authority.

| Concern | Primary API |
| --- | --- |
| Open and inspect | `open`, `open_protected`, `configure_protected`, `status`, `close` |
| Node and ID | `create_node`, `create_nodes`, `create_node_with_id`, `get_node`, `resolve_identity` |
| Atomic mutation | `apply_batch`, including `Mutation::CreateNode` with initial Properties/Relationships |
| Property | `set_property`, `get_property`, `list_properties`, `remove_property`, `query_properties` |
| Relationship | `add_relationship`, `remove_relationship`, `query_relationships` |
| Lifecycle | `tombstone_node`, `restore_node`, `purge_node` |
| Definitions | register/get/list/update/deprecate/usage/remove operations |
| Derived Index | `search`, `graph`, `backlinks`, orphan queries, projection replacement |
| Index repair | `reconcile_index`, `rebuild_index` |
| Recovery | `reconcile_recovery`; open-time validation/reconstruction |
| Keyset traversal | `list_nodes_cursor` |
| Exact canonical scan | `with_stable_read_snapshot` and snapshot cursor pages |
| Compatibility reads | whole-collection helpers and `ReadPage` OFFSET windows |

## Six primitives

- **Node:** minimal addressable entity with lifecycle.
- **ID:** permanent UUIDv7 identity; issued IDs are never reused.
- **Property:** typed value attached to a Node by a generic key.
- **Relationship:** first-class directed fact between permanent endpoint identities.
- **Record:** canonical durable/history contract implemented by the Record Store.
- **Index:** derived, rebuildable query/navigation projection.

Definitions constrain and describe Property/Relationship vocabularies but are not a seventh primitive. SQLite, recovery, transactions, migrations, and cursors are implementation machinery.

## Failure semantics

Mutation `Ok(...)` means the canonical operation succeeded. A mutation batch commits its Record changes atomically; an empty batch is rejected with `Conflict`. Success does not certify that Index synchronization or local recovery catch-up succeeded. Ordinary follow-up is best-effort and can leave the Index stale or recovery lagging.

Callers that require current derived results or verified recovery coverage must inspect `status().index_sync` and `status().recovery` after mutation. Use `reconcile_index` (or `rebuild_index` when needed) and `reconcile_recovery`, then inspect status again. Canonical reads remain authoritative; Index-backed queries can return an older projection while stale. Repair the follow-up instead of repeating the canonical mutation.

Purge has an additional recovery-barrier step. If `complete_purge_barrier` fails after the canonical purge commits, Core returns `CoreError::PostCommitFailure { revision, follow_up: "purge recovery barrier", reason }`. The Node is already retired; repair with `reconcile_recovery` and verify the barrier/coverage, then reconcile the Index. Do not retry purge as if it rolled back. Successful purge barrier completion still does not guarantee successful best-effort Index synchronization.

`PostCommitFailure` is not an exhaustive classification of every error after commit: lock acquisition failures can return a storage error, and a physical SQLite commit error can have an indeterminate outcome. Inspect canonical state after reopening when the outcome is uncertain; never infer rollback solely from an error category.

Use `CoreError::category()` for stable programmatic handling. Exact error prose is diagnostic, not a parsing contract.

## Canonical versus derived reads

Identity, Node Records, Properties, Relationships, definitions, revision history, and issued IDs are canonical Record concerns. Search, graph, backlinks, orphan projections, and indexed Property queries are derived Index concerns. `status().index_sync` reports whether the Index revision matches Record revision.

## Limits

Cursor and `ReadPage` requests clamp each call to 1,000 items. This is a request-safety bound, not a Store capacity statement. Whole-collection compatibility helpers can allocate in proportion to the entire result. Search and some specialized query paths have their own explicit bounds. No API claims unlimited size or constant performance.

## Above Core

Document models, filesystems, editors, consumer ontologies, application permissions, networking, automation, and product UI belong to consuming applications. They must not become prerequisites for this API.

## Performance evidence boundary

The proven contract bounds returned items, provides deterministic keyset continuation and revision-watermark/concurrent-write semantics, and covers correctness beyond 1,000 Nodes. It does not bound underlying database work per page or prove asymptotically efficient large-scale execution. The current Node Index has no index matching `modified_revision DESC, node_uuid ASC`; continuation may scan and sort. Stable snapshot paging also bounds results, not total database work. Historical large-scale benchmarks do not establish performance for these newer traversal paths.
