# Genesis Core

Genesis Core is an embedded, domain-neutral information engine for applications that need durable identity, typed properties, first-class relationships, canonical records, and indexed retrieval without hard-coding a product or domain ontology.

Its conceptual boundary is exactly six primitives:

~~~text
Node
ID
Property
Relationship
Record
Index
~~~

Record is canonical. Index is derived and rebuildable. Everything else—storage adapters, transactions, migrations, recovery, and security enforcement—is implementation infrastructure for those six primitives.

## Why use it?

Applications repeatedly reinvent identity, metadata, relationships, persistence, history, and indexing for their domain objects. Genesis Core supplies those generic capabilities without imposing a UI, hierarchy, content format, workflow, account system, server, or fixed ontology.

Generic consumers may model people, projects, documents, assets, research items, knowledge objects, or other application entities above the same Core. Those examples are not built-in semantics.

## What Core solves

- permanent UUIDv7 Node identity with explicit active, tombstoned, retired, and never-issued resolution;
- arbitrary typed Properties and first-class directed Relationships;
- canonical Record durability with transactional mutation;
- rebuildable Index projections for search, graph, backlinks, and filtered retrieval;
- tombstone, restore, and confirmed purge lifecycle behavior;
- deterministic migrations, integrity checks, and validated local recovery;
- bounded cursor traversal and exact stable Record snapshot scans.

## What Core deliberately does not solve

Genesis Core does not provide a user interface, document editor, filesystem policy, product ontology, account service, synchronization service, hosted backend, or application workflow. Consumers own those decisions above the Core API.

## Quick start

Requires a stable Rust toolchain.

~~~bash
cargo check
cargo test
cargo run --example headless
~~~

The example creates Nodes, sets and reads a Property, creates and traverses a Relationship, queries the Index, exercises cursor and stable-snapshot traversal, removes the derived Index, rebuilds it from canonical Record truth, and verifies that canonical data is unchanged.

Use \`CoreState\` as the public entry point. Start with the [quickstart](docs/QUICKSTART.md) and [API guide](docs/API.md).

## Public API boundary

| Concern | Public operations |
| --- | --- |
| Open/status | \`open\`, \`open_protected\`, \`configure_protected\`, \`status\`, \`close\` |
| Node/ID | \`create_node\`, \`create_nodes\`, \`create_node_with_id\`, \`get_node\`, \`resolve_identity\`, \`list_nodes_cursor\` |
| Property | \`set_property\`, \`get_property\`, \`list_properties\`, \`remove_property\`, \`query_properties\` |
| Relationship | \`add_relationship\`, \`remove_relationship\`, \`query_relationships\` |
| Lifecycle | \`apply_batch\`, \`tombstone_node\`, \`restore_node\`, \`purge_node\` |
| Definitions | Property/Relationship definition registration, inspection, update, deprecation, usage, and removal |
| Index | \`reconcile_index\`, \`rebuild_index\`, \`search\`, \`graph\`, \`backlinks\`, orphan queries |
| Recovery | \`reconcile_recovery\`; migration and reconstruction through \`open\` |
| Stable scan | \`with_stable_read_snapshot\` with bounded cursor pages |

SQLite connections, tables, recovery artifacts, and migration internals are private implementation details. \`records.sqlite\` is canonical; \`index.sqlite\` is disposable.

## Verification

~~~bash
cargo fmt -- --check
cargo check --locked
cargo test --locked
cargo test --locked --doc
cargo clippy --locked --all-targets -- -D warnings
cargo run --locked --example headless
~~~

The retained validation suite covers exact byte fidelity, Unicode, empty and duplicate content, heterogeneous formats, deep/wide/cyclic topology, lifecycle mutation, issued-ID permanence, Index rebuild, recovery reconstruction, cursor watermark behavior, concurrent mutation, stable snapshots, and portable reconstruction.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Architecture evolution](docs/ARCHITECTURE-EVOLUTION.md)
- [API guide](docs/API.md)
- [Semantic contract](docs/SEMANTIC-CONTRACT.md)
- [ID contract](docs/ID-CONTRACT.md)
- [Record/Index transaction contract](docs/RECORD-INDEX-TRANSACTION-CONTRACT.md)
- [Traversal contract](docs/TRAVERSAL-CONTRACT.md)
- [Schema, migration, and recovery](docs/SCHEMA-MIGRATION-RECOVERY.md)
- [Security policy](SECURITY.md)
- [Compatibility and versioning](VERSIONING.md)
- [Architectural decisions](docs/decisions/)

## Developer Preview

The current package version is a Developer Preview. No crates.io publication or release tag is implied. See [CHANGELOG.md](CHANGELOG.md) and [VERSIONING.md](VERSIONING.md).

Genesis Core is licensed under the [Apache License 2.0](LICENSE).
