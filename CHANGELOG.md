# Changelog

All notable Genesis Core changes are documented here.

Genesis Core follows [Semantic Versioning](VERSIONING.md). The project is currently in Developer Preview and has not been published to crates.io.

## Unreleased — Developer Preview

### Added

- the six-primitive boundary: Node, ID, Property, Relationship, Record, and Index;
- permanent UUIDv7 identity with an issued-ID ledger;
- typed Properties, first-class Relationships, definitions, and atomic batch mutation;
- canonical SQLite Record Store and derived rebuildable Index;
- tombstone, restore, purge, and explicit identity-resolution states;
- deterministic migrations, optional protected-store root identity and sentinel validation, integrity validation, and recovery reconstruction;
- bounded OFFSET compatibility reads, opaque keyset traversal, and exact stable Record snapshot scans;
- self-contained real-content and heterogeneous/topology validation corpora;
- public API, architecture, recovery, security, governance, contribution, and compatibility documentation;
- Apache License 2.0.

### Clarified

- Record is canonical; Index is derived and rebuildable.
- Permanent history semantics apply to issued identity, canonical facts, ownership, and lifecycle; the operational reconciliation journal is bounded and may be pruned.
- Node IDs are permanent and never reused.
- Protected mode validates configured-root identity plus a persistent sentinel; it is not a complete filesystem or process sandbox.
- ordinary mutation success means canonical success; derived Index/recovery follow-up can lag and be reconciled.
- whole-collection and OFFSET APIs are compatibility surfaces, not stable scalable scans.
- UI, content formats, application ontologies, accounts, networking, and hosted services remain outside Core.

### Validation

- headless independence proof;
- adversarial correctness, malformed-state, process-boundary, portability, and recovery tests;
- full lifecycle and traversal regression suites;
- neutral real-content exact-fidelity validation;
- neutral heterogeneous-content and structural-topology validation.

## 0.7.0 — Technical foundation checkpoint

The 0.7.0 code line established the executable foundation that became the independent engine. Subsequent work separated product-specific assumptions, hardened durability and recovery, completed the six-primitive public contract, and prepared the standalone Developer Preview without changing the Core architecture.
