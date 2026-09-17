# Architecture

Genesis Core is a headless embedded engine with a strict six-primitive conceptual boundary.

## The primitives

| Primitive | Responsibility |
| --- | --- |
| Node | Generic entity with lifecycle state |
| ID | Permanent UUIDv7 identity and issued-ID history |
| Property | Typed fact attached to a Node |
| Relationship | First-class directed edge between Nodes |
| Record | Canonical durable truth, identity, ownership, and lifecycle authority |
| Index | Derived, disposable retrieval projection |

Storage connections, schemas, transactions, migrations, validation, recovery artifacts, and security enforcement implement these primitives; they are not additional primitives.

## Authority and data flow

Consumers call `CoreState`. Mutations commit atomically to the canonical Record Store. Index and recovery projections follow from the committed Record revision and can be reconciled after lag or loss.

~~~text
consumer
   |
CoreState
   |
canonical Record Store ----> local recovery material
   |
derived Index
~~~

`records.sqlite` is authoritative. `index.sqlite` is rebuildable. Recovery material is redundant local reconstruction evidence, not a separate source of truth or off-device backup.

## Identity and lifecycle

Node IDs are issued once and never reused. Identity resolution distinguishes:

- active;
- tombstoned;
- retired after purge;
- never issued.

Tombstone is reversible. Purge removes Node-owned reconstructable state after a recovery barrier while retaining issued-ID history and independently surviving facts.

The permanence contract applies to issued identity and canonical fact/lifecycle semantics. The operational change journal used for reconciliation is bounded, may be pruned, and is not an unlimited event-history contract.

## Reads and traversal

Bounded OFFSET reads remain compatibility helpers. Ordinary current-state Node traversal uses opaque keyset cursors with a revision watermark. Exact export, migration, backup preparation, and verification use a stable canonical Record snapshot.

## Failure model

An ordinary successful mutation means canonical Record success. Derived Index or recovery follow-up can lag and be repaired. A post-commit purge-barrier failure identifies a revision already committed canonically but not yet fully protected by the required recovery barrier.

## Boundary

Core does not define a UI, document model, filesystem policy, hierarchy, product ontology, workflow, account system, synchronization service, network protocol, or hosted backend. Those belong to consuming applications.
