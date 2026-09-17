# Genesis Core quickstart

Genesis Core is an embedded Rust engine. It has no account, server, UI, document model, or application-shell dependency.

## Build and prove the package

```bash
cargo check --locked
cargo test --locked
cargo run --locked --example headless
```

## Minimal headless use

```rust
use genesis_core::{CoreState, PropertyValue, RelationshipDirection};

fn main() -> Result<(), genesis_core::CoreError> {
    let core = CoreState::open("./my-library")?;
    let source = core.create_node()?;
    let target = core.create_node()?;

    core.set_property(source.id, "title", PropertyValue::Text("Example".into()))?;
    core.add_relationship(source.id, "links_to", target.id)?;

    let title = core.get_property(source.id, "title")?;
    let links = core.query_relationships(
        source.id,
        RelationshipDirection::Outgoing,
        Some("links_to"),
    )?;
    assert!(title.is_some());
    assert_eq!(links.len(), 1);
    Ok(())
}
```

`CoreState::open` creates `.genesis/records.sqlite`, a rebuildable `.genesis/index.sqlite`, and local Record recovery material under the chosen root.

## Bounded traversal

Use opaque cursor paging for an ordinary current-state traversal:

```rust
let mut cursor = None;
loop {
    let page = core.list_nodes_cursor(500, cursor.as_ref())?;
    for node in page.items { /* consume */ }
    cursor = page.next_cursor;
    if cursor.is_none() { break; }
}
```

Use a stable snapshot for export, verification, migration, or backup preparation:

```rust
core.with_stable_read_snapshot(|snapshot| {
    let mut cursor = None;
    loop {
        let page = snapshot.nodes_page(500, cursor.as_ref())?;
        for node in page.items { /* export */ }
        cursor = page.next_cursor;
        if cursor.is_none() { break; }
    }
    Ok(())
})?;
```

Do not call canonical operations on the same `CoreState` inside the snapshot callback. See [the traversal contract](TRAVERSAL-CONTRACT.md) for the exact consistency contract.

## Lifecycle and recovery

- `tombstone_node` is recoverable and `restore_node` returns it to active use.
- `purge_node` removes the Node and Node-owned reconstructable state but permanently reserves its issued ID.
- `resolve_identity` distinguishes active, tombstoned, retired, and never-issued IDs.
- `rebuild_index` reconstructs derived Index state from Records.
- `reconcile_recovery` advances local redundant recovery material.

Read `SCHEMA-MIGRATION-RECOVERY.md`, `SECURITY.md`, and `VERSIONING.md` before using valuable data.
