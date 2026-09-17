use std::collections::BTreeSet;

use genesis_core::{
    CoreErrorCategory, CoreState, LifecycleState, Mutation, NewPropertyDefinition,
    NewRelationshipDefinition, PropertyValue, ReadCursor,
};

#[test]
fn cursor_traversal_crosses_one_thousand_without_offset_drift() {
    let root = tempfile::tempdir().expect("temporary Store");
    let core = CoreState::open(root.path()).expect("open Core");
    let original = core.create_nodes(1_205).expect("create Nodes");
    let initial_order = core.list_nodes().expect("initial Index order");

    let first = core
        .list_nodes_cursor(400, None)
        .expect("first cursor page");
    assert_eq!(first.items.len(), 400);
    let changed = initial_order[1_000].id;
    let tombstoned = initial_order[1_100].id;
    let inserted = core.create_node().expect("concurrent insert");
    core.set_property(changed, "changed", PropertyValue::Boolean(true))
        .expect("concurrent sort-key update");
    core.tombstone_node(tombstoned)
        .expect("concurrent tombstone");

    let mut traversed = first.items;
    let mut cursor = first.next_cursor;
    while let Some(next) = cursor {
        let page = core
            .list_nodes_cursor(400, Some(&next))
            .expect("continued cursor page");
        traversed.extend(page.items);
        cursor = page.next_cursor;
    }

    let ids = traversed.iter().map(|node| node.id).collect::<Vec<_>>();
    let unique = ids.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        ids.len(),
        unique.len(),
        "cursor traversal duplicated a Node"
    );
    assert_eq!(ids.len(), original.len() - 2);
    assert!(
        !unique.contains(&inserted.id),
        "later insert crossed watermark"
    );
    assert!(!unique.contains(&changed), "later update crossed watermark");
    assert!(
        !unique.contains(&tombstoned),
        "later tombstone crossed watermark"
    );

    let invalid = ReadCursor::from_token("not-a-valid-token");
    let error = core
        .list_nodes_cursor(10, Some(&invalid))
        .expect_err("invalid cursor must fail");
    assert_eq!(error.category(), CoreErrorCategory::Conflict);
}

#[test]
fn stable_snapshot_keeps_exact_state_while_another_connection_writes() {
    let root = tempfile::tempdir().expect("temporary Store");
    let primary = CoreState::open(root.path()).expect("open primary Core");
    let original = primary.create_nodes(1_205).expect("create Nodes");
    let writer = CoreState::open(root.path()).expect("open concurrent Core");
    let victim = original[1_100].id;
    let mut concurrent_insert = None;

    let scanned = primary
        .with_stable_read_snapshot(|snapshot| {
            let first = snapshot.nodes_page(500, None)?;
            concurrent_insert = Some(writer.create_node()?);
            writer.tombstone_node(victim)?;

            let mut nodes = first.items;
            let mut cursor = first.next_cursor;
            while let Some(next) = cursor {
                let page = snapshot.nodes_page(500, Some(&next))?;
                nodes.extend(page.items);
                cursor = page.next_cursor;
            }
            Ok(nodes)
        })
        .expect("stable scan");

    let inserted = concurrent_insert.expect("concurrent insert captured");
    assert_eq!(scanned.len(), original.len());
    assert!(scanned
        .iter()
        .all(|node| node.lifecycle_state == LifecycleState::Active));
    assert!(scanned.iter().any(|node| node.id == victim));
    assert!(!scanned.iter().any(|node| node.id == inserted.id));

    let state = primary.get_node(victim).expect("post-snapshot Node");
    assert_eq!(state.lifecycle_state, LifecycleState::Tombstoned);
}

#[test]
fn stable_snapshot_covers_identity_facts_relationships_and_definitions() {
    let root = tempfile::tempdir().expect("temporary Store");
    let core = CoreState::open(root.path()).expect("open Core");
    let source = core.create_node().expect("source");
    let target = core.create_node().expect("target");
    let retired = core.create_node().expect("retired");
    core.apply_batch(&[
        Mutation::SetProperty {
            node_id: source.id,
            key: "alpha".into(),
            value: PropertyValue::Integer(1),
        },
        Mutation::SetProperty {
            node_id: source.id,
            key: "beta".into(),
            value: PropertyValue::Text("two".into()),
        },
        Mutation::AddRelationship {
            source: source.id,
            key: "links".into(),
            target: target.id,
        },
    ])
    .expect("facts");
    core.register_property_definition(&NewPropertyDefinition {
        key: "alpha".into(),
        label: "Alpha".into(),
        description: None,
        expected_value_kind: None,
        origin: "test".into(),
    })
    .expect("Property definition");
    core.register_relationship_definition(&NewRelationshipDefinition {
        key: "links".into(),
        forward_label: "Links".into(),
        inverse_label: None,
        description: None,
        origin: "test".into(),
    })
    .expect("Relationship definition");
    core.tombstone_node(retired.id).expect("tombstone identity");
    core.purge_node(retired.id).expect("retire identity");

    core.with_stable_read_snapshot(|snapshot| {
        let ids = collect(snapshot.issued_ids_page(1, None)?, |cursor| {
            snapshot.issued_ids_page(1, Some(cursor))
        })?;
        assert!(ids.contains(&retired.id));

        let properties = collect(snapshot.properties_page(source.id, 1, None)?, |cursor| {
            snapshot.properties_page(source.id, 1, Some(cursor))
        })?;
        assert_eq!(properties.len(), 2);

        let relationships = collect(snapshot.relationships_page(1, None)?, |cursor| {
            snapshot.relationships_page(1, Some(cursor))
        })?;
        assert_eq!(relationships.len(), 1);

        assert_eq!(snapshot.property_definitions_page(1, None)?.items.len(), 1);
        assert_eq!(
            snapshot.relationship_definitions_page(1, None)?.items.len(),
            1
        );

        let node_cursor = snapshot
            .nodes_page(1, None)?
            .next_cursor
            .expect("more than one Node");
        let mismatch = snapshot
            .issued_ids_page(1, Some(&node_cursor))
            .expect_err("cursor must be collection-bound");
        assert_eq!(mismatch.category(), CoreErrorCategory::Conflict);
        Ok(())
    })
    .expect("stable surfaces");
}

fn collect<T, F>(
    first: genesis_core::CursorPage<T>,
    mut next: F,
) -> Result<Vec<T>, genesis_core::CoreError>
where
    F: FnMut(&ReadCursor) -> Result<genesis_core::CursorPage<T>, genesis_core::CoreError>,
{
    let mut items = first.items;
    let mut cursor = first.next_cursor;
    while let Some(current) = cursor {
        let page = next(&current)?;
        items.extend(page.items);
        cursor = page.next_cursor;
    }
    Ok(items)
}

#[test]
fn cursor_rejects_out_of_range_and_inconsistent_revisions() {
    let root = tempfile::tempdir().expect("temporary Store");
    let core = CoreState::open(root.path()).expect("open Core");
    core.create_nodes(3).expect("Nodes in one revision");
    let first = core.list_nodes_cursor(1, None).expect("first page");
    let cursor = first.next_cursor.expect("continuation");
    let bytes = cursor
        .token()
        .as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    // Test-only decoding deliberately exercises the current private encoding.
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for (watermark, ordering_revision) in [
        (1, u64::MAX),
        (1, i64::MAX as u64 + 1),
        (1, 2),
        (1, 0),
        (0, 1),
        (u64::MAX, 1),
    ] {
        let mut payload = original.clone();
        payload["revision"] = serde_json::json!(watermark);
        payload["after"][0] = serde_json::json!(ordering_revision.to_string());
        let token = serde_json::to_vec(&payload)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let error = core
            .list_nodes_cursor(1, Some(&ReadCursor::from_token(token)))
            .expect_err("invalid numeric cursor must not return an empty page");
        assert_eq!(error.category(), CoreErrorCategory::Conflict);
    }
    let next = core
        .list_nodes_cursor(1, Some(&cursor))
        .expect("unchanged valid cursor still works");
    assert_eq!(next.items.len(), 1);
    assert_ne!(next.items[0].id, first.items[0].id);
}
