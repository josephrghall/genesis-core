use std::fs;

use genesis_core::{
    CoreState, PropertyFilter, PropertyQuery, PropertySort, PropertyValue, RelationshipDirection,
    INDEX_STORE_FILENAME,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let library = tempfile::tempdir()?;
    let core = CoreState::open(library.path())?;
    let source = core.create_node()?;
    let target = core.create_node()?;

    core.set_property(
        source.id,
        "title",
        PropertyValue::Text("Headless Core".into()),
    )?;
    core.add_relationship(source.id, "links_to", target.id)?;

    let property = core.get_property(source.id, "title")?;
    let relationships =
        core.query_relationships(source.id, RelationshipDirection::Outgoing, Some("links_to"))?;
    core.remove_relationship(source.id, "links_to", target.id)?;
    assert!(core
        .query_relationships(source.id, RelationshipDirection::Outgoing, Some("links_to"))?
        .is_empty());
    core.add_relationship(source.id, "links_to", target.id)?;
    let indexed = core.query_properties(&PropertyQuery {
        key: "title".into(),
        filter: Some(PropertyFilter::Equals(PropertyValue::Text(
            "Headless Core".into(),
        ))),
        sort: PropertySort::Ascending,
        limit: 10,
    })?;
    let cursor_page = core.list_nodes_cursor(1, None)?;
    assert_eq!(cursor_page.items.len(), 1);
    assert!(cursor_page.next_cursor.is_some());
    let stable_node_count = core.with_stable_read_snapshot(|snapshot| {
        let mut count = 0;
        let mut page = snapshot.nodes_page(1, None)?;
        loop {
            count += page.items.len();
            let Some(cursor) = page.next_cursor else {
                break;
            };
            page = snapshot.nodes_page(1, Some(&cursor))?;
        }
        Ok(count)
    })?;
    assert_eq!(stable_node_count, 2);
    core.reconcile_recovery()?;
    core.close()?;

    fs::remove_file(library.path().join(".genesis").join(INDEX_STORE_FILENAME))?;
    let reopened = CoreState::open(library.path())?;
    reopened.rebuild_index()?;
    let canonical_property = reopened.get_property(source.id, "title")?;

    assert_eq!(property, canonical_property);
    assert_eq!(relationships.len(), 1);
    assert_eq!(indexed.len(), 1);
    println!(
        "headless-proof=PASS source={} target={} record_revision={}",
        source.id,
        target.id,
        reopened.status()?.index_sync.record_revision
    );
    Ok(())
}
