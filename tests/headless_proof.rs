use std::fs;

use genesis_core::{
    CoreState, PropertyFilter, PropertyQuery, PropertySort, PropertyValue, RelationshipDirection,
    INDEX_STORE_FILENAME, RECORD_STORE_FILENAME,
};

#[test]
fn six_primitives_survive_index_destruction_and_record_recovery() {
    let library = tempfile::tempdir().expect("temporary Core library");
    let core = CoreState::open(library.path()).expect("open headless Core");
    let source = core.create_node().expect("create source Node");
    let target = core.create_node().expect("create target Node");

    core.set_property(source.id, "title", PropertyValue::Text("Headless".into()))
        .expect("set Property");
    core.add_relationship(source.id, "links_to", target.id)
        .expect("create Relationship");

    assert_eq!(
        core.get_property(source.id, "title")
            .expect("read Property"),
        Some(PropertyValue::Text("Headless".into()))
    );
    assert_eq!(
        core.query_relationships(source.id, RelationshipDirection::Outgoing, Some("links_to"))
            .expect("traverse Relationship")
            .len(),
        1
    );
    core.remove_relationship(source.id, "links_to", target.id)
        .expect("remove Relationship");
    assert!(core
        .query_relationships(source.id, RelationshipDirection::Outgoing, Some("links_to"))
        .expect("confirm Relationship removal")
        .is_empty());
    core.add_relationship(source.id, "links_to", target.id)
        .expect("restore Relationship for persistence proof");
    assert_eq!(
        core.query_properties(&PropertyQuery {
            key: "title".into(),
            filter: Some(PropertyFilter::Equals(PropertyValue::Text(
                "Headless".into()
            ))),
            sort: PropertySort::Ascending,
            limit: 10,
        })
        .expect("query Index")
        .len(),
        1
    );

    core.reconcile_recovery().expect("write recovery material");
    core.close().expect("close before deleting derived Index");
    fs::remove_file(library.path().join(".genesis").join(INDEX_STORE_FILENAME))
        .expect("delete derived Index");

    let rebuilt = CoreState::open(library.path()).expect("reopen without Index");
    rebuilt
        .rebuild_index()
        .expect("rebuild Index from Record truth");
    assert_eq!(
        rebuilt.get_node(source.id).expect("canonical Node").id,
        source.id
    );
    assert_eq!(
        rebuilt
            .get_property(source.id, "title")
            .expect("canonical Property"),
        Some(PropertyValue::Text("Headless".into()))
    );
    rebuilt.close().expect("close before recovery proof");

    fs::remove_file(library.path().join(".genesis").join(RECORD_STORE_FILENAME))
        .expect("remove canonical Store to exercise recovery entry point");
    let recovered = CoreState::open(library.path()).expect("recover canonical Record truth");
    assert_eq!(
        recovered.get_node(source.id).expect("recovered Node").id,
        source.id
    );
    assert_eq!(
        recovered
            .query_relationships(source.id, RelationshipDirection::Outgoing, Some("links_to"))
            .expect("recovered Relationship")
            .len(),
        1
    );
    assert!(
        recovered
            .status()
            .expect("status")
            .record_store
            .integrity_ok
    );
}
