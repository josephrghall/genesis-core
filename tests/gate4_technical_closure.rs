use genesis_core::{
    CoreState, IdentityResolution, Mutation, NodeId, PropertyFilter, PropertyQuery, PropertySort,
    PropertyValue, ReadPage, RelationshipDirection,
};

#[test]
fn identity_resolution_distinguishes_all_four_states_across_restart() {
    let library = tempfile::tempdir().expect("temporary library");
    let core = CoreState::open(library.path()).expect("open Core");
    let never_issued = NodeId::new();
    let node = core.create_node().expect("create Node");

    assert_eq!(
        core.resolve_identity(node.id).expect("active identity"),
        IdentityResolution::Active
    );
    assert_eq!(
        core.resolve_identity(never_issued)
            .expect("never-issued identity"),
        IdentityResolution::NeverIssued
    );

    core.tombstone_node(node.id).expect("tombstone Node");
    assert_eq!(
        core.resolve_identity(node.id).expect("tombstoned identity"),
        IdentityResolution::Tombstoned
    );
    core.purge_node(node.id).expect("purge Node");
    assert_eq!(
        core.resolve_identity(node.id).expect("retired identity"),
        IdentityResolution::Retired
    );

    core.close().expect("close Core");
    let reopened = CoreState::open(library.path()).expect("reopen Core");
    assert_eq!(
        reopened
            .resolve_identity(node.id)
            .expect("retired after restart"),
        IdentityResolution::Retired
    );
    assert_eq!(
        reopened
            .resolve_identity(never_issued)
            .expect("never-issued after restart"),
        IdentityResolution::NeverIssued
    );
}

#[test]
fn node_creation_and_generic_initialization_commit_or_rollback_together() {
    let library = tempfile::tempdir().expect("temporary library");
    let core = CoreState::open(library.path()).expect("open Core");
    let target = core.create_node().expect("target Node");
    let created = NodeId::new();

    let commit = core
        .apply_batch(&[
            Mutation::CreateNode { node_id: created },
            Mutation::SetProperty {
                node_id: created,
                key: "core.title".into(),
                value: PropertyValue::Text("atomic".into()),
            },
            Mutation::AddRelationship {
                source: created,
                key: "core.related".into(),
                target: target.id,
            },
        ])
        .expect("atomic create and initialize");
    assert_eq!(commit.change_count, 3);
    assert_eq!(
        core.resolve_identity(created).expect("created identity"),
        IdentityResolution::Active
    );
    assert_eq!(
        core.get_property(created, "core.title").expect("Property"),
        Some(PropertyValue::Text("atomic".into()))
    );
    assert_eq!(
        core.query_relationships(created, RelationshipDirection::Outgoing, None)
            .expect("Relationship")
            .len(),
        1
    );

    let rolled_back = NodeId::new();
    assert!(core
        .apply_batch(&[
            Mutation::CreateNode {
                node_id: rolled_back,
            },
            Mutation::SetProperty {
                node_id: rolled_back,
                key: " invalid".into(),
                value: PropertyValue::Text("must not commit".into()),
            },
        ])
        .is_err());
    assert_eq!(
        core.resolve_identity(rolled_back)
            .expect("rolled-back identity resolution"),
        IdentityResolution::NeverIssued,
        "a failed transaction must not reserve the ID or leave a Node"
    );
}

#[test]
fn high_cardinality_reads_have_deterministic_bounded_windows() {
    let library = tempfile::tempdir().expect("temporary library");
    let core = CoreState::open(library.path()).expect("open Core");
    let nodes = core.create_nodes(6).expect("Nodes");
    for (ordinal, node) in nodes.iter().enumerate() {
        core.set_property(
            node.id,
            format!("key-{ordinal:02}"),
            PropertyValue::Integer(ordinal as i64),
        )
        .expect("Property");
    }
    for target in nodes.iter().skip(1) {
        core.add_relationship(nodes[0].id, "core.related", target.id)
            .expect("Relationship");
    }

    let all_nodes = core.list_nodes().expect("all Nodes");
    let first_nodes = core
        .list_nodes_page(ReadPage::new(0, 2))
        .expect("first Node page");
    let second_nodes = core
        .list_nodes_page(ReadPage::new(2, 2))
        .expect("second Node page");
    assert_eq!(first_nodes, all_nodes[0..2]);
    assert_eq!(second_nodes, all_nodes[2..4]);

    let properties = core.list_properties(nodes[0].id).expect("all Properties");
    let property_keys = properties.keys().cloned().collect::<Vec<_>>();
    let property_page = core
        .list_properties_page(nodes[0].id, ReadPage::new(0, 1))
        .expect("Property page");
    assert_eq!(property_page[0].0, property_keys[0]);

    let all_relationships = core
        .query_relationships(nodes[0].id, RelationshipDirection::Outgoing, None)
        .expect("all Relationships");
    let relationship_page = core
        .query_relationships_page(
            nodes[0].id,
            RelationshipDirection::Outgoing,
            None,
            ReadPage::new(1, 2),
        )
        .expect("Relationship page");
    assert_eq!(relationship_page, all_relationships[1..3]);

    let query = PropertyQuery {
        key: "key-01".into(),
        filter: Some(PropertyFilter::Equals(PropertyValue::Integer(1))),
        sort: PropertySort::Ascending,
        limit: 100,
    };
    assert_eq!(
        core.query_properties_page(&query, ReadPage::new(0, 1))
            .expect("indexed Property page")
            .len(),
        1
    );

    let graph = core.graph().expect("small compatibility graph");
    assert_eq!(
        core.graph_nodes_page(ReadPage::new(0, 2))
            .expect("graph Node page"),
        graph.nodes[0..2]
    );
    assert_eq!(
        core.graph_edges_page(ReadPage::new(1, 2))
            .expect("graph edge page"),
        graph.edges[1..3]
    );

    let all_backlinks = core.backlinks(nodes[1].id).expect("all backlinks");
    assert_eq!(
        core.backlinks_page(nodes[1].id, ReadPage::new(0, 1))
            .expect("backlink page"),
        all_backlinks[0..1]
    );

    let orphan_page = core
        .orphan_node_ids_page(ReadPage::new(0, usize::MAX))
        .expect("bounded orphan page");
    assert!(orphan_page.len() <= ReadPage::MAX_LIMIT);
}
