use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    time::Instant,
};

use genesis_core::{
    CoreError, CoreErrorCategory, CoreState, CursorPage, IdentityResolution, IndexSyncState,
    LifecycleState, Mutation, NodeId, PropertyValue, ReadCursor, SearchDocumentInput,
    GENESIS_METADATA_DIRECTORY, INDEX_STORE_FILENAME, RECORD_STORE_FILENAME,
};
use sha2::{Digest, Sha256};

const SOURCE_SHA: &str = "public-corpus-v1";
const RELATIONSHIP_KEY: &str = "content.references";

#[derive(Clone)]
struct CorpusItem {
    path: &'static str,
    title: &'static str,
    content: &'static str,
    kind: &'static str,
}

struct ExpectedNode {
    id: NodeId,
    properties: BTreeMap<String, PropertyValue>,
    retired: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct PortableItem {
    id: NodeId,
    path: String,
    title: String,
    content: String,
    sha256: String,
}

#[test]
fn neutral_public_content_survives_the_complete_public_contract_campaign() {
    let corpus = corpus();
    let corpus_item_count = corpus.len();
    let corpus_total_bytes = corpus.iter().map(|item| item.content.len()).sum::<usize>();
    let largest_item = corpus
        .iter()
        .max_by_key(|item| item.content.len())
        .expect("non-empty validation corpus");
    let corpus_hash = corpus_aggregate_hash(&corpus);
    verify_corpus_manifest(&corpus, &corpus_hash);
    let root = tempfile::tempdir().expect("temporary real-content Store");

    let ingest_started = Instant::now();
    let mut core = CoreState::open(root.path()).expect("open real-content Core");
    let (mut expected, expected_relationships) = ingest(&core, &corpus);
    let ingest_elapsed = ingest_started.elapsed();
    let properties_created = expected
        .iter()
        .map(|node| node.properties.len())
        .sum::<usize>();
    let relationships_created = expected_relationships.len();
    let mut exact_content_verifications =
        verify_expected(&core, &expected, &expected_relationships);
    rebuild_content_projection(&core, &expected);
    assert_unique_search_hit(&core, &expected, "quasarneedle");
    core.reconcile_recovery()
        .expect("materialize recovery after initial ingest");

    core.close().expect("clean close after ingest");
    let reopen_started = Instant::now();
    core = CoreState::open(root.path()).expect("reopen real-content Core");
    let reopen_elapsed = reopen_started.elapsed();
    exact_content_verifications += verify_expected(&core, &expected, &expected_relationships);

    run_mutation_campaign(&core, &mut expected, &expected_relationships);
    exact_content_verifications += verify_expected(&core, &expected, &expected_relationships);

    let canonical_before_index_loss = canonical_digest(&core, &expected_relationships);
    core.reconcile_recovery()
        .expect("recovery current before Index loss");
    core.close().expect("close before deleting derived Index");
    fs::remove_file(
        root.path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join(INDEX_STORE_FILENAME),
    )
    .expect("remove derived Index");
    core = CoreState::open(root.path()).expect("open without derived Index");
    let rebuild_started = Instant::now();
    core.rebuild_index()
        .expect("rebuild Index from Record truth");
    let rebuild_elapsed = rebuild_started.elapsed();
    assert_eq!(
        core.status()
            .expect("status after rebuild")
            .index_sync
            .state,
        IndexSyncState::Current
    );
    assert_eq!(
        canonical_digest(&core, &expected_relationships),
        canonical_before_index_loss,
        "Index loss/rebuild changed canonical truth"
    );
    exact_content_verifications += verify_expected(&core, &expected, &expected_relationships);
    rebuild_content_projection(&core, &expected);
    assert_unique_search_hit(&core, &expected, "mutationneedle");

    let traversal_started = Instant::now();
    run_cursor_traversal_with_concurrent_mutation(&core, &mut expected);
    run_stable_snapshot_with_concurrent_writer(root.path(), &core, &mut expected);
    let traversal_elapsed = traversal_started.elapsed();

    let portable = export_portable_view(&core);
    assert_eq!(portable, expected_portable_view(&expected));
    exact_content_verifications += portable.len();

    core.reconcile_recovery()
        .expect("recovery current before reconstruction");
    core.close().expect("close before canonical recovery proof");
    fs::remove_file(
        root.path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join(RECORD_STORE_FILENAME),
    )
    .expect("remove canonical Store to exercise supported recovery");
    let recovery_started = Instant::now();
    core = CoreState::open(root.path()).expect("recover Record Store from recovery material");
    let recovery_elapsed = recovery_started.elapsed();
    exact_content_verifications += verify_expected(&core, &expected, &expected_relationships);
    assert!(
        core.status()
            .expect("recovered status")
            .record_store
            .integrity_ok
    );

    let metadata = root.path().join(GENESIS_METADATA_DIRECTORY);
    let record_bytes = fs::metadata(metadata.join(RECORD_STORE_FILENAME))
        .expect("Record Store metadata")
        .len();
    let index_bytes = fs::metadata(metadata.join(INDEX_STORE_FILENAME))
        .expect("Index Store metadata")
        .len();

    println!(
        "GENESIS_REAL_CONTENT_METRICS={}",
        serde_json::json!({
            "source_sha": SOURCE_SHA,
            "corpus_items": corpus_item_count,
            "corpus_total_bytes": corpus_total_bytes,
            "largest_item": largest_item.path,
            "largest_item_bytes": largest_item.content.len(),
            "aggregate_corpus_sha256": corpus_hash,
            "nodes_created_initially": corpus_item_count,
            "properties_created_initially": properties_created,
            "relationships_created_initially": relationships_created,
            "ingest_elapsed_us": ingest_elapsed.as_micros() as u64,
            "reopen_elapsed_us": reopen_elapsed.as_micros() as u64,
            "traversal_elapsed_us": traversal_elapsed.as_micros() as u64,
            "index_rebuild_elapsed_us": rebuild_elapsed.as_micros() as u64,
            "recovery_elapsed_us": recovery_elapsed.as_micros() as u64,
            "record_store_bytes": record_bytes,
            "index_store_bytes": index_bytes,
            "exact_content_verifications": exact_content_verifications,
            "injected_degraded_events": 1,
            "unexpected_failures": 0,
        })
    );
}

fn corpus() -> Vec<CorpusItem> {
    vec![
        CorpusItem {
            path: "README.md",
            title: "Genesis Core",
            content: include_str!("../README.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "CHANGELOG.md",
            title: "Changelog",
            content: include_str!("../CHANGELOG.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "Cargo.toml",
            title: "Rust crate manifest",
            content: include_str!("../Cargo.toml"),
            kind: "structured_text",
        },
        CorpusItem {
            path: "docs/ARCHITECTURE.md",
            title: "Architecture",
            content: include_str!("../docs/ARCHITECTURE.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "docs/API.md",
            title: "API guide",
            content: include_str!("../docs/API.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "docs/SEMANTIC-CONTRACT.md",
            title: "Core semantic contract",
            content: include_str!("../docs/SEMANTIC-CONTRACT.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "docs/ID-CONTRACT.md",
            title: "ID contract",
            content: include_str!("../docs/ID-CONTRACT.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "docs/RECORD-INDEX-TRANSACTION-CONTRACT.md",
            title: "Record and Index transaction contract",
            content: include_str!("../docs/RECORD-INDEX-TRANSACTION-CONTRACT.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "docs/TRAVERSAL-CONTRACT.md",
            title: "Traversal contract",
            content: include_str!("../docs/TRAVERSAL-CONTRACT.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "src/value.rs",
            title: "Typed value implementation",
            content: include_str!("../src/value.rs"),
            kind: "source_code",
        },
        CorpusItem {
            path: "docs/QUICKSTART.md",
            title: "Quickstart",
            content: include_str!("../docs/QUICKSTART.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "docs/SCHEMA-MIGRATION-RECOVERY.md",
            title: "Schema, migration, and recovery",
            content: include_str!("../docs/SCHEMA-MIGRATION-RECOVERY.md"),
            kind: "public_markdown",
        },
        CorpusItem {
            path: "validation/empty.md",
            title: "Empty fixture",
            content: "",
            kind: "bounded_edge_fixture",
        },
        CorpusItem {
            path: "validation/unicode.md",
            title: "Unicode fidelity",
            content: "# Création — 日本語 — Ελληνικά\n\nFaith • stewardship • misión • 🧬\nquasarneedle\n",
            kind: "bounded_edge_fixture",
        },
        CorpusItem {
            path: "validation/duplicate-a.md",
            title: "Duplicate content A",
            content: "# Duplicate payload\n\nDistinct identities must remain distinct.\n",
            kind: "bounded_edge_fixture",
        },
        CorpusItem {
            path: "validation/duplicate-b.md",
            title: "Duplicate content B",
            content: "# Duplicate payload\n\nDistinct identities must remain distinct.\n",
            kind: "bounded_edge_fixture",
        },
        CorpusItem {
            path: "validation/same-title-a.md",
            title: "Shared title",
            content: "First body under a shared title.\n",
            kind: "bounded_edge_fixture",
        },
        CorpusItem {
            path: "validation/same-title-b.md",
            title: "Shared title",
            content: "Second, different body under a shared title.\n",
            kind: "bounded_edge_fixture",
        },
        CorpusItem {
            path: "validation/structured.json",
            title: "Structured JSON",
            content: "{\"name\":\"neutral fixture\",\"enabled\":true,\"count\":3}\n",
            kind: "structured_text",
        },
    ]
}
fn ingest(
    core: &CoreState,
    corpus: &[CorpusItem],
) -> (Vec<ExpectedNode>, BTreeSet<(NodeId, String, NodeId)>) {
    let ids = corpus.iter().map(|_| NodeId::new()).collect::<Vec<_>>();
    assert_eq!(
        ids.iter().copied().collect::<BTreeSet<_>>().len(),
        corpus.len()
    );
    let mut mutations = Vec::new();
    let mut expected = Vec::new();

    for (ordinal, (item, id)) in corpus.iter().zip(ids.iter().copied()).enumerate() {
        let properties = item_properties(item, ordinal);
        mutations.push(Mutation::CreateNode { node_id: id });
        for (key, value) in &properties {
            mutations.push(Mutation::SetProperty {
                node_id: id,
                key: key.clone(),
                value: value.clone(),
            });
        }
        expected.push(ExpectedNode {
            id,
            properties,
            retired: false,
        });
    }

    let declared_pairs = [
        (0, 4),
        (0, 6),
        (4, 5),
        (5, 7),
        (7, 8),
        (9, 10),
        (10, 11),
        (14, 12),
        (14, 15),
        (16, 17),
    ];
    let mut relationships = BTreeSet::new();
    for (source, target) in declared_pairs {
        let relationship: (NodeId, String, NodeId) =
            (ids[source], RELATIONSHIP_KEY.into(), ids[target]);
        mutations.push(Mutation::AddRelationship {
            source: relationship.0,
            key: relationship.1.clone(),
            target: relationship.2,
        });
        relationships.insert(relationship);
    }

    core.apply_batch(&mutations)
        .expect("atomic real-content ingest");
    assert_eq!(
        core.list_nodes().expect("initial Nodes").len(),
        corpus.len()
    );
    assert_ne!(expected[14].id, expected[15].id);
    assert_eq!(
        text_property(&expected[14].properties, "content.body"),
        text_property(&expected[15].properties, "content.body")
    );
    assert_eq!(
        text_property(&expected[16].properties, "content.title"),
        text_property(&expected[17].properties, "content.title")
    );
    assert_ne!(
        text_property(&expected[16].properties, "content.body"),
        text_property(&expected[17].properties, "content.body")
    );
    (expected, relationships)
}

fn item_properties(item: &CorpusItem, ordinal: usize) -> BTreeMap<String, PropertyValue> {
    BTreeMap::from([
        (
            "content.body".into(),
            PropertyValue::Text(item.content.into()),
        ),
        (
            "content.byte_length".into(),
            PropertyValue::Integer(item.content.len() as i64),
        ),
        ("content.kind".into(), PropertyValue::Text(item.kind.into())),
        (
            "content.ordinal".into(),
            PropertyValue::Integer(ordinal as i64),
        ),
        ("content.path".into(), PropertyValue::Text(item.path.into())),
        (
            "content.sha256".into(),
            PropertyValue::Text(sha256(item.content.as_bytes())),
        ),
        (
            "content.title".into(),
            PropertyValue::Text(item.title.into()),
        ),
    ])
}

fn verify_expected(
    core: &CoreState,
    expected: &[ExpectedNode],
    expected_relationships: &BTreeSet<(NodeId, String, NodeId)>,
) -> usize {
    let active_expected = expected.iter().filter(|node| !node.retired).count();
    assert_eq!(
        core.list_nodes().expect("indexed Nodes").len(),
        active_expected
    );
    let mut verified = 0;
    for node in expected {
        if node.retired {
            assert_eq!(
                core.resolve_identity(node.id).expect("retired identity"),
                IdentityResolution::Retired
            );
            continue;
        }
        assert_eq!(
            core.resolve_identity(node.id).expect("active identity"),
            IdentityResolution::Active
        );
        assert_eq!(
            core.list_properties(node.id).expect("canonical Properties"),
            node.properties
        );
        let content = text_property(&node.properties, "content.body");
        assert_eq!(
            core.get_property(node.id, "content.body")
                .expect("content Property"),
            Some(PropertyValue::Text(content.clone()))
        );
        assert_eq!(
            core.get_property(node.id, "content.sha256")
                .expect("hash Property"),
            Some(PropertyValue::Text(sha256(content.as_bytes())))
        );
        verified += 1;
    }
    assert_eq!(all_relationships(core), *expected_relationships);
    verified
}

fn run_mutation_campaign(
    core: &CoreState,
    expected: &mut Vec<ExpectedNode>,
    expected_relationships: &BTreeSet<(NodeId, String, NodeId)>,
) {
    let original_id = expected[0].id;
    let edited = format!(
        "{}\n\n## Deterministic validation edit\n\nmutationneedle preserves identity.\n",
        text_property(&expected[0].properties, "content.body")
    );
    set_expected_content(&mut expected[0], edited.clone());
    core.apply_batch(&[
        Mutation::SetProperty {
            node_id: original_id,
            key: "content.body".into(),
            value: PropertyValue::Text(edited.clone()),
        },
        Mutation::SetProperty {
            node_id: original_id,
            key: "content.byte_length".into(),
            value: PropertyValue::Integer(edited.len() as i64),
        },
        Mutation::SetProperty {
            node_id: original_id,
            key: "content.sha256".into(),
            value: PropertyValue::Text(sha256(edited.as_bytes())),
        },
    ])
    .expect("substantial content edit");
    assert_eq!(
        core.get_node(original_id).expect("identity after edit").id,
        original_id
    );

    let renamed = "Changelog — validated".to_string();
    core.set_property(
        expected[1].id,
        "content.title",
        PropertyValue::Text(renamed.clone()),
    )
    .expect("metadata/title change");
    expected[1]
        .properties
        .insert("content.title".into(), PropertyValue::Text(renamed));

    core.set_property(
        original_id,
        "content.reviewed",
        PropertyValue::Boolean(false),
    )
    .expect("Property add");
    core.set_property(
        original_id,
        "content.reviewed",
        PropertyValue::Boolean(true),
    )
    .expect("Property change");
    core.remove_property(original_id, "content.reviewed")
        .expect("Property remove");

    let removable = expected_relationships
        .iter()
        .next()
        .expect("declared Relationship")
        .clone();
    core.remove_relationship(removable.0, &removable.1, removable.2)
        .expect("Relationship remove");
    core.add_relationship(removable.0, &removable.1, removable.2)
        .expect("Relationship restore");

    let new_id = NodeId::new();
    let new_item = CorpusItem {
        path: "validation/created-during-mutation.md",
        title: "Created during mutation",
        content: "# New item\n\nCreated through the same generic adapter.\n",
        kind: "bounded_edge_fixture",
    };
    let new_properties = item_properties(&new_item, expected.len());
    let mut mutations = vec![Mutation::CreateNode { node_id: new_id }];
    mutations.extend(
        new_properties
            .iter()
            .map(|(key, value)| Mutation::SetProperty {
                node_id: new_id,
                key: key.clone(),
                value: value.clone(),
            }),
    );
    core.apply_batch(&mutations).expect("new content item");
    expected.push(ExpectedNode {
        id: new_id,
        properties: new_properties,
        retired: false,
    });

    let purge_index = 12;
    let purge_id = expected[purge_index].id;
    core.tombstone_node(purge_id).expect("tombstone");
    assert_eq!(
        core.resolve_identity(purge_id)
            .expect("tombstoned identity"),
        IdentityResolution::Tombstoned
    );
    core.restore_node(purge_id).expect("restore");
    assert_eq!(
        core.resolve_identity(purge_id).expect("restored identity"),
        IdentityResolution::Active
    );
    core.tombstone_node(purge_id)
        .expect("tombstone before purge");
    core.purge_node(purge_id)
        .expect("purge within existing contract");
    expected[purge_index].retired = true;
    assert!(expected_relationships
        .iter()
        .any(|relationship| relationship.2 == purge_id));
    assert_eq!(
        all_relationships(core),
        *expected_relationships,
        "independently surviving Relationship must remain historical fact"
    );
}

fn run_cursor_traversal_with_concurrent_mutation(
    core: &CoreState,
    expected: &mut Vec<ExpectedNode>,
) {
    let before = core.list_nodes().expect("pre-traversal Nodes");
    let first = core.list_nodes_cursor(4, None).expect("first cursor page");
    assert_eq!(first.items.len(), 4);
    let changed = before.last().expect("Node outside first page").id;
    assert!(!first.items.iter().any(|node| node.id == changed));

    core.set_property(
        changed,
        "content.concurrent_marker",
        PropertyValue::Boolean(true),
    )
    .expect("controlled concurrent update");
    expected
        .iter_mut()
        .find(|node| node.id == changed)
        .expect("expected changed Node")
        .properties
        .insert(
            "content.concurrent_marker".into(),
            PropertyValue::Boolean(true),
        );

    let inserted_id = NodeId::new();
    let inserted_item = CorpusItem {
        path: "validation/ordinary-cursor-insert.md",
        title: "Cursor watermark insert",
        content: "Inserted after the first ordinary cursor page.\n",
        kind: "bounded_edge_fixture",
    };
    let inserted_properties = item_properties(&inserted_item, expected.len());
    let mut mutations = vec![Mutation::CreateNode {
        node_id: inserted_id,
    }];
    mutations.extend(
        inserted_properties
            .iter()
            .map(|(key, value)| Mutation::SetProperty {
                node_id: inserted_id,
                key: key.clone(),
                value: value.clone(),
            }),
    );
    core.apply_batch(&mutations).expect("concurrent insert");
    expected.push(ExpectedNode {
        id: inserted_id,
        properties: inserted_properties,
        retired: false,
    });

    let mut delivered = first.items;
    let mut cursor = first.next_cursor;
    while let Some(next) = cursor {
        let page = core
            .list_nodes_cursor(4, Some(&next))
            .expect("continue cursor traversal");
        delivered.extend(page.items);
        cursor = page.next_cursor;
    }
    let delivered_ids = delivered.iter().map(|node| node.id).collect::<Vec<_>>();
    let delivered_set = delivered_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut expected_set = before.iter().map(|node| node.id).collect::<BTreeSet<_>>();
    expected_set.remove(&changed);
    assert_eq!(
        delivered_ids.len(),
        delivered_set.len(),
        "no duplicate delivery"
    );
    assert_eq!(
        delivered_set, expected_set,
        "only documented update exclusion"
    );
    assert!(
        !delivered_set.contains(&inserted_id),
        "watermark excludes insert"
    );

    let invalid = ReadCursor::from_token("not-a-valid-token");
    assert_eq!(
        core.list_nodes_cursor(4, Some(&invalid))
            .expect_err("invalid cursor")
            .category(),
        CoreErrorCategory::Conflict
    );
}

fn run_stable_snapshot_with_concurrent_writer(
    root: &std::path::Path,
    primary: &CoreState,
    expected: &mut Vec<ExpectedNode>,
) {
    let before = primary.list_nodes().expect("pre-snapshot Nodes");
    let before_ids = before.iter().map(|node| node.id).collect::<BTreeSet<_>>();
    let victim = before.last().expect("snapshot victim").id;
    let writer = CoreState::open(root).expect("concurrent writer");
    let inserted_id = NodeId::new();
    let inserted_item = CorpusItem {
        path: "validation/stable-snapshot-insert.md",
        title: "Stable snapshot insert",
        content: "Inserted while the canonical snapshot is open.\n",
        kind: "bounded_edge_fixture",
    };
    let inserted_properties = item_properties(&inserted_item, expected.len());

    let scanned = primary
        .with_stable_read_snapshot(|snapshot| {
            let first = snapshot.nodes_page(5, None)?;
            let mut mutations = vec![Mutation::CreateNode {
                node_id: inserted_id,
            }];
            mutations.extend(inserted_properties.iter().map(|(key, value)| {
                Mutation::SetProperty {
                    node_id: inserted_id,
                    key: key.clone(),
                    value: value.clone(),
                }
            }));
            writer.apply_batch(&mutations)?;
            writer.tombstone_node(victim)?;
            collect_pages(first, |cursor| snapshot.nodes_page(5, Some(cursor)))
        })
        .expect("stable snapshot scan");
    assert_eq!(
        scanned.iter().map(|node| node.id).collect::<BTreeSet<_>>(),
        before_ids
    );
    assert!(scanned
        .iter()
        .any(|node| node.id == victim && node.lifecycle_state == LifecycleState::Active));
    assert!(!scanned.iter().any(|node| node.id == inserted_id));
    assert_eq!(
        primary
            .get_node(victim)
            .expect("post-snapshot victim")
            .lifecycle_state,
        LifecycleState::Tombstoned
    );
    writer
        .restore_node(victim)
        .expect("restore snapshot victim");
    writer.close().expect("close concurrent writer");
    expected.push(ExpectedNode {
        id: inserted_id,
        properties: inserted_properties,
        retired: false,
    });
}

fn export_portable_view(core: &CoreState) -> Vec<PortableItem> {
    core.with_stable_read_snapshot(|snapshot| {
        let nodes = collect_pages(snapshot.nodes_page(4, None)?, |cursor| {
            snapshot.nodes_page(4, Some(cursor))
        })?;
        let mut portable = Vec::new();
        for node in nodes {
            let properties =
                collect_pages(snapshot.properties_page(node.id, 3, None)?, |cursor| {
                    snapshot.properties_page(node.id, 3, Some(cursor))
                })?
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            portable.push(PortableItem {
                id: node.id,
                path: text_property(&properties, "content.path"),
                title: text_property(&properties, "content.title"),
                content: text_property(&properties, "content.body"),
                sha256: text_property(&properties, "content.sha256"),
            });
        }
        portable.sort_by_key(|item| item.id);
        Ok(portable)
    })
    .expect("portable reconstruction from canonical snapshot")
}

fn expected_portable_view(expected: &[ExpectedNode]) -> Vec<PortableItem> {
    let mut portable = expected
        .iter()
        .filter(|node| !node.retired)
        .map(|node| PortableItem {
            id: node.id,
            path: text_property(&node.properties, "content.path"),
            title: text_property(&node.properties, "content.title"),
            content: text_property(&node.properties, "content.body"),
            sha256: text_property(&node.properties, "content.sha256"),
        })
        .collect::<Vec<_>>();
    portable.sort_by_key(|item| item.id);
    portable
}

fn rebuild_content_projection(core: &CoreState, expected: &[ExpectedNode]) {
    let documents = expected
        .iter()
        .filter(|node| !node.retired)
        .map(|node| SearchDocumentInput {
            document_key: text_property(&node.properties, "content.path"),
            node_id: node.id,
            source_kind: text_property(&node.properties, "content.kind"),
            title: text_property(&node.properties, "content.title"),
            body: text_property(&node.properties, "content.body"),
            metadata: "real-content-validation".into(),
            source_locator: Some(text_property(&node.properties, "content.path")),
            source_fingerprint: text_property(&node.properties, "content.sha256"),
        })
        .collect::<Vec<_>>();
    core.replace_content_projection(&documents, &[])
        .expect("adapter rebuilds derived content projection");
}

fn assert_unique_search_hit(core: &CoreState, expected: &[ExpectedNode], query: &str) {
    let expected_id = expected
        .iter()
        .find(|node| text_property(&node.properties, "content.body").contains(query))
        .expect("query fixture")
        .id;
    let hits = core.search(query, 10).expect("derived search query");
    assert!(hits.iter().any(|hit| hit.node_id == expected_id));
}

fn canonical_digest(
    core: &CoreState,
    relationships: &BTreeSet<(NodeId, String, NodeId)>,
) -> String {
    let portable = export_portable_view(core);
    let value = serde_json::json!({
        "items": portable.iter().map(|item| serde_json::json!({
            "id": item.id,
            "path": item.path,
            "title": item.title,
            "content": item.content,
            "sha256": item.sha256,
        })).collect::<Vec<_>>(),
        "relationships": relationships,
    });
    sha256(
        serde_json::to_vec(&value)
            .expect("canonical JSON")
            .as_slice(),
    )
}

fn all_relationships(core: &CoreState) -> BTreeSet<(NodeId, String, NodeId)> {
    core.with_stable_read_snapshot(|snapshot| {
        Ok(
            collect_pages(snapshot.relationships_page(4, None)?, |cursor| {
                snapshot.relationships_page(4, Some(cursor))
            })?
            .into_iter()
            .map(|relationship| (relationship.source, relationship.key, relationship.target))
            .collect(),
        )
    })
    .expect("canonical Relationships")
}

fn collect_pages<T, F>(first: CursorPage<T>, mut next: F) -> Result<Vec<T>, CoreError>
where
    F: FnMut(&ReadCursor) -> Result<CursorPage<T>, CoreError>,
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

fn set_expected_content(node: &mut ExpectedNode, content: String) {
    node.properties
        .insert("content.body".into(), PropertyValue::Text(content.clone()));
    node.properties.insert(
        "content.byte_length".into(),
        PropertyValue::Integer(content.len() as i64),
    );
    node.properties.insert(
        "content.sha256".into(),
        PropertyValue::Text(sha256(content.as_bytes())),
    );
}

fn text_property(properties: &BTreeMap<String, PropertyValue>, key: &str) -> String {
    match properties.get(key) {
        Some(PropertyValue::Text(value)) => value.clone(),
        other => panic!("expected text Property {key}, received {other:?}"),
    }
}

fn corpus_aggregate_hash(corpus: &[CorpusItem]) -> String {
    let mut hasher = Sha256::new();
    for item in corpus {
        hasher.update(item.path.as_bytes());
        hasher.update([0]);
        hasher.update(sha256(item.content.as_bytes()).as_bytes());
        hasher.update([0]);
        hasher.update(item.content.len().to_string().as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

fn verify_corpus_manifest(corpus: &[CorpusItem], aggregate_hash: &str) {
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/real-content-corpus-manifest.json"))
            .expect("parse corpus manifest");
    assert_eq!(manifest["source_sha"], SOURCE_SHA);
    assert_eq!(manifest["item_count"], corpus.len());
    assert_eq!(
        manifest["total_bytes"],
        corpus.iter().map(|item| item.content.len()).sum::<usize>()
    );
    assert_eq!(manifest["aggregate_sha256"], aggregate_hash);
    let items = manifest["items"].as_array().expect("manifest items");
    assert_eq!(items.len(), corpus.len());
    for (item, entry) in corpus.iter().zip(items) {
        assert_eq!(entry["path"], item.path);
        assert_eq!(entry["title"], item.title);
        assert_eq!(entry["kind"], item.kind);
        assert_eq!(entry["bytes"], item.content.len());
        assert_eq!(entry["sha256"], sha256(item.content.as_bytes()));
    }
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
