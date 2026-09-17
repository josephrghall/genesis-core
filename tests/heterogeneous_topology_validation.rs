use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    path::Path,
    time::Instant,
};

use genesis_core::{
    CoreError, CoreState, CursorPage, IdentityResolution, IndexSyncState, LifecycleState, Mutation,
    NodeId, PropertyValue, ReadCursor, RelationshipDirection, GENESIS_METADATA_DIRECTORY,
    INDEX_STORE_FILENAME, RECORD_STORE_FILENAME,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const SOURCE_SHA: &str = "public-corpus-v1";
const PARENT: &str = "topology.parent";
const CROSS_LINK: &str = "topology.cross_link";

#[derive(Debug, Deserialize)]
struct Manifest {
    source_sha: String,
    item_count: usize,
    total_bytes: usize,
    aggregate_sha256: String,
    items: Vec<ManifestItem>,
}

#[derive(Debug, Clone, Deserialize)]
struct ManifestItem {
    path: String,
    title: String,
    logical_type: String,
    mime: String,
    bytes: usize,
    sha256: String,
    treatment_mode: String,
    fixture_class: String,
    generator: Option<usize>,
}

#[derive(Clone)]
struct CorpusItem {
    manifest: ManifestItem,
    bytes: Vec<u8>,
}

#[derive(Clone)]
struct ExpectedNode {
    properties: BTreeMap<String, PropertyValue>,
    lifecycle: LifecycleState,
}

#[derive(Default)]
struct ExpectedModel {
    nodes: BTreeMap<NodeId, ExpectedNode>,
    retired: BTreeSet<NodeId>,
    relationships: BTreeSet<(NodeId, String, NodeId)>,
}

struct Topology {
    deep: Vec<NodeId>,
    wide_root: NodeId,
    wide_children: Vec<NodeId>,
    balanced_levels: Vec<Vec<NodeId>>,
    dag_parents: Vec<NodeId>,
    dag_shared: Vec<NodeId>,
    cross_links: Vec<(NodeId, String, NodeId)>,
    component_roots: Vec<NodeId>,
    cycle: Vec<NodeId>,
    taxonomy_roots: Vec<NodeId>,
    taxonomy_items: Vec<NodeId>,
    taxonomy_edges: BTreeSet<(NodeId, String, NodeId)>,
    purge_parent: NodeId,
    purge_child: NodeId,
}

#[derive(Debug, PartialEq)]
struct PortableState {
    nodes: BTreeMap<NodeId, (LifecycleState, BTreeMap<String, PropertyValue>)>,
    relationships: BTreeSet<(NodeId, String, NodeId)>,
}

#[test]
fn heterogeneous_content_and_structural_topology_survive_public_contracts() {
    let manifest: Manifest = serde_json::from_str(include_str!(
        "fixtures/heterogeneous-topology-corpus-manifest.json"
    ))
    .expect("parse heterogeneous corpus manifest");
    let corpus = build_corpus(&manifest);
    verify_manifest(&manifest, &corpus);
    let root = tempfile::tempdir().expect("temporary heterogeneous/topology Store");

    let ingest_started = Instant::now();
    let mut core = CoreState::open(root.path()).expect("open Core");
    let mut model = ExpectedModel::default();
    let corpus_ids = ingest_corpus(&core, &corpus, &mut model);
    let topology = ingest_topology(&core, &mut model);
    let ingest_elapsed = ingest_started.elapsed();
    let initial_nodes = model.nodes.len();
    let initial_properties = model
        .nodes
        .values()
        .map(|node| node.properties.len())
        .sum::<usize>();
    let initial_relationships = model.relationships.len();
    let native_payload_items = corpus
        .iter()
        .filter(|item| item.manifest.treatment_mode == "native_utf8_text")
        .count();
    let consumer_encoded_items = corpus.len() - native_payload_items;
    let mut exact_comparisons = verify_corpus(&core, &corpus, &corpus_ids);

    let topology_started = Instant::now();
    verify_model(&core, &model);
    verify_topology(&core, &topology);
    let topology_reconstruction_elapsed = topology_started.elapsed();
    core.reconcile_recovery()
        .expect("materialize initial recovery state");

    core.close().expect("clean close");
    let reopen_started = Instant::now();
    core = CoreState::open(root.path()).expect("reopen Core");
    let reopen_elapsed = reopen_started.elapsed();
    verify_model(&core, &model);
    verify_topology(&core, &topology);
    exact_comparisons += verify_corpus(&core, &corpus, &corpus_ids);

    run_structural_mutations(&core, &mut model, &topology, &corpus, &corpus_ids);
    exact_comparisons += verify_corpus(&core, &corpus, &corpus_ids);
    let canonical_before_index_loss = model_digest(&model);

    core.reconcile_recovery()
        .expect("recovery current before Index loss");
    core.close().expect("close before Index removal");
    fs::remove_file(
        root.path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join(INDEX_STORE_FILENAME),
    )
    .expect("physically remove derived Index");
    core = CoreState::open(root.path()).expect("open without derived Index");
    let rebuild_started = Instant::now();
    core.rebuild_index().expect("requested Index rebuild");
    let rebuild_elapsed = rebuild_started.elapsed();
    assert_eq!(
        core.status().expect("post-rebuild status").index_sync.state,
        IndexSyncState::Current
    );
    verify_model(&core, &model);
    assert_eq!(model_digest(&model), canonical_before_index_loss);
    exact_comparisons += verify_corpus(&core, &corpus, &corpus_ids);

    let traversal_started = Instant::now();
    run_cursor_traversal_with_mutation(&core, &mut model);
    run_stable_snapshot_with_writer(root.path(), &core, &mut model);
    verify_topology(&core, &topology);
    let traversal_elapsed = traversal_started.elapsed();

    let portable_started = Instant::now();
    let portable = portable_state(&core);
    assert_eq!(portable, expected_portable_state(&model));
    let portable_reconstruction_elapsed = portable_started.elapsed();

    core.reconcile_recovery()
        .expect("recovery current before reconstruction");
    core.close().expect("close before recovery reconstruction");
    fs::remove_file(
        root.path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join(RECORD_STORE_FILENAME),
    )
    .expect("remove canonical Store to exercise supported recovery");
    let recovery_started = Instant::now();
    core = CoreState::open(root.path()).expect("recover from supported recovery material");
    let recovery_elapsed = recovery_started.elapsed();
    verify_model(&core, &model);
    verify_topology(&core, &topology);
    exact_comparisons += verify_corpus(&core, &corpus, &corpus_ids);

    let metadata = root.path().join(GENESIS_METADATA_DIRECTORY);
    let record_bytes = fs::metadata(metadata.join(RECORD_STORE_FILENAME))
        .expect("Record Store metadata")
        .len();
    let index_bytes = fs::metadata(metadata.join(INDEX_STORE_FILENAME))
        .expect("Index Store metadata")
        .len();
    let multi_parent_nodes = count_multi_parent_nodes(&model.relationships);

    println!(
        "GENESIS_HETEROGENEOUS_TOPOLOGY_METRICS={}",
        serde_json::json!({
            "source_sha": SOURCE_SHA,
            "corpus_items": corpus.len(),
            "corpus_total_bytes": manifest.total_bytes,
            "content_type_count": corpus.iter().map(|item| item.manifest.logical_type.as_str()).collect::<BTreeSet<_>>().len(),
            "native_exact_payload_items": native_payload_items,
            "consumer_encoded_items": consumer_encoded_items,
            "nodes_initially": initial_nodes,
            "properties_initially": initial_properties,
            "relationships_initially": initial_relationships,
            "maximum_hierarchy_depth": topology.deep.len(),
            "maximum_branching_factor": topology.wide_children.len(),
            "multi_parent_node_count": multi_parent_nodes,
            "cross_link_count": model.relationships.iter().filter(|edge| edge.1 == CROSS_LINK).count(),
            "disconnected_structural_component_count": topology.component_roots.len(),
            "ingest_elapsed_us": ingest_elapsed.as_micros() as u64,
            "reopen_elapsed_us": reopen_elapsed.as_micros() as u64,
            "topology_reconstruction_elapsed_us": topology_reconstruction_elapsed.as_micros() as u64,
            "traversal_elapsed_us": traversal_elapsed.as_micros() as u64,
            "index_rebuild_elapsed_us": rebuild_elapsed.as_micros() as u64,
            "recovery_elapsed_us": recovery_elapsed.as_micros() as u64,
            "portable_reconstruction_elapsed_us": portable_reconstruction_elapsed.as_micros() as u64,
            "record_store_bytes": record_bytes,
            "index_store_bytes": index_bytes,
            "exact_hash_or_byte_comparisons": exact_comparisons,
            "unexpected_failures": 0,
        })
    );
}

fn build_corpus(manifest: &Manifest) -> Vec<CorpusItem> {
    let repository_sources: BTreeMap<&str, &[u8]> = BTreeMap::from([
        ("README.md", include_bytes!("../README.md").as_slice()),
        ("CHANGELOG.md", include_bytes!("../CHANGELOG.md").as_slice()),
        (
            "docs/SEMANTIC-CONTRACT.md",
            include_bytes!("../docs/SEMANTIC-CONTRACT.md").as_slice(),
        ),
        (
            "docs/TRAVERSAL-CONTRACT.md",
            include_bytes!("../docs/TRAVERSAL-CONTRACT.md").as_slice(),
        ),
        (
            ".github/workflows/verify.yml",
            include_bytes!("../.github/workflows/verify.yml").as_slice(),
        ),
        ("Cargo.toml", include_bytes!("../Cargo.toml").as_slice()),
        ("docs/API.md", include_bytes!("../docs/API.md").as_slice()),
        (
            "docs/ARCHITECTURE.md",
            include_bytes!("../docs/ARCHITECTURE.md").as_slice(),
        ),
        ("src/lib.rs", include_bytes!("../src/lib.rs").as_slice()),
        ("src/value.rs", include_bytes!("../src/value.rs").as_slice()),
        (
            "src/index/query.rs",
            include_bytes!("../src/index/query.rs").as_slice(),
        ),
        (
            "tests/headless_proof.rs",
            include_bytes!("headless_proof.rs").as_slice(),
        ),
        (
            "examples/headless.rs",
            include_bytes!("../examples/headless.rs").as_slice(),
        ),
        ("SECURITY.md", include_bytes!("../SECURITY.md").as_slice()),
        (
            "VERSIONING.md",
            include_bytes!("../VERSIONING.md").as_slice(),
        ),
        (
            "CONTRIBUTING.md",
            include_bytes!("../CONTRIBUTING.md").as_slice(),
        ),
    ]);

    manifest
        .items
        .iter()
        .cloned()
        .map(|entry| {
            let bytes = if let Some(source) = repository_sources.get(entry.path.as_str()) {
                source.to_vec()
            } else if let Some(generator) = entry.generator {
                generated_fixture(generator, &entry.logical_type)
            } else {
                binary_fixture(&entry.logical_type)
            };
            CorpusItem {
                manifest: entry,
                bytes,
            }
        })
        .collect()
}

fn generated_fixture(index: usize, logical_type: &str) -> Vec<u8> {
    match index {
        0 => Vec::new(),
        1 => b"x".to_vec(),
        2 => "Création — 日本語 — Ελληνικά — misión — 🧬
"
        .as_bytes()
        .to_vec(),
        3 => "é versus é; Å; क़
"
        .as_bytes()
        .to_vec(),
        4 => "“quotes” ‘apostrophes’ — … § ¶ → ← | / \\ [ ] { } < >
"
        .as_bytes()
        .to_vec(),
        5 | 6 => b"byte-identical payload under distinct identities
"
        .to_vec(),
        7 => b"same title fixture: first content
"
        .to_vec(),
        8 => b"same title fixture: second content
"
        .to_vec(),
        9 => b"near duplicate payload version 0001
"
        .to_vec(),
        10 => b"near duplicate payload version 0002
"
        .to_vec(),
        11 => "large bounded text line
"
        .repeat(2_048)
        .into_bytes(),
        _ => generated_by_type(index, logical_type).into_bytes(),
    }
}

fn generated_by_type(index: usize, logical_type: &str) -> String {
    match logical_type {
        "plain_text" => format!("fixture {index:03}\nplain text payload\n"),
        "log" => format!(
            "2026-09-16T20:{:02}:00Z INFO fixture={index:03} status=ok\n",
            index % 60
        ),
        "json" => format!("{{\"fixture\":{index},\"valid\":true,\"tags\":[\"alpha\",\"beta\"]}}\n"),
        "yaml" => format!("fixture: {index}\nvalid: true\ntags:\n  - alpha\n  - beta\n"),
        "toml" => format!("fixture = {index}\nvalid = true\ntags = [\"alpha\", \"beta\"]\n"),
        "csv" => format!("fixture,status,label\n{index},ok,item-{index:03}\n"),
        "xml" => format!(
            "<?xml version=\"1.0\"?><fixture id=\"{index}\"><status>ok</status></fixture>\n"
        ),
        "html" => {
            format!("<!doctype html><html><body><p data-id=\"{index}\">fixture</p></body></html>\n")
        }
        "rust" => format!("pub const FIXTURE_{index}: usize = {index};\n"),
        "typescript" => format!("export const fixture{index}: number = {index};\n"),
        "javascript" => format!("export const fixture{index} = {index};\n"),
        "css" => format!(".fixture-{index} {{ order: {index}; }}\n"),
        "sql" => {
            format!("INSERT INTO fixtures(id, label) VALUES ({index}, 'fixture-{index:03}');\n")
        }
        "shell" => format!("#!/bin/sh\nfixture_id='{index}'\nprintf '%s\\n' \"$fixture_id\"\n"),
        other => panic!("unsupported generated logical type {other}"),
    }
}

fn binary_fixture(logical_type: &str) -> Vec<u8> {
    match logical_type {
        "pdf" => b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\n%%EOF\n".to_vec(),
        "png" => vec![
            0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H',
            b'D', b'R', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        ],
        "zip" => vec![
            b'P', b'K', 0x03, 0x04, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ],
        other => panic!("unsupported binary logical type {other}"),
    }
}

fn verify_manifest(manifest: &Manifest, corpus: &[CorpusItem]) {
    assert_eq!(manifest.source_sha, SOURCE_SHA);
    assert_eq!(manifest.item_count, corpus.len());
    assert!((100..=500).contains(&corpus.len()));
    assert_eq!(
        manifest.total_bytes,
        corpus.iter().map(|item| item.bytes.len()).sum::<usize>()
    );
    for item in corpus {
        assert_eq!(
            item.manifest.bytes,
            item.bytes.len(),
            "{}",
            item.manifest.path
        );
        assert_eq!(
            item.manifest.sha256,
            sha256(&item.bytes),
            "{}",
            item.manifest.path
        );
    }
    assert_eq!(manifest.aggregate_sha256, corpus_aggregate_hash(corpus));
}

fn ingest_corpus(
    core: &CoreState,
    corpus: &[CorpusItem],
    model: &mut ExpectedModel,
) -> Vec<NodeId> {
    let mut mutations = Vec::new();
    let mut ids = Vec::new();
    for (ordinal, item) in corpus.iter().enumerate() {
        let id = NodeId::new();
        ids.push(id);
        let payload = if item.manifest.treatment_mode == "native_utf8_text" {
            String::from_utf8(item.bytes.clone()).expect("text fixture is UTF-8")
        } else {
            hex_encode(&item.bytes)
        };
        let properties = BTreeMap::from([
            (
                "consumer.source_identifier".into(),
                PropertyValue::Text(item.manifest.path.clone()),
            ),
            (
                "consumer.logical_title".into(),
                PropertyValue::Text(item.manifest.title.clone()),
            ),
            (
                "consumer.declared_type".into(),
                PropertyValue::Text(item.manifest.logical_type.clone()),
            ),
            (
                "consumer.mime".into(),
                PropertyValue::Text(item.manifest.mime.clone()),
            ),
            (
                "consumer.byte_size".into(),
                PropertyValue::Integer(item.bytes.len() as i64),
            ),
            (
                "consumer.sha256".into(),
                PropertyValue::Text(item.manifest.sha256.clone()),
            ),
            (
                "consumer.ordinal".into(),
                PropertyValue::Integer(ordinal as i64),
            ),
            (
                "consumer.fixture_class".into(),
                PropertyValue::Text(item.manifest.fixture_class.clone()),
            ),
            (
                "consumer.treatment_mode".into(),
                PropertyValue::Text(item.manifest.treatment_mode.clone()),
            ),
            (
                "consumer.payload_encoding".into(),
                PropertyValue::Text(
                    if item.manifest.treatment_mode == "native_utf8_text" {
                        "utf-8"
                    } else {
                        "hex"
                    }
                    .into(),
                ),
            ),
            ("consumer.payload".into(), PropertyValue::Text(payload)),
        ]);
        push_node_mutations(id, &properties, &mut mutations);
        model.nodes.insert(
            id,
            ExpectedNode {
                properties,
                lifecycle: LifecycleState::Active,
            },
        );
    }
    apply_chunks(core, mutations, "heterogeneous corpus");
    ids
}

fn ingest_topology(core: &CoreState, model: &mut ExpectedModel) -> Topology {
    let mut node_mutations = Vec::new();
    let mut ordinal = 0usize;
    let mut new_node = |label: String, class: &str, model: &mut ExpectedModel| {
        ordinal += 1;
        let id = NodeId::new();
        let properties = topology_properties(&label, class, ordinal);
        push_node_mutations(id, &properties, &mut node_mutations);
        model.nodes.insert(
            id,
            ExpectedNode {
                properties,
                lifecycle: LifecycleState::Active,
            },
        );
        id
    };

    let deep = (0..80)
        .map(|i| new_node(format!("deep-{i:03}"), "deep_hierarchy", model))
        .collect::<Vec<_>>();
    let wide_root = new_node("wide-root".into(), "wide_hierarchy", model);
    let wide_children = (0..600)
        .map(|i| new_node(format!("wide-child-{i:03}"), "wide_hierarchy", model))
        .collect::<Vec<_>>();

    let balanced_root = new_node("balanced-root".into(), "balanced_tree", model);
    let mut balanced_levels = vec![vec![balanced_root]];
    for level in 1..=5 {
        let mut children = Vec::new();
        for parent_ordinal in 0..balanced_levels[level - 1].len() {
            for branch in 0..3 {
                children.push(new_node(
                    format!("balanced-{level}-{parent_ordinal}-{branch}"),
                    "balanced_tree",
                    model,
                ));
            }
        }
        balanced_levels.push(children);
    }

    let dag_parents = (0..3)
        .map(|i| new_node(format!("dag-parent-{i}"), "multi_parent_dag", model))
        .collect::<Vec<_>>();
    let dag_shared = (0..12)
        .map(|i| new_node(format!("dag-shared-{i:02}"), "multi_parent_dag", model))
        .collect::<Vec<_>>();

    let component_roots = (0..4)
        .map(|component| {
            new_node(
                format!("component-{component}-root"),
                "disconnected_component",
                model,
            )
        })
        .collect::<Vec<_>>();
    let component_children = component_roots
        .iter()
        .enumerate()
        .map(|(component, _)| {
            (0..4)
                .map(|child| {
                    new_node(
                        format!("component-{component}-child-{child}"),
                        "disconnected_component",
                        model,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let cycle = (0..3)
        .map(|i| new_node(format!("cycle-{i}"), "cycle", model))
        .collect::<Vec<_>>();

    let mut taxonomy_roots = Vec::new();
    let mut taxonomy_items = Vec::new();
    let mut taxonomy_edges = BTreeSet::new();
    for root_index in 0..2 {
        let root = new_node(format!("Root {root_index}"), "taxonomy_example", model);
        taxonomy_roots.push(root);
        for group_index in 0..2 {
            let group = new_node(
                format!("Group {root_index}.{group_index}"),
                "taxonomy_example",
                model,
            );
            taxonomy_edges.insert((root, PARENT.into(), group));
            for branch_index in 0..2 {
                let branch = new_node(
                    format!("Branch {root_index}.{group_index}.{branch_index}"),
                    "taxonomy_example",
                    model,
                );
                taxonomy_edges.insert((group, PARENT.into(), branch));
                let labels = ["Section", "Topic", "Item", "Detail", "Leaf"];
                let mut parent = branch;
                for label in labels {
                    let child = new_node(
                        format!("{label} {root_index}.{group_index}.{branch_index}"),
                        "taxonomy_example",
                        model,
                    );
                    if label == "Item" {
                        taxonomy_items.push(child);
                    }
                    taxonomy_edges.insert((parent, PARENT.into(), child));
                    parent = child;
                }
            }
        }
    }
    let mut nested_parent = *taxonomy_items.first().expect("taxonomy Item");
    for depth in 0..8 {
        let nested = new_node(
            format!("Deep taxonomy nesting {depth}"),
            "taxonomy_example",
            model,
        );
        taxonomy_edges.insert((nested_parent, PARENT.into(), nested));
        nested_parent = nested;
    }

    let purge_parent = new_node("purge-parent".into(), "mutation_fixture", model);
    let purge_child = new_node("purge-child".into(), "mutation_fixture", model);
    apply_chunks(core, node_mutations, "topology Nodes");

    let mut edges = BTreeSet::new();
    for pair in deep.windows(2) {
        edges.insert((pair[0], PARENT.into(), pair[1]));
    }
    for child in &wide_children {
        edges.insert((wide_root, PARENT.into(), *child));
    }
    for level in 1..balanced_levels.len() {
        for (index, child) in balanced_levels[level].iter().enumerate() {
            edges.insert((balanced_levels[level - 1][index / 3], PARENT.into(), *child));
        }
    }
    for (index, shared) in dag_shared.iter().enumerate() {
        edges.insert((dag_parents[index % 3], PARENT.into(), *shared));
        edges.insert((dag_parents[(index + 1) % 3], PARENT.into(), *shared));
    }
    for (root, children) in component_roots.iter().zip(component_children.iter()) {
        for child in children {
            edges.insert((*root, PARENT.into(), *child));
        }
    }
    edges.insert((cycle[0], PARENT.into(), cycle[1]));
    edges.insert((cycle[1], PARENT.into(), cycle[2]));
    edges.insert((cycle[2], PARENT.into(), cycle[0]));
    edges.extend(taxonomy_edges.iter().cloned());
    edges.insert((taxonomy_items[0], PARENT.into(), dag_shared[0]));
    edges.insert((taxonomy_items[1], PARENT.into(), dag_shared[0]));
    edges.insert((purge_parent, PARENT.into(), purge_child));

    let cross_links = vec![
        (deep[12], CROSS_LINK.into(), wide_children[42]),
        (balanced_levels[3][5], CROSS_LINK.into(), dag_shared[4]),
        (component_roots[0], CROSS_LINK.into(), taxonomy_roots[1]),
        (taxonomy_items[2], CROSS_LINK.into(), deep[63]),
    ];
    edges.extend(cross_links.iter().cloned());
    apply_relationships(core, &edges, "topology Relationships");
    model.relationships.extend(edges);

    Topology {
        deep,
        wide_root,
        wide_children,
        balanced_levels,
        dag_parents,
        dag_shared,
        cross_links,
        component_roots,
        cycle,
        taxonomy_roots,
        taxonomy_items,
        taxonomy_edges,
        purge_parent,
        purge_child,
    }
}

fn run_structural_mutations(
    core: &CoreState,
    model: &mut ExpectedModel,
    topology: &Topology,
    corpus: &[CorpusItem],
    corpus_ids: &[NodeId],
) {
    let balanced_child = topology.balanced_levels[3][0];
    let old_parent = topology.balanced_levels[2][0];
    let new_parent = topology.balanced_levels[2][1];
    replace_edge(core, model, old_parent, new_parent, balanced_child);
    verify_stage(core, model, corpus, corpus_ids);

    let shared = topology.dag_shared[0];
    add_edge(core, model, topology.dag_parents[2], PARENT, shared);
    verify_stage(core, model, corpus, corpus_ids);
    remove_edge(core, model, topology.dag_parents[0], PARENT, shared);
    assert_eq!(incoming(core, shared, PARENT).len(), 4);
    verify_stage(core, model, corpus, corpus_ids);

    remove_edge(core, model, topology.deep[39], PARENT, topology.deep[40]);
    add_edge(core, model, topology.deep[10], PARENT, topology.deep[40]);
    verify_stage(core, model, corpus, corpus_ids);
    remove_edge(core, model, topology.deep[10], PARENT, topology.deep[40]);
    add_edge(core, model, topology.deep[39], PARENT, topology.deep[40]);
    verify_stage(core, model, corpus, corpus_ids);

    let temporary_cross = (topology.wide_children[7], CROSS_LINK, topology.deep[7]);
    add_edge(
        core,
        model,
        temporary_cross.0,
        temporary_cross.1,
        temporary_cross.2,
    );
    verify_stage(core, model, corpus, corpus_ids);
    remove_edge(
        core,
        model,
        temporary_cross.0,
        temporary_cross.1,
        temporary_cross.2,
    );
    verify_stage(core, model, corpus, corpus_ids);

    core.tombstone_node(topology.wide_root)
        .expect("tombstone parent");
    model
        .nodes
        .get_mut(&topology.wide_root)
        .expect("wide root")
        .lifecycle = LifecycleState::Tombstoned;
    verify_stage(core, model, corpus, corpus_ids);
    core.restore_node(topology.wide_root)
        .expect("restore parent");
    model
        .nodes
        .get_mut(&topology.wide_root)
        .expect("wide root")
        .lifecycle = LifecycleState::Active;
    verify_stage(core, model, corpus, corpus_ids);

    core.tombstone_node(topology.purge_child)
        .expect("tombstone purge child");
    model
        .nodes
        .get_mut(&topology.purge_child)
        .expect("purge child")
        .lifecycle = LifecycleState::Tombstoned;
    verify_stage(core, model, corpus, corpus_ids);
    core.purge_node(topology.purge_child)
        .expect("purge within existing contract");
    model.nodes.remove(&topology.purge_child);
    model.retired.insert(topology.purge_child);
    assert!(model.relationships.contains(&(
        topology.purge_parent,
        PARENT.into(),
        topology.purge_child
    )));
    verify_stage(core, model, corpus, corpus_ids);

    let identity = corpus_ids[0];
    let revised_hash = sha256(b"consumer metadata mutation preserves identity");
    core.set_property(
        identity,
        "consumer.validation_marker",
        PropertyValue::Text(revised_hash.clone()),
    )
    .expect("mutate Property");
    model
        .nodes
        .get_mut(&identity)
        .expect("corpus identity")
        .properties
        .insert(
            "consumer.validation_marker".into(),
            PropertyValue::Text(revised_hash),
        );
    assert_eq!(
        core.get_node(identity)
            .expect("identity after Property edit")
            .id,
        identity
    );
    verify_stage(core, model, corpus, corpus_ids);
}

fn run_cursor_traversal_with_mutation(core: &CoreState, model: &mut ExpectedModel) {
    let before = core.list_nodes().expect("Nodes before cursor traversal");
    let first = core.list_nodes_cursor(97, None).expect("first cursor page");
    assert!(!first.items.is_empty());
    let changed = before
        .iter()
        .find(|node| {
            !first
                .items
                .iter()
                .any(|first_node| first_node.id == node.id)
        })
        .expect("Node after first cursor page")
        .id;
    core.set_property(
        changed,
        "adapter.concurrent_change",
        PropertyValue::Boolean(true),
    )
    .expect("concurrent cursor mutation");
    model
        .nodes
        .get_mut(&changed)
        .expect("changed model Node")
        .properties
        .insert(
            "adapter.concurrent_change".into(),
            PropertyValue::Boolean(true),
        );

    let inserted = NodeId::new();
    let properties = topology_properties("cursor-insert", "traversal_fixture", model.nodes.len());
    let mut mutations = Vec::new();
    push_node_mutations(inserted, &properties, &mut mutations);
    core.apply_batch(&mutations).expect("cursor-time insert");
    model.nodes.insert(
        inserted,
        ExpectedNode {
            properties,
            lifecycle: LifecycleState::Active,
        },
    );

    let mut delivered = first.items;
    let mut cursor = first.next_cursor;
    while let Some(next) = cursor {
        let page = core
            .list_nodes_cursor(97, Some(&next))
            .expect("continue cursor");
        delivered.extend(page.items);
        cursor = page.next_cursor;
    }
    let delivered_ids = delivered.iter().map(|node| node.id).collect::<Vec<_>>();
    assert_eq!(
        delivered_ids.len(),
        delivered_ids.iter().copied().collect::<BTreeSet<_>>().len()
    );
    assert!(!delivered_ids.contains(&inserted));
    assert!(!delivered_ids.contains(&changed));
}

fn run_stable_snapshot_with_writer(root: &Path, core: &CoreState, model: &mut ExpectedModel) {
    let before = portable_state(core);
    let victim = *before.nodes.keys().next_back().expect("snapshot victim");
    let writer = CoreState::open(root).expect("concurrent writer");
    let inserted = NodeId::new();
    let properties = topology_properties("snapshot-insert", "traversal_fixture", model.nodes.len());
    let scanned = core
        .with_stable_read_snapshot(|snapshot| {
            let first = snapshot.nodes_page(83, None)?;
            let mut mutations = Vec::new();
            push_node_mutations(inserted, &properties, &mut mutations);
            writer.apply_batch(&mutations)?;
            writer.tombstone_node(victim)?;
            collect_pages(first, |cursor| snapshot.nodes_page(83, Some(cursor)))
        })
        .expect("stable snapshot traversal");
    assert_eq!(
        scanned.iter().map(|node| node.id).collect::<BTreeSet<_>>(),
        before.nodes.keys().copied().collect()
    );
    assert!(!scanned.iter().any(|node| node.id == inserted));
    writer
        .restore_node(victim)
        .expect("restore snapshot victim");
    writer.close().expect("close concurrent writer");
    model.nodes.insert(
        inserted,
        ExpectedNode {
            properties,
            lifecycle: LifecycleState::Active,
        },
    );
}

fn verify_stage(
    core: &CoreState,
    model: &ExpectedModel,
    corpus: &[CorpusItem],
    corpus_ids: &[NodeId],
) {
    verify_model(core, model);
    verify_corpus(core, corpus, corpus_ids);
}

fn verify_model(core: &CoreState, model: &ExpectedModel) {
    assert_eq!(portable_state(core), expected_portable_state(model));
    for (id, expected) in &model.nodes {
        assert_eq!(
            core.resolve_identity(*id).expect("identity resolution"),
            match expected.lifecycle {
                LifecycleState::Active => IdentityResolution::Active,
                LifecycleState::Tombstoned => IdentityResolution::Tombstoned,
            }
        );
    }
    for id in &model.retired {
        assert_eq!(
            core.resolve_identity(*id).expect("retired identity"),
            IdentityResolution::Retired
        );
    }
}

fn verify_corpus(core: &CoreState, corpus: &[CorpusItem], ids: &[NodeId]) -> usize {
    let mut comparisons = 0;
    for (item, id) in corpus.iter().zip(ids) {
        let properties = core.list_properties(*id).expect("corpus Properties");
        assert_eq!(
            properties.get("consumer.byte_size"),
            Some(&PropertyValue::Integer(item.bytes.len() as i64))
        );
        assert_eq!(
            properties.get("consumer.sha256"),
            Some(&PropertyValue::Text(sha256(&item.bytes)))
        );
        let payload = text_property(&properties, "consumer.payload");
        let reconstructed = if item.manifest.treatment_mode == "native_utf8_text" {
            payload.into_bytes()
        } else {
            hex_decode(&payload)
        };
        assert_eq!(reconstructed, item.bytes);
        comparisons += 2;
    }
    comparisons
}

fn verify_topology(core: &CoreState, topology: &Topology) {
    let relationships = all_relationships(core);
    assert_eq!(
        walk_single_chain(core, topology.deep[0], RelationshipDirection::Outgoing).len(),
        topology.deep.len()
    );
    assert_eq!(
        walk_single_chain(
            core,
            *topology.deep.last().expect("deep leaf"),
            RelationshipDirection::Incoming
        )
        .len(),
        topology.deep.len()
    );
    assert_eq!(
        outgoing(core, topology.wide_root, PARENT)
            .iter()
            .map(|edge| edge.2)
            .collect::<BTreeSet<_>>(),
        topology.wide_children.iter().copied().collect()
    );
    for level in 1..topology.balanced_levels.len() {
        assert_eq!(
            topology.balanced_levels[level].len(),
            topology.balanced_levels[level - 1].len() * 3
        );
    }
    assert!(incoming(core, topology.dag_shared[1], PARENT).len() >= 2);
    for edge in &topology.cross_links {
        assert!(relationships.contains(edge));
    }
    assert_eq!(cycle_walk(core, topology.cycle[0]), 3);
    for root in &topology.component_roots {
        assert_eq!(outgoing(core, *root, PARENT).len(), 4);
    }
    assert_eq!(topology.taxonomy_roots.len(), 2);
    assert!(topology.taxonomy_items.len() >= 8);
    assert!(topology
        .taxonomy_edges
        .iter()
        .all(|edge| relationships.contains(edge)));
}

fn walk_single_chain(
    core: &CoreState,
    start: NodeId,
    direction: RelationshipDirection,
) -> Vec<NodeId> {
    let mut result = vec![start];
    let mut current = start;
    loop {
        let relationships = core
            .query_relationships(current, direction, Some(PARENT))
            .expect("chain Relationships");
        let next = relationships
            .iter()
            .filter_map(|relationship| match direction {
                RelationshipDirection::Outgoing if relationship.source == current => {
                    Some(relationship.target)
                }
                RelationshipDirection::Incoming if relationship.target == current => {
                    Some(relationship.source)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if next.is_empty() {
            break;
        }
        assert_eq!(next.len(), 1, "deep fixture remains a single chain");
        current = next[0];
        result.push(current);
    }
    result
}

fn cycle_walk(core: &CoreState, start: NodeId) -> usize {
    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([start]);
    while let Some(node) = queue.pop_front() {
        if !visited.insert(node) {
            continue;
        }
        for (_, _, target) in outgoing(core, node, PARENT) {
            if !visited.contains(&target) {
                queue.push_back(target);
            }
        }
    }
    visited.len()
}

fn portable_state(core: &CoreState) -> PortableState {
    core.with_stable_read_snapshot(|snapshot| {
        let nodes = collect_pages(snapshot.nodes_page(127, None)?, |cursor| {
            snapshot.nodes_page(127, Some(cursor))
        })?;
        let mut portable_nodes = BTreeMap::new();
        for node in nodes {
            let properties =
                collect_pages(snapshot.properties_page(node.id, 37, None)?, |cursor| {
                    snapshot.properties_page(node.id, 37, Some(cursor))
                })?
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            portable_nodes.insert(node.id, (node.lifecycle_state, properties));
        }
        let relationships = collect_pages(snapshot.relationships_page(131, None)?, |cursor| {
            snapshot.relationships_page(131, Some(cursor))
        })?
        .into_iter()
        .map(|relationship| (relationship.source, relationship.key, relationship.target))
        .collect();
        Ok(PortableState {
            nodes: portable_nodes,
            relationships,
        })
    })
    .expect("portable canonical reconstruction")
}

fn expected_portable_state(model: &ExpectedModel) -> PortableState {
    PortableState {
        nodes: model
            .nodes
            .iter()
            .map(|(id, node)| (*id, (node.lifecycle, node.properties.clone())))
            .collect(),
        relationships: model.relationships.clone(),
    }
}

fn model_digest(model: &ExpectedModel) -> String {
    let value = serde_json::json!({
        "nodes": model.nodes.iter().map(|(id, node)| serde_json::json!({
            "id": id,
            "lifecycle": node.lifecycle,
            "properties": node.properties,
        })).collect::<Vec<_>>(),
        "retired": model.retired,
        "relationships": model.relationships,
    });
    sha256(&serde_json::to_vec(&value).expect("model JSON"))
}

fn topology_properties(
    label: &str,
    class: &str,
    ordinal: usize,
) -> BTreeMap<String, PropertyValue> {
    BTreeMap::from([
        ("adapter.label".into(), PropertyValue::Text(label.into())),
        ("adapter.class".into(), PropertyValue::Text(class.into())),
        (
            "adapter.ordinal".into(),
            PropertyValue::Integer(ordinal as i64),
        ),
        (
            "adapter.content_hash".into(),
            PropertyValue::Text(sha256(label.as_bytes())),
        ),
    ])
}

fn push_node_mutations(
    id: NodeId,
    properties: &BTreeMap<String, PropertyValue>,
    mutations: &mut Vec<Mutation>,
) {
    mutations.push(Mutation::CreateNode { node_id: id });
    mutations.extend(properties.iter().map(|(key, value)| Mutation::SetProperty {
        node_id: id,
        key: key.clone(),
        value: value.clone(),
    }));
}

fn apply_chunks(core: &CoreState, mutations: Vec<Mutation>, label: &str) {
    for chunk in mutations.chunks(500) {
        core.apply_batch(chunk).unwrap_or_else(|error| {
            panic!("{label} mutation chunk failed: {error}");
        });
    }
}

fn apply_relationships(core: &CoreState, edges: &BTreeSet<(NodeId, String, NodeId)>, label: &str) {
    let mutations = edges
        .iter()
        .map(|(source, key, target)| Mutation::AddRelationship {
            source: *source,
            key: key.clone(),
            target: *target,
        })
        .collect::<Vec<_>>();
    apply_chunks(core, mutations, label);
}

fn add_edge(
    core: &CoreState,
    model: &mut ExpectedModel,
    source: NodeId,
    key: &str,
    target: NodeId,
) {
    core.add_relationship(source, key, target)
        .expect("add structural edge");
    model.relationships.insert((source, key.into(), target));
}

fn remove_edge(
    core: &CoreState,
    model: &mut ExpectedModel,
    source: NodeId,
    key: &str,
    target: NodeId,
) {
    core.remove_relationship(source, key, target)
        .expect("remove structural edge");
    assert!(model.relationships.remove(&(source, key.into(), target)));
}

fn replace_edge(
    core: &CoreState,
    model: &mut ExpectedModel,
    old_parent: NodeId,
    new_parent: NodeId,
    child: NodeId,
) {
    core.apply_batch(&[
        Mutation::RemoveRelationship {
            source: old_parent,
            key: PARENT.into(),
            target: child,
        },
        Mutation::AddRelationship {
            source: new_parent,
            key: PARENT.into(),
            target: child,
        },
    ])
    .expect("atomic reparent");
    assert!(model
        .relationships
        .remove(&(old_parent, PARENT.into(), child)));
    model
        .relationships
        .insert((new_parent, PARENT.into(), child));
}

fn outgoing(core: &CoreState, node: NodeId, key: &str) -> Vec<(NodeId, String, NodeId)> {
    core.query_relationships(node, RelationshipDirection::Outgoing, Some(key))
        .expect("outgoing Relationships")
        .into_iter()
        .filter(|relationship| relationship.source == node)
        .map(|relationship| (relationship.source, relationship.key, relationship.target))
        .collect()
}

fn incoming(core: &CoreState, node: NodeId, key: &str) -> Vec<(NodeId, String, NodeId)> {
    core.query_relationships(node, RelationshipDirection::Incoming, Some(key))
        .expect("incoming Relationships")
        .into_iter()
        .filter(|relationship| relationship.target == node)
        .map(|relationship| (relationship.source, relationship.key, relationship.target))
        .collect()
}

fn all_relationships(core: &CoreState) -> BTreeSet<(NodeId, String, NodeId)> {
    portable_state(core).relationships
}

fn count_multi_parent_nodes(relationships: &BTreeSet<(NodeId, String, NodeId)>) -> usize {
    let mut incoming_counts = BTreeMap::<NodeId, usize>::new();
    for (_, key, target) in relationships {
        if key == PARENT {
            *incoming_counts.entry(*target).or_default() += 1;
        }
    }
    incoming_counts.values().filter(|count| **count > 1).count()
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

fn corpus_aggregate_hash(corpus: &[CorpusItem]) -> String {
    let mut hasher = Sha256::new();
    for item in corpus {
        hasher.update(item.manifest.path.as_bytes());
        hasher.update([0]);
        hasher.update(item.manifest.sha256.as_bytes());
        hasher.update([0]);
        hasher.update(item.bytes.len().to_string().as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

fn text_property(properties: &BTreeMap<String, PropertyValue>, key: &str) -> String {
    match properties.get(key) {
        Some(PropertyValue::Text(value)) => value.clone(),
        other => panic!("expected text Property {key}, got {other:?}"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(value: &str) -> Vec<u8> {
    assert!(value.len().is_multiple_of(2));
    value
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("hex UTF-8"), 16)
                .expect("valid hex")
        })
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
