//! Isolated process-death and interprocess lock probes. The child is the test
//! binary itself, started with one ignored entry point and a disposable Store.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use genesis_core::{
    CoreError, CoreState, NodeId, PropertyValue, RelationshipDirection, GENESIS_METADATA_DIRECTORY,
    RECORD_STORE_FILENAME,
};
use rusqlite::Connection;

const CHILD_CASE: &str = "GENESIS_GATE3_CHILD_CASE";
const CHILD_ROOT: &str = "GENESIS_GATE3_CHILD_ROOT";
const CHILD_ID: &str = "GENESIS_GATE3_CHILD_ID";
const CHILD_MARKER: &str = "GENESIS_GATE3_CHILD_MARKER";

fn record_path(root: &Path) -> PathBuf {
    root.join(GENESIS_METADATA_DIRECTORY)
        .join(RECORD_STORE_FILENAME)
}

fn spawn_child(case: &str, root: &Path, id: NodeId, marker: &Path) -> Child {
    Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "process_child", "--ignored", "--nocapture"])
        .env(CHILD_CASE, case)
        .env(CHILD_ROOT, root)
        .env(CHILD_ID, id.to_string())
        .env(CHILD_MARKER, marker)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("launch independent child process")
}

fn wait_for_marker(child: &mut Child, marker: &Path) {
    for _ in 0..250 {
        if marker.is_file() {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll child") {
            panic!("child exited before marker: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("child did not reach the controlled boundary");
}

fn terminate(child: &mut Child) {
    child.kill().expect("terminate child process");
    child.wait().expect("reap terminated child");
}

fn await_termination() -> ! {
    loop {
        thread::sleep(Duration::from_millis(100));
    }
}

#[test]
#[ignore = "child entry point; invoked by the independent process probes"]
fn process_child() {
    let case = std::env::var(CHILD_CASE).expect("child case");
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).expect("child root"));
    let id: NodeId = std::env::var(CHILD_ID)
        .expect("child ID")
        .parse()
        .expect("valid Node ID");
    let marker = PathBuf::from(std::env::var_os(CHILD_MARKER).expect("child marker"));

    match case.as_str() {
        "uncommitted" => {
            let connection = Connection::open(record_path(&root)).expect("child Record connection");
            connection
                .execute_batch("BEGIN IMMEDIATE;")
                .expect("begin uncommitted Record transaction");
            connection
                .execute(
                    "INSERT INTO issued_node_ids(node_uuid) VALUES (?1)",
                    [id.as_bytes().as_slice()],
                )
                .expect("reserve uncommitted identity");
            connection
                .execute(
                    "INSERT INTO nodes(node_uuid, lifecycle_state, created_revision, modified_revision) \
                     VALUES (?1, 'active', 1, 1)",
                    [id.as_bytes().as_slice()],
                )
                .expect("insert uncommitted Node");
            connection
                .execute("UPDATE store_meta SET current_revision=1", [])
                .expect("set uncommitted revision");
            fs::write(marker, b"uncommitted transaction active").expect("signal parent");
            await_termination();
        }
        "committed" => {
            let core = CoreState::open(&root).expect("child Core");
            core.create_node_with_id(id)
                .expect("commit identity and Node");
            fs::write(marker, b"Core API returned after commit").expect("signal parent");
            await_termination();
        }
        "purged" => {
            let core = CoreState::open(&root).expect("child Core");
            core.tombstone_node(id).expect("tombstone Node");
            core.purge_node(id).expect("complete purge barrier");
            fs::write(marker, b"Core purge API returned").expect("signal parent");
            await_termination();
        }
        "locked" => {
            let connection = Connection::open(record_path(&root)).expect("child Record connection");
            connection
                .execute_batch("BEGIN IMMEDIATE;")
                .expect("hold independent process write lock");
            fs::write(marker, b"write lock held by child").expect("signal parent");
            await_termination();
        }
        other => panic!("unknown child case: {other}"),
    }
}

#[test]
fn killed_uncommitted_record_transaction_rolls_back_ledger_node_and_revision() {
    let library = tempfile::tempdir().expect("isolated library");
    CoreState::open(library.path())
        .expect("initialize Core")
        .close()
        .expect("close Core");
    let id = NodeId::new();
    let marker = library.path().join("uncommitted.marker");
    let mut child = spawn_child("uncommitted", library.path(), id, &marker);
    wait_for_marker(&mut child, &marker);
    terminate(&mut child);

    let connection = Connection::open(record_path(library.path())).expect("inspect Record");
    let counts: (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM issued_node_ids), \
             (SELECT COUNT(*) FROM nodes), (SELECT current_revision FROM store_meta)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("inspect atomic rollback");
    assert_eq!(counts, (0, 0, 0));
    drop(connection);
    let reopened = CoreState::open(library.path()).expect("reopen after actual process death");
    assert_eq!(
        reopened.create_node_with_id(id).expect("unissued ID").id,
        id
    );
}

#[test]
fn killed_process_after_create_return_preserves_issued_node_and_index_rebuild() {
    let library = tempfile::tempdir().expect("isolated library");
    let id = NodeId::new();
    let marker = library.path().join("committed.marker");
    let mut child = spawn_child("committed", library.path(), id, &marker);
    wait_for_marker(&mut child, &marker);
    terminate(&mut child);

    let reopened = CoreState::open(library.path()).expect("reopen after actual process death");
    assert_eq!(reopened.get_node(id).expect("canonical Node").id, id);
    assert!(matches!(
        reopened.create_node_with_id(id),
        Err(CoreError::AlreadyExists { .. })
    ));
    reopened.rebuild_index().expect("rebuild derived Index");
    assert_eq!(reopened.get_node(id).expect("Node after rebuild").id, id);
}

#[test]
fn killed_process_after_purge_return_cannot_resurrect_or_reissue_identity() {
    let library = tempfile::tempdir().expect("isolated library");
    let core = CoreState::open(library.path()).expect("initial Core");
    let node = core.create_node().expect("Node");
    let peer = core.create_node().expect("peer");
    core.set_property(node.id, "fact", PropertyValue::Boolean(true))
        .expect("canonical Property");
    core.add_relationship(node.id, "link", peer.id)
        .expect("canonical Relationship");
    core.close().expect("close fixture");

    let marker = library.path().join("purged.marker");
    let mut child = spawn_child("purged", library.path(), node.id, &marker);
    wait_for_marker(&mut child, &marker);
    terminate(&mut child);

    let reopened = CoreState::open(library.path()).expect("reopen after process death");
    assert!(matches!(
        reopened.get_node(node.id),
        Err(CoreError::NotFound { .. })
    ));
    assert!(matches!(
        reopened.create_node_with_id(node.id),
        Err(CoreError::AlreadyExists { .. })
    ));
    let surviving = reopened
        .query_relationships(peer.id, RelationshipDirection::Either, None)
        .expect("independent Relationship history after process death");
    assert_eq!(surviving.len(), 1);
    assert_eq!(surviving[0].source, node.id);
    assert!(matches!(
        reopened.get_property(node.id, "fact"),
        Err(CoreError::NotFound { .. })
    ));
}

#[test]
fn independent_process_write_lock_timeout_preserves_truth_and_retry_succeeds() {
    let library = tempfile::tempdir().expect("isolated library");
    let core = CoreState::open(library.path()).expect("initial Core");
    let node = core.create_node().expect("Node");
    let before = core.status().expect("status").index_sync.record_revision;
    let marker = library.path().join("locked.marker");
    let mut child = spawn_child("locked", library.path(), node.id, &marker);
    wait_for_marker(&mut child, &marker);

    assert_eq!(
        core.get_node(node.id)
            .expect("read during other process write")
            .id,
        node.id
    );
    assert!(core
        .set_property(node.id, "blocked", PropertyValue::Boolean(true))
        .is_err());
    assert_eq!(
        core.status()
            .expect("post-timeout")
            .index_sync
            .record_revision,
        before
    );
    assert_eq!(
        core.get_property(node.id, "blocked")
            .expect("no partial fact"),
        None
    );
    terminate(&mut child);
    core.set_property(node.id, "blocked", PropertyValue::Boolean(true))
        .expect("explicit retry after lock release");
    core.close().expect("close after retry");
    let reopened = CoreState::open(library.path()).expect("reopen after contention");
    assert_eq!(
        reopened
            .get_property(node.id, "blocked")
            .expect("durable retry"),
        Some(PropertyValue::Boolean(true))
    );
}
