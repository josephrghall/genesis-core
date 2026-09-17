//! Genesis Core: the headless six-primitive information engine.
//!
//! The public surface is intentionally independent from UI, filesystem layout,
//! document models, and application semantics.

mod api;
mod error;
mod id;
mod index;
mod node;
mod record_store;
mod recovery;
mod security;
mod value;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

pub use api::{
    Commit, CursorPage, DefinitionState, DefinitionUsage, DerivedRelationshipInput, GraphEdge,
    GraphNode, GraphProjection, IdentityResolution, IndexRebuildReason, IndexSyncState,
    IndexSyncStatus, IndexedPropertyMatch, IndexedRelationship, Mutation, NewPropertyDefinition,
    NewRelationshipDefinition, PropertyDefinition, PropertyDefinitionUpdate, PropertyFilter,
    PropertyQuery, PropertySort, ReadCursor, ReadPage, ReconcileOutcome, Relationship,
    RelationshipDefinition, RelationshipDefinitionUpdate, RelationshipDirection,
    RelationshipProvenance, SearchDocumentInput, SearchHit,
};
pub use error::{CoreError, CoreErrorCategory};
pub use id::NodeId;
pub use node::{LifecycleState, Node};
pub use record_store::StableReadSnapshot;
pub use recovery::{RecoveryState, RecoveryStatus};
pub use value::{PropertyValue, PropertyValueKind};

use index::IndexStore;
use record_store::RecordStore;
use recovery::RecoveryManager;
use security::StorageBoundary;

pub const GENESIS_METADATA_DIRECTORY: &str = ".genesis";
pub const RECORD_STORE_FILENAME: &str = "records.sqlite";
pub const INDEX_STORE_FILENAME: &str = "index.sqlite";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreStatus {
    pub path: PathBuf,
    pub schema_version: u32,
    pub integrity_ok: bool,
    /// Nonfatal loss of provenance or context; surviving canonical data is
    /// still readable, but must not be represented as fully verified.
    pub degraded_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreStatus {
    pub library_root: PathBuf,
    pub record_store: StoreStatus,
    pub index_store: StoreStatus,
    pub index_sync: IndexSyncStatus,
    pub recovery: RecoveryStatus,
}

/// Owns Core's database connections. Higher layers receive operations and
/// status, never raw SQLite connections.
///
/// Mutation `Ok` reports canonical success, not successful Index/recovery
/// synchronization. Ordinary follow-up is best-effort: inspect `status` and
/// use `reconcile_index`, `rebuild_index`, or `reconcile_recovery` when needed.
/// Purge separately requires a recovery barrier; its completion failure is
/// reported as `PostCommitFailure` after the canonical purge has committed.
pub struct CoreState {
    library_root: PathBuf,
    record_store: Mutex<RecordStore>,
    index_store: Mutex<IndexStore>,
    recovery: Mutex<RecoveryManager>,
    boundary: StorageBoundary,
}

impl CoreState {
    pub fn open(library_root: impl AsRef<Path>) -> Result<Self, CoreError> {
        let library_root = library_root.as_ref().to_path_buf();
        let boundary = StorageBoundary::load_if_present(&library_root)?;
        boundary.validate()?;
        if !boundary.is_protected() {
            fs::create_dir_all(&library_root)?;
        }
        Self::open_existing(library_root, boundary)
    }

    pub fn open_protected(library_root: impl AsRef<Path>) -> Result<Self, CoreError> {
        let library_root = library_root.as_ref().to_path_buf();
        let boundary = StorageBoundary::load_required(&library_root)?;
        boundary.validate()?;
        Self::open_existing(library_root, boundary)
    }

    pub fn configure_protected(library_root: impl AsRef<Path>) -> Result<(), CoreError> {
        StorageBoundary::configure_protected(library_root.as_ref())
    }

    fn open_existing(library_root: PathBuf, boundary: StorageBoundary) -> Result<Self, CoreError> {
        let metadata_root = library_root.join(GENESIS_METADATA_DIRECTORY);
        if !boundary.is_protected() {
            fs::create_dir_all(&metadata_root)?;
        } else if !metadata_root.is_dir() {
            return Err(CoreError::ProtectedStoreUnavailable(
                "protected Store metadata root is unavailable".into(),
            ));
        }

        let record_path = metadata_root.join(RECORD_STORE_FILENAME);
        if !record_path.is_file() && RecoveryManager::manifest_exists(&metadata_root) {
            RecoveryManager::reconstruct(&metadata_root)?;
        }

        // Canonical truth is always recovered/opened and validated before derived state.
        let mut record_store = match RecordStore::open(record_path) {
            Ok(store) => store,
            Err(error @ CoreError::UnsupportedSchema { .. }) => return Err(error),
            Err(_error) if RecoveryManager::manifest_exists(&metadata_root) => {
                RecoveryManager::reconstruct(&metadata_root)?;
                RecordStore::open(metadata_root.join(RECORD_STORE_FILENAME))?
            }
            Err(error) => return Err(error),
        };
        let mut recovery = RecoveryManager::open(&metadata_root)?;
        recovery.ensure_initialized(&record_store)?;
        recovery.reconcile_startup(&record_store)?;
        let (index_store, _) =
            IndexStore::open_for_core(metadata_root.join(INDEX_STORE_FILENAME), &record_store)?;
        record_store.prune_change_journal()?;

        Ok(Self {
            library_root,
            record_store: Mutex::new(record_store),
            index_store: Mutex::new(index_store),
            recovery: Mutex::new(recovery),
            boundary,
        })
    }

    /// Revalidates the configured storage boundary before an above-Core
    /// capability begins a write that will later call Core.
    pub fn assert_writable(&self) -> Result<(), CoreError> {
        self.boundary.validate()
    }

    pub fn status(&self) -> Result<CoreStatus, CoreError> {
        let record_store = self
            .record_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Record Store"))?;
        let index_store = self
            .index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?;
        let recovery = self
            .recovery
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Record recovery"))?;

        let record_revision = record_store.current_revision()?;
        let applied_revision = index_store.applied_revision()?;
        Ok(CoreStatus {
            library_root: self.library_root.clone(),
            record_store: record_store.status()?,
            index_store: index_store.status()?,
            index_sync: IndexSyncStatus {
                record_revision,
                applied_revision,
                state: if record_revision == applied_revision {
                    IndexSyncState::Current
                } else {
                    IndexSyncState::Stale
                },
            },
            recovery: recovery.status(&record_store),
        })
    }

    pub fn create_node(&self) -> Result<Node, CoreError> {
        self.assert_writable()?;
        let node = self.records()?.create_node()?;
        self.sync_derived_best_effort();
        Ok(node)
    }

    pub fn create_nodes(&self, count: usize) -> Result<Vec<Node>, CoreError> {
        self.assert_writable()?;
        let nodes = self.records()?.create_nodes(count)?;
        self.sync_derived_best_effort();
        Ok(nodes)
    }

    /// Creates a Node with a caller-supplied permanent UUIDv7. This is useful
    /// for migrations and capability operation journals that must allocate an
    /// identity before completing work. Core still validates uniqueness and
    /// UUIDv7 form.
    pub fn create_node_with_id(&self, node_id: NodeId) -> Result<Node, CoreError> {
        self.assert_writable()?;
        let node = self.records()?.create_node_with_id(node_id)?;
        self.sync_derived_best_effort();
        Ok(node)
    }

    pub fn get_node(&self, id: NodeId) -> Result<Node, CoreError> {
        self.records()?.get_node(id)
    }

    /// Resolves permanent identity independently from Node Record retrieval.
    pub fn resolve_identity(&self, id: NodeId) -> Result<IdentityResolution, CoreError> {
        self.records()?.resolve_identity(id)
    }

    /// Returns the rebuildable Index projection used by higher-layer
    /// navigation. Canonical Node truth remains in the Record Store.
    pub fn list_nodes(&self) -> Result<Vec<Node>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .list_nodes()
    }

    /// Returns a bounded page in the same deterministic order as `list_nodes`.
    pub fn list_nodes_page(&self, page: ReadPage) -> Result<Vec<Node>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .list_nodes_page(page)
    }

    /// Traverses the rebuildable Node Index through an opaque keyset cursor.
    ///
    /// The first page fixes a revision watermark. Later inserts and updates
    /// are excluded; a concurrently changed or purged row can disappear from
    /// the remaining traversal. Use `with_stable_read_snapshot` when every
    /// page must observe one exact canonical database snapshot.
    pub fn list_nodes_cursor(
        &self,
        limit: usize,
        cursor: Option<&ReadCursor>,
    ) -> Result<CursorPage<Node>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .list_nodes_cursor(limit, cursor)
    }

    /// Runs a caller-controlled sequence of bounded reads against one exact
    /// canonical SQLite snapshot.
    ///
    /// The callback is synchronous and must not call canonical operations on
    /// this `CoreState`, because the Record Store is held for the snapshot's
    /// lifetime. Separate Core connections may continue to write in WAL mode.
    pub fn with_stable_read_snapshot<T, F>(&self, read: F) -> Result<T, CoreError>
    where
        F: FnOnce(&StableReadSnapshot<'_>) -> Result<T, CoreError>,
    {
        self.record_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Record Store"))?
            .with_stable_read_snapshot(read)
    }

    /// Replaces derived read models supplied by an above-Core capability.
    /// This never writes canonical Relationships or Properties.
    pub fn replace_content_projection(
        &self,
        documents: &[SearchDocumentInput],
        relationships: &[DerivedRelationshipInput],
    ) -> Result<(), CoreError> {
        self.assert_writable()?;
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .replace_content_projection(documents, relationships)
    }

    /// Replaces the derived rows for one content source after a normal edit.
    pub fn replace_node_content_projection(
        &self,
        node_id: NodeId,
        documents: &[SearchDocumentInput],
        relationships: &[DerivedRelationshipInput],
    ) -> Result<(), CoreError> {
        self.assert_writable()?;
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .replace_node_content_projection(node_id, documents, relationships)
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .search(query, limit)
    }

    pub fn backlinks(&self, node_id: NodeId) -> Result<Vec<IndexedRelationship>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .backlinks(node_id)
    }

    pub fn backlinks_page(
        &self,
        node_id: NodeId,
        page: ReadPage,
    ) -> Result<Vec<IndexedRelationship>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .backlinks_page(node_id, page)
    }

    pub fn graph(&self) -> Result<GraphProjection, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .graph()
    }

    /// Returns a bounded page of active graph Nodes in title/UUID order.
    pub fn graph_nodes_page(&self, page: ReadPage) -> Result<Vec<GraphNode>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .graph_nodes_page(page)
    }

    /// Returns a bounded page of visible graph edges in deterministic order.
    pub fn graph_edges_page(&self, page: ReadPage) -> Result<Vec<GraphEdge>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .graph_edges_page(page)
    }

    pub fn orphan_node_ids(&self) -> Result<Vec<NodeId>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .orphan_node_ids()
    }

    /// Counts active orphan Nodes without materializing the complete result set.
    pub fn orphan_node_count(&self) -> Result<u64, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .orphan_node_count()
    }

    /// Returns a bounded UUID-ordered page of active orphan Nodes.
    pub fn orphan_node_ids_limited(&self, limit: usize) -> Result<Vec<NodeId>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .orphan_node_ids_limited(limit)
    }

    /// Returns a bounded UUID-ordered page of active orphan Nodes.
    pub fn orphan_node_ids_page(&self, page: ReadPage) -> Result<Vec<NodeId>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .orphan_node_ids_page(page)
    }

    pub fn apply_batch(&self, mutations: &[Mutation]) -> Result<Commit, CoreError> {
        self.assert_writable()?;
        let commit = self.records()?.apply_batch(mutations)?;
        if mutations
            .iter()
            .any(|mutation| matches!(mutation, Mutation::PurgeNode { .. }))
        {
            let records = self.records()?;
            if let Err(error) = self
                .recovery
                .lock()
                .map_err(|_| CoreError::LockPoisoned("Record recovery"))?
                .complete_purge_barrier(&records, commit.revision)
            {
                return Err(CoreError::PostCommitFailure {
                    revision: commit.revision,
                    follow_up: "purge recovery barrier",
                    reason: error.to_string(),
                });
            }
            drop(records);
            self.sync_index_best_effort();
        } else {
            self.sync_derived_best_effort();
        }
        Ok(commit)
    }

    pub fn set_property(
        &self,
        node_id: NodeId,
        key: impl Into<String>,
        value: PropertyValue,
    ) -> Result<Commit, CoreError> {
        self.apply_batch(&[Mutation::SetProperty {
            node_id,
            key: key.into(),
            value,
        }])
    }

    pub fn get_property(
        &self,
        node_id: NodeId,
        key: &str,
    ) -> Result<Option<PropertyValue>, CoreError> {
        self.records()?.get_property(node_id, key)
    }

    pub fn list_properties(
        &self,
        node_id: NodeId,
    ) -> Result<BTreeMap<String, PropertyValue>, CoreError> {
        self.records()?.list_properties(node_id)
    }

    pub fn list_properties_page(
        &self,
        node_id: NodeId,
        page: ReadPage,
    ) -> Result<Vec<(String, PropertyValue)>, CoreError> {
        self.records()?.list_properties_page(node_id, page)
    }

    /// Runs a generic filter/sort against the rebuildable Index Property projection.
    pub fn query_properties(
        &self,
        query: &PropertyQuery,
    ) -> Result<Vec<IndexedPropertyMatch>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .query_properties(query)
    }

    /// Runs the same Property query through a bounded deterministic window.
    pub fn query_properties_page(
        &self,
        query: &PropertyQuery,
        page: ReadPage,
    ) -> Result<Vec<IndexedPropertyMatch>, CoreError> {
        self.index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?
            .query_properties_page(query, page)
    }

    pub fn remove_property(
        &self,
        node_id: NodeId,
        key: impl Into<String>,
    ) -> Result<Commit, CoreError> {
        self.apply_batch(&[Mutation::RemoveProperty {
            node_id,
            key: key.into(),
        }])
    }

    pub fn add_relationship(
        &self,
        source: NodeId,
        key: impl Into<String>,
        target: NodeId,
    ) -> Result<Commit, CoreError> {
        self.apply_batch(&[Mutation::AddRelationship {
            source,
            key: key.into(),
            target,
        }])
    }

    pub fn remove_relationship(
        &self,
        source: NodeId,
        key: impl Into<String>,
        target: NodeId,
    ) -> Result<Commit, CoreError> {
        self.apply_batch(&[Mutation::RemoveRelationship {
            source,
            key: key.into(),
            target,
        }])
    }

    pub fn query_relationships(
        &self,
        node_id: NodeId,
        direction: RelationshipDirection,
        key: Option<&str>,
    ) -> Result<Vec<Relationship>, CoreError> {
        self.records()?.query_relationships(node_id, direction, key)
    }

    pub fn query_relationships_page(
        &self,
        node_id: NodeId,
        direction: RelationshipDirection,
        key: Option<&str>,
        page: ReadPage,
    ) -> Result<Vec<Relationship>, CoreError> {
        self.records()?
            .query_relationships_page(node_id, direction, key, page)
    }

    pub fn tombstone_node(&self, node_id: NodeId) -> Result<Commit, CoreError> {
        self.apply_batch(&[Mutation::TombstoneNode { node_id }])
    }

    pub fn restore_node(&self, node_id: NodeId) -> Result<Commit, CoreError> {
        self.apply_batch(&[Mutation::RestoreNode { node_id }])
    }

    pub fn purge_node(&self, node_id: NodeId) -> Result<Commit, CoreError> {
        self.apply_batch(&[Mutation::PurgeNode { node_id }])
    }

    pub fn register_property_definition(
        &self,
        definition: &NewPropertyDefinition,
    ) -> Result<PropertyDefinition, CoreError> {
        let definition = self.records()?.register_property_definition(definition)?;
        self.sync_derived_best_effort();
        Ok(definition)
    }

    pub fn get_property_definition(&self, key: &str) -> Result<PropertyDefinition, CoreError> {
        self.records()?.get_property_definition(key)
    }

    pub fn list_property_definitions(&self) -> Result<Vec<PropertyDefinition>, CoreError> {
        self.records()?.list_property_definitions()
    }

    pub fn list_property_definitions_page(
        &self,
        page: ReadPage,
    ) -> Result<Vec<PropertyDefinition>, CoreError> {
        self.records()?.list_property_definitions_page(page)
    }

    pub fn update_property_definition(
        &self,
        key: &str,
        update: &PropertyDefinitionUpdate,
    ) -> Result<PropertyDefinition, CoreError> {
        let definition = self.records()?.update_property_definition(key, update)?;
        self.sync_derived_best_effort();
        Ok(definition)
    }

    pub fn deprecate_property_definition(
        &self,
        key: &str,
        replacement: Option<&str>,
    ) -> Result<PropertyDefinition, CoreError> {
        let definition = self
            .records()?
            .deprecate_property_definition(key, replacement)?;
        self.sync_derived_best_effort();
        Ok(definition)
    }

    pub fn property_definition_usage(&self, key: &str) -> Result<DefinitionUsage, CoreError> {
        self.records()?.property_definition_usage(key)
    }

    pub fn remove_property_definition(&self, key: &str) -> Result<Commit, CoreError> {
        let commit = self.records()?.remove_property_definition(key)?;
        self.sync_derived_best_effort();
        Ok(commit)
    }

    pub fn register_relationship_definition(
        &self,
        definition: &NewRelationshipDefinition,
    ) -> Result<RelationshipDefinition, CoreError> {
        let definition = self
            .records()?
            .register_relationship_definition(definition)?;
        self.sync_derived_best_effort();
        Ok(definition)
    }

    pub fn get_relationship_definition(
        &self,
        key: &str,
    ) -> Result<RelationshipDefinition, CoreError> {
        self.records()?.get_relationship_definition(key)
    }

    pub fn list_relationship_definitions(&self) -> Result<Vec<RelationshipDefinition>, CoreError> {
        self.records()?.list_relationship_definitions()
    }

    pub fn list_relationship_definitions_page(
        &self,
        page: ReadPage,
    ) -> Result<Vec<RelationshipDefinition>, CoreError> {
        self.records()?.list_relationship_definitions_page(page)
    }

    pub fn update_relationship_definition(
        &self,
        key: &str,
        update: &RelationshipDefinitionUpdate,
    ) -> Result<RelationshipDefinition, CoreError> {
        let definition = self
            .records()?
            .update_relationship_definition(key, update)?;
        self.sync_derived_best_effort();
        Ok(definition)
    }

    pub fn deprecate_relationship_definition(
        &self,
        key: &str,
        replacement: Option<&str>,
    ) -> Result<RelationshipDefinition, CoreError> {
        let definition = self
            .records()?
            .deprecate_relationship_definition(key, replacement)?;
        self.sync_derived_best_effort();
        Ok(definition)
    }

    pub fn relationship_definition_usage(&self, key: &str) -> Result<DefinitionUsage, CoreError> {
        self.records()?.relationship_definition_usage(key)
    }

    pub fn remove_relationship_definition(&self, key: &str) -> Result<Commit, CoreError> {
        let commit = self.records()?.remove_relationship_definition(key)?;
        self.sync_derived_best_effort();
        Ok(commit)
    }

    pub fn reconcile_index(&self) -> Result<ReconcileOutcome, CoreError> {
        self.assert_writable()?;
        let mut records = self.records()?;
        let mut index = self
            .index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?;
        let outcome = index.reconcile(&records)?;
        let _ = records.prune_change_journal();
        Ok(outcome)
    }

    pub fn rebuild_index(&self) -> Result<ReconcileOutcome, CoreError> {
        self.assert_writable()?;
        let mut records = self.records()?;
        let mut index = self
            .index_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?;
        let outcome = index.rebuild(&records, IndexRebuildReason::Requested)?;
        let _ = records.prune_change_journal();
        Ok(outcome)
    }

    pub fn reconcile_recovery(&self) -> Result<RecoveryStatus, CoreError> {
        self.assert_writable()?;
        let records = self.records()?;
        let mut recovery = self
            .recovery
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Record recovery"))?;
        recovery.reconcile(&records)?;
        Ok(recovery.status(&records))
    }

    pub fn close(self) -> Result<(), CoreError> {
        let record_store = self
            .record_store
            .into_inner()
            .map_err(|_| CoreError::LockPoisoned("Record Store"))?;
        let index_store = self
            .index_store
            .into_inner()
            .map_err(|_| CoreError::LockPoisoned("Index Store"))?;
        self.recovery
            .into_inner()
            .map_err(|_| CoreError::LockPoisoned("Record recovery"))?;
        record_store.close()?;
        index_store.close()?;
        Ok(())
    }

    fn records(&self) -> Result<std::sync::MutexGuard<'_, RecordStore>, CoreError> {
        self.record_store
            .lock()
            .map_err(|_| CoreError::LockPoisoned("Record Store"))
    }

    fn sync_index_best_effort(&self) {
        let Ok(mut records) = self.record_store.lock() else {
            return;
        };
        let Ok(mut index) = self.index_store.lock() else {
            return;
        };
        if index.reconcile(&records).is_ok() {
            let _ = records.prune_change_journal();
        }
    }

    fn sync_recovery_best_effort(&self) {
        let Ok(records) = self.record_store.lock() else {
            return;
        };
        let Ok(mut recovery) = self.recovery.lock() else {
            return;
        };
        let _ = recovery.catch_up(&records);
    }

    fn sync_derived_best_effort(&self) {
        self.sync_recovery_best_effort();
        self.sync_index_best_effort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn ordinary_batch_ok_preserves_commit_while_both_follow_ups_lag() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let id = NodeId::new();
        let content = include_str!("../docs/SEMANTIC-CONTRACT.md");
        let content_hash = format!("{:x}", Sha256::digest(content.as_bytes()));
        core.index_store
            .lock()
            .expect("Index lock")
            .inject_failure(index::FailurePoint::BeforeApply);
        core.recovery
            .lock()
            .expect("Recovery lock")
            .inject_catch_up_failure();
        let commit = core
            .apply_batch(&[
                Mutation::CreateNode { node_id: id },
                Mutation::SetProperty {
                    node_id: id,
                    key: "content.body".into(),
                    value: PropertyValue::Text(content.into()),
                },
                Mutation::SetProperty {
                    node_id: id,
                    key: "content.sha256".into(),
                    value: PropertyValue::Text(content_hash.clone()),
                },
            ])
            .expect("Ok is canonical success even when both follow-ups fail");
        assert_eq!(commit.change_count, 3);
        assert_eq!(
            core.resolve_identity(id).unwrap(),
            IdentityResolution::Active
        );
        assert_eq!(
            core.get_property(id, "content.body").unwrap(),
            Some(PropertyValue::Text(content.into()))
        );
        assert_eq!(
            core.get_property(id, "content.sha256").unwrap(),
            Some(PropertyValue::Text(content_hash))
        );
        let status = core.status().expect("inspect lag");
        assert_eq!(status.index_sync.record_revision, commit.revision);
        assert_eq!(status.index_sync.state, IndexSyncState::Stale);
        assert_eq!(status.recovery.state, RecoveryState::Lagging);
        core.reconcile_recovery()
            .expect("repair recovery without retrying mutation");
        core.reconcile_index()
            .expect("repair Index without retrying mutation");
        let repaired = core.status().expect("repaired status");
        assert_eq!(repaired.index_sync.record_revision, commit.revision);
        assert_eq!(repaired.index_sync.state, IndexSyncState::Current);
        assert_eq!(repaired.recovery.state, RecoveryState::Current);
        assert_eq!(core.list_nodes().unwrap().len(), 1);
    }

    #[test]
    fn indexed_property_query_filters_and_sorts_generic_values() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let nodes = core.create_nodes(3).expect("Nodes");
        core.set_property(nodes[0].id, "score", PropertyValue::Integer(3))
            .expect("score");
        core.set_property(nodes[1].id, "score", PropertyValue::Integer(8))
            .expect("score");
        core.set_property(nodes[2].id, "score", PropertyValue::Integer(5))
            .expect("score");

        let matches = core
            .query_properties(&PropertyQuery {
                key: "score".into(),
                filter: Some(PropertyFilter::AtLeast(PropertyValue::Integer(5))),
                sort: PropertySort::Descending,
                limit: 10,
            })
            .expect("query indexed Properties");
        assert_eq!(
            matches
                .iter()
                .map(|item| item.value.clone())
                .collect::<Vec<_>>(),
            vec![PropertyValue::Integer(8), PropertyValue::Integer(5)]
        );
    }

    #[test]
    fn navigator_projection_lists_uuid_nodes_and_survives_index_rebuild() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let first = core.create_node().expect("create first Node");
        let second = core.create_node().expect("create second Node");

        let before = core.list_nodes().expect("list indexed Nodes");
        assert_eq!(before.len(), 2);
        assert!(before.iter().any(|node| node.id == first.id));
        assert!(before.iter().any(|node| node.id == second.id));

        core.rebuild_index().expect("rebuild Index");
        let after = core.list_nodes().expect("list rebuilt projection");
        assert_eq!(after, before);
    }

    #[test]
    fn initializes_closes_and_reopens_without_changing_store_identity() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open new Core");
        let first = core.status().expect("Core status");

        assert_eq!(first.record_store.schema_version, 5);
        assert_eq!(first.index_store.schema_version, 4);
        assert!(first.record_store.integrity_ok);
        assert!(first.index_store.integrity_ok);
        assert!(first.record_store.path.ends_with(RECORD_STORE_FILENAME));
        assert!(first.index_store.path.ends_with(INDEX_STORE_FILENAME));

        core.close().expect("clean close");
        let reopened = CoreState::open(library.path()).expect("reopen Core");
        assert_eq!(reopened.status().expect("reopened status"), first);
    }

    #[test]
    fn record_and_index_are_physically_separate() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let status = core.status().expect("Core status");

        assert_ne!(status.record_store.path, status.index_store.path);
        assert!(status.record_store.path.is_file());
        assert!(status.index_store.path.is_file());
    }

    #[test]
    fn phase_two_headless_graph_round_trips_across_restart() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let source = core.create_node().expect("create source");
        let target = core.create_node().expect("create target");

        core.register_property_definition(&NewPropertyDefinition {
            key: "genesis.title".into(),
            label: "Title".into(),
            description: None,
            expected_value_kind: Some(PropertyValueKind::Text),
            origin: "genesis".into(),
        })
        .expect("register title");
        core.register_relationship_definition(&NewRelationshipDefinition {
            key: "genesis.links_to".into(),
            forward_label: "Links to".into(),
            inverse_label: Some("Linked from".into()),
            description: None,
            origin: "genesis".into(),
        })
        .expect("register relationship");

        let commit = core
            .apply_batch(&[
                Mutation::SetProperty {
                    node_id: source.id,
                    key: "genesis.title".into(),
                    value: PropertyValue::Text("Source".into()),
                },
                Mutation::SetProperty {
                    node_id: source.id,
                    key: "legacy.score".into(),
                    value: PropertyValue::Integer(7),
                },
                Mutation::AddRelationship {
                    source: source.id,
                    key: "genesis.links_to".into(),
                    target: target.id,
                },
            ])
            .expect("commit graph");
        assert_eq!(commit.change_count, 3);
        core.close().expect("close Core");

        let reopened = CoreState::open(library.path()).expect("reopen Core");
        assert_eq!(reopened.get_node(source.id).expect("source"), source);
        assert_eq!(reopened.get_node(target.id).expect("target"), target);
        assert_eq!(
            reopened.list_properties(source.id).expect("properties"),
            BTreeMap::from([
                ("genesis.title".into(), PropertyValue::Text("Source".into())),
                ("legacy.score".into(), PropertyValue::Integer(7)),
            ])
        );
        assert_eq!(
            reopened
                .query_relationships(source.id, RelationshipDirection::Outgoing, None)
                .expect("relationships"),
            vec![Relationship {
                source: source.id,
                key: "genesis.links_to".into(),
                target: target.id,
            }]
        );
    }

    #[test]
    fn every_property_kind_round_trips_and_identity_stays_fixed() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("create node");
        let peer = core.create_node().expect("create peer");
        let values: BTreeMap<String, PropertyValue> = BTreeMap::from([
            ("value.null".into(), PropertyValue::Null),
            ("value.boolean".into(), PropertyValue::Boolean(true)),
            ("value.integer".into(), PropertyValue::Integer(-9)),
            ("value.real".into(), PropertyValue::Real(2.5)),
            ("value.text".into(), PropertyValue::Text("∆".into())),
            (
                "value.date".into(),
                PropertyValue::Date("2026-09-04".into()),
            ),
            (
                "value.datetime".into(),
                PropertyValue::DateTime("2026-09-04T12:00:00-07:00".into()),
            ),
            (
                "value.list".into(),
                PropertyValue::List(vec![PropertyValue::Integer(1), PropertyValue::Null]),
            ),
            (
                "value.structured".into(),
                PropertyValue::Structured(serde_json::json!({"portable": true})),
            ),
        ]);
        let mut mutations = values
            .iter()
            .map(|(key, value)| Mutation::SetProperty {
                node_id: node.id,
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        mutations.push(Mutation::AddRelationship {
            source: node.id,
            key: "related".into(),
            target: peer.id,
        });
        core.apply_batch(&mutations).expect("apply values");

        assert_eq!(core.list_properties(node.id).expect("list values"), values);
        assert_eq!(core.get_node(node.id).expect("same node").id, node.id);
        assert!(matches!(
            core.set_property(node.id, "bad.real", PropertyValue::Real(f64::NAN)),
            Err(CoreError::InvalidPropertyValue(_))
        ));
    }

    #[test]
    fn relationships_are_tuple_unique_but_allow_multiple_types() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let source = core.create_node().expect("source");
        let target = core.create_node().expect("target");
        core.add_relationship(source.id, "links_to", target.id)
            .expect("first type");
        core.add_relationship(source.id, "depends_on", target.id)
            .expect("second type");
        let duplicate = core.add_relationship(source.id, "links_to", target.id);
        assert!(matches!(duplicate, Err(CoreError::AlreadyExists { .. })));
        assert_eq!(
            core.query_relationships(source.id, RelationshipDirection::Outgoing, None)
                .expect("query")
                .len(),
            2
        );
        core.set_property(source.id, "label", PropertyValue::Text("source".into()))
            .expect("Property refresh");
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (2, 1, 2),
            "refreshing a Node must preserve its unchanged Relationships"
        );
    }

    #[test]
    fn definition_metadata_changes_never_rewrite_or_destroy_facts() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("node");
        core.register_property_definition(&NewPropertyDefinition {
            key: "priority".into(),
            label: "Priority".into(),
            description: None,
            expected_value_kind: Some(PropertyValueKind::Integer),
            origin: "user".into(),
        })
        .expect("register");
        core.set_property(node.id, "priority", PropertyValue::Integer(2))
            .expect("set fact");
        let updated = core
            .update_property_definition(
                "priority",
                &PropertyDefinitionUpdate {
                    label: "Importance".into(),
                    description: Some("Presentation changed".into()),
                    expected_value_kind: Some(PropertyValueKind::Integer),
                },
            )
            .expect("rename label");
        assert_eq!(updated.label, "Importance");
        assert_eq!(updated.version, 2);
        assert_eq!(
            core.get_property(node.id, "priority").expect("fact"),
            Some(PropertyValue::Integer(2))
        );
        assert_eq!(
            core.property_definition_usage("priority")
                .expect("usage")
                .fact_count,
            1
        );
        assert!(matches!(
            core.remove_property_definition("priority"),
            Err(CoreError::Conflict(_))
        ));
        core.deprecate_property_definition("priority", None)
            .expect("deprecate");
        assert_eq!(
            core.get_property(node.id, "priority").expect("legacy fact"),
            Some(PropertyValue::Integer(2))
        );
        assert!(matches!(
            core.set_property(node.id, "priority", PropertyValue::Integer(3)),
            Err(CoreError::Conflict(_))
        ));

        core.set_property(
            node.id,
            "undefined.legacy",
            PropertyValue::Text("kept".into()),
        )
        .expect("undefined vocabulary remains preservable");
        assert_eq!(
            core.get_property(node.id, "undefined.legacy")
                .expect("undefined fact"),
            Some(PropertyValue::Text("kept".into()))
        );
    }

    #[test]
    fn relationship_definitions_support_update_replacement_usage_and_safe_removal() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let source = core.create_node().expect("source");
        let target = core.create_node().expect("target");
        for (key, label) in [("links_to", "Links to"), ("references", "References")] {
            core.register_relationship_definition(&NewRelationshipDefinition {
                key: key.into(),
                forward_label: label.into(),
                inverse_label: None,
                description: None,
                origin: "genesis".into(),
            })
            .expect("register definition");
        }
        core.add_relationship(source.id, "links_to", target.id)
            .expect("add fact");
        let updated = core
            .update_relationship_definition(
                "links_to",
                &RelationshipDefinitionUpdate {
                    forward_label: "Points to".into(),
                    inverse_label: Some("Pointed from".into()),
                    description: Some("Presentation metadata".into()),
                },
            )
            .expect("update metadata");
        assert_eq!(updated.forward_label, "Points to");
        assert_eq!(updated.version, 2);
        let deprecated = core
            .deprecate_relationship_definition("links_to", Some("references"))
            .expect("deprecate with replacement");
        assert_eq!(deprecated.state, DefinitionState::Deprecated);
        assert_eq!(deprecated.replacement_key.as_deref(), Some("references"));
        assert_eq!(
            core.relationship_definition_usage("links_to")
                .expect("usage")
                .fact_count,
            1
        );
        assert_eq!(
            core.query_relationships(source.id, RelationshipDirection::Outgoing, Some("links_to"))
                .expect("legacy edge")
                .len(),
            1
        );
        assert!(matches!(
            core.add_relationship(target.id, "links_to", source.id),
            Err(CoreError::Conflict(_))
        ));
        assert!(matches!(
            core.remove_relationship_definition("links_to"),
            Err(CoreError::Conflict(_))
        ));
        assert_eq!(core.list_relationship_definitions().expect("list").len(), 2);
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .definition_projection_counts()
                .expect("definition projection counts"),
            (0, 2)
        );

        core.register_relationship_definition(&NewRelationshipDefinition {
            key: "unused".into(),
            forward_label: "Unused".into(),
            inverse_label: None,
            description: None,
            origin: "user".into(),
        })
        .expect("unused definition");
        core.remove_relationship_definition("unused")
            .expect("remove unused");
        assert!(matches!(
            core.get_relationship_definition("unused"),
            Err(CoreError::DefinitionNotFound(_))
        ));
    }

    #[test]
    fn failed_atomic_batch_rolls_back_every_change_and_revision() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("node");
        let before = core
            .set_property(node.id, "stable", PropertyValue::Integer(1))
            .expect("baseline");
        let failure = core.apply_batch(&[
            Mutation::SetProperty {
                node_id: node.id,
                key: "temporary".into(),
                value: PropertyValue::Text("rollback".into()),
            },
            Mutation::RemoveProperty {
                node_id: node.id,
                key: "missing".into(),
            },
        ]);
        assert!(matches!(failure, Err(CoreError::NotFound { .. })));
        assert_eq!(
            core.get_property(node.id, "temporary").expect("query"),
            None
        );
        let after = core
            .set_property(node.id, "stable", PropertyValue::Integer(2))
            .expect("next commit");
        assert_eq!(after.revision, before.revision + 1);
    }

    #[test]
    fn lifecycle_requires_tombstone_before_purge_and_preserves_independent_relationships() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("node");
        let peer = core.create_node().expect("peer");
        core.set_property(node.id, "fact", PropertyValue::Boolean(true))
            .expect("property");
        core.add_relationship(node.id, "edge", peer.id)
            .expect("edge");
        assert!(matches!(
            core.purge_node(node.id),
            Err(CoreError::Conflict(_))
        ));
        core.tombstone_node(node.id).expect("tombstone");
        assert_eq!(
            core.get_node(node.id)
                .expect("tombstone retained")
                .lifecycle_state,
            LifecycleState::Tombstoned
        );
        assert!(matches!(
            core.set_property(node.id, "fact", PropertyValue::Boolean(false)),
            Err(CoreError::Conflict(_))
        ));
        core.restore_node(node.id).expect("restore");
        assert_eq!(
            core.get_property(node.id, "fact").expect("restored fact"),
            Some(PropertyValue::Boolean(true))
        );
        core.tombstone_node(node.id).expect("tombstone again");
        core.purge_node(node.id).expect("purge");
        assert!(matches!(
            core.get_node(node.id),
            Err(CoreError::NotFound { .. })
        ));
        let edges = core
            .query_relationships(peer.id, RelationshipDirection::Either, None)
            .expect("independent canonical Relationship");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].source, node.id);
        assert_eq!(
            core.query_relationships(node.id, RelationshipDirection::Outgoing, None)
                .expect("retired ID remains a queryable Relationship endpoint"),
            edges
        );
    }

    #[test]
    fn issued_id_ledger_tracks_node_lifecycle_without_becoming_node_state() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        let peer = core.create_node().expect("peer");
        assert!(core
            .record_store
            .lock()
            .expect("Record lock")
            .is_id_issued(node.id)
            .expect("issued active ID"));
        assert!(matches!(
            core.create_node_with_id(node.id),
            Err(CoreError::AlreadyExists { .. })
        ));

        core.tombstone_node(node.id).expect("tombstone");
        assert_eq!(
            core.get_node(node.id)
                .expect("tombstoned Node")
                .lifecycle_state,
            LifecycleState::Tombstoned
        );
        assert!(core
            .record_store
            .lock()
            .expect("Record lock")
            .is_id_issued(node.id)
            .expect("issued tombstoned ID"));
        assert!(matches!(
            core.create_node_with_id(node.id),
            Err(CoreError::AlreadyExists { .. })
        ));

        core.restore_node(node.id).expect("restore");
        assert_eq!(
            core.get_node(node.id)
                .expect("restored Node")
                .lifecycle_state,
            LifecycleState::Active
        );
        core.set_property(node.id, "temporary", PropertyValue::Boolean(true))
            .expect("Property");
        core.add_relationship(node.id, "temporary", peer.id)
            .expect("Relationship");
        core.tombstone_node(node.id)
            .expect("tombstone before purge");
        core.purge_node(node.id).expect("purge");

        assert!(matches!(
            core.get_node(node.id),
            Err(CoreError::NotFound { .. })
        ));
        assert!(matches!(
            core.get_property(node.id, "temporary"),
            Err(CoreError::NotFound { .. })
        ));
        let relationships = core
            .query_relationships(peer.id, RelationshipDirection::Either, None)
            .expect("surviving Relationships");
        assert_eq!(relationships.len(), 1);
        assert_eq!(relationships[0].source, node.id);
        assert!(core
            .record_store
            .lock()
            .expect("Record lock")
            .is_id_issued(node.id)
            .expect("retired ID remains issued"));
        let replacement = core.create_node().expect("different new Node");
        assert_ne!(replacement.id, node.id);
    }

    #[test]
    fn purged_id_reuse_regression_is_rejected() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node ABC");
        core.tombstone_node(node.id).expect("tombstone ABC");
        core.purge_node(node.id).expect("purge ABC");

        assert!(matches!(
            core.create_node_with_id(node.id),
            Err(CoreError::AlreadyExists { .. })
        ));
    }

    #[test]
    fn retired_identity_survives_reopen_and_index_rebuild_without_projection() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        core.replace_content_projection(
            &[SearchDocumentInput {
                document_key: "issued-id-ledger-test".into(),
                node_id: node.id,
                source_kind: "test".into(),
                title: "Retired identity".into(),
                body: "retiredidentitytoken".into(),
                metadata: String::new(),
                source_locator: None,
                source_fingerprint: "test-v1".into(),
            }],
            &[],
        )
        .expect("derived search projection");
        assert_eq!(
            core.search("retiredidentitytoken", 10)
                .expect("search")
                .len(),
            1
        );
        core.tombstone_node(node.id).expect("tombstone");
        core.purge_node(node.id).expect("purge");
        let metadata = library.path().join(GENESIS_METADATA_DIRECTORY);
        core.close().expect("close Core");
        remove_test_sqlite(&metadata.join(INDEX_STORE_FILENAME));

        let reopened = CoreState::open(library.path()).expect("reopen without Index");
        assert!(reopened
            .list_nodes()
            .expect("rebuilt Node projection")
            .iter()
            .all(|candidate| candidate.id != node.id));
        assert!(reopened
            .search("retiredidentitytoken", 10)
            .expect("search rebuilt Index")
            .is_empty());
        assert!(matches!(
            reopened.create_node_with_id(node.id),
            Err(CoreError::AlreadyExists { .. })
        ));
        reopened.rebuild_index().expect("explicit Index rebuild");
        assert!(reopened
            .search("retiredidentitytoken", 10)
            .expect("search explicitly rebuilt Index")
            .is_empty());
        assert!(matches!(
            reopened.create_node_with_id(node.id),
            Err(CoreError::AlreadyExists { .. })
        ));
    }

    #[test]
    fn record_commit_survives_index_failure_and_incrementally_catches_up() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        core.index_store
            .lock()
            .expect("Index lock")
            .inject_failure(index::FailurePoint::BeforeApply);

        let node = core.create_node().expect("canonical create succeeds");
        assert_eq!(core.get_node(node.id).expect("canonical Node"), node);
        let stale = core.status().expect("stale status");
        assert_eq!(stale.index_sync.record_revision, 1);
        assert_eq!(stale.index_sync.applied_revision, 0);
        assert_eq!(stale.index_sync.state, IndexSyncState::Stale);
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (0, 0, 0)
        );

        assert!(matches!(
            core.reconcile_index().expect("catch up"),
            ReconcileOutcome::Incremental {
                from_revision: 0,
                to_revision: 1,
                ..
            }
        ));
        assert_eq!(
            core.status().expect("current status").index_sync.state,
            IndexSyncState::Current
        );
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (1, 0, 0)
        );
    }

    #[test]
    fn index_transaction_failure_never_advances_checkpoint_or_partial_projection() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("create Node");
        let before = core.status().expect("before status").index_sync;
        core.index_store
            .lock()
            .expect("Index lock")
            .inject_failure(index::FailurePoint::BeforeCheckpoint);

        let commit = core
            .set_property(node.id, "phase", PropertyValue::Integer(3))
            .expect("canonical Property commit");
        assert_eq!(commit.revision, before.record_revision + 1);
        let stale = core.status().expect("stale status").index_sync;
        assert_eq!(stale.applied_revision, before.applied_revision);
        assert_eq!(stale.state, IndexSyncState::Stale);
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (1, 0, 0),
            "the uncommitted Property projection must roll back"
        );
        core.reconcile_index().expect("retry reconcile");
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (1, 1, 0)
        );
    }

    #[test]
    fn stale_checkpoint_with_journal_uses_incremental_projection() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core
            .record_store
            .lock()
            .expect("Record lock")
            .create_node()
            .expect("direct canonical test mutation");
        assert_eq!(
            core.status().expect("stale status").index_sync.state,
            IndexSyncState::Stale
        );
        assert!(matches!(
            core.reconcile_index().expect("incremental reconcile"),
            ReconcileOutcome::Incremental {
                from_revision: 0,
                to_revision: 1,
                ..
            }
        ));
        assert_eq!(core.get_node(node.id).expect("canonical Node"), node);
    }

    #[test]
    fn checkpoint_older_than_retained_journal_forces_full_rebuild() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        {
            let mut records = core.record_store.lock().expect("Record lock");
            records.create_node().expect("revision one");
            records.create_node().expect("revision two");
            records
                .prune_changes_through(1)
                .expect("prune first revision");
        }
        assert!(matches!(
            core.reconcile_index().expect("rebuild"),
            ReconcileOutcome::Rebuilt {
                revision: 2,
                reason: IndexRebuildReason::JournalUnavailable,
            }
        ));
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (2, 0, 0)
        );
        assert!(library
            .path()
            .join(GENESIS_METADATA_DIRECTORY)
            .join("index.previous.sqlite")
            .is_file());
    }

    #[test]
    fn missing_or_corrupt_index_is_rebuilt_without_losing_canonical_meaning() {
        for corruption in ["missing", "corrupt"] {
            let library = tempfile::tempdir().expect("temporary library");
            let core = CoreState::open(library.path()).expect("open Core");
            let source = core.create_node().expect("source");
            let target = core.create_node().expect("target");
            core.set_property(source.id, "title", PropertyValue::Text("kept".into()))
                .expect("Property");
            core.add_relationship(source.id, "links_to", target.id)
                .expect("Relationship");
            let index_path = core.status().expect("status").index_store.path;
            core.close().expect("close Core");

            if corruption == "missing" {
                for entry in std::fs::read_dir(index_path.parent().expect("metadata directory"))
                    .expect("read metadata")
                {
                    let path = entry.expect("entry").path();
                    if path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("index."))
                    {
                        std::fs::remove_file(path).expect("delete disposable Index file");
                    }
                }
            } else {
                std::fs::write(&index_path, b"deliberately corrupt Index")
                    .expect("corrupt disposable Index");
            }

            let reopened = CoreState::open(library.path()).expect("rebuild on open");
            assert_eq!(
                reopened.get_property(source.id, "title").expect("Property"),
                Some(PropertyValue::Text("kept".into()))
            );
            assert_eq!(
                reopened
                    .query_relationships(source.id, RelationshipDirection::Outgoing, None)
                    .expect("Relationships")
                    .len(),
                1
            );
            assert_eq!(
                reopened.status().expect("status").index_sync.state,
                IndexSyncState::Current
            );
            assert_eq!(
                reopened
                    .index_store
                    .lock()
                    .expect("Index lock")
                    .projection_counts()
                    .expect("projection counts"),
                (2, 1, 1)
            );
        }
    }

    #[test]
    fn failed_rebuild_keeps_prior_valid_index_usable() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        core.create_node().expect("Node");
        let before = core.status().expect("before status");
        core.index_store
            .lock()
            .expect("Index lock")
            .inject_failure(index::FailurePoint::BeforePromotion);
        assert!(matches!(
            core.rebuild_index(),
            Err(CoreError::IndexUnavailable(_))
        ));
        let after = core.status().expect("prior Index still usable");
        assert_eq!(after.index_sync, before.index_sync);
        assert_eq!(
            core.index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (1, 0, 0)
        );
    }

    #[test]
    fn destroyed_record_store_reconstructs_graph_definitions_and_index() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let source = core.create_node().expect("source");
        let target = core.create_node().expect("target");
        core.register_property_definition(&NewPropertyDefinition {
            key: "genesis.title".into(),
            label: "Title".into(),
            description: Some("Recovered definition".into()),
            expected_value_kind: Some(PropertyValueKind::Text),
            origin: "genesis".into(),
        })
        .expect("Property definition");
        core.register_relationship_definition(&NewRelationshipDefinition {
            key: "genesis.links_to".into(),
            forward_label: "Links to".into(),
            inverse_label: Some("Linked from".into()),
            description: Some("Recovered relationship definition".into()),
            origin: "genesis".into(),
        })
        .expect("Relationship definition");
        core.set_property(
            source.id,
            "genesis.title",
            PropertyValue::Text("Recovered".into()),
        )
        .expect("Property");
        core.add_relationship(source.id, "genesis.links_to", target.id)
            .expect("Relationship");
        let status = core.status().expect("current status");
        assert_eq!(status.recovery.state, RecoveryState::Current);
        let metadata = library.path().join(GENESIS_METADATA_DIRECTORY);
        core.close().expect("close Core");

        remove_test_sqlite(&metadata.join(RECORD_STORE_FILENAME));
        remove_test_sqlite(&metadata.join(INDEX_STORE_FILENAME));

        let reopened = CoreState::open(library.path()).expect("reconstruct Core");
        assert_eq!(reopened.get_node(source.id).expect("source ID"), source);
        assert_eq!(reopened.get_node(target.id).expect("target ID"), target);
        assert!(reopened
            .record_store
            .lock()
            .expect("Record lock")
            .is_id_issued(source.id)
            .expect("recovered issued ID"));
        assert_eq!(
            reopened
                .get_property(source.id, "genesis.title")
                .expect("Property"),
            Some(PropertyValue::Text("Recovered".into()))
        );
        assert_eq!(
            reopened
                .query_relationships(source.id, RelationshipDirection::Outgoing, None)
                .expect("Relationship")
                .len(),
            1
        );
        assert_eq!(
            reopened
                .get_property_definition("genesis.title")
                .expect("Property definition")
                .label,
            "Title"
        );
        assert_eq!(
            reopened
                .get_relationship_definition("genesis.links_to")
                .expect("Relationship definition")
                .forward_label,
            "Links to"
        );
        let recovered_status = reopened.status().expect("recovered status");
        assert_eq!(recovered_status.recovery.state, RecoveryState::Current);
        assert_eq!(recovered_status.index_sync.state, IndexSyncState::Current);
        assert_eq!(
            reopened
                .index_store
                .lock()
                .expect("Index lock")
                .projection_counts()
                .expect("projection counts"),
            (2, 1, 1)
        );
    }

    #[test]
    fn corrupt_record_store_is_preserved_until_replacement_validates() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        core.set_property(node.id, "title", PropertyValue::Text("recover me".into()))
            .expect("Property");
        let metadata = library.path().join(GENESIS_METADATA_DIRECTORY);
        let record_path = metadata.join(RECORD_STORE_FILENAME);
        core.close().expect("close Core");
        std::fs::write(&record_path, b"damaged canonical database").expect("damage Record");

        let reopened = CoreState::open(library.path()).expect("validated replacement");
        assert_eq!(
            reopened.get_property(node.id, "title").expect("Property"),
            Some(PropertyValue::Text("recover me".into()))
        );
        assert!(std::fs::read_dir(&metadata)
            .expect("metadata directory")
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_str().is_some_and(|name| name
                .starts_with("records.damaged-")
                && name.ends_with(".sqlite"))));
    }

    #[test]
    fn corrupt_newest_baseline_falls_back_to_prior_baseline_and_segments() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        {
            let records = core.record_store.lock().expect("Record lock");
            core.recovery
                .lock()
                .expect("Recovery lock")
                .force_rotate(&records)
                .expect("first rotation");
        }
        core.set_property(node.id, "title", PropertyValue::Text("from segment".into()))
            .expect("Property");
        let newest = {
            let records = core.record_store.lock().expect("Record lock");
            let mut recovery = core.recovery.lock().expect("Recovery lock");
            recovery.force_rotate(&records).expect("second rotation");
            recovery
                .baseline_paths()
                .expect("baseline paths")
                .into_iter()
                .max_by_key(|(revision, _)| *revision)
                .expect("newest baseline")
                .1
        };
        std::fs::write(&newest, b"corrupt newest baseline").expect("corrupt baseline");
        let metadata = library.path().join(GENESIS_METADATA_DIRECTORY);
        core.close().expect("close Core");
        remove_test_sqlite(&metadata.join(RECORD_STORE_FILENAME));
        remove_test_sqlite(&metadata.join(INDEX_STORE_FILENAME));

        let reopened = CoreState::open(library.path()).expect("fallback recovery");
        assert_eq!(
            reopened.get_property(node.id, "title").expect("Property"),
            Some(PropertyValue::Text("from segment".into()))
        );
        assert_eq!(
            reopened.status().expect("status").recovery.state,
            RecoveryState::Current
        );
    }

    #[test]
    fn corrupt_segment_stops_at_exact_verified_confidence_boundary() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("revision one");
        core.set_property(node.id, "one", PropertyValue::Integer(1))
            .expect("revision two");
        core.set_property(node.id, "two", PropertyValue::Integer(2))
            .expect("revision three");
        let corrupt_segment = core
            .recovery
            .lock()
            .expect("Recovery lock")
            .segment_paths()
            .expect("segments")
            .into_iter()
            .find(|(from, through, _)| *from == 1 && *through == 2)
            .expect("second segment")
            .2;
        std::fs::write(&corrupt_segment, b"corrupt segment").expect("corrupt segment");

        let degraded = core.status().expect("degraded status").recovery;
        assert_eq!(degraded.state, RecoveryState::Degraded);
        assert_eq!(degraded.recoverable_revision, Some(1));
        assert!(degraded
            .note
            .as_deref()
            .is_some_and(|note| note.contains("confidence boundary is revision 1")));

        let metadata = library.path().join(GENESIS_METADATA_DIRECTORY);
        core.close().expect("close Core");
        remove_test_sqlite(&metadata.join(RECORD_STORE_FILENAME));
        remove_test_sqlite(&metadata.join(INDEX_STORE_FILENAME));
        let reopened = CoreState::open(library.path()).expect("bounded recovery");
        assert_eq!(reopened.get_node(node.id).expect("verified Node"), node);
        assert_eq!(
            reopened.get_property(node.id, "one").expect("Property"),
            None
        );
        let recovered = reopened.status().expect("recovered status").recovery;
        assert_eq!(recovered.manifest_revision, Some(1));
        assert!(recovered
            .note
            .as_deref()
            .is_some_and(|note| note.contains("confidence boundary is revision 1")));
    }

    #[test]
    fn recovery_lag_is_diagnosed_and_caught_up_from_live_truth() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        core.recovery
            .lock()
            .expect("Recovery lock")
            .inject_catch_up_failure();
        core.set_property(node.id, "title", PropertyValue::Text("canonical".into()))
            .expect("canonical commit survives recovery lag");

        let lagging = core.status().expect("lag status").recovery;
        assert_eq!(lagging.state, RecoveryState::Lagging);
        assert_eq!(lagging.live_revision, 2);
        assert_eq!(lagging.manifest_revision, Some(1));
        let caught_up = core.reconcile_recovery().expect("recovery catch-up");
        assert_eq!(caught_up.state, RecoveryState::Current);
        assert_eq!(caught_up.recoverable_revision, Some(2));
    }

    #[test]
    fn purge_barrier_failure_reports_that_canonical_commit_completed() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        core.tombstone_node(node.id).expect("tombstone Node");
        core.recovery
            .lock()
            .expect("Recovery lock")
            .inject_purge_barrier_failure();

        let error = core
            .purge_node(node.id)
            .expect_err("injected barrier failure");
        let revision = match error {
            CoreError::PostCommitFailure {
                revision,
                follow_up: "purge recovery barrier",
                ..
            } => revision,
            other => panic!("expected explicit post-commit failure, received {other}"),
        };
        assert_eq!(
            core.resolve_identity(node.id).expect("retired identity"),
            IdentityResolution::Retired,
            "the returned error must not imply that the canonical purge rolled back"
        );

        let repaired = core
            .reconcile_recovery()
            .expect("retry recovery across retained purge journal");
        assert_eq!(repaired.state, RecoveryState::Current);
        assert_eq!(repaired.purge_barrier_revision, Some(revision));
        assert_eq!(repaired.recoverable_revision, Some(revision));
    }

    #[test]
    fn manifest_tampering_is_detected_and_healed_only_from_live_truth() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        let manifest_path = metadata_recovery_root(library.path()).join("manifest.json");
        let mut envelope: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).expect("read manifest"))
                .expect("parse manifest");
        envelope["manifest"]["recovery_revision"] = serde_json::json!(999);
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&envelope).expect("serialize tampered manifest"),
        )
        .expect("tamper manifest without updating checksum");
        assert_eq!(
            core.status().expect("diagnostic status").recovery.state,
            RecoveryState::Unavailable
        );
        core.close().expect("close Core");

        let reopened = CoreState::open(library.path()).expect("heal from live Record");
        assert_eq!(reopened.get_node(node.id).expect("Node survives"), node);
        assert_eq!(
            reopened.status().expect("healed status").recovery.state,
            RecoveryState::Current
        );
    }

    #[test]
    fn completed_purge_barrier_prevents_recovery_from_reconstructing_node() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        core.set_property(node.id, "secret", PropertyValue::Text("retire".into()))
            .expect("Property");
        {
            let records = core.record_store.lock().expect("Record lock");
            core.recovery
                .lock()
                .expect("Recovery lock")
                .force_rotate(&records)
                .expect("baseline containing Node");
        }
        core.tombstone_node(node.id).expect("tombstone");
        let purge = core.purge_node(node.id).expect("purge with barrier");
        let status = core.status().expect("status").recovery;
        assert_eq!(status.purge_barrier_revision, Some(purge.revision));
        assert_eq!(status.baseline_count, 1);
        assert_eq!(status.segment_count, 0);
        let recovery_root = metadata_recovery_root(library.path());
        assert_eq!(
            std::fs::read_dir(&recovery_root)
                .expect("recovery directory")
                .filter_map(Result::ok)
                .filter(|entry| entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("baseline-") && name.ends_with(".sqlite")))
                .count(),
            1,
            "the purge barrier must physically retire older baselines"
        );

        let metadata = library.path().join(GENESIS_METADATA_DIRECTORY);
        core.close().expect("close Core");
        remove_test_sqlite(&metadata.join(RECORD_STORE_FILENAME));
        remove_test_sqlite(&metadata.join(INDEX_STORE_FILENAME));
        let reopened = CoreState::open(library.path()).expect("recover after purge");
        assert!(matches!(
            reopened.get_node(node.id),
            Err(CoreError::NotFound { .. })
        ));
        assert!(reopened
            .record_store
            .lock()
            .expect("Record lock")
            .is_id_issued(node.id)
            .expect("recovered retired ID"));
        assert!(matches!(
            reopened.create_node_with_id(node.id),
            Err(CoreError::AlreadyExists { .. })
        ));
        assert_eq!(
            reopened
                .status()
                .expect("status")
                .recovery
                .purge_barrier_revision,
            Some(purge.revision)
        );
    }

    #[test]
    fn recovery_rotation_keeps_two_baselines_and_bounded_segments() {
        let library = tempfile::tempdir().expect("temporary library");
        let core = CoreState::open(library.path()).expect("open Core");
        let node = core.create_node().expect("Node");
        for revision in 0..130 {
            core.set_property(node.id, "counter", PropertyValue::Integer(revision))
                .expect("mutation");
        }
        let status = core.status().expect("status").recovery;
        assert_eq!(status.state, RecoveryState::Current);
        assert_eq!(status.baseline_count, 2);
        assert!(status.segment_count <= 128);
    }

    fn remove_test_sqlite(path: &Path) {
        for member in [
            path.to_path_buf(),
            PathBuf::from(format!("{}-wal", path.display())),
            PathBuf::from(format!("{}-shm", path.display())),
        ] {
            if member.exists() {
                std::fs::remove_file(member).expect("remove test SQLite artifact");
            }
        }
    }

    fn metadata_recovery_root(library: &Path) -> PathBuf {
        library
            .join(GENESIS_METADATA_DIRECTORY)
            .join("recovery/record")
    }
}
