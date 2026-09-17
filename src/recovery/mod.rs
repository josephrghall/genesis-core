use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::record_store::{JournalDelta, ProjectionPatch, RecordStore, RECORD_SCHEMA_VERSION};
use super::{CoreError, RECORD_STORE_FILENAME};

const RECOVERY_FORMAT_VERSION: u32 = 1;
const RECOVERY_DIRECTORY: &str = "recovery/record";
const MANIFEST_FILENAME: &str = "manifest.json";
const SEGMENTS_DIRECTORY: &str = "segments";
const BASELINE_A: &str = "baseline-A.sqlite";
const BASELINE_B: &str = "baseline-B.sqlite";
const SEGMENTS_PER_BASELINE: usize = 64;
const MAX_RECOVERY_SEGMENTS: usize = SEGMENTS_PER_BASELINE * 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryState {
    Current,
    Lagging,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryStatus {
    pub state: RecoveryState,
    pub live_revision: u64,
    pub manifest_revision: Option<u64>,
    pub recoverable_revision: Option<u64>,
    pub baseline_count: usize,
    pub segment_count: usize,
    pub purge_barrier_revision: Option<u64>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecoveryManifest {
    format_version: u32,
    record_schema_version: u32,
    generation: u64,
    recovery_revision: u64,
    purge_barrier_revision: Option<u64>,
    baselines: Vec<BaselineArtifact>,
    segments: Vec<SegmentArtifact>,
    last_recovery_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestEnvelope {
    manifest: RecoveryManifest,
    checksum_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaselineArtifact {
    filename: String,
    revision: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentArtifact {
    filename: String,
    from_revision: u64,
    through_revision: u64,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecoverySegment {
    format_version: u32,
    from_revision: u64,
    through_revision: u64,
    patch: ProjectionPatch,
}

struct RecoveryPlan {
    baseline: BaselineArtifact,
    segments: Vec<ProjectionPatch>,
    recovered_revision: u64,
    note: Option<String>,
}

pub(crate) struct RecoveryManager {
    root: PathBuf,
    #[cfg(test)]
    fail_next_catch_up: bool,
    #[cfg(test)]
    fail_next_purge_barrier: bool,
}

impl RecoveryManager {
    pub(crate) fn open(metadata_root: &Path) -> Result<Self, CoreError> {
        let root = metadata_root.join(RECOVERY_DIRECTORY);
        fs::create_dir_all(root.join(SEGMENTS_DIRECTORY))?;
        Ok(Self {
            root,
            #[cfg(test)]
            fail_next_catch_up: false,
            #[cfg(test)]
            fail_next_purge_barrier: false,
        })
    }

    pub(crate) fn manifest_exists(metadata_root: &Path) -> bool {
        metadata_root
            .join(RECOVERY_DIRECTORY)
            .join(MANIFEST_FILENAME)
            .is_file()
    }

    pub(crate) fn ensure_initialized(&mut self, records: &RecordStore) -> Result<(), CoreError> {
        match self.load_manifest() {
            Ok(_) => Ok(()),
            Err(_) => self.reset_to_single_baseline(
                records,
                None,
                Some("recovery metadata was initialized from live canonical truth".into()),
            ),
        }
    }

    pub(crate) fn catch_up(&mut self, records: &RecordStore) -> Result<(), CoreError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_catch_up) {
            return Err(CoreError::CorruptStore {
                store: "Record recovery",
                reason: "injected catch-up failure".into(),
            });
        }

        let mut manifest = self.load_manifest()?;
        let live_revision = records.current_revision()?;
        if manifest.recovery_revision == live_revision {
            return Ok(());
        }
        if manifest.recovery_revision > live_revision {
            return self.reset_to_single_baseline(
                records,
                manifest.purge_barrier_revision,
                Some("recovery checkpoint was ahead of live canonical truth and was reset".into()),
            );
        }

        match records.projection_delta_since(manifest.recovery_revision)? {
            JournalDelta::Current(_) => Ok(()),
            JournalDelta::Unavailable { current_revision } => self.reset_to_single_baseline(
                records,
                manifest.purge_barrier_revision,
                Some(format!(
                    "change journal could not bridge to revision {current_revision}; a clean baseline was created"
                )),
            ),
            JournalDelta::Available(patch) => {
                if patch.nodes.iter().any(|(_, node)| node.is_none()) {
                    return self.reset_to_single_baseline(
                        records,
                        Some(patch.target_revision),
                        Some("recovery reset across a retained purge revision".into()),
                    );
                }
                let artifact = self.write_segment(&patch)?;
                manifest.recovery_revision = patch.target_revision;
                manifest.generation += 1;
                manifest.segments.push(artifact);
                manifest.last_recovery_note = None;
                self.save_manifest(&manifest)?;

                let newest_baseline = manifest
                    .baselines
                    .iter()
                    .map(|baseline| baseline.revision)
                    .max()
                    .unwrap_or(0);
                let new_segment_count = manifest
                    .segments
                    .iter()
                    .filter(|segment| segment.through_revision > newest_baseline)
                    .count();
                if new_segment_count >= SEGMENTS_PER_BASELINE {
                    self.rotate_baseline(records, &mut manifest)?;
                }
                Ok(())
            }
        }
    }

    /// Startup only needs the live canonical revision and bounded journal. Full
    /// recovery artifact verification remains available through explicit status,
    /// reconciliation, and reconstruction paths.
    pub(crate) fn reconcile_startup(&mut self, records: &RecordStore) -> Result<(), CoreError> {
        let manifest = self.load_manifest()?;
        let live_revision = records.current_revision()?;
        if manifest.recovery_revision > live_revision {
            return self.reset_to_single_baseline(
                records,
                manifest.purge_barrier_revision,
                Some("recovery checkpoint was ahead of live canonical truth and was reset".into()),
            );
        }
        self.catch_up(records)
    }

    pub(crate) fn reconcile(&mut self, records: &RecordStore) -> Result<(), CoreError> {
        let manifest = self.load_manifest()?;
        let (recoverable, note) = self.best_recoverable_revision(&manifest);
        if recoverable != Some(manifest.recovery_revision) {
            return self.reset_to_single_baseline(
                records,
                manifest.purge_barrier_revision,
                Some(note.unwrap_or_else(|| {
                    "recovery artifacts did not reach their advertised revision; rebuilt from live canonical truth".into()
                })),
            );
        }
        self.catch_up(records)
    }

    pub(crate) fn complete_purge_barrier(
        &mut self,
        records: &RecordStore,
        revision: u64,
    ) -> Result<(), CoreError> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_purge_barrier) {
            return Err(CoreError::CorruptStore {
                store: "Record recovery",
                reason: "injected purge-barrier failure".into(),
            });
        }
        if records.current_revision()? != revision {
            return Err(recovery_corrupt(
                "purge barrier revision does not match canonical truth",
            ));
        }
        self.reset_to_single_baseline(records, Some(revision), None)
    }

    pub(crate) fn status(&self, records: &RecordStore) -> RecoveryStatus {
        let live_revision = records.current_revision().unwrap_or(0);
        let manifest = match self.load_manifest() {
            Ok(manifest) => manifest,
            Err(error) => {
                return RecoveryStatus {
                    state: RecoveryState::Unavailable,
                    live_revision,
                    manifest_revision: None,
                    recoverable_revision: None,
                    baseline_count: 0,
                    segment_count: 0,
                    purge_barrier_revision: None,
                    note: Some(error.to_string()),
                };
            }
        };
        let (recoverable_revision, chain_note) = self.best_recoverable_revision(&manifest);
        let note = chain_note.or_else(|| manifest.last_recovery_note.clone());
        let state = match recoverable_revision {
            None => RecoveryState::Unavailable,
            Some(recoverable) if recoverable < manifest.recovery_revision => {
                RecoveryState::Degraded
            }
            Some(_) if manifest.recovery_revision < live_revision => RecoveryState::Lagging,
            Some(_) if manifest.recovery_revision == live_revision => RecoveryState::Current,
            Some(_) => RecoveryState::Degraded,
        };
        RecoveryStatus {
            state,
            live_revision,
            manifest_revision: Some(manifest.recovery_revision),
            recoverable_revision,
            baseline_count: manifest.baselines.len(),
            segment_count: manifest.segments.len(),
            purge_barrier_revision: manifest.purge_barrier_revision,
            note,
        }
    }

    /// Reconstruct missing or damaged canonical truth into a new file, validate it,
    /// and only then promote it to the canonical path.
    pub(crate) fn reconstruct(metadata_root: &Path) -> Result<u64, CoreError> {
        let mut manager = Self::open(metadata_root)?;
        let manifest = manager.load_manifest()?;
        let plan = manager.best_plan(&manifest)?;
        let candidate = metadata_root.join("records.recovery-candidate.sqlite");
        remove_sqlite_family(&candidate)?;
        fs::copy(manager.root.join(&plan.baseline.filename), &candidate)?;

        let mut recovered = RecordStore::open(candidate.clone())?;
        for patch in &plan.segments {
            recovered.apply_recovery_patch(patch)?;
        }
        recovered.clear_change_journal()?;
        let status = recovered.status()?;
        if !status.integrity_ok || recovered.current_revision()? != plan.recovered_revision {
            return Err(recovery_corrupt(
                "reconstructed candidate failed integrity or revision validation",
            ));
        }
        recovered.prepare_for_promotion()?;
        recovered.close()?;

        let canonical = metadata_root.join(RECORD_STORE_FILENAME);
        preserve_damaged_sqlite(&canonical, manifest.generation)?;
        fs::rename(&candidate, &canonical)?;
        sync_directory(metadata_root)?;

        let reopened = RecordStore::open(canonical)?;
        let note = plan.note.or_else(|| {
            (plan.recovered_revision < manifest.recovery_revision).then(|| {
                format!(
                    "recovery stopped at verified revision {}; manifest advertised revision {}",
                    plan.recovered_revision, manifest.recovery_revision
                )
            })
        });
        manager.reset_to_single_baseline(
            &reopened,
            manifest
                .purge_barrier_revision
                .filter(|barrier| *barrier <= plan.recovered_revision),
            note,
        )?;
        reopened.close()?;
        Ok(plan.recovered_revision)
    }

    fn reset_to_single_baseline(
        &mut self,
        records: &RecordStore,
        purge_barrier_revision: Option<u64>,
        note: Option<String>,
    ) -> Result<(), CoreError> {
        let revision = records.current_revision()?;
        let baseline = self.create_baseline(records, BASELINE_A, revision)?;
        let old_files = self.recovery_artifact_files()?;
        let manifest = RecoveryManifest {
            format_version: RECOVERY_FORMAT_VERSION,
            record_schema_version: RECORD_SCHEMA_VERSION,
            generation: self
                .load_manifest()
                .map(|manifest| manifest.generation + 1)
                .unwrap_or(1),
            recovery_revision: revision,
            purge_barrier_revision,
            baselines: vec![baseline],
            segments: Vec::new(),
            last_recovery_note: note,
        };
        self.save_manifest(&manifest)?;
        for path in old_files {
            if path.file_name().and_then(|name| name.to_str()) != Some(BASELINE_A) {
                remove_sqlite_family(&path)?;
            }
        }
        self.remove_unreferenced_segments(&manifest)?;
        sync_directory(&self.root)?;
        Ok(())
    }

    fn rotate_baseline(
        &mut self,
        records: &RecordStore,
        manifest: &mut RecoveryManifest,
    ) -> Result<(), CoreError> {
        let slot = if !manifest
            .baselines
            .iter()
            .any(|baseline| baseline.filename == BASELINE_B)
        {
            BASELINE_B.to_owned()
        } else {
            manifest
                .baselines
                .iter()
                .min_by_key(|baseline| baseline.revision)
                .map(|baseline| baseline.filename.clone())
                .unwrap_or_else(|| BASELINE_A.to_owned())
        };
        let revision = records.current_revision()?;
        let baseline = self.create_baseline(records, &slot, revision)?;
        manifest.baselines.retain(|item| item.filename != slot);
        manifest.baselines.push(baseline);
        manifest.baselines.sort_by_key(|item| item.revision);
        if manifest.baselines.len() > 2 {
            manifest.baselines.remove(0);
        }
        let oldest = manifest
            .baselines
            .iter()
            .map(|item| item.revision)
            .min()
            .unwrap_or(revision);
        manifest
            .segments
            .retain(|segment| segment.through_revision > oldest);
        manifest.generation += 1;
        self.save_manifest(manifest)?;
        self.remove_unreferenced_segments(manifest)?;
        Ok(())
    }

    fn create_baseline(
        &self,
        records: &RecordStore,
        filename: &str,
        revision: u64,
    ) -> Result<BaselineArtifact, CoreError> {
        validate_local_filename(filename)?;
        let destination = self.root.join(filename);
        let temporary = self.root.join(format!(".{filename}.tmp"));
        remove_sqlite_family(&temporary)?;
        records.backup_to(&temporary)?;
        let validation = RecordStore::open(temporary.clone())?;
        if validation.current_revision()? != revision || !validation.status()?.integrity_ok {
            return Err(recovery_corrupt("online baseline failed validation"));
        }
        validation.close()?;
        fs::rename(&temporary, &destination)?;
        sync_directory(&self.root)?;
        Ok(BaselineArtifact {
            filename: filename.into(),
            revision,
            size_bytes: fs::metadata(&destination)?.len(),
            checksum_sha256: checksum_file(&destination)?,
        })
    }

    fn write_segment(&self, patch: &ProjectionPatch) -> Result<SegmentArtifact, CoreError> {
        let filename = format!(
            "segment-{:020}-{:020}.json",
            patch.from_revision + 1,
            patch.target_revision
        );
        let segment = RecoverySegment {
            format_version: RECOVERY_FORMAT_VERSION,
            from_revision: patch.from_revision,
            through_revision: patch.target_revision,
            patch: patch.clone(),
        };
        let bytes = serde_json::to_vec(&segment)
            .map_err(|error| recovery_corrupt(format!("cannot serialize segment: {error}")))?;
        let destination = self.root.join(SEGMENTS_DIRECTORY).join(&filename);
        atomic_write(&destination, &bytes)?;
        Ok(SegmentArtifact {
            filename,
            from_revision: patch.from_revision,
            through_revision: patch.target_revision,
            size_bytes: bytes.len() as u64,
            checksum_sha256: checksum_bytes(&bytes),
        })
    }

    fn load_manifest(&self) -> Result<RecoveryManifest, CoreError> {
        let bytes = fs::read(self.root.join(MANIFEST_FILENAME))?;
        let envelope: ManifestEnvelope = serde_json::from_slice(&bytes)
            .map_err(|error| recovery_corrupt(format!("invalid recovery manifest: {error}")))?;
        let manifest_bytes = serde_json::to_vec(&envelope.manifest).map_err(|error| {
            recovery_corrupt(format!("cannot verify recovery manifest: {error}"))
        })?;
        if checksum_bytes(&manifest_bytes) != envelope.checksum_sha256 {
            return Err(recovery_corrupt("recovery manifest checksum mismatch"));
        }
        if envelope.manifest.format_version != RECOVERY_FORMAT_VERSION
            || envelope.manifest.record_schema_version != RECORD_SCHEMA_VERSION
            || envelope.manifest.baselines.len() > 2
            || envelope.manifest.segments.len() > MAX_RECOVERY_SEGMENTS
        {
            return Err(recovery_corrupt(
                "recovery manifest version or artifact bounds are invalid",
            ));
        }
        for baseline in &envelope.manifest.baselines {
            validate_local_filename(&baseline.filename)?;
        }
        for segment in &envelope.manifest.segments {
            validate_local_filename(&segment.filename)?;
        }
        Ok(envelope.manifest)
    }

    fn save_manifest(&self, manifest: &RecoveryManifest) -> Result<(), CoreError> {
        let manifest_bytes = serde_json::to_vec(manifest)
            .map_err(|error| recovery_corrupt(format!("cannot serialize manifest: {error}")))?;
        let envelope = ManifestEnvelope {
            manifest: manifest.clone(),
            checksum_sha256: checksum_bytes(&manifest_bytes),
        };
        let bytes = serde_json::to_vec_pretty(&envelope)
            .map_err(|error| recovery_corrupt(format!("cannot serialize manifest: {error}")))?;
        atomic_write(&self.root.join(MANIFEST_FILENAME), &bytes)
    }

    fn best_recoverable_revision(
        &self,
        manifest: &RecoveryManifest,
    ) -> (Option<u64>, Option<String>) {
        match self.best_plan(manifest) {
            Ok(plan) => (Some(plan.recovered_revision), plan.note),
            Err(error) => (None, Some(error.to_string())),
        }
    }

    fn best_plan(&self, manifest: &RecoveryManifest) -> Result<RecoveryPlan, CoreError> {
        let mut best: Option<RecoveryPlan> = None;
        let mut baseline_errors = Vec::new();
        let mut baselines = manifest.baselines.clone();
        baselines.sort_by_key(|baseline| std::cmp::Reverse(baseline.revision));
        for baseline in baselines {
            match self.plan_from_baseline(manifest, &baseline) {
                Ok(plan)
                    if best
                        .as_ref()
                        .map(|current| plan.recovered_revision > current.recovered_revision)
                        .unwrap_or(true) =>
                {
                    best = Some(plan)
                }
                Ok(_) => {}
                Err(error) => baseline_errors.push(error.to_string()),
            }
        }
        let mut best = best.ok_or_else(|| {
            recovery_corrupt(format!(
                "no valid recovery baseline: {}",
                baseline_errors.join("; ")
            ))
        })?;
        if best.note.is_none() && !baseline_errors.is_empty() {
            best.note = Some(format!(
                "recovery ignored invalid redundant artifacts: {}",
                baseline_errors.join("; ")
            ));
        }
        Ok(best)
    }

    fn plan_from_baseline(
        &self,
        manifest: &RecoveryManifest,
        baseline: &BaselineArtifact,
    ) -> Result<RecoveryPlan, CoreError> {
        let baseline_path = self.root.join(&baseline.filename);
        verify_artifact(
            &baseline_path,
            baseline.size_bytes,
            &baseline.checksum_sha256,
        )?;
        let validation = RecordStore::open(baseline_path.clone())?;
        let actual_revision = validation.current_revision()?;
        let valid = validation.status()?.integrity_ok;
        validation.close()?;
        if !valid || actual_revision != baseline.revision {
            return Err(recovery_corrupt(format!(
                "baseline {} failed semantic validation",
                baseline.filename
            )));
        }

        let mut cursor = baseline.revision;
        let mut patches = Vec::new();
        let mut note = None;
        let mut segments = manifest.segments.clone();
        segments.sort_by_key(|segment| (segment.from_revision, segment.through_revision));
        for artifact in segments {
            if artifact.through_revision <= cursor {
                continue;
            }
            if artifact.from_revision != cursor {
                note = Some(format!(
                    "recovery confidence boundary is revision {cursor}; the next segment begins after revision {}",
                    artifact.from_revision
                ));
                break;
            }
            match self.read_segment(&artifact) {
                Ok(segment)
                    if segment.from_revision == cursor
                        && segment.through_revision == artifact.through_revision
                        && segment.patch.from_revision == cursor
                        && segment.patch.target_revision == artifact.through_revision =>
                {
                    cursor = segment.through_revision;
                    patches.push(segment.patch);
                }
                Ok(_) => {
                    note = Some(format!(
                        "recovery confidence boundary is revision {cursor}; segment metadata is inconsistent"
                    ));
                    break;
                }
                Err(error) => {
                    note = Some(format!(
                        "recovery confidence boundary is revision {cursor}; {error}"
                    ));
                    break;
                }
            }
        }
        Ok(RecoveryPlan {
            baseline: baseline.clone(),
            segments: patches,
            recovered_revision: cursor,
            note,
        })
    }

    fn read_segment(&self, artifact: &SegmentArtifact) -> Result<RecoverySegment, CoreError> {
        let path = self.root.join(SEGMENTS_DIRECTORY).join(&artifact.filename);
        verify_artifact(&path, artifact.size_bytes, &artifact.checksum_sha256)?;
        let bytes = fs::read(path)?;
        let segment: RecoverySegment = serde_json::from_slice(&bytes)
            .map_err(|error| recovery_corrupt(format!("invalid recovery segment: {error}")))?;
        if segment.format_version != RECOVERY_FORMAT_VERSION {
            return Err(recovery_corrupt("unsupported recovery segment version"));
        }
        Ok(segment)
    }

    fn recovery_artifact_files(&self) -> Result<Vec<PathBuf>, CoreError> {
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            if path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("baseline-") && name.ends_with(".sqlite"))
            {
                files.push(path);
            }
        }
        Ok(files)
    }

    fn remove_unreferenced_segments(&self, manifest: &RecoveryManifest) -> Result<(), CoreError> {
        let referenced = manifest
            .segments
            .iter()
            .map(|segment| segment.filename.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        for entry in fs::read_dir(self.root.join(SEGMENTS_DIRECTORY))? {
            let path = entry?.path();
            if path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| !referenced.contains(name))
            {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_catch_up_failure(&mut self) {
        self.fail_next_catch_up = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_purge_barrier_failure(&mut self) {
        self.fail_next_purge_barrier = true;
    }

    #[cfg(test)]
    pub(crate) fn force_rotate(&mut self, records: &RecordStore) -> Result<(), CoreError> {
        let mut manifest = self.load_manifest()?;
        self.rotate_baseline(records, &mut manifest)
    }

    #[cfg(test)]
    pub(crate) fn baseline_paths(&self) -> Result<Vec<(u64, PathBuf)>, CoreError> {
        Ok(self
            .load_manifest()?
            .baselines
            .into_iter()
            .map(|baseline| (baseline.revision, self.root.join(baseline.filename)))
            .collect())
    }

    #[cfg(test)]
    pub(crate) fn segment_paths(&self) -> Result<Vec<(u64, u64, PathBuf)>, CoreError> {
        Ok(self
            .load_manifest()?
            .segments
            .into_iter()
            .map(|segment| {
                (
                    segment.from_revision,
                    segment.through_revision,
                    self.root.join(SEGMENTS_DIRECTORY).join(segment.filename),
                )
            })
            .collect())
    }
}

fn verify_artifact(path: &Path, size: u64, checksum: &str) -> Result<(), CoreError> {
    let metadata = fs::metadata(path)?;
    if metadata.len() != size || checksum_file(path)? != checksum {
        return Err(recovery_corrupt(format!(
            "artifact {} failed checksum validation",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("<invalid>")
        )));
    }
    Ok(())
}

fn checksum_file(path: &Path) -> Result<String, CoreError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn checksum_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), CoreError> {
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent() {
        sync_directory(parent)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), CoreError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn validate_local_filename(filename: &str) -> Result<(), CoreError> {
    let path = Path::new(filename);
    if path.components().count() != 1 || filename.is_empty() {
        return Err(recovery_corrupt("recovery artifact filename is not local"));
    }
    Ok(())
}

fn preserve_damaged_sqlite(path: &Path, generation: u64) -> Result<(), CoreError> {
    if !path.exists() {
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| recovery_corrupt("canonical Record path has no parent"))?;
    let damaged = parent.join(format!("records.damaged-{generation}.sqlite"));
    remove_sqlite_family(&damaged)?;
    fs::rename(path, &damaged)?;
    for suffix in ["-wal", "-shm"] {
        let auxiliary = PathBuf::from(format!("{}{suffix}", path.display()));
        if auxiliary.exists() {
            fs::rename(
                &auxiliary,
                PathBuf::from(format!("{}{suffix}", damaged.display())),
            )?;
        }
    }
    Ok(())
}

fn remove_sqlite_family(path: &Path) -> Result<(), CoreError> {
    for member in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        if member.exists() {
            fs::remove_file(member)?;
        }
    }
    Ok(())
}

fn recovery_corrupt(reason: impl Into<String>) -> CoreError {
    CoreError::CorruptStore {
        store: "Record recovery",
        reason: reason.into(),
    }
}
