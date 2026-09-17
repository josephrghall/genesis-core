//! Storage-boundary evidence for protected Genesis libraries.
//!
//! Protected storage is an explicit boundary above Core. Core does not own
//! encryption or keys; it only refuses to write when the selected protected
//! root can no longer be proven to be the same root that was configured.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::CoreError;

const BOUNDARY_FORMAT_VERSION: u32 = 1;
const BOUNDARY_MANIFEST: &str = "storage-boundary.json";
const BOUNDARY_SENTINEL: &str = "protected-root.sentinel";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageBoundary {
    Unprotected,
    Protected {
        library_root: PathBuf,
        root_fingerprint: String,
        sentinel: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BoundaryManifest {
    format_version: u32,
    mode: String,
    root_fingerprint: String,
    sentinel: String,
}

impl StorageBoundary {
    pub(crate) fn load_if_present(library_root: &Path) -> Result<Self, CoreError> {
        let metadata_root = library_root.join(super::GENESIS_METADATA_DIRECTORY);
        let manifest_path = metadata_root.join(BOUNDARY_MANIFEST);
        if !manifest_path.exists() {
            return Ok(Self::Unprotected);
        }
        Self::load_required(library_root)
    }

    pub(crate) fn load_required(library_root: &Path) -> Result<Self, CoreError> {
        if !library_root.is_dir() {
            return Err(unavailable("the selected protected Store root is missing"));
        }
        let manifest_path = library_root
            .join(super::GENESIS_METADATA_DIRECTORY)
            .join(BOUNDARY_MANIFEST);
        let bytes = fs::read(&manifest_path).map_err(|error| {
            unavailable(format!(
                "protected Store boundary manifest is unavailable: {error}"
            ))
        })?;
        let manifest: BoundaryManifest = serde_json::from_slice(&bytes).map_err(|error| {
            unavailable(format!(
                "protected Store boundary manifest is invalid: {error}"
            ))
        })?;
        if manifest.format_version != BOUNDARY_FORMAT_VERSION || manifest.mode != "protected" {
            return Err(unavailable(
                "protected Store boundary manifest version or mode is invalid",
            ));
        }
        if manifest.root_fingerprint.is_empty() || manifest.sentinel.is_empty() {
            return Err(unavailable(
                "protected Store boundary evidence is incomplete",
            ));
        }
        Ok(Self::Protected {
            library_root: library_root.to_path_buf(),
            root_fingerprint: manifest.root_fingerprint,
            sentinel: manifest.sentinel,
        })
    }

    /// Explicitly marks an already selected root as protected.
    pub(crate) fn configure_protected(library_root: &Path) -> Result<(), CoreError> {
        if !library_root.is_dir() {
            return Err(unavailable(
                "a protected Store must be configured on an existing root",
            ));
        }
        let metadata_root = library_root.join(super::GENESIS_METADATA_DIRECTORY);
        fs::create_dir_all(&metadata_root)?;
        let sentinel = Uuid::now_v7().to_string();
        let manifest = BoundaryManifest {
            format_version: BOUNDARY_FORMAT_VERSION,
            mode: "protected".into(),
            root_fingerprint: root_fingerprint(library_root)?,
            sentinel: sentinel.clone(),
        };
        let manifest_bytes =
            serde_json::to_vec_pretty(&manifest).map_err(|error| CoreError::CorruptStore {
                store: "Storage boundary",
                reason: format!("could not serialize boundary manifest: {error}"),
            })?;
        write_new(&metadata_root.join(BOUNDARY_SENTINEL), sentinel.as_bytes())?;
        write_new(&metadata_root.join(BOUNDARY_MANIFEST), &manifest_bytes)?;
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), CoreError> {
        let Self::Protected {
            library_root,
            root_fingerprint,
            sentinel,
        } = self
        else {
            return Ok(());
        };

        if !library_root.is_dir() {
            return Err(unavailable(
                "the protected Store root disappeared or is unmounted",
            ));
        }
        if root_fingerprint_for_open(library_root)? != *root_fingerprint {
            return Err(unavailable(
                "the protected Store root does not match its configured storage identity",
            ));
        }
        let sentinel_path = library_root
            .join(super::GENESIS_METADATA_DIRECTORY)
            .join(BOUNDARY_SENTINEL);
        let actual_sentinel = fs::read_to_string(&sentinel_path).map_err(|error| {
            unavailable(format!(
                "the protected Store sentinel is unavailable: {error}"
            ))
        })?;
        if actual_sentinel != *sentinel {
            return Err(unavailable("the protected Store sentinel does not match"));
        }
        Ok(())
    }

    pub(crate) fn is_protected(&self) -> bool {
        matches!(self, Self::Protected { .. })
    }
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), CoreError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn root_fingerprint(root: &Path) -> Result<String, CoreError> {
    let canonical = root.canonicalize()?;
    let metadata = fs::metadata(&canonical)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!(
            "unix:{}:{}:{}",
            canonical.to_string_lossy(),
            metadata.dev(),
            metadata.ino()
        ))
    }
    #[cfg(windows)]
    {
        // Stable Rust does not expose Windows volume/file-index evidence on
        // all supported toolchains. The sentinel remains mandatory, so a
        // replacement root cannot be accepted merely because its path matches.
        let _ = metadata;
        return Ok(format!("windows-path:{}", canonical.to_string_lossy()));
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = metadata;
        Ok(format!("path:{}", canonical.to_string_lossy()))
    }
}

fn root_fingerprint_for_open(root: &Path) -> Result<String, CoreError> {
    root_fingerprint(root)
}

fn unavailable(reason: impl Into<String>) -> CoreError {
    CoreError::ProtectedStoreUnavailable(reason.into())
}
