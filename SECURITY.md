# Genesis Core security and threat model

**Status:** Developer Preview threat model. Genesis Core has not received an independent security audit.

## Security boundary

Genesis Core is an embedded, local-first Rust library. Its boundary is the selected library root, the `.genesis` metadata directory, the host process, and the caller using the public API. Core does not run a network service, authenticate users, authorize tenants, or encrypt data by itself.

Protected mode is optional. `CoreState::configure_protected` requires an existing selected root, canonicalizes it, and creates a versioned storage-boundary manifest plus a unique persistent sentinel. On Unix, the recorded root fingerprint contains canonical path, device ID, and inode. On Windows, it contains canonical path and the sentinel remains mandatory. `CoreState::open_protected` and protected writes fail closed when the root is missing or the configured fingerprint or sentinel no longer matches.

This baseline validates configured-root identity; it is not a complete filesystem access-control or process sandbox. Core does not currently guarantee host path ownership or rejection of every symlink/reparse-point substitution. The operating system, stricter path policy, encrypted volume, mount, account ownership and permissions, container/service-account restrictions, process sandbox, backup system, and key management remain external deployment controls.

## Trust assumptions

Core assumes:

- the host process and code loaded into it are trusted;
- the caller is authorized to read and mutate the selected Store;
- the OS, SQLite runtime bundled through `rusqlite`, and filesystem provide their documented behavior;
- the library root is not simultaneously replaced, copied over, or manipulated outside the API while open;
- separate Core processes/connections follow SQLite locking semantics and do not bypass the schema with raw writes;
- the caller limits untrusted input sizes before allocating application-level payloads.

Core treats cursor tokens, IDs, Property keys/values, Relationship keys, schema metadata, SQLite rows, and recovery artifacts as potentially malformed and validates them before assigning meaning.

## Canonical and derived data

- `records.sqlite` is canonical for current Core Record truth.
- `index.sqlite` is derived, disposable, and rebuildable; it is never sufficient backup or authority.
- issued Node IDs remain canonical identity history after Purge without retaining a secret purged Node Record.
- recovery baselines, segments, manifests, and checksums are redundant local recovery material. They are not canonical during normal operation and are not an off-device backup.
- the `record_changes` reconciliation journal is bounded and may be pruned; it is not an unlimited security or audit log.
- ordinary mutation `Ok` reports canonical success even if best-effort Index/recovery follow-up fails; inspect status and repair without retrying the mutation. `PostCommitFailure` specifically reports a failed purge recovery-barrier completion after canonical commit; other storage/lock errors may require canonical-state inspection. See [API failure semantics](docs/API.md#failure-semantics).

## Corruption and malformed stores

Open performs schema checks, `quick_check`, foreign-key validation, version validation, and revision reconciliation. Unsupported newer schemas are rejected directly. Other live Record-open failures can trigger validated reconstruction when a recovery manifest exists; otherwise invalid state is rejected. Interpretable missing provenance can surface degraded status. The Record-open recovery branch currently includes operational errors as well as corruption; see the [precise recovery and diagnostic-evidence limits](docs/SCHEMA-MIGRATION-RECOVERY.md#validation-rejection-versus-reconstruction). Validation does not delete inconsistent rows to make them pass.

Recovery validates manifest/checksum/version information and reconstructs only from a validated baseline plus a contiguous applicable segment chain. A Purge forces a recovery barrier so local recovery does not resurrect purged Node-owned state.

## Concurrency

One `CoreState` serializes each Record and Index connection with mutexes. Canonical mutations are SQLite transactions. Separate connections/processes rely on SQLite WAL/locking and the configured busy timeout.

`with_stable_read_snapshot` holds one canonical read transaction for the callback. Calling canonical operations on the same `CoreState` from that callback can deadlock and is prohibited. Other connections may write; the snapshot continues to see its pinned state. Long snapshots can retain WAL pages and consume disk, so callers must bound their duration.

## Input-validation boundary

Core validates UUID version/shape, keys, typed Property carriers, definition state, relationship endpoints, lifecycle transitions, schema/recovery versions, opaque cursors, and protected-root metadata. Above-Core parsers, document formats, filenames, URLs, editor content, network payloads, and application authorization are caller responsibilities.

Cursor tokens are opaque continuation state, not signed capabilities. They contain no authorization and must not be used as proof that a caller may access a Store.

## Migration and recovery attack surface

Migration executes only compiled, version-selected SQL against the chosen local databases. It does not download or execute migration code. A malicious actor with filesystem write access can still replace databases, recovery files, binaries, or configuration; Core's validation detects many structural failures but does not defend against a fully privileged host attacker or guarantee forensic authenticity.

Before upgrading irreplaceable data, use a separate verified backup. Do not use live recovery material as the only backup or copy a Store while writes are in flight without a SQLite-safe backup method.

## Supply chain

The crate's direct runtime dependencies are listed in `Cargo.toml`; CI builds with this repository's lockfile and runs RustSec audit. Formatting, check, tests, doc tests, Clippy, and the headless example are verification gates.

Genesis Core has no npm dependency and executes no JavaScript. A relevant Rust advisory is fixed with the narrowest compatible update when possible; otherwise its reachability and disposition must be recorded before release.

## Explicit non-claims

Genesis Core does not claim to provide:

- encryption, key custody, authentication, authorization, sandboxing, or multi-tenant isolation;
- protection from a compromised process, administrator, kernel, filesystem, or physical host;
- secure deletion from SSDs, snapshots, backups, WAL pages, or third-party copies;
- network security, synchronization security, conflict-free distributed writes, or remote collaboration;
- denial-of-service resistance against an authorized caller supplying unbounded work;
- independent audit, formal verification, SOC 2, HIPAA, FIPS, or production/enterprise certification.

## Reporting

Do not publish exploit details in a public issue. Use GitHub's private security-advisory channel when available or contact the repository owner privately. Include the affected commit/version, impact, minimal reproduction, and mitigation if known.
