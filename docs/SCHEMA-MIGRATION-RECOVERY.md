# Schema, migration, Index rebuild, and recovery

## Current formats

| Store/material | Current version | Authority |
| --- | ---: | --- |
| Record Store | 5 | Canonical current Core truth |
| Index Store | 4 | Derived and disposable |
| Local Record recovery | 1 | Redundant recovery material |

Format versions are independent from the crate's Semantic Version.

## Record schema

The Record Store contains Store metadata/revision, Nodes, issued Node IDs, typed Properties, Relationships using permanent endpoint UUIDs, Property/Relationship definitions, and a bounded change journal used for derived reconciliation/recovery. `records.sqlite` is private storage implementation; consumers use the public API.

The change journal is operational machinery, not canonical authority or an unlimited event archive. The current retention policy keeps journal changes for the most recent 1,024 Record revisions and prunes older entries. Canonical current state, issued-ID history, and lifecycle semantics remain authoritative outside that journal window; reconciliation rebuilds from Record truth when a checkpoint can no longer be bridged.

Supported open paths are an empty Store, recognized legacy Record schemas 1–4, and current schema 5. Migrations are ordered and transactional. Pre-migration foreign-key violations, inconsistent metadata, non-empty unversioned databases, and unsupported versions reject the live Record-open path without deleting rows to make validation pass. Public `CoreState::open` can instead reconstruct from validated recovery material as described below; unsupported schema errors are returned directly. Schema 2/3 legacy capability tables may remain inert for above-Core import; Core neither reads nor recreates them.

Core does not promise downgrade writes. Back up valuable data before opening it with a newer engine.

## Index schema and rebuild

The Index Store contains projections, search material, graph/backlink inputs, and its applied Record revision. It can be deleted and rebuilt from canonical Records. An absent, corrupt, or incompatible Index is reconstructed when the Record Store can be validated. Losing only `index.sqlite` must not lose canonical Core facts.

Above-Core content-derived search/relationship inputs are projections supplied by a caller and may need to be resupplied after rebuild; they do not become canonical Core facts.

## Local recovery

Core maintains checksummed alternating baselines plus bounded revision segments under `.genesis/recovery/record`. Startup reconciles the live Record Store and recovery state. If `records.sqlite` is unavailable and recovery material validates, Core can reconstruct canonical state through the recoverable revision.

Recovery is intentionally local redundancy. It is not an independent backup, not remote replication, and not guaranteed to survive loss/compromise of the same root. A confirmed Purge establishes a recovery barrier and resets material where necessary so Node-owned purged state is not resurrected.

## Operational entry points

- `CoreState::open`: validate, migrate, recover if required, reconcile derived state.
- `status`: inspect schema, integrity, Index sync, and recovery state.
- `reconcile_index`: apply available canonical journal changes or rebuild when required.
- `rebuild_index`: force derived reconstruction.
- `reconcile_recovery`: advance/rebuild local recovery material.
- `with_stable_read_snapshot`: obtain a point-in-time canonical read for export/migration verification.

## Backup and restore guidance

For valuable data, use a SQLite-safe backup while the Store is quiescent or through the engine's stable snapshot/backup integration; do not copy a live WAL database as unrelated loose files and assume consistency. Keep backups outside the library root and test restoration against a disposable location. The embedded recovery directory must not be the only copy.

### Validation rejection versus reconstruction

`CoreState::open` first validates the live Record Store. `UnsupportedSchema` from that open is returned directly. For other Record-open errors, the current implementation attempts reconstruction if a recovery manifest exists; this catch-all does not distinguish transient operational database/I/O errors from recoverable corruption. The existence of a manifest is only an attempt condition, not proof that recovery can succeed. No reconstruction is attempted for failures outside this Record-open branch, such as the initial protected-boundary check.

Without a usable recovery path, invalid state is rejected. With a validated baseline and applicable contiguous segments, Core can promote a reconstructed candidate through the verified revision. It may be older than the last live commit: inspect recovery status, notes, and the recoverable revision. The original Record-open error is not separately returned when reconstruction succeeds.

Before promotion, existing canonical files are preserved as `.genesis/records.damaged-<generation>.sqlite` with any `-wal`/`-shm` companions. This is local diagnostic evidence, not an immutable archive (a same-generation destination can be replaced). Structural inconsistency rejection tests that remove recovery material prove the no-recovery branch; they do not establish unconditional rejection by public `open`. Do not equate successful reconstruction with proof that every damaged live fact was recovered. Recovery remains local redundancy, not an independent backup.
