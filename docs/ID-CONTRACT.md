# ID contract

Every Node has one permanent UUIDv7 ID.

## Guarantees

- IDs are created independently of title, path, content, type, hierarchy, or storage location.
- An issued ID is never reassigned.
- Tombstone preserves the ID and permits restore.
- Purge retires the ID permanently.
- Resolution distinguishes active, tombstoned, retired, and never-issued states.
- Creation inside an atomic batch rolls back ID issuance if the batch fails.

UUIDv7 timestamp bits support sortable identity generation; they are not editable metadata and must not be interpreted as consumer-domain meaning.

The issued-ID ledger is canonical Record state. Indexes may project identity information but cannot establish whether an ID was ever issued.
