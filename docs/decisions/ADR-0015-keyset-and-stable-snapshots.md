# ADR-0015: Keyset traversal and stable Record snapshots

Status: Accepted

Genesis Core separates two traversal contracts:

- opaque keyset cursors with revision watermarks for ordinary current-state paging;
- stable canonical Record snapshots for exact multi-page export, migration, backup preparation, and verification.

OFFSET remains a bounded compatibility API but is not the scalable stable traversal contract.
