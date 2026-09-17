# ADR-0012: Issued-ID ledger and retired identity

Status: Accepted

Canonical Record state includes an issued-ID ledger. Purge removes reconstructable Node-owned state but never makes an ID reusable.

Identity resolution distinguishes active, tombstoned, retired, and never-issued states. Atomic creation failure must not leave an issued-ID trace.
