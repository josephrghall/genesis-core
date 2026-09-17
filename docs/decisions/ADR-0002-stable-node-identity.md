# ADR-0002: Stable Node identity

Status: Accepted

Node identity is a permanent UUIDv7 value independent of representation or meaning.

Titles, paths, content, type, hierarchy, and storage placement may change without changing identity. Issued IDs are never reused. Tombstone is reversible; purge retires the ID permanently.
