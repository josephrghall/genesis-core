# ADR-0008: Protected Store security boundary

Status: Accepted

Canonical Record, derived Index, and local recovery artifacts operate inside a protected store directory.

Core validates path ownership and rejects symlink substitution before trusted access. Security checks fail closed rather than silently accepting an ambiguous store boundary.
