# ADR-0003: Record authority and rebuildable Index

Status: Accepted

Record is the canonical source of truth. Index is a derived query projection that may be deleted and reconstructed from Record.

No mutation is considered canonically committed merely because an Index write succeeded. Index loss must not cause information loss.
