# ADR-0013: Surviving facts and identity history

Status: Accepted

Purge removes facts owned by the purged Node. Independently owned Relationships or facts may survive when their meaning and authority do not depend on reconstructing that Node.

Permanence applies to the canonical identity, fact, ownership, and lifecycle contract: an issued ID is never reused, retired identity remains distinguishable from never-issued identity, and surviving facts continue under their own contracts. Current-state queries still reflect lifecycle and removal semantics.

This decision does not require an unlimited append-only operational event log. Reconciliation journal entries are implementation machinery, are retained only within the bounded journal policy, and may be pruned without weakening the canonical identity or surviving-fact guarantees.
