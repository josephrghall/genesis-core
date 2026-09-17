# ADR-0001: Six-primitive boundary

Status: Accepted

Genesis Core consists of exactly Node, ID, Property, Relationship, Record, and Index.

Application entities and workflows are modeled above Core. Transactions, storage, migrations, recovery, and security are implementation infrastructure, not additional primitives.

This narrow boundary preserves domain neutrality and prevents consumer concepts from becoming engine prerequisites.
