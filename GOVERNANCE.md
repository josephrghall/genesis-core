# Governance

Genesis Core is maintainer-led during Developer Preview.

## Decision authority

The repository owner appoints maintainers and retains final authority for architecture, licensing, security response, and release approval. Maintainers review changes, enforce scope, and may delegate bounded areas.

## Architectural invariants

- exactly six primitives: Node, ID, Property, Relationship, Record, Index;
- Record is canonical;
- Index is derived and rebuildable;
- Node IDs are permanent and never reused;
- Core remains headless and domain-neutral;
- application ontology and product behavior remain above Core.

Architecture-significant changes require a written ADR or an explicit amendment to an existing ADR. A proposal must name the problem, alternatives, compatibility impact, migration implications, and verification.

## Releases

Maintainers may prepare release candidates, but publication, registry upload, and release tags require explicit owner approval. Security fixes may use an embargoed private process before coordinated disclosure.

## Conduct

Participation should remain constructive, evidence-based, and focused on the technical project. Harassment and personal attacks are not accepted.
