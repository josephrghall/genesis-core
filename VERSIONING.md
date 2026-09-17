# Compatibility and versioning

Genesis Core uses Semantic Versioning. The current \`0.7.0\` package is a Developer Preview checkpoint and has not been published as an independent registry release.

Before \`1.0.0\`, minor releases may make breaking public-API or on-disk changes when clearly documented and accompanied by deterministic migration or explicit rejection behavior.

## Compatibility surfaces

- the Rust public API exported by the crate;
- the six-primitive conceptual contract;
- canonical Record schema and migration path;
- protected-store and recovery behavior;
- cursor and stable-snapshot traversal semantics;
- portable reconstruction semantics.

Private SQLite tables, internal modules, derived Index layouts, recovery artifact layout, and undocumented implementation details are not stable public APIs.

## Required change handling

- additive compatible changes require tests and documentation;
- breaking API changes require a minor-version increment during Developer Preview;
- canonical schema changes require deterministic migration, downgrade rejection, and recovery tests;
- traversal or lifecycle semantic changes require an ADR;
- removing issued-ID permanence, making Index canonical, or adding a seventh primitive is an architectural redefinition rather than an ordinary compatible change.

Release tags and registry publication require separate owner authorization.
