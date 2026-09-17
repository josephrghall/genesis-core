# Contributing to Genesis Core

Genesis Core accepts focused contributions that preserve its headless, domain-neutral six-primitive boundary.

## Before opening a change

- describe the concrete defect, ambiguity, or capability gap;
- keep Node, ID, Property, Relationship, Record, and Index as the complete primitive set;
- preserve Record as canonical and Index as derived/rebuildable;
- keep application semantics and product ontology outside Core;
- include tests for contract or behavior changes.

For architecture-significant changes, add or amend an ADR in \`docs/decisions/\`.

## Local verification

Run from the repository root:

~~~bash
cargo fmt -- --check
cargo check --locked
cargo test --locked
cargo test --locked --doc
cargo clippy --locked --all-targets -- -D warnings
cargo run --locked --example headless
~~~

Security-sensitive changes should also run \`cargo audit\`.

## Pull requests

Keep pull requests bounded. State the affected contract, the verification performed, and any compatibility impact. Do not combine unrelated product or application work with Core changes.

Contributions are submitted under the Apache License 2.0 unless explicitly marked otherwise.
