## Summary

## Core contract affected

## Verification

- [ ] \`cargo fmt -- --check\`
- [ ] \`cargo check --locked\`
- [ ] \`cargo test --locked\`
- [ ] \`cargo test --locked --doc\`
- [ ] \`cargo clippy --locked --all-targets -- -D warnings\`
- [ ] \`cargo run --locked --example headless\`

## Boundary check

- [ ] The primitive set remains Node, ID, Property, Relationship, Record, and Index.
- [ ] Record remains canonical and Index remains derived/rebuildable.
- [ ] No application or domain semantics were added to Core.
