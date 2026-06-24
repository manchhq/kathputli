default:
    @just --list

# fmt + clippy, both with warnings as errors
check:
    cargo fmt --check
    cargo clippy --all-targets --all-features -- -D warnings

# run the test suite with nextest (default + all features) plus doctests
# (nextest does not run doctests, so run them separately)
test:
    cargo nextest run
    cargo nextest run --all-features
    cargo test --doc --all-features

# build API docs the way docs.rs will (all features)
doc:
    cargo doc --all-features --no-deps --open

# everything CI runs
ci: check test

# cut a release: bump version, commit, tag vX.Y.Z, push — CI publishes to crates.io
# usage: just release patch|minor|major  (dry-run first with `just release-dry patch`)
release level:
    cargo release {{level}} --execute

release-dry level:
    cargo release {{level}}
