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

# Change-risk: coverage + cargo-crap; fails any function above CRAP 30.
# Requires: cargo binstall cargo-llvm-cov cargo-crap. Args pass through (e.g. `just crap --format github`).
crap *args:
    cargo llvm-cov --all-features --lcov --output-path lcov.info
    cargo crap --lcov lcov.info --fail-above --threshold 30 {{args}}

# everything CI runs
ci: check test crap

# cut a release: bump version, commit, tag vX.Y.Z, push — CI publishes to crates.io
# usage: just release patch|minor|major  (dry-run first with `just release-dry patch`)
release level:
    cargo release {{level}} --execute

release-dry level:
    cargo release {{level}}
