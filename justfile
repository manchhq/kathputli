default:
    @just --list

# fmt + clippy, both with warnings as errors
check:
    cargo fmt --check
    cargo clippy --all-targets --all-features -- -D warnings

# run the test suite (default features + all features)
test:
    cargo test
    cargo test --all-features

# everything CI runs
ci: check test

# cut a release: bump version, commit, tag vX.Y.Z, push — CI publishes to crates.io
# usage: just release patch|minor|major  (dry-run first with `just release-dry patch`)
release level:
    cargo release {{level}} --execute

release-dry level:
    cargo release {{level}}
