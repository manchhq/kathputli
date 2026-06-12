# kathputli

**कठपुतली** — Hindi for "marionette." A minimal, typed-mailbox actor framework over Tokio: actors respond to messages like puppets respond to strings.

Part of [Manch](https://github.com/manchhq) — *Katha records, Kathputli performs, Manch presents.*

The crate lives in [`kathputli/`](kathputli/) — see its [README](kathputli/README.md) for the quick start and API tour.

## Releasing

`just release patch|minor|major` (via [cargo-release](https://github.com/crate-ci/cargo-release)) bumps the version, commits, tags `vX.Y.Z`, and pushes; the [release workflow](.github/workflows/release.yml) then tests and publishes to crates.io using the `CARGO_REGISTRY_TOKEN` repository secret.

## License

MIT OR Apache-2.0.
