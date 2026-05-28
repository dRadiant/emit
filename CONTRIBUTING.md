# Contributing

## Getting set up

Prerequisites: Zig 0.15.2. A synced Nethermind execution client is helpful for local development against real data but not required for the test suite.

```sh
git clone https://github.com/dradiant/emit
cd emit
zig build test --summary all
```

The test suite is hermetic; nothing reaches the network.

## Making changes

Branch from `main`. Rebase to stay current; the project keeps a linear history with no merge commits on `main`.

Commit messages use subsystem-prefix imperative mood:

- `engine: pin RocksDB read snapshot during import`
- `sdk: cache Multicall3 batch responses per block`
- `core, engine: harmonize atomic_file callers`

Valid prefixes: `core`, `engine`, `sdk`, `examples/erc20`, `examples/uniswap-v2`, `docs`, `build`, `ci`. Multiple subsystems comma-separated (`core, engine:`); whole-tree changes use `all:`. Subject line under 72 characters; body wrapped at 72.

PR titles follow the same prefix style. The PR body explains *what* changed and *why*. The code carries the *how*.

## Testing

`zig build test` runs the full suite. Bug fixes ship with a regression test. New features ship with coverage.

For performance-sensitive changes, include a measurement in the PR description (mean of three warm runs after `sync && echo 3 > /proc/sys/vm/drop_caches`).

## AI-assisted contributions

The PR author bears full responsibility:

- You vouch for code quality, test coverage, and license attribution. You are not absolved of review responsibility.
- All dependencies and copied snippets must be AGPL-3.0 compatible.

Disclosure is appreciated.

## Code review

PRs are reviewed within a few days when possible. CI must be green before merge. Squash-merge for single-purpose fixes; rebase-merge for feature PRs with multiple logical commits.

## License

EMIT is licensed under AGPL-3.0. Contributions are licensed under the same terms. For different terms (e.g. proprietary embedding), open an issue to discuss dual-licensing.

## Where to ask

GitHub Issues for bug reports and feature requests. Discussions for design questions and broader topics.
