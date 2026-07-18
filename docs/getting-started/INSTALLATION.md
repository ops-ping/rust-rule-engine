# Installation

rust-rule-engine uses Rust 2021 and builds with the stable Rust toolchain.

```sh
rustc --version
cargo --version
```

Install Rust through [rustup](https://rustup.rs/) when the toolchain is not
available.

## crates.io dependency

```toml
[dependencies]
rust-rule-engine = "1.20.3"
```

## Feature selection

| Feature | Adds | Runtime requirement |
|---|---|---|
| default | GRL, forward engine, RETE, facts, functions, actions | none |
| `backward-chaining` | `BackwardEngine` and proof/query APIs | none |
| `streaming-core` | synchronous `StreamProcessor` and streaming primitives | none |
| `streaming` | Tokio `StreamRuleEngine` channel driver | Tokio |
| `streaming-redis` | synchronous Redis `StateStore` backend | Redis server |

Features compose:

```toml
[dependencies.rust-rule-engine]
version = "1.20.3"
features = ["backward-chaining", "streaming", "streaming-redis"]
```

`streaming` and `streaming-redis` each include `streaming-core`.
`streaming-redis` does not enable Tokio.

## Git dependency

```toml
[dependencies]
rust-rule-engine = {
    git = "https://github.com/KSD-CO/rust-rule-engine",
    branch = "main"
}
```

Pin a commit with `rev` when reproducible builds require an exact Git revision.

## Verify

Create a project and compile the selected features:

```sh
cargo new rule-engine-example
cd rule-engine-example
cargo check
```

For a repository checkout:

```sh
cargo check --no-default-features
cargo check --no-default-features --features backward-chaining
cargo check --no-default-features --features streaming-core
cargo check --no-default-features --features streaming
cargo check --no-default-features --features streaming-redis
```

## Redis

The Redis backend uses the URL supplied in `StateBackend::Redis`. Redis
installation, authentication, TLS, persistence, replication, and backup remain
deployment concerns. The library does not silently fall back to memory when a
result-returning Redis operation fails.

No direct `redis` or `tokio` dependency is required in an application that only
uses synchronous `streaming-redis`.

## WebAssembly

Use `streaming-core` for runtime-neutral streaming builds. The Tokio driver and
synchronous Redis client depend on host capabilities and are not required by
the core parser or forward engine.

## Related documentation

- [Quick start](QUICK_START.md)
- [Streaming](../advanced-features/STREAMING.md)
- [Redis state](../advanced-features/REDIS_STATE_BACKEND.md)
- [GRL syntax](../core-features/GRL_SYNTAX.md)
