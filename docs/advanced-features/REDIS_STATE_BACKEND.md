# Redis State Backend

The `streaming-redis` feature adds a synchronous Redis backend to `StateStore`.
It is optional and does not enable Tokio or the async stream driver.

## Enable the backend

```toml
[dependencies]
rust-rule-engine = { version = "1.20.3", features = ["streaming-redis"] }
```

`streaming-redis` includes `streaming-core`, so `StreamProcessor` is available
in the same build.

## Configure a processor

```rust
use rust_rule_engine::streaming::{
    StateBackend, StateConfig, StreamConfig, StreamProcessor,
};
use std::time::Duration;

let processor = StreamProcessor::with_config(StreamConfig {
    state: StateConfig {
        backend: StateBackend::Redis {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: "orders".to_string(),
        },
        auto_checkpoint: false,
        checkpoint_interval: Duration::from_secs(60),
        max_checkpoints: 10,
        enable_ttl: true,
        default_ttl: Duration::from_secs(3_600),
    },
    ..StreamConfig::default()
});
```

The key prefix namespaces every direct Redis key:

```text
orders:stream.sequence
orders:stream.watermark
orders:stream.latest_by_source
orders:stream.window
orders:stream.analytics
```

Choose a distinct prefix for each independent processor state partition.

## Direct StateStore use

```rust
use rust_rule_engine::streaming::{StateBackend, StateStore};
use rust_rule_engine::Value;

let mut store = StateStore::new(StateBackend::Redis {
    url: "redis://127.0.0.1:6379".to_string(),
    key_prefix: "example".to_string(),
});

store.put("counter", Value::Integer(1))?;
assert_eq!(store.get("counter")?, Some(Value::Integer(1)));
# Ok::<(), rust_rule_engine::RuleEngineError>(())
```

The Redis implementation supports synchronous `put`, `put_with_ttl`, `get`,
`update`, and `delete`. Values are serialized as JSON. TTL values use Redis
expiration.

## Processor behavior

After every accepted event, `StreamProcessor` reads the bounded latest-event map
and writes its operational state through `StateStore`. Redis connection,
serialization, read, and write failures return from `process_event`; the
processor does not switch to memory or report success.

The backend creates a Redis client from the configured URL and obtains a
synchronous connection for each operation. It does not provide connection
pooling, cluster routing, retries, or an async Redis client.

## Durability and checkpoints

Redis durability is controlled by the Redis server's RDB or AOF configuration.
`StateStore::checkpoint` records in-process checkpoint metadata for a Redis
backend; it does not create a Redis snapshot. `restore` assumes the configured
Redis dataset already contains the durable state and does not roll keys back to
an earlier checkpoint.

Use the file backend when the application requires library-managed snapshot
files and restoration by checkpoint ID.

## Error and inspection boundaries

The result-returning Redis operations surface backend errors. Convenience
inspection methods such as `contains` and `keys` have non-result return types
and therefore cannot expose detailed Redis errors; use `get` and explicit
application keys when error fidelity matters.

In-process statistics and checkpoint metadata describe the local
`StateStore` instance. They are not a Redis-wide key count or distributed
checkpoint catalog.

## Deployment requirements

- Supply a valid Redis URL through application configuration.
- Protect credentials with the host's secret-management mechanism; do not put
  them in GRL or source code.
- Configure authentication, TLS, persistence, replication, and backup in Redis.
- Assign stable key prefixes and state partitions before running multiple
  processor instances.
- Treat Redis availability as part of event-processing availability because
  backend failures propagate.

## Scope

The Redis backend is a state-storage option inherited by every
`StreamProcessor`. It does not change GRL, rule evaluation, window semantics, or
the synchronous one-event/one-result contract.
