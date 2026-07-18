# Streaming

rust-rule-engine provides a synchronous, runtime-neutral stream processor and
optional transport and state integrations. The core contract is one
`StreamEvent` input and one `StreamProcessingResult` output.

## Features

```toml
[dependencies]
rust-rule-engine = { version = "1.20.3", features = ["streaming-core"] }
```

| Feature | Capability |
|---|---|
| `streaming-core` | Synchronous `StreamProcessor`, events, windows, watermarks, joins, operators, and state |
| `streaming` | `streaming-core` plus the Tokio `StreamRuleEngine` channel driver |
| `streaming-redis` | `streaming-core` plus the synchronous Redis `StateStore` backend |

`streaming-redis` does not require Tokio. Enable `streaming` separately when an
async channel adapter is needed.

## Synchronous processing

```rust
use rust_rule_engine::streaming::{StreamEvent, StreamProcessor};
use rust_rule_engine::Value;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut processor = StreamProcessor::new();
    processor.add_rule(
        r#"
        rule "HighOrder" no-loop {
            when Order.value > 100
            then Order.flagged = true;
        }
        "#,
    )?;

    let event = StreamEvent::with_timestamp(
        "Order",
        HashMap::from([("value".to_string(), Value::Number(125.0))]),
        "orders",
        1_000,
    );
    let result = processor.process_event(event)?;

    assert!(result.accepted);
    assert_eq!(result.fired_rules, vec!["HighOrder"]);
    assert_eq!(
        result.facts.get_nested("Order.flagged"),
        Some(Value::Boolean(true))
    );
    Ok(())
}
```

`StreamProcessor` owns the canonical `RustRuleEngine`. Custom functions and
actions use the same registration APIs as ordinary forward evaluation:

```rust
processor.register_function("is_large", |args, _facts| {
    Ok(Value::Boolean(matches!(
        args.first(),
        Some(Value::Number(value)) if *value >= 50.0
    )))
});
```

Function and action errors propagate from `process_event`.

## Stream patterns

GRL stream patterns filter the current event and bind an alias:

```grl
rule "LoginAudit" no-loop {
    when login: LoginEvent from stream("logins")
    then login.audited = true;
}
```

The mapping is exact and case-sensitive:

- `stream("logins")` matches `StreamEvent.metadata.source == "logins"`.
- `LoginEvent` matches `StreamEvent.event_type == "LoginEvent"`.
- Omitting the event type accepts any event from the named source.
- `login` is bound to an object containing the payload plus `id`,
  `event_type`, `source`, `timestamp`, and `sequence`.
- A stream pattern does not match during non-stream engine execution because no
  current-event context exists.

The internal current-event context is removed from returned facts.

## Event facts and aggregates

An accepted event is available under both its event type and source names.
Numeric fields in the selected window also produce:

- `WindowEventCount`
- `WindowStartTime`
- `WindowEndTime`
- `WindowDurationMs`
- `<field>Sum`
- `<field>Average`
- `<field>Min`
- `<field>Max`

The complete result contains event disposition, fired rule names, final facts,
window counts, watermark, late-data statistics, analytics, canonical engine
metrics, and processing duration.

## Configuration

```rust
use rust_rule_engine::streaming::{
    LateDataStrategy, StreamConfig, StreamProcessor, WatermarkStrategy,
    WindowType,
};
use std::time::Duration;

let processor = StreamProcessor::with_config(StreamConfig {
    window_type: WindowType::Sliding,
    window_duration: Duration::from_secs(60),
    max_events_per_window: 1_000,
    max_windows: 100,
    max_state_sources: 100,
    watermark_strategy: WatermarkStrategy::BoundedOutOfOrder {
        max_delay: Duration::from_secs(5),
    },
    late_data_strategy: LateDataStrategy::Drop,
    ..StreamConfig::default()
});
```

### Window semantics

- **Sliding** retains events in
  `[greatest accepted event time - duration, greatest accepted event time]`.
  In-range out-of-order events remain in the moving window.
- **Tumbling** assigns events to fixed, aligned, non-overlapping buckets.
- **Session** groups events while inactivity gaps are less than or equal to the
  configured timeout. A larger gap starts another retained session.

Per-window event limits and active-window limits bound retained data.

### Watermarks and late data

Watermark strategies are periodic, bounded out-of-order, monotonic ascending,
or custom. Late-data strategies can drop, accept within a lateness bound, route
to side output, or accept through the `RecomputeWindows` path for normal window
insertion. The result reports whether the input was accepted, dropped, or
routed to side output.

## State

Every accepted event writes bounded operational state through the configured
`StateStore`: sequence, watermark, latest event by source, window counts, and
analytics. Memory and file backends are part of `streaming-core`; Redis is
available with `streaming-redis`.

```rust
processor.checkpoint("before-maintenance")?;
processor.restore_state("checkpoint_id")?;
```

Backend errors propagate from `process_event`. See
[Redis State Backend](REDIS_STATE_BACKEND.md) for Redis-specific behavior.

## Optional Tokio driver

`StreamRuleEngine` is a channel adapter around one `StreamProcessor`. It does
not define another evaluator or streaming semantics.

```rust
use rust_rule_engine::streaming::{StreamEvent, StreamRuleEngine};

# async fn run(event: StreamEvent) -> rust_rule_engine::Result<()> {
let mut driver = StreamRuleEngine::new();
driver.add_rule(
    r#"rule "Accept" { when Order.value > 0 then Order.accepted = true; }"#,
).await?;
driver.start().await?;
let result = driver.process_event(event).await?;
driver.stop().await;
# Ok(())
# }
```

The channel provides bounded submission and async response delivery. Delegated
events still pass sequentially through the same synchronous processor.

## Related documentation

- [Streaming Architecture](STREAMING_ARCHITECTURE.md)
- [Stream Operators](STREAM_OPERATORS.md)
- [Redis State Backend](REDIS_STATE_BACKEND.md)
