# Quick Start

## Install

```toml
[dependencies]
rust-rule-engine = "1.20.3"
```

The default build provides GRL parsing, forward chaining, RETE, facts, custom
functions, and action handlers.

## Parse and execute GRL

```rust
use rust_rule_engine::{
    Facts, GRLParser, KnowledgeBase, RustRuleEngine, Value,
};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grl = r#"
        rule "AdultAccess" no-loop {
            when User.age >= 18
            then User.access = "granted";
        }
    "#;

    let knowledge_base = KnowledgeBase::new("access");
    for rule in GRLParser::parse_rules(grl)? {
        knowledge_base.add_rule(rule)?;
    }
    let mut engine = RustRuleEngine::new(knowledge_base);

    let facts = Facts::new();
    facts.add_value(
        "User",
        Value::Object(HashMap::from([
            ("age".to_string(), Value::Integer(25)),
        ])),
    )?;

    let result = engine.execute(&facts)?;
    assert_eq!(result.rules_fired, 1);
    assert_eq!(
        facts.get_nested("User.access"),
        Some(Value::String("granted".to_string()))
    );
    Ok(())
}
```

`GRLParser` is the canonical thread-safe parser. `RegexGRLParser` exposes the
legacy regex parser when explicit compatibility testing requires it.

## Register a function

```rust
engine.register_function("is_large", |args, _facts| {
    Ok(Value::Boolean(matches!(
        args.first(),
        Some(Value::Number(value)) if *value >= 50.0
    )))
});
```

Call the function from GRL:

```grl
rule "LargeReading" {
    when is_large(Reading.value) == true
    then Reading.large = true;
}
```

Missing functions and function errors return from `execute`.

## Enable proof or streaming

```toml
[dependencies.rust-rule-engine]
version = "1.20.3"
features = ["backward-chaining", "streaming-core"]
```

- `backward-chaining` exposes `BackwardEngine`.
- `streaming-core` exposes synchronous `StreamProcessor`.
- `streaming` adds the optional Tokio channel driver.
- `streaming-redis` adds the synchronous Redis state backend.

```rust
use rust_rule_engine::streaming::{StreamEvent, StreamProcessor};
use rust_rule_engine::Value;
use std::collections::HashMap;

# fn run() -> rust_rule_engine::Result<()> {
let mut processor = StreamProcessor::new();
processor.add_rule(
    r#"rule "Accept" { when Order.value > 0 then Order.accepted = true; }"#,
)?;
let result = processor.process_event(StreamEvent::new(
    "Order",
    HashMap::from([("value".to_string(), Value::Number(10.0))]),
    "orders",
))?;
assert!(result.accepted);
# Ok(())
# }
```

## Next steps

- [GRL syntax](../core-features/GRL_SYNTAX.md)
- [Streaming](../advanced-features/STREAMING.md)
- [Backward chaining](../BACKWARD_CHAINING_QUICK_START.md)
- [API reference](../api-reference/API_REFERENCE.md)
