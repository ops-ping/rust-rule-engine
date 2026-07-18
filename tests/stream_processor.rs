#![cfg(feature = "streaming-core")]

use rust_rule_engine::streaming::{
    LateDataStrategy, StreamConfig, StreamEvent, StreamEventStatus, StreamProcessor,
    WatermarkStrategy, WindowType,
};
use rust_rule_engine::Value;

use std::collections::HashMap;
use std::time::Duration;

fn event(event_type: &str, source: &str, timestamp: u64, value: f64) -> StreamEvent {
    StreamEvent::with_timestamp(
        event_type,
        HashMap::from([("value".to_string(), Value::Number(value))]),
        source,
        timestamp,
    )
}

#[test]
fn sync_one_event_returns_fired_rule_and_final_facts() {
    let mut processor = StreamProcessor::new();
    processor
        .add_rule(
            r#"
            rule "HighOrder" no-loop {
                when Order.value > 100
                then Order.flagged = true;
            }
            "#,
        )
        .unwrap();

    let result = processor
        .process_event(event("Order", "orders", 1_000, 125.0))
        .unwrap();

    assert_eq!(result.status, StreamEventStatus::Accepted);
    assert!(result.accepted);
    assert_eq!(result.fired_rules, vec!["HighOrder"]);
    assert_eq!(
        result.facts.get_nested("Order.flagged"),
        Some(Value::Boolean(true))
    );
    assert_eq!(result.active_window_count, 1);
    assert_eq!(result.active_event_count, 1);

    let next = processor
        .process_event(event("Order", "orders", 2_000, 150.0))
        .unwrap();
    assert_eq!(next.fired_rules, vec!["HighOrder"]);
}

#[test]
fn window_aggregates_sequential_events_before_evaluation() {
    let config = StreamConfig {
        window_type: WindowType::Tumbling,
        window_duration: Duration::from_secs(60),
        ..StreamConfig::default()
    };
    let mut processor = StreamProcessor::with_config(config);
    processor
        .add_rule(
            r#"
            rule "WindowReady" no-loop {
                when WindowEventCount >= 2
                then Metric.ready = true;
            }
            "#,
        )
        .unwrap();

    let first = processor
        .process_event(event("Metric", "metrics", 1_000, 10.0))
        .unwrap();
    let second = processor
        .process_event(event("Metric", "metrics", 2_000, 20.0))
        .unwrap();

    assert!(first.fired_rules.is_empty());
    assert_eq!(second.fired_rules, vec!["WindowReady"]);
    assert_eq!(second.facts.get("valueSum"), Some(Value::Number(30.0)));
    assert_eq!(
        second.facts.get_nested("Metric.ready"),
        Some(Value::Boolean(true))
    );
    assert_eq!(second.active_event_count, 2);
}

#[test]
fn watermark_drops_late_event_without_evaluating_it() {
    let config = StreamConfig {
        watermark_strategy: WatermarkStrategy::MonotonicAscending,
        late_data_strategy: LateDataStrategy::Drop,
        ..StreamConfig::default()
    };
    let mut processor = StreamProcessor::with_config(config);

    let accepted = processor
        .process_event(event("Metric", "metrics", 2_000, 20.0))
        .unwrap();
    let dropped = processor
        .process_event(event("Metric", "metrics", 1_000, 10.0))
        .unwrap();

    assert!(accepted.accepted);
    assert_eq!(dropped.status, StreamEventStatus::Dropped);
    assert!(dropped.dropped);
    assert!(dropped.rule_execution.is_none());
    assert_eq!(dropped.active_event_count, 1);
    assert_eq!(dropped.watermark.timestamp, 2_000);
    assert_eq!(dropped.late_data_stats.total_late, 1);
    assert_eq!(dropped.late_data_stats.dropped, 1);
}

#[test]
fn registered_custom_function_is_used_by_stream_rules() {
    let mut processor = StreamProcessor::new();
    processor.register_function("is_large", |args, _| {
        Ok(Value::Boolean(matches!(
            args.first(),
            Some(Value::Number(value)) if *value >= 50.0
        )))
    });
    processor
        .add_rule(
            r#"
            rule "CustomFunctionRule" no-loop {
                when is_large(Reading.value) == true
                then Reading.matched = true;
            }
            "#,
        )
        .unwrap();

    let result = processor
        .process_event(event("Reading", "sensors", 1_000, 75.0))
        .unwrap();

    assert_eq!(result.fired_rules, vec!["CustomFunctionRule"]);
    assert_eq!(
        result.facts.get_nested("Reading.matched"),
        Some(Value::Boolean(true))
    );
}

#[test]
fn accepted_events_update_bounded_memory_state() {
    use rust_rule_engine::streaming::StateConfig;

    let config = StreamConfig {
        max_state_sources: 2,
        state: StateConfig {
            auto_checkpoint: true,
            checkpoint_interval: Duration::ZERO,
            ..StateConfig::default()
        },
        ..StreamConfig::default()
    };
    let mut processor = StreamProcessor::with_config(config);

    processor
        .process_event(event("Metric", "metrics", 1_000, 10.0))
        .unwrap();
    processor
        .process_event(event("Alert", "alerts", 2_000, 20.0))
        .unwrap();
    processor
        .process_event(event("Audit", "audits", 3_000, 30.0))
        .unwrap();

    let state = processor.state_store();
    assert_eq!(
        state.get(StreamProcessor::STATE_SEQUENCE_KEY).unwrap(),
        Some(Value::Integer(3))
    );
    assert_eq!(
        state.get(StreamProcessor::STATE_WATERMARK_KEY).unwrap(),
        Some(Value::Integer(3_000))
    );
    let latest = state
        .get(StreamProcessor::STATE_LATEST_BY_SOURCE_KEY)
        .unwrap()
        .unwrap();
    assert!(matches!(
        latest,
        Value::Object(events)
            if events.len() == 2
                && !events.contains_key("metrics")
                && events.get("alerts").and_then(|event| event.get_property("value"))
                    == Some(Value::Number(20.0))
                && events.get("audits").and_then(|event| event.get_property("value"))
                    == Some(Value::Number(30.0))
    ));
    assert!(matches!(
        state.get(StreamProcessor::STATE_WINDOW_KEY).unwrap(),
        Some(Value::Object(window))
            if window.get("active_windows") == Some(&Value::Integer(1))
                && window.get("active_events") == Some(&Value::Integer(3))
    ));
    assert!(matches!(
        state.get(StreamProcessor::STATE_ANALYTICS_KEY).unwrap(),
        Some(Value::Object(analytics))
            if analytics.get("late_events") == Some(&Value::Number(0.0))
    ));
    assert!(state.latest_checkpoint().is_some());
}

#[test]
fn state_backend_failure_is_returned_from_processing() {
    use rust_rule_engine::streaming::{StateBackend, StateConfig};

    let config = StreamConfig {
        state: StateConfig {
            backend: StateBackend::Custom {
                name: "unavailable".to_string(),
            },
            auto_checkpoint: true,
            checkpoint_interval: Duration::ZERO,
            ..StateConfig::default()
        },
        ..StreamConfig::default()
    };
    let mut processor = StreamProcessor::with_config(config);

    let error = processor
        .process_event(event("Metric", "metrics", 1_000, 10.0))
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Custom backend checkpointing not implemented"));
}

#[test]
fn stream_patterns_match_source_type_and_bind_aliases() {
    let mut processor = StreamProcessor::new();
    processor
        .add_rule(
            r#"
            rule "LoginStreamRule" no-loop {
                when login: LoginEvent from stream("logins")
                then login.matched = "login";
            }

            rule "PaymentStreamRule" no-loop {
                when payment: PaymentEvent from stream("payments")
                then payment.matched = "payment";
            }

            rule "AnyAuditEvent" no-loop {
                when audit: from stream("audit")
                then audit.alias_bound = true;
            }
            "#,
        )
        .unwrap();

    let login = processor
        .process_event(event("LoginEvent", "logins", 1_000, 1.0))
        .unwrap();
    assert_eq!(login.fired_rules, vec!["LoginStreamRule"]);
    assert_eq!(
        login.facts.get_nested("login.matched"),
        Some(Value::String("login".to_string()))
    );
    assert_eq!(
        login.facts.get_nested("login.event_type"),
        Some(Value::String("LoginEvent".to_string()))
    );

    let wrong_stream_and_type = processor
        .process_event(event("LoginEvent", "payments", 2_000, 2.0))
        .unwrap();
    assert!(wrong_stream_and_type.fired_rules.is_empty());
    assert!(!wrong_stream_and_type.facts.contains("login"));
    assert!(!wrong_stream_and_type.facts.contains("payment"));

    let payment = processor
        .process_event(event("PaymentEvent", "payments", 3_000, 3.0))
        .unwrap();
    assert_eq!(payment.fired_rules, vec!["PaymentStreamRule"]);
    assert_eq!(
        payment.facts.get_nested("payment.matched"),
        Some(Value::String("payment".to_string()))
    );

    let untyped = processor
        .process_event(event("AuditRecord", "audit", 4_000, 4.0))
        .unwrap();
    assert_eq!(untyped.fired_rules, vec!["AnyAuditEvent"]);
    assert_eq!(
        untyped.facts.get_nested("audit.alias_bound"),
        Some(Value::Boolean(true))
    );
}

#[cfg(feature = "streaming-redis")]
#[test]
fn redis_state_backend_is_constructed_without_connecting() {
    use rust_rule_engine::streaming::{StateBackend, StateConfig};

    let config = StreamConfig {
        state: StateConfig {
            backend: StateBackend::Redis {
                url: "redis://127.0.0.1:6379".to_string(),
                key_prefix: "stream-test".to_string(),
            },
            ..StateConfig::default()
        },
        ..StreamConfig::default()
    };

    let processor = StreamProcessor::with_config(config);
    assert!(processor.state_store().is_empty());
}

#[cfg(feature = "streaming-redis")]
#[test]
fn redis_state_error_propagates_without_a_server() {
    use rust_rule_engine::streaming::{StateBackend, StateConfig};

    let config = StreamConfig {
        state: StateConfig {
            backend: StateBackend::Redis {
                url: "invalid-redis-url".to_string(),
                key_prefix: "stream-test".to_string(),
            },
            ..StateConfig::default()
        },
        ..StreamConfig::default()
    };
    let mut processor = StreamProcessor::with_config(config);

    let error = processor
        .process_event(event("Metric", "metrics", 1_000, 10.0))
        .unwrap_err();
    assert!(error.to_string().contains("Redis client not initialized"));
}

#[cfg(feature = "streaming")]
#[tokio::test]
async fn async_driver_delegates_one_event_processing() {
    use rust_rule_engine::streaming::StreamRuleEngine;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let mut driver = StreamRuleEngine::new();
    let action_count = Arc::new(AtomicUsize::new(0));
    let observed_count = Arc::clone(&action_count);
    driver
        .register_action_handler("record", move |_| {
            observed_count.fetch_add(1, Ordering::SeqCst);
        })
        .await;
    driver
        .add_rule(
            r#"
            rule "AsyncDelegated" no-loop {
                when Order.value > 10
                then
                    Order.accepted = true;
                    record("accepted");
            }
            "#,
        )
        .await
        .unwrap();
    driver.start().await.unwrap();

    let result = driver
        .process_event(event("Order", "orders", 1_000, 25.0))
        .await
        .unwrap();
    let summary = driver.execute_rules().await.unwrap();

    assert_eq!(result.fired_rules, vec!["AsyncDelegated"]);
    assert_eq!(
        result.facts.get_nested("Order.accepted"),
        Some(Value::Boolean(true))
    );
    assert_eq!(summary.events_processed, 1);
    assert_eq!(summary.rules_fired, 1);
    assert_eq!(summary.actions.len(), 1);
    assert_eq!(action_count.load(Ordering::SeqCst), 1);
    driver.stop().await;
}
