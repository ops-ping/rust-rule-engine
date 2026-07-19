use rust_rule_engine::types::{CostTier, FunctionMeta, ReturnKind};
use rust_rule_engine::{Facts, GRLParser, KnowledgeBase, RustRuleEngine, Value};

fn engine(grl: &str) -> RustRuleEngine {
    let knowledge_base = KnowledgeBase::new("function-meta");
    for rule in GRLParser::parse_rules(grl).unwrap() {
        knowledge_base.add_rule(rule).unwrap();
    }
    RustRuleEngine::new(knowledge_base)
}

fn constant(value: Value) -> impl Fn(&[Value], &Facts) -> rust_rule_engine::Result<Value> {
    move |_: &[Value], _: &Facts| Ok(value.clone())
}

#[test]
fn raw_scalar_threshold_is_a_violation() {
    let mut engine = engine(
        r#"
        rule "Raw" {
            when s_cosine(Input.text, "anchor") > 0.7
            then Decision.hit = true;
        }
        "#,
    );
    engine.register_function_with_meta(
        "s_cosine",
        FunctionMeta::hot(ReturnKind::RawScalar),
        constant(Value::Number(0.9)),
    );

    let violations = engine.validate_function_usage();
    assert_eq!(violations.len(), 1);
    assert_eq!(violations[0].function, "s_cosine");
    assert!(violations[0].message.contains("calibrate"));
    assert!(engine.validate_function_usage_strict().is_err());
}

#[test]
fn calibrated_scalar_threshold_is_allowed() {
    let mut engine = engine(
        r#"
        rule "Calibrated" {
            when c_project(Input.text, "urgency") > 95.0
            then Decision.hold = true;
        }
        "#,
    );
    engine.register_function_with_meta(
        "c_project",
        FunctionMeta::hot(ReturnKind::CalibratedScalar),
        constant(Value::Number(99.0)),
    );

    assert!(engine.validate_function_usage().is_empty());
    assert!(engine.validate_function_usage_strict().is_ok());
}

#[test]
fn boolean_ordering_is_a_violation() {
    let mut engine = engine(
        r#"
        rule "Bool" {
            when b_member(Input.text, "region") > 0.5
            then Decision.hit = true;
        }
        "#,
    );
    engine.register_function_with_meta(
        "b_member",
        FunctionMeta::hot(ReturnKind::Boolean),
        constant(Value::Boolean(true)),
    );

    let violations = engine.validate_function_usage();
    assert_eq!(violations.len(), 1);
    assert!(violations[0].message.contains("=="));
}

#[test]
fn boolean_equality_is_allowed() {
    let mut engine = engine(
        r#"
        rule "Bool" {
            when b_member(Input.text, "region") == true
            then Decision.hit = true;
        }
        "#,
    );
    engine.register_function_with_meta(
        "b_member",
        FunctionMeta::hot(ReturnKind::Boolean),
        constant(Value::Boolean(true)),
    );

    assert!(engine.validate_function_usage().is_empty());
}

#[test]
fn text_ordering_is_a_violation() {
    let mut engine = engine(
        r#"
        rule "Text" {
            when m_label(Input.text) > "a"
            then Decision.hit = true;
        }
        "#,
    );
    engine.register_function_with_meta(
        "m_label",
        FunctionMeta::hot(ReturnKind::Text),
        constant(Value::String("label".to_string())),
    );

    let violations = engine.validate_function_usage();
    assert_eq!(violations.len(), 1);
    assert!(violations[0].message.contains("ordering"));
}

#[test]
fn offline_tier_in_live_rule_is_a_violation() {
    let mut engine = engine(
        r#"
        rule "Offline" {
            when s_separation(Input.a, Input.b) == 1.0
            then Decision.hit = true;
        }
        "#,
    );
    engine.register_function_with_meta(
        "s_separation",
        FunctionMeta {
            return_kind: ReturnKind::CalibratedScalar,
            cost_tier: CostTier::Offline,
        },
        constant(Value::Number(1.0)),
    );

    let violations = engine.validate_function_usage();
    assert_eq!(violations.len(), 1);
    assert!(violations[0].message.contains("offline"));
}

#[test]
fn unregistered_when_function_is_a_violation() {
    let engine = engine(
        r#"
        rule "Missing" {
            when nope(Input.text) == true
            then Decision.hit = true;
        }
        "#,
    );
    let violations = engine.validate_function_usage();
    assert_eq!(violations.len(), 1);
    assert!(violations[0].message.contains("not registered"));
}

#[test]
fn functions_without_meta_are_unrestricted() {
    let mut engine = engine(
        r#"
        rule "Legacy" {
            when legacy_score(Input.text) > 0.5
            then Decision.hit = true;
        }
        "#,
    );
    engine.register_function("legacy_score", constant(Value::Number(0.9)));
    assert!(engine.validate_function_usage().is_empty());
}

#[test]
fn then_assignment_dispatches_registered_function() {
    let mut engine = engine(
        r#"
        rule "Assign" {
            when Order.total > 10.0
            then Decision.score = double_it(Order.total);
        }
        "#,
    );
    engine.register_function("double_it", |args: &[Value], _: &Facts| {
        let value = match args.first() {
            Some(Value::Number(n)) => *n,
            Some(Value::Integer(i)) => *i as f64,
            other => panic!("unexpected arg {other:?}"),
        };
        Ok(Value::Number(value * 2.0))
    });

    let facts = Facts::new();
    facts
        .add_value(
            "Order",
            Value::Object([("total".to_string(), Value::Number(21.0))].into()),
        )
        .unwrap();
    facts
        .add_value("Decision", Value::Object([].into()))
        .unwrap();

    engine.execute(&facts).unwrap();
    let decision = facts.get("Decision").unwrap();
    let Value::Object(decision) = decision else {
        panic!("Decision should be an object");
    };
    assert_eq!(decision.get("score"), Some(&Value::Number(42.0)));
}

#[test]
fn then_assignment_resolves_string_and_numeric_literals() {
    let mut engine = engine(
        r#"
        rule "Literals" {
            when Order.total > 10.0
            then Decision.tag = tag_of("high value", 3);
        }
        "#,
    );
    engine.register_function("tag_of", |args: &[Value], _: &Facts| {
        assert_eq!(args[0], Value::String("high value".to_string()));
        assert_eq!(args[1], Value::Integer(3));
        Ok(Value::String("hv3".to_string()))
    });

    let facts = Facts::new();
    facts
        .add_value(
            "Order",
            Value::Object([("total".to_string(), Value::Number(21.0))].into()),
        )
        .unwrap();
    facts
        .add_value("Decision", Value::Object([].into()))
        .unwrap();

    engine.execute(&facts).unwrap();
    let Some(Value::Object(decision)) = facts.get("Decision") else {
        panic!("Decision should be an object");
    };
    assert_eq!(decision.get("tag"), Some(&Value::String("hv3".to_string())));
}

#[test]
fn when_clause_string_literal_args_arrive_unquoted() {
    let mut engine = engine(
        r#"
        rule "Quoted" {
            when check(Input.text, "login") == true
            then Decision.hit = true;
        }
        "#,
    );
    engine.register_function("check", |args: &[Value], _: &Facts| {
        assert_eq!(args[1], Value::String("login".to_string()));
        Ok(Value::Boolean(true))
    });

    let facts = Facts::new();
    facts
        .add_value(
            "Input",
            Value::Object([("text".to_string(), Value::String("hello".into()))].into()),
        )
        .unwrap();
    facts
        .add_value("Decision", Value::Object([].into()))
        .unwrap();
    engine.execute(&facts).unwrap();
    let Some(Value::Object(decision)) = facts.get("Decision") else {
        panic!("Decision should be an object");
    };
    assert_eq!(decision.get("hit"), Some(&Value::Boolean(true)));
}

#[test]
fn custom_method_takes_precedence_and_mutates_facts() {
    let mut engine = engine(
        r#"
        rule "Method" {
            when Order.total > 10.0
            then $Order.flag("review");
        }
        "#,
    );
    engine.register_method(
        "flag",
        |object: &str, value: &Value, args: &[Value], facts: &Facts| {
            assert_eq!(object, "Order");
            assert!(matches!(value, Value::Object(_)));
            let label = match args.first() {
                Some(Value::String(label)) => label.clone(),
                other => panic!("unexpected arg {other:?}"),
            };
            facts
                .set_nested("Order.flagged", Value::String(label))
                .unwrap();
            Ok(Value::Boolean(true))
        },
    );

    let facts = Facts::new();
    facts
        .add_value(
            "Order",
            Value::Object([("total".to_string(), Value::Number(21.0))].into()),
        )
        .unwrap();

    engine.execute(&facts).unwrap();
    assert_eq!(
        facts.get_nested("Order.flagged"),
        Some(Value::String("review".to_string()))
    );
}
