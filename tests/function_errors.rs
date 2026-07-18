use rust_rule_engine::{Facts, GRLParser, KnowledgeBase, RuleEngineError, RustRuleEngine, Value};

fn engine(grl: &str) -> RustRuleEngine {
    let knowledge_base = KnowledgeBase::new("function-errors");
    for rule in GRLParser::parse_rules(grl).unwrap() {
        knowledge_base.add_rule(rule).unwrap();
    }
    RustRuleEngine::new(knowledge_base)
}

#[test]
fn registered_function_errors_are_propagated() {
    let mut engine = engine(
        r#"
        rule "FunctionError" {
            when failing(Input.value) == true
            then Input.matched = true;
        }
        "#,
    );
    engine.register_function("failing", |_, _| {
        Err(RuleEngineError::EvaluationError {
            message: "function failed".to_string(),
        })
    });
    let facts = Facts::new();
    facts
        .add_value(
            "Input",
            Value::Object([("value".to_string(), Value::Integer(1))].into()),
        )
        .unwrap();

    let error = engine.execute(&facts).unwrap_err();
    assert!(error.to_string().contains("function failed"));
}

#[test]
fn missing_functions_are_errors() {
    let mut engine = engine(
        r#"
        rule "MissingFunction" {
            when missing(Input.value) == true
            then Input.matched = true;
        }
        "#,
    );
    let facts = Facts::new();
    facts
        .add_value(
            "Input",
            Value::Object([("value".to_string(), Value::Integer(1))].into()),
        )
        .unwrap();

    let error = engine.execute(&facts).unwrap_err();
    assert!(error.to_string().contains("missing"));
}
