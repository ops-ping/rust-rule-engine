use rust_rule_engine::{Facts, KnowledgeBase, RustRuleEngine, Value};

#[test]
fn test_nested_array_args_in_function_call() {
    let grl = r#"
    rule "TestNestedArray" no-loop {
        when
            test(v_sim(["v:add", ["v:sub", "king", "man"], User.woman], Concept.target))
        then
            Result.matched = true;
    }
    "#;

    let kb = KnowledgeBase::new("test");
    let rules = rust_rule_engine::GRLParser::parse_rules(grl).expect("rule parsing should succeed");
    assert_eq!(rules.len(), 1);

    for rule in rules {
        kb.add_rule(rule).unwrap();
    }

    let mut engine = RustRuleEngine::new(kb);

    // Register v_sim custom function that inspects its arguments
    engine.register_function("v_sim", |args: &[Value], _facts: &Facts| {
        assert_eq!(args.len(), 2);
        
        // Arg 0 should be Value::Array with 3 elements:
        // ["v:add", Value::Array(["v:sub", "king", "man"]), "woman"]
        let Value::Array(add_arr) = &args[0] else {
            panic!("Expected Arg 0 to be Value::Array, got {:?}", args[0]);
        };
        assert_eq!(add_arr.len(), 3);
        assert_eq!(add_arr[0], Value::String("v:add".to_string()));

        let Value::Array(sub_arr) = &add_arr[1] else {
            panic!("Expected sub_arr to be Value::Array, got {:?}", add_arr[1]);
        };
        assert_eq!(sub_arr.len(), 3);
        assert_eq!(sub_arr[0], Value::String("v:sub".to_string()));
        assert_eq!(sub_arr[1], Value::String("king".to_string()));
        assert_eq!(sub_arr[2], Value::String("man".to_string()));

        // Arg 0 element 2 was "User.woman", resolved from facts as "woman"
        assert_eq!(add_arr[2], Value::String("woman".to_string()));

        // Arg 1 was "Concept.target", resolved from facts as "queen"
        assert_eq!(args[1], Value::String("queen".to_string()));

        Ok(Value::Boolean(true))
    });

    let facts = Facts::new();
    let mut user = std::collections::HashMap::new();
    user.insert("woman".to_string(), Value::String("woman".to_string()));
    facts.add_value("User", Value::Object(user)).unwrap();

    let mut concept = std::collections::HashMap::new();
    concept.insert("target".to_string(), Value::String("queen".to_string()));
    facts.add_value("Concept", Value::Object(concept)).unwrap();

    let res = engine.execute(&facts).expect("execution should succeed");
    assert_eq!(res.rules_fired, 1);
}
