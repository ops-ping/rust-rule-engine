use rust_rule_engine::GRLParserNoRegex;

use std::sync::Arc;
use std::thread;

#[test]
fn parses_identical_grl_in_parallel() {
    let grl = Arc::new(
        r#"
        rule "VIPRule" {
            when
                User.Points >= 1000
            then
                User.IsVIP = true;
        }
        "#
        .to_string(),
    );

    let mut handles = Vec::new();
    for _ in 0..16 {
        let grl = Arc::clone(&grl);
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                let rules = GRLParserNoRegex::parse_rules(&grl).expect("parse should succeed");
                assert_eq!(rules.len(), 1);
                assert_eq!(rules[0].name, "VIPRule");
            }
        }));
    }

    for handle in handles {
        handle.join().expect("thread should complete");
    }
}

#[test]
fn parses_distinct_grl_in_parallel() {
    let inputs = vec![
        (
            "RuleA",
            r#"rule "RuleA" { when User.Age >= 18 then User.IsAdult = true; }"#,
        ),
        (
            "RuleB",
            r#"rule "RuleB" { when Order.Total > 100 then Order.Discount = 0.1; }"#,
        ),
        (
            "RuleC",
            r#"rule "RuleC" { when Device.Trusted == true then Session.Allowed = true; }"#,
        ),
        (
            "RuleD",
            r#"rule "RuleD" { when Request.Path == "/admin" then Request.NeedsMfa = true; }"#,
        ),
    ];

    let mut handles = Vec::new();
    for (expected_name, grl) in inputs {
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let rules = GRLParserNoRegex::parse_rules(grl).expect("parse should succeed");
                assert_eq!(rules.len(), 1);
                assert_eq!(rules[0].name, expected_name);
            }
        }));
    }

    for handle in handles {
        handle.join().expect("thread should complete");
    }
}
