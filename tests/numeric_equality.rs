use rust_rule_engine::{Operator, Value};

#[test]
fn integer_and_number_values_compare_by_numeric_value() {
    assert!(Operator::Equal.evaluate(&Value::Integer(25), &Value::Number(25.0)));
    assert!(!Operator::NotEqual.evaluate(&Value::Number(25.0), &Value::Integer(25)));
    assert!(!Operator::Equal.evaluate(&Value::Integer(25), &Value::Number(25.5)));
}

#[test]
fn numeric_strings_remain_strings_for_equality() {
    assert!(!Operator::Equal.evaluate(&Value::String("25".into()), &Value::Integer(25)));
}
