/// GRL (Grule Rule Language) parser implementation
pub mod grl;
/// GRL parser implementation without regex engine state
pub mod grl_no_regex;

#[allow(dead_code)]
mod literal_search;

pub use grl::GRLParser as RegexGRLParser;
pub use grl_no_regex::{GRLParserNoRegex, GRLParserNoRegex as GRLParser};
