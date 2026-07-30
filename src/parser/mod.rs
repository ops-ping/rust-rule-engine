/// GRL (Grule Rule Language) parser implementation
pub mod grl;

#[allow(dead_code)]
mod literal_search;

pub use grl::{GRLParser, GRLParser as GRLParserNoRegex};
