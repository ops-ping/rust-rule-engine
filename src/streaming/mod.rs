//! Runtime-neutral streaming primitives and rule processing.
//!
//! [`StreamProcessor`] is the synchronous one-event-in/one-result-out core.
//! The `streaming` feature additionally exposes [`StreamRuleEngine`] as a Tokio
//! channel driver around the same processor.

pub mod aggregator;
#[cfg(feature = "streaming")]
pub mod engine;
pub mod event;
pub mod join_manager;
pub mod join_optimizer;
pub mod operators;
pub mod processor;
pub mod state;
pub mod watermark;
pub mod window;

pub use aggregator::{AggregationType, Aggregator};
#[cfg(feature = "streaming")]
pub use engine::{StreamAction, StreamExecutionResult, StreamRuleEngine};
pub use event::{EventMetadata, StreamEvent};
pub use join_manager::StreamJoinManager;
pub use join_optimizer::{JoinOptimization, JoinOptimizer, OptimizedJoinPlan, StreamStats};
pub use operators::{
    AggregateResult, Aggregation, Average, Count, CustomAggregator, DataStream, GroupedStream,
    KeyedStream, Max, Min, Sum, WindowConfig, WindowedStream,
};
pub use processor::{StreamConfig, StreamEventStatus, StreamProcessingResult, StreamProcessor};
pub use state::{
    CheckpointMetadata, StateBackend, StateConfig, StateStatistics, StateStore, StatefulOperator,
};
pub use watermark::{
    LateDataHandler, LateDataStats, LateDataStrategy, LateEventDecision, Watermark,
    WatermarkGenerator, WatermarkStrategy, WatermarkedEventStatus, WatermarkedStream,
};
pub use window::{TimeWindow, WindowManager, WindowStatistics, WindowType};
