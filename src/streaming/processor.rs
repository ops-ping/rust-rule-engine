//! Synchronous streaming rule processing.

use crate::engine::facts::Facts;
use crate::engine::knowledge_base::KnowledgeBase;
use crate::engine::rule::{
    STREAM_EVENT_CONTEXT_FACT, STREAM_EVENT_CONTEXT_SOURCE, STREAM_EVENT_CONTEXT_TYPE,
    STREAM_EVENT_CONTEXT_VALUE,
};
use crate::engine::{GruleExecutionResult, RustRuleEngine};
use crate::parser::GRLParser;
use crate::streaming::aggregator::StreamAnalytics;
use crate::streaming::event::StreamEvent;
use crate::streaming::join_manager::StreamJoinManager;
use crate::streaming::state::{StateConfig, StateStore};
use crate::streaming::watermark::{
    LateDataStats, LateDataStrategy, Watermark, WatermarkStrategy, WatermarkedEventStatus,
    WatermarkedStream,
};
use crate::streaming::window::{TimeWindow, WindowManager, WindowStatistics, WindowType};
use crate::types::Value;
use crate::Result;

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::{Duration, Instant};

/// Configuration shared by the synchronous processor and async driver.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Buffer size used by the optional async driver.
    pub buffer_size: usize,
    /// Window duration for aggregations.
    pub window_duration: Duration,
    /// Maximum events retained per window.
    pub max_events_per_window: usize,
    /// Maximum number of active windows.
    pub max_windows: usize,
    /// Maximum number of sources retained in operational latest-event state.
    pub max_state_sources: usize,
    /// Window assignment strategy.
    pub window_type: WindowType,
    /// Cache TTL for stream analytics.
    pub analytics_cache_ttl_ms: u64,
    /// Poll interval used by the optional async driver.
    pub processing_interval: Duration,
    /// Event-time watermark strategy.
    pub watermark_strategy: WatermarkStrategy,
    /// Policy for events arriving behind the watermark.
    pub late_data_strategy: LateDataStrategy,
    /// State backend and checkpoint configuration.
    pub state: StateConfig,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10_000,
            window_duration: Duration::from_secs(60),
            max_events_per_window: 1_000,
            max_windows: 100,
            max_state_sources: 100,
            window_type: WindowType::Sliding,
            analytics_cache_ttl_ms: 30_000,
            processing_interval: Duration::from_millis(100),
            watermark_strategy: WatermarkStrategy::BoundedOutOfOrder {
                max_delay: Duration::ZERO,
            },
            late_data_strategy: LateDataStrategy::Drop,
            state: StateConfig::default(),
        }
    }
}

/// Downstream disposition of one input event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamEventStatus {
    /// The event passed watermark handling and was evaluated.
    Accepted,
    /// The event was dropped by the late-data policy.
    Dropped,
    /// The event was retained in the late-data side output.
    SideOutput,
}

/// Complete result produced for one input event.
#[derive(Debug, Clone)]
pub struct StreamProcessingResult {
    /// Identifier of the input event.
    pub event_id: String,
    /// Downstream disposition.
    pub status: StreamEventStatus,
    /// Whether the event was evaluated by the rule engine.
    pub accepted: bool,
    /// Whether the event was discarded.
    pub dropped: bool,
    /// Rule names reported by the canonical rule engine.
    pub fired_rules: Vec<String>,
    /// Facts after rule actions have completed.
    pub facts: Facts,
    /// Number of active windows after processing.
    pub active_window_count: usize,
    /// Number of events across active windows.
    pub active_event_count: usize,
    /// Current event-time watermark.
    pub watermark: Watermark,
    /// Cumulative late-data statistics.
    pub late_data_stats: LateDataStats,
    /// Current stream analytics.
    pub analytics: HashMap<String, Value>,
    /// Canonical rule-engine execution metrics, when evaluated.
    pub rule_execution: Option<GruleExecutionResult>,
    /// End-to-end synchronous processing time.
    pub processing_time: Duration,
}

/// Runtime-neutral, deterministic processor for one event at a time.
pub struct StreamProcessor {
    config: StreamConfig,
    rule_engine: RustRuleEngine,
    window_manager: WindowManager,
    watermarked_stream: WatermarkedStream,
    state_store: StateStore,
    join_manager: StreamJoinManager,
    analytics: StreamAnalytics,
    next_sequence: u64,
}

impl StreamProcessor {
    /// State key containing the sequence of the latest accepted event.
    pub const STATE_SEQUENCE_KEY: &'static str = "stream.sequence";
    /// State key containing the current watermark.
    pub const STATE_WATERMARK_KEY: &'static str = "stream.watermark";
    /// State key containing a bounded map of latest accepted events by source.
    pub const STATE_LATEST_BY_SOURCE_KEY: &'static str = "stream.latest_by_source";
    /// State key containing current window counts.
    pub const STATE_WINDOW_KEY: &'static str = "stream.window";
    /// State key containing current late-data and stream analytics.
    pub const STATE_ANALYTICS_KEY: &'static str = "stream.analytics";

    /// Create a processor with the default stream configuration.
    pub fn new() -> Self {
        Self::with_config(StreamConfig::default())
    }

    /// Create a processor from reusable stream configuration.
    pub fn with_config(config: StreamConfig) -> Self {
        let rule_engine = RustRuleEngine::new(KnowledgeBase::new("StreamKB"));
        Self::with_engine(config, rule_engine)
    }

    /// Create a processor around an existing canonical rule engine.
    pub fn with_engine(config: StreamConfig, rule_engine: RustRuleEngine) -> Self {
        let state_store = StateStore::with_config(config.state.clone());
        Self::with_components(config, rule_engine, state_store, StreamJoinManager::new())
    }

    /// Create a processor from existing engine, state, and join components.
    pub fn with_components(
        config: StreamConfig,
        rule_engine: RustRuleEngine,
        state_store: StateStore,
        join_manager: StreamJoinManager,
    ) -> Self {
        let window_manager = WindowManager::new(
            config.window_type.clone(),
            config.window_duration,
            config.max_events_per_window,
            config.max_windows,
        );
        let watermarked_stream = WatermarkedStream::new(
            config.watermark_strategy.clone(),
            config.late_data_strategy.clone(),
        );
        let analytics = StreamAnalytics::new(config.analytics_cache_ttl_ms);

        Self {
            config,
            rule_engine,
            window_manager,
            watermarked_stream,
            state_store,
            join_manager,
            analytics,
            next_sequence: 1,
        }
    }

    /// Parse and add one or more GRL rules with the canonical parser.
    pub fn add_rule(&mut self, grl: &str) -> Result<()> {
        for rule in GRLParser::parse_rules(grl)? {
            self.rule_engine.knowledge_base_mut().add_rule(rule)?;
        }
        Ok(())
    }

    /// Read, parse, and add GRL rules from a file.
    pub fn add_rule_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let grl = std::fs::read_to_string(path)?;
        self.add_rule(&grl)
    }

    /// Register a canonical rule-engine custom function.
    pub fn register_function<F>(&mut self, name: &str, function: F)
    where
        F: Fn(&[Value], &Facts) -> Result<Value> + Send + Sync + 'static,
    {
        self.rule_engine.register_function(name, function);
    }

    /// Register a canonical rule-engine custom action handler.
    pub fn register_action_handler<F>(&mut self, action_type: &str, handler: F)
    where
        F: Fn(&HashMap<String, Value>, &Facts) -> Result<()> + Send + Sync + 'static,
    {
        self.rule_engine
            .register_action_handler(action_type, handler);
    }

    /// Process exactly one event synchronously.
    pub fn process_event(&mut self, mut event: StreamEvent) -> Result<StreamProcessingResult> {
        let started = Instant::now();
        let event_id = event.id.clone();
        event.metadata.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);

        let watermark_status = self.watermarked_stream.add_event(event.clone())?;
        let status = match watermark_status {
            WatermarkedEventStatus::Accepted => StreamEventStatus::Accepted,
            WatermarkedEventStatus::Dropped => StreamEventStatus::Dropped,
            WatermarkedEventStatus::SideOutput => StreamEventStatus::SideOutput,
        };
        let accepted = status == StreamEventStatus::Accepted;
        let facts = Facts::new();
        let mut fired_rules = Vec::new();
        let mut rule_execution = None;

        if accepted {
            self.window_manager.process_event(event.clone());
            self.join_manager.process_event(event.clone());
            self.join_manager.update_watermark(
                &event.metadata.source,
                self.watermarked_stream.current_watermark().timestamp as i64,
            );
            self.persist_operational_state(&event)?;

            self.add_event_facts(&facts, &event)?;
            if let Some(window) = self.window_for_event(&event) {
                Self::add_window_aggregations_to_facts(&facts, window)?;
            }

            self.rule_engine.reset_no_loop_tracking();
            let execution = self
                .rule_engine
                .execute_with_callback(&facts, |name, _| fired_rules.push(name.to_string()));
            facts.remove(STREAM_EVENT_CONTEXT_FACT);
            rule_execution = Some(execution?);
        }

        let analytics = self.current_analytics();
        Ok(StreamProcessingResult {
            event_id,
            status,
            accepted,
            dropped: status == StreamEventStatus::Dropped,
            fired_rules,
            facts,
            active_window_count: self.window_manager.active_windows().len(),
            active_event_count: self.window_manager.total_event_count(),
            watermark: self.watermarked_stream.current_watermark(),
            late_data_stats: self.watermarked_stream.late_stats(),
            analytics,
            rule_execution,
            processing_time: started.elapsed(),
        })
    }

    /// Return stream configuration.
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    /// Return current window statistics.
    pub fn window_statistics(&self) -> WindowStatistics {
        self.window_manager.get_statistics()
    }

    /// Return analytics for a numeric field across active windows.
    pub fn field_analytics(&self, field: &str) -> HashMap<String, Value> {
        let windows = self.window_manager.active_windows();
        let mut results = HashMap::new();
        let values: Vec<f64> = windows
            .iter()
            .flat_map(|window| {
                window
                    .events()
                    .iter()
                    .filter_map(|event| event.get_numeric(field))
            })
            .collect();

        let total_sum = values.iter().sum::<f64>();
        results.insert("total_sum".to_string(), Value::Number(total_sum));
        results.insert(
            "total_count".to_string(),
            Value::Number(values.len() as f64),
        );
        if !values.is_empty() {
            results.insert(
                "overall_average".to_string(),
                Value::Number(total_sum / values.len() as f64),
            );
            results.insert(
                "global_min".to_string(),
                Value::Number(values.iter().fold(f64::INFINITY, |a, value| a.min(*value))),
            );
            results.insert(
                "global_max".to_string(),
                Value::Number(
                    values
                        .iter()
                        .fold(f64::NEG_INFINITY, |a, value| a.max(*value)),
                ),
            );
        }
        results
    }

    /// Access the canonical rule engine.
    pub fn rule_engine(&self) -> &RustRuleEngine {
        &self.rule_engine
    }

    /// Mutably access the canonical rule engine.
    pub fn rule_engine_mut(&mut self) -> &mut RustRuleEngine {
        &mut self.rule_engine
    }

    /// Access the window manager.
    pub fn window_manager(&self) -> &WindowManager {
        &self.window_manager
    }

    /// Mutably access the window manager.
    pub fn window_manager_mut(&mut self) -> &mut WindowManager {
        &mut self.window_manager
    }

    /// Access the watermarked stream.
    pub fn watermarked_stream(&self) -> &WatermarkedStream {
        &self.watermarked_stream
    }

    /// Mutably access the watermarked stream.
    pub fn watermarked_stream_mut(&mut self) -> &mut WatermarkedStream {
        &mut self.watermarked_stream
    }

    /// Access the configured state store.
    pub fn state_store(&self) -> &StateStore {
        &self.state_store
    }

    /// Mutably access the configured state store.
    pub fn state_store_mut(&mut self) -> &mut StateStore {
        &mut self.state_store
    }

    /// Create a named checkpoint through the configured state backend.
    pub fn checkpoint(&mut self, name: impl Into<String>) -> Result<String> {
        self.state_store.checkpoint(name)
    }

    /// Restore a state checkpoint through the configured state backend.
    pub fn restore_state(&mut self, checkpoint_id: &str) -> Result<()> {
        self.state_store.restore(checkpoint_id)?;
        if let Some(sequence) = self.state_store.get(Self::STATE_SEQUENCE_KEY)? {
            self.next_sequence = match sequence {
                Value::Integer(sequence) if sequence >= 0 => (sequence as u64).saturating_add(1),
                Value::Number(sequence) if sequence >= 0.0 => (sequence as u64).saturating_add(1),
                actual => {
                    return Err(crate::RuleEngineError::TypeMismatch {
                        expected: "non-negative stream sequence".to_string(),
                        actual: format!("{actual:?}"),
                    });
                }
            };
        }
        Ok(())
    }

    /// Access the stream join manager.
    pub fn join_manager(&self) -> &StreamJoinManager {
        &self.join_manager
    }

    /// Mutably access the stream join manager.
    pub fn join_manager_mut(&mut self) -> &mut StreamJoinManager {
        &mut self.join_manager
    }

    /// Access stream analytics helpers.
    pub fn analytics(&self) -> &StreamAnalytics {
        &self.analytics
    }

    /// Mutably access stream analytics helpers.
    pub fn analytics_mut(&mut self) -> &mut StreamAnalytics {
        &mut self.analytics
    }

    fn add_event_facts(&self, facts: &Facts, event: &StreamEvent) -> Result<()> {
        let event_fact = Self::event_value(event);
        facts.add_value(
            STREAM_EVENT_CONTEXT_FACT,
            Value::Object(HashMap::from([
                (
                    STREAM_EVENT_CONTEXT_SOURCE.to_string(),
                    Value::String(event.metadata.source.clone()),
                ),
                (
                    STREAM_EVENT_CONTEXT_TYPE.to_string(),
                    Value::String(event.event_type.clone()),
                ),
                (STREAM_EVENT_CONTEXT_VALUE.to_string(), event_fact.clone()),
            ])),
        )?;
        facts.add_value(&event.event_type, event_fact.clone())?;
        facts.add_value(&event.metadata.source, event_fact)?;
        Ok(())
    }

    fn event_value(event: &StreamEvent) -> Value {
        let mut object = event.data.clone();
        object.insert("id".to_string(), Value::String(event.id.clone()));
        object.insert(
            "event_type".to_string(),
            Value::String(event.event_type.clone()),
        );
        object.insert(
            "source".to_string(),
            Value::String(event.metadata.source.clone()),
        );
        object.insert(
            "timestamp".to_string(),
            Value::Integer(event.metadata.timestamp as i64),
        );
        object.insert(
            "sequence".to_string(),
            Value::Integer(event.metadata.sequence as i64),
        );
        Value::Object(object)
    }

    fn persist_operational_state(&mut self, event: &StreamEvent) -> Result<()> {
        let mut latest_by_source = match self.state_store.get(Self::STATE_LATEST_BY_SOURCE_KEY)? {
            Some(Value::Object(events)) => events,
            None => HashMap::new(),
            Some(actual) => {
                return Err(crate::RuleEngineError::TypeMismatch {
                    expected: "stream latest-by-source object".to_string(),
                    actual: format!("{actual:?}"),
                });
            }
        };
        latest_by_source.insert(event.metadata.source.clone(), Self::event_value(event));

        let source_limit = self.config.max_state_sources.max(1);
        while latest_by_source.len() > source_limit {
            let oldest_source = latest_by_source
                .iter()
                .min_by(|(left_source, left), (right_source, right)| {
                    Self::event_sequence(left)
                        .cmp(&Self::event_sequence(right))
                        .then_with(|| left_source.cmp(right_source))
                })
                .map(|(source, _)| source.clone());
            if let Some(source) = oldest_source {
                latest_by_source.remove(&source);
            }
        }

        let window_stats = self.window_manager.get_statistics();
        let window_state = Value::Object(HashMap::from([
            (
                "active_windows".to_string(),
                Value::Integer(window_stats.total_windows as i64),
            ),
            (
                "active_events".to_string(),
                Value::Integer(window_stats.total_events as i64),
            ),
            (
                "oldest_window_start".to_string(),
                window_stats
                    .oldest_window_start
                    .map(|timestamp| Value::Integer(timestamp as i64))
                    .unwrap_or(Value::Null),
            ),
            (
                "newest_window_start".to_string(),
                window_stats
                    .newest_window_start
                    .map(|timestamp| Value::Integer(timestamp as i64))
                    .unwrap_or(Value::Null),
            ),
        ]));
        let analytics = Value::Object(self.current_analytics());

        self.state_store.put(
            Self::STATE_SEQUENCE_KEY,
            Value::Integer(event.metadata.sequence as i64),
        )?;
        self.state_store.put(
            Self::STATE_WATERMARK_KEY,
            Value::Integer(self.watermarked_stream.current_watermark().timestamp as i64),
        )?;
        self.state_store.put(
            Self::STATE_LATEST_BY_SOURCE_KEY,
            Value::Object(latest_by_source),
        )?;
        self.state_store.put(Self::STATE_WINDOW_KEY, window_state)?;
        self.state_store.put(Self::STATE_ANALYTICS_KEY, analytics)?;
        self.state_store.checkpoint_if_due("stream-auto")?;
        Ok(())
    }

    fn event_sequence(event: &Value) -> i64 {
        match event {
            Value::Object(event) => match event.get("sequence") {
                Some(Value::Integer(sequence)) => *sequence,
                _ => i64::MIN,
            },
            _ => i64::MIN,
        }
    }

    fn window_for_event(&self, event: &StreamEvent) -> Option<&TimeWindow> {
        self.window_manager
            .active_windows()
            .iter()
            .find(|window| window.contains_timestamp(event.metadata.timestamp))
            .or_else(|| self.window_manager.latest_window())
    }

    fn add_window_aggregations_to_facts(facts: &Facts, window: &TimeWindow) -> Result<()> {
        facts.add_value("WindowEventCount", Value::Number(window.count() as f64))?;
        facts.add_value("WindowStartTime", Value::Number(window.start_time as f64))?;
        facts.add_value("WindowEndTime", Value::Number(window.end_time as f64))?;
        facts.add_value(
            "WindowDurationMs",
            Value::Number(window.duration_ms() as f64),
        )?;

        let numeric_fields: BTreeSet<String> = window
            .events()
            .iter()
            .flat_map(|event| event.data.iter())
            .filter(|(_, value)| matches!(value, Value::Number(_) | Value::Integer(_)))
            .map(|(field, _)| field.clone())
            .collect();

        for field in numeric_fields {
            facts.add_value(&format!("{field}Sum"), Value::Number(window.sum(&field)))?;
            if let Some(average) = window.average(&field) {
                facts.add_value(&format!("{field}Average"), Value::Number(average))?;
            }
            if let Some(minimum) = window.min(&field) {
                facts.add_value(&format!("{field}Min"), Value::Number(minimum))?;
            }
            if let Some(maximum) = window.max(&field) {
                facts.add_value(&format!("{field}Max"), Value::Number(maximum))?;
            }
        }
        Ok(())
    }

    fn current_analytics(&self) -> HashMap<String, Value> {
        let late = self.watermarked_stream.late_stats();
        HashMap::from([
            (
                "total_events".to_string(),
                Value::Number(self.window_manager.total_event_count() as f64),
            ),
            (
                "window_count".to_string(),
                Value::Number(self.window_manager.active_windows().len() as f64),
            ),
            (
                "watermark".to_string(),
                Value::Number(self.watermarked_stream.current_watermark().timestamp as f64),
            ),
            (
                "late_events".to_string(),
                Value::Number(late.total_late as f64),
            ),
            (
                "dropped_events".to_string(),
                Value::Number(late.dropped as f64),
            ),
            (
                "allowed_late_events".to_string(),
                Value::Number(late.allowed as f64),
            ),
            (
                "side_output_events".to_string(),
                Value::Number(late.side_output as f64),
            ),
        ])
    }
}

impl Default for StreamProcessor {
    fn default() -> Self {
        Self::new()
    }
}
