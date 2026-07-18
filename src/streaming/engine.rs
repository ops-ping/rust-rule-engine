//! Optional Tokio driver for the synchronous stream processor.

#![allow(clippy::type_complexity)]

use crate::engine::facts::Facts;
use crate::streaming::event::StreamEvent;
use crate::streaming::processor::{StreamConfig, StreamProcessingResult, StreamProcessor};
use crate::streaming::window::WindowStatistics;
use crate::types::Value;
use crate::{Result, RuleEngineError};

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, RwLock};
use tokio::time::interval;

type EventRequest = (StreamEvent, oneshot::Sender<Result<StreamProcessingResult>>);

/// Aggregated results received through the async driver since the last read.
#[derive(Debug, Clone)]
pub struct StreamExecutionResult {
    /// Number of canonical rule-engine firings.
    pub rules_fired: usize,
    /// Number of accepted events processed.
    pub events_processed: usize,
    /// Total processing duration.
    pub processing_time_ms: u64,
    /// Actions observed by compatibility action handlers.
    pub actions: Vec<StreamAction>,
    /// Analytics from the latest result.
    pub analytics: HashMap<String, Value>,
}

/// Compatibility action passed to async stream action callbacks.
#[derive(Debug, Clone)]
pub struct StreamAction {
    /// Action type identifier.
    pub action_type: String,
    /// Canonical action parameters.
    pub parameters: HashMap<String, Value>,
    /// Timestamp when the handler ran.
    pub timestamp: u64,
    /// Rule name, when available from the rule engine.
    pub rule_name: String,
}

/// Tokio channel driver around [`StreamProcessor`].
pub struct StreamRuleEngine {
    config: StreamConfig,
    processor: Arc<Mutex<StreamProcessor>>,
    event_sender: Option<mpsc::Sender<EventRequest>>,
    completed_results: Arc<AsyncMutex<VecDeque<StreamProcessingResult>>>,
    observed_actions: Arc<Mutex<VecDeque<StreamAction>>>,
    is_running: Arc<RwLock<bool>>,
}

impl StreamRuleEngine {
    /// Create a driver with default configuration.
    pub fn new() -> Self {
        Self::with_config(StreamConfig::default())
    }

    /// Create a driver with stream configuration shared with its processor.
    pub fn with_config(config: StreamConfig) -> Self {
        Self::with_processor(StreamProcessor::with_config(config))
    }

    /// Create a driver around an existing synchronous processor.
    pub fn with_processor(processor: StreamProcessor) -> Self {
        let config = processor.config().clone();
        Self {
            config,
            processor: Arc::new(Mutex::new(processor)),
            event_sender: None,
            completed_results: Arc::new(AsyncMutex::new(VecDeque::new())),
            observed_actions: Arc::new(Mutex::new(VecDeque::new())),
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    /// Add GRL rules through the processor's canonical parser.
    pub async fn add_rule(&mut self, grl: &str) -> Result<()> {
        self.lock_processor()?.add_rule(grl)
    }

    /// Add GRL rules from a file.
    pub async fn add_rule_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        self.lock_processor()?.add_rule_file(path)
    }

    /// Register a canonical custom function on the delegated rule engine.
    pub async fn register_function<F>(&self, name: &str, function: F) -> Result<()>
    where
        F: Fn(&[Value], &Facts) -> Result<Value> + Send + Sync + 'static,
    {
        self.lock_processor()?.register_function(name, function);
        Ok(())
    }

    /// Register a compatibility action callback on the canonical rule engine.
    pub async fn register_action_handler<F>(&self, action_type: &str, handler: F)
    where
        F: Fn(&StreamAction) + Send + Sync + 'static,
    {
        let action_type_owned = action_type.to_string();
        let observed_actions = Arc::clone(&self.observed_actions);
        let mut processor = self
            .processor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        processor.register_action_handler(action_type, move |parameters, _| {
            let action = StreamAction {
                action_type: action_type_owned.clone(),
                parameters: parameters.clone(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                rule_name: String::new(),
            };
            handler(&action);
            observed_actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push_back(action);
            Ok(())
        });
    }

    /// Start the channel driver.
    pub async fn start(&mut self) -> Result<()> {
        if *self.is_running.read().await {
            return Ok(());
        }

        let (sender, mut receiver) = mpsc::channel::<EventRequest>(self.config.buffer_size);
        self.event_sender = Some(sender);
        *self.is_running.write().await = true;

        let processor = Arc::clone(&self.processor);
        let completed_results = Arc::clone(&self.completed_results);
        let is_running = Arc::clone(&self.is_running);
        let processing_interval = self.config.processing_interval;

        tokio::spawn(async move {
            let mut timer = interval(processing_interval);
            loop {
                tokio::select! {
                    request = receiver.recv() => {
                        let Some((event, response)) = request else {
                            break;
                        };
                        let result = match processor.lock() {
                            Ok(mut processor) => processor.process_event(event),
                            Err(_) => Err(Self::poisoned_processor_error()),
                        };
                        if let Ok(processed) = &result {
                            completed_results.lock().await.push_back(processed.clone());
                        }
                        let _ = response.send(result);
                    }
                    _ = timer.tick() => {
                        if !*is_running.read().await {
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop accepting events through the channel driver.
    pub async fn stop(&self) {
        *self.is_running.write().await = false;
    }

    /// Submit one event and wait until delegated processing completes.
    pub async fn send_event(&self, event: StreamEvent) -> Result<()> {
        self.process_event(event).await.map(|_| ())
    }

    /// Submit one event and return the processor's one-event result.
    pub async fn process_event(&self, event: StreamEvent) -> Result<StreamProcessingResult> {
        let sender = self.event_sender.as_ref().ok_or_else(|| {
            RuleEngineError::ExecutionError(
                "StreamRuleEngine must be started before sending events".to_string(),
            )
        })?;
        let (response_sender, response_receiver) = oneshot::channel();
        sender.send((event, response_sender)).await.map_err(|_| {
            RuleEngineError::ExecutionError("StreamRuleEngine event channel is closed".to_string())
        })?;
        response_receiver.await.map_err(|_| {
            RuleEngineError::ExecutionError(
                "StreamRuleEngine processor stopped before returning a result".to_string(),
            )
        })?
    }

    /// Drain and aggregate results already produced by delegated processing.
    pub async fn execute_rules(&mut self) -> Result<StreamExecutionResult> {
        let mut completed = self.completed_results.lock().await;
        let results: Vec<_> = completed.drain(..).collect();
        drop(completed);

        let rules_fired = results
            .iter()
            .filter_map(|result| result.rule_execution.as_ref())
            .map(|execution| execution.rules_fired)
            .sum();
        let events_processed = results.iter().filter(|result| result.accepted).count();
        let processing_time_ms = results
            .iter()
            .map(|result| result.processing_time.as_millis() as u64)
            .sum();
        let analytics = results
            .last()
            .map(|result| result.analytics.clone())
            .unwrap_or_default();

        let mut observed_actions = self
            .observed_actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let actions = observed_actions.drain(..).collect();

        Ok(StreamExecutionResult {
            rules_fired,
            events_processed,
            processing_time_ms,
            actions,
            analytics,
        })
    }

    /// Get current delegated window statistics.
    pub async fn get_window_statistics(&self) -> WindowStatistics {
        self.processor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .window_statistics()
    }

    /// Get delegated analytics for a numeric field.
    pub async fn get_field_analytics(&self, field: &str) -> HashMap<String, Value> {
        self.processor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .field_analytics(field)
    }

    /// Check whether the channel driver is running.
    pub async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }

    /// Synchronously access the delegated processor and its upstream components.
    pub fn processor(&self) -> Result<MutexGuard<'_, StreamProcessor>> {
        self.lock_processor()
    }

    fn lock_processor(&self) -> Result<MutexGuard<'_, StreamProcessor>> {
        self.processor
            .lock()
            .map_err(|_| Self::poisoned_processor_error())
    }

    fn poisoned_processor_error() -> RuleEngineError {
        RuleEngineError::ExecutionError("StreamProcessor lock is poisoned".to_string())
    }
}

impl Default for StreamRuleEngine {
    fn default() -> Self {
        Self::new()
    }
}
