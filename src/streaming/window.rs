//! Time Window Management for Stream Processing
//!
//! Provides time-based windows for event aggregation and analysis.

use crate::streaming::event::StreamEvent;
use std::collections::VecDeque;
use std::time::Duration;

/// Type of time window
#[derive(Debug, Clone, PartialEq)]
pub enum WindowType {
    /// Sliding window - continuously moves forward
    Sliding,
    /// Tumbling window - non-overlapping fixed intervals
    Tumbling,
    /// Session window - based on inactivity gaps
    Session { timeout: Duration },
}

/// Time-based window for event processing
#[derive(Debug)]
pub struct TimeWindow {
    /// Window type
    pub window_type: WindowType,
    /// Window duration
    pub duration: Duration,
    /// Events in this window
    events: VecDeque<StreamEvent>,
    /// Window start time (milliseconds since epoch)
    pub start_time: u64,
    /// Window end time (milliseconds since epoch)
    pub end_time: u64,
    /// Maximum number of events to retain
    max_events: usize,
}

impl TimeWindow {
    /// Create a new time window
    pub fn new(
        window_type: WindowType,
        duration: Duration,
        start_time: u64,
        max_events: usize,
    ) -> Self {
        let end_time = start_time.saturating_add(duration.as_millis() as u64);

        Self {
            window_type,
            duration,
            events: VecDeque::new(),
            start_time,
            end_time,
            max_events,
        }
    }

    /// Add event to window if it fits
    pub fn add_event(&mut self, event: StreamEvent) -> bool {
        if self.contains_timestamp(event.metadata.timestamp) {
            self.insert_event(event);
            true
        } else {
            false
        }
    }

    /// Check if timestamp falls within this window
    pub fn contains_timestamp(&self, timestamp: u64) -> bool {
        match self.window_type {
            WindowType::Sliding | WindowType::Session { .. } => {
                timestamp >= self.start_time && timestamp <= self.end_time
            }
            WindowType::Tumbling => timestamp >= self.start_time && timestamp < self.end_time,
        }
    }

    /// Get all events in window
    pub fn events(&self) -> &VecDeque<StreamEvent> {
        &self.events
    }

    /// Get event count
    pub fn count(&self) -> usize {
        self.events.len()
    }

    /// Check if window is expired
    pub fn is_expired(&self, current_time: u64) -> bool {
        match self.window_type {
            WindowType::Tumbling => current_time >= self.end_time,
            WindowType::Sliding | WindowType::Session { .. } => current_time > self.end_time,
        }
    }

    /// Get window duration in milliseconds
    pub fn duration_ms(&self) -> u64 {
        self.duration.as_millis() as u64
    }

    /// Clear all events from window
    pub fn clear(&mut self) {
        self.events.clear();
    }

    /// Get events filtered by type
    pub fn events_by_type(&self, event_type: &str) -> Vec<&StreamEvent> {
        self.events
            .iter()
            .filter(|e| e.event_type == event_type)
            .collect()
    }

    /// Calculate sum of numeric field across events
    pub fn sum(&self, field: &str) -> f64 {
        self.events
            .iter()
            .filter_map(|e| e.get_numeric(field))
            .sum()
    }

    /// Calculate average of numeric field across events
    pub fn average(&self, field: &str) -> Option<f64> {
        let values: Vec<f64> = self
            .events
            .iter()
            .filter_map(|e| e.get_numeric(field))
            .collect();

        if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<f64>() / values.len() as f64)
        }
    }

    /// Find minimum value of numeric field
    pub fn min(&self, field: &str) -> Option<f64> {
        self.events
            .iter()
            .filter_map(|e| e.get_numeric(field))
            .fold(None, |acc, x| match acc {
                None => Some(x),
                Some(min) => Some(min.min(x)),
            })
    }

    /// Find maximum value of numeric field
    pub fn max(&self, field: &str) -> Option<f64> {
        self.events
            .iter()
            .filter_map(|e| e.get_numeric(field))
            .fold(None, |acc, x| match acc {
                None => Some(x),
                Some(max) => Some(max.max(x)),
            })
    }

    /// Get the latest event timestamp
    pub fn latest_timestamp(&self) -> Option<u64> {
        self.events.iter().map(|e| e.metadata.timestamp).max()
    }

    /// Get events within a sub-window
    pub fn events_in_range(&self, start: u64, end: u64) -> Vec<&StreamEvent> {
        self.events
            .iter()
            .filter(|e| e.metadata.timestamp >= start && e.metadata.timestamp < end)
            .collect()
    }

    fn insert_event(&mut self, event: StreamEvent) {
        let index = self
            .events
            .iter()
            .position(|existing| existing.metadata.timestamp > event.metadata.timestamp)
            .unwrap_or(self.events.len());
        self.events.insert(index, event);
        while self.events.len() > self.max_events {
            self.events.pop_front();
        }
    }

    fn update_session_bounds(&mut self, timeout: Duration) {
        if let (Some(first), Some(last)) = (self.events.front(), self.events.back()) {
            self.start_time = first.metadata.timestamp;
            self.end_time = last
                .metadata
                .timestamp
                .saturating_add(timeout.as_millis() as u64);
            self.duration = Duration::from_millis(self.end_time.saturating_sub(self.start_time));
        }
    }
}

/// Manages multiple time windows for stream processing
#[derive(Debug)]
pub struct WindowManager {
    /// Active windows
    windows: Vec<TimeWindow>,
    /// Window configuration
    window_type: WindowType,
    /// Window duration
    duration: Duration,
    /// Maximum events per window
    max_events_per_window: usize,
    /// Maximum number of windows to keep
    max_windows: usize,
    /// Greatest event timestamp observed by a moving window.
    greatest_event_time: Option<u64>,
}

impl WindowManager {
    /// Create a new window manager
    pub fn new(
        window_type: WindowType,
        duration: Duration,
        max_events_per_window: usize,
        max_windows: usize,
    ) -> Self {
        Self {
            windows: Vec::new(),
            window_type,
            duration,
            max_events_per_window,
            max_windows,
            greatest_event_time: None,
        }
    }

    /// Process a new event through the window system
    pub fn process_event(&mut self, event: StreamEvent) {
        match self.window_type.clone() {
            WindowType::Sliding => self.process_sliding_event(event),
            WindowType::Tumbling => self.process_tumbling_event(event),
            WindowType::Session { timeout } => self.process_session_event(event, timeout),
        }
        self.enforce_window_limit();
    }

    fn process_sliding_event(&mut self, event: StreamEvent) {
        let event_time = event.metadata.timestamp;
        let greatest = self
            .greatest_event_time
            .map_or(event_time, |current| current.max(event_time));
        self.greatest_event_time = Some(greatest);

        let start = greatest.saturating_sub(self.duration.as_millis() as u64);
        if self.windows.is_empty() {
            self.windows.push(TimeWindow::new(
                WindowType::Sliding,
                self.duration,
                start,
                self.max_events_per_window,
            ));
        }
        self.windows.truncate(1);

        let window = &mut self.windows[0];
        window.start_time = start;
        window.end_time = greatest;
        window
            .events
            .retain(|existing| existing.metadata.timestamp >= start);
        if event_time >= start {
            window.insert_event(event);
        }
    }

    fn process_tumbling_event(&mut self, event: StreamEvent) {
        let window_ms = (self.duration.as_millis() as u64).max(1);
        let start = (event.metadata.timestamp / window_ms) * window_ms;
        if let Some(window) = self
            .windows
            .iter_mut()
            .find(|window| window.start_time == start)
        {
            window.add_event(event);
        } else {
            let mut window = TimeWindow::new(
                WindowType::Tumbling,
                Duration::from_millis(window_ms),
                start,
                self.max_events_per_window,
            );
            window.add_event(event);
            self.windows.push(window);
        }
        self.windows.sort_by_key(|window| window.start_time);
    }

    fn process_session_event(&mut self, event: StreamEvent, timeout: Duration) {
        let event_time = event.metadata.timestamp;
        let timeout_ms = timeout.as_millis() as u64;
        let matching: Vec<usize> = self
            .windows
            .iter()
            .enumerate()
            .filter_map(|(index, window)| {
                let first = window.events.front()?.metadata.timestamp;
                let last = window.events.back()?.metadata.timestamp;
                (event_time <= last.saturating_add(timeout_ms)
                    && event_time.saturating_add(timeout_ms) >= first)
                    .then_some(index)
            })
            .collect();

        let mut events = vec![event];
        for index in matching.into_iter().rev() {
            let window = self.windows.remove(index);
            events.extend(window.events);
        }
        events.sort_by_key(|event| event.metadata.timestamp);

        let start = events
            .first()
            .map(|event| event.metadata.timestamp)
            .unwrap_or(event_time);
        let mut session = TimeWindow::new(
            WindowType::Session { timeout },
            timeout,
            start,
            self.max_events_per_window,
        );
        for event in events {
            session.insert_event(event);
        }
        session.update_session_bounds(timeout);
        self.windows.push(session);
        self.windows.sort_by_key(|window| window.start_time);
    }

    fn enforce_window_limit(&mut self) {
        while self.windows.len() > self.max_windows {
            self.windows.remove(0);
        }
    }

    /// Get all active windows
    pub fn active_windows(&self) -> &[TimeWindow] {
        &self.windows
    }

    /// Get the latest window
    pub fn latest_window(&self) -> Option<&TimeWindow> {
        self.windows.last()
    }

    /// Get total event count across all windows
    pub fn total_event_count(&self) -> usize {
        self.windows.iter().map(|w| w.count()).sum()
    }

    /// Get windows that contain events of a specific type
    pub fn windows_with_event_type(&self, event_type: &str) -> Vec<&TimeWindow> {
        self.windows
            .iter()
            .filter(|w| w.events().iter().any(|e| e.event_type == event_type))
            .collect()
    }

    /// Calculate aggregate across all windows
    pub fn aggregate_across_windows<F>(&self, aggregator: F) -> f64
    where
        F: Fn(&TimeWindow) -> f64,
    {
        self.windows.iter().map(aggregator).sum()
    }

    /// Get window statistics
    pub fn get_statistics(&self) -> WindowStatistics {
        WindowStatistics {
            total_windows: self.windows.len(),
            total_events: self.total_event_count(),
            oldest_window_start: self.windows.first().map(|w| w.start_time),
            newest_window_start: self.windows.last().map(|w| w.start_time),
            average_events_per_window: if self.windows.is_empty() {
                0.0
            } else {
                self.total_event_count() as f64 / self.windows.len() as f64
            },
        }
    }
}

/// Statistics about window manager state
#[derive(Debug, Clone)]
pub struct WindowStatistics {
    /// Total number of active windows
    pub total_windows: usize,
    /// Total events across all windows
    pub total_events: usize,
    /// Start time of oldest window
    pub oldest_window_start: Option<u64>,
    /// Start time of newest window
    pub newest_window_start: Option<u64>,
    /// Average events per window
    pub average_events_per_window: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use std::collections::HashMap;

    fn event(timestamp: u64, value: f64) -> StreamEvent {
        StreamEvent::with_timestamp(
            "TestEvent",
            HashMap::from([("value".to_string(), Value::Number(value))]),
            "test",
            timestamp,
        )
    }

    #[test]
    fn test_time_window_creation() {
        let window = TimeWindow::new(WindowType::Sliding, Duration::from_secs(60), 1000, 100);

        assert_eq!(window.start_time, 1000);
        assert_eq!(window.end_time, 61000);
        assert_eq!(window.count(), 0);
    }

    #[test]
    fn test_window_event_addition() {
        let mut window = TimeWindow::new(WindowType::Sliding, Duration::from_secs(60), 1000, 100);

        let mut data = HashMap::new();
        data.insert("value".to_string(), Value::Number(10.0));

        let event = StreamEvent::with_timestamp("TestEvent", data, "test", 30000);

        assert!(window.add_event(event));
        assert_eq!(window.count(), 1);
    }

    #[test]
    fn test_window_aggregations() {
        let mut window = TimeWindow::new(WindowType::Sliding, Duration::from_secs(60), 1000, 100);

        // Add test events
        for i in 0..5 {
            let mut data = HashMap::new();
            data.insert("value".to_string(), Value::Number(i as f64));

            let event = StreamEvent::with_timestamp("TestEvent", data, "test", 30000 + i);
            window.add_event(event);
        }

        assert_eq!(window.sum("value"), 10.0); // 0+1+2+3+4
        assert_eq!(window.average("value"), Some(2.0));
        assert_eq!(window.min("value"), Some(0.0));
        assert_eq!(window.max("value"), Some(4.0));
    }

    #[test]
    fn test_window_manager() {
        let mut manager = WindowManager::new(WindowType::Sliding, Duration::from_secs(60), 100, 10);

        let mut data = HashMap::new();
        data.insert("value".to_string(), Value::Number(1.0));

        let event = StreamEvent::with_timestamp("TestEvent", data, "test", 30000);
        manager.process_event(event);

        assert_eq!(manager.active_windows().len(), 1);
        assert_eq!(manager.total_event_count(), 1);
    }

    #[test]
    fn test_sliding_window_moves_with_greatest_event_time() {
        let mut manager =
            WindowManager::new(WindowType::Sliding, Duration::from_millis(10), 100, 10);

        manager.process_event(event(100, 1.0));
        manager.process_event(event(105, 2.0));
        manager.process_event(event(111, 3.0));

        let window = manager.latest_window().unwrap();
        assert_eq!(window.start_time, 101);
        assert_eq!(window.end_time, 111);
        assert_eq!(
            window
                .events()
                .iter()
                .map(|event| event.metadata.timestamp)
                .collect::<Vec<_>>(),
            vec![105, 111]
        );

        manager.process_event(event(103, 4.0));
        assert_eq!(
            manager
                .latest_window()
                .unwrap()
                .events()
                .iter()
                .map(|event| event.metadata.timestamp)
                .collect::<Vec<_>>(),
            vec![103, 105, 111]
        );

        manager.process_event(event(99, 5.0));
        assert_eq!(manager.total_event_count(), 3);
    }

    #[test]
    fn test_tumbling_windows_use_aligned_buckets() {
        let mut manager =
            WindowManager::new(WindowType::Tumbling, Duration::from_millis(10), 100, 10);

        manager.process_event(event(1, 1.0));
        manager.process_event(event(9, 2.0));
        manager.process_event(event(10, 3.0));

        assert_eq!(manager.active_windows().len(), 2);
        assert_eq!(manager.active_windows()[0].start_time, 0);
        assert_eq!(manager.active_windows()[0].end_time, 10);
        assert_eq!(manager.active_windows()[0].count(), 2);
        assert_eq!(manager.active_windows()[1].start_time, 10);
        assert_eq!(manager.active_windows()[1].end_time, 20);
        assert_eq!(manager.active_windows()[1].count(), 1);
    }

    #[test]
    fn test_session_windows_extend_until_inactivity_gap() {
        let timeout = Duration::from_millis(10);
        let mut manager = WindowManager::new(WindowType::Session { timeout }, timeout, 100, 10);

        manager.process_event(event(100, 1.0));
        manager.process_event(event(110, 2.0));
        manager.process_event(event(121, 3.0));

        assert_eq!(manager.active_windows().len(), 2);
        assert_eq!(manager.active_windows()[0].start_time, 100);
        assert_eq!(manager.active_windows()[0].end_time, 120);
        assert_eq!(manager.active_windows()[0].count(), 2);
        assert_eq!(manager.active_windows()[1].start_time, 121);
        assert_eq!(manager.active_windows()[1].end_time, 131);
    }

    #[test]
    fn test_session_windows_preserve_event_and_window_limits() {
        let timeout = Duration::from_millis(5);
        let mut manager = WindowManager::new(WindowType::Session { timeout }, timeout, 2, 2);

        manager.process_event(event(100, 1.0));
        manager.process_event(event(101, 2.0));
        manager.process_event(event(102, 3.0));
        assert_eq!(manager.latest_window().unwrap().count(), 2);

        manager.process_event(event(110, 4.0));
        manager.process_event(event(120, 5.0));
        assert_eq!(manager.active_windows().len(), 2);
        assert_eq!(manager.active_windows()[0].start_time, 110);
        assert_eq!(manager.active_windows()[1].start_time, 120);
    }
}
