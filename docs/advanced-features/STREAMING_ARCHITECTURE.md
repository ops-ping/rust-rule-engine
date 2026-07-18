# Streaming Architecture

The streaming architecture has one semantic core: `StreamProcessor`. It
processes one event synchronously and delegates rule evaluation to
`RustRuleEngine`. Optional drivers and state backends do not introduce another
rule model.

## Component model

```text
StreamEvent
    |
    v
StreamProcessor
    +-- WatermarkedStream
    +-- WindowManager
    +-- StreamJoinManager
    +-- StateStore
    +-- StreamAnalytics
    |
    v
RustRuleEngine
    |
    v
StreamProcessingResult
```

`StreamRuleEngine`, enabled by `streaming`, adds a bounded Tokio channel around
the processor:

```text
async caller -> channel -> one StreamProcessor -> one async response
```

The channel changes transport and scheduling only. Parsing, matching, actions,
facts, state, and result semantics remain in `StreamProcessor` and
`RustRuleEngine`.

## Processing order

For each input event, the processor:

1. assigns the next local sequence number;
2. updates the event-time watermark and classifies late data;
3. returns a dropped or side-output result without rule evaluation when the
   late-data policy requires it;
4. inserts an accepted event into windows and join state;
5. persists bounded operational state through `StateStore`;
6. materializes event facts and window aggregates;
7. evaluates the canonical `RustRuleEngine`;
8. removes the reserved current-event context; and
9. returns one `StreamProcessingResult`.

State and rule-engine errors are returned to the caller. There is no
success-shaped fallback for a failed configured backend or custom function.

## Current-event binding

The processor creates an internal fact containing:

- event source;
- event type; and
- the event object.

`ConditionGroup::StreamPattern` reads that fact:

| GRL field | Event field | Matching |
|---|---|---|
| `stream_name` | `metadata.source` | exact, case-sensitive |
| optional event type | `event_type` | exact, case-sensitive |
| alias | event object | payload plus metadata fields |

Missing stream context means no match. This preserves ordinary non-stream
execution: parsing a stream rule does not make it fire outside a processor.

## Windows

`WindowManager` owns bounded retained events.

### Sliding

A sliding window ends at the greatest accepted event timestamp. Its start is
that timestamp minus the configured duration. Events older than the start are
removed; accepted out-of-order events inside the interval are inserted in event
time order.

### Tumbling

Tumbling windows use fixed aligned buckets:

```text
bucket_start = (event_timestamp / duration) * duration
```

The start is inclusive and the end is exclusive.

### Session

Session windows group events separated by inactivity gaps less than or equal to
the timeout. An event may bridge retained sessions; matching sessions merge and
their events remain event-time ordered. A larger gap starts a distinct session.

`max_events_per_window` and `max_windows` bound all three strategies.

## Watermarks and late events

`WatermarkedStream` tracks event-time progress independently of processing
time. The configured strategy produces the watermark:

- periodic;
- bounded out-of-order;
- monotonic ascending; or
- custom.

Events behind the watermark follow the configured late-data strategy. Their
disposition and cumulative counts are exposed in every result.

## Operational state

The processor writes five bounded state entries after each accepted event:

| Key | Value |
|---|---|
| `stream.sequence` | latest accepted sequence |
| `stream.watermark` | current watermark |
| `stream.latest_by_source` | bounded latest event map |
| `stream.window` | active window and event counts |
| `stream.analytics` | watermark and late-data analytics |

`max_state_sources` bounds the latest-event map. The processor does not persist
an event-per-ID log.

`StateStore` selects memory, file, Redis, or a custom backend. Automatic
checkpointing uses the store's configured interval. `checkpoint` and
`restore_state` delegate to the same store, and restore also advances the
processor sequence from persisted state.

## Joins and operators

`StreamJoinManager` and the types in `operators` are runtime-neutral streaming
primitives. They share event, window, and value types with the processor. GRL
evaluation still occurs only through `RustRuleEngine`; operators do not compile
or interpret a second rule language.

## Concurrency model

A `StreamProcessor` is mutable and processes calls sequentially. The async
driver owns one processor behind synchronization and serializes channel
requests through it. Applications scale by explicitly partitioning streams
across processor instances and owning the routing and state-partition contract.

The library does not claim distributed coordination, transactional event
delivery, or exactly-once processing. Those guarantees belong to the host and
its transport. The processor guarantees one returned result or error for each
direct `process_event` call.

## Feature boundaries

| Build | Runtime dependency | State backends |
|---|---|---|
| `streaming-core` | none | memory, file, custom |
| `streaming` | Tokio | memory, file, custom |
| `streaming-redis` | synchronous Redis client | memory, file, Redis, custom |
| `streaming,streaming-redis` | Tokio and synchronous Redis client | all |

This split keeps the core usable in stdio loops, batch jobs, embedded
applications, and other hosts that do not need an async runtime.
