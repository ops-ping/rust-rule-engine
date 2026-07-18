# Advanced Features

Advanced capabilities for production use.

---

## 📚 Available Documentation

### 🌊 Streaming & Complex Event Processing

#### [Streaming Guide](STREAMING.md)
Introduction to stream processing and CEP.

**Topics:**
- Synchronous one-event/one-result processing
- Stream-pattern binding
- Windowing and watermarking
- Optional Tokio transport

#### [Streaming Architecture](STREAMING_ARCHITECTURE.md)
Deep dive into streaming architecture and patterns.

**Topics:**
- Architecture overview
- State management
- Sequential processing and feature boundaries
- Host-owned delivery guarantees

#### [Stream Operators](STREAM_OPERATORS.md)
Complete reference for stream operators.

**Topics:**
- Transformation operators
- Windowing operators
- Join operators
- Aggregation operators

### 🔌 Extensions & Integration

#### [Plugins & Extensions](PLUGINS.md)
Build custom plugins and extend the engine.

**Topics:**
- Plugin architecture
- Custom functions
- Built-in plugins
- Plugin best practices

#### [Redis State Backend](REDIS_STATE_BACKEND.md)
Optional synchronous Redis-backed state.

**Topics:**
- Redis integration
- Error propagation
- Server-owned persistence
- Deployment boundaries

### ⚡ Performance & Optimization

#### [RETE Optimization Guide](RETE_OPTIMIZATION.md)
Comprehensive RETE optimization techniques for maximum performance.

**Topics:**
- Beta Memory Indexing (11x to 1,235x speedup)
- Alpha Memory Indexing (up to 800x speedup)
- Node Sharing (98% memory reduction)
- Alpha Memory Compaction
- Token Pooling

#### [RETE Optimization Benchmarks](RETE_OPTIMIZATION_BENCHMARKS.md)
Real benchmark data and performance analysis.

**Topics:**
- Detailed benchmark results
- Performance comparisons
- Memory measurements
- Optimization recommendations

#### [ProofGraph Caching](PROOF_GRAPH_CACHING.md) 🆕
Global proof caching for backward chaining with TMS integration.

**Topics:**
- Proof caching (100-1000x speedup)
- Dependency tracking
- TMS-aware invalidation
- Multiple justifications
- Performance benchmarks

#### [Performance Guide](PERFORMANCE.md)
Optimize your rule engine for production.

**Topics:**
- RETE optimization
- Indexing strategies
- Memory management
- Benchmarking

#### [Advanced Usage](ADVANCED_USAGE.md)
Advanced patterns and techniques.

**Topics:**
- Complex rule patterns
- Custom integrations
- Production deployment

---

## 🎯 Quick Navigation by Task

### I want to...

#### Process Streams
- 📖 [Start with Streaming Guide](STREAMING.md)
- 🏗️ [Understand Architecture](STREAMING_ARCHITECTURE.md)
- 📋 [Use Stream Operators](STREAM_OPERATORS.md)

#### Extend the Engine
- 🔌 [Create Plugins](PLUGINS.md)
- 🔧 [Advanced Patterns](ADVANCED_USAGE.md)

#### Scale to Production
- ⚡ [Optimize Performance](PERFORMANCE.md)
- 🗄️ [Use Redis State](REDIS_STATE_BACKEND.md)

---

## 🔗 Related Documentation

- **[Getting Started](../getting-started/QUICK_START.md)** - Quick start guide
- **[Core Features](../core-features/)** - Essential capabilities
- **[API Reference](../api-reference/API_REFERENCE.md)** - Complete API

---

## Navigation

📚 **[Documentation Home](../README.md)** | 📖 **[Getting Started](../getting-started/QUICK_START.md)**
