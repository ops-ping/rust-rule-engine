# Rust Rule Engine Documentation

> **Version:** 1.18.26
> **Last Updated:** January 26, 2026

Complete documentation for the Rust Rule Engine with RETE algorithm, backward chaining inference, ProofGraph caching, and GRL syntax support.

---

## 📚 Documentation Structure

### 🚀 [Getting Started](getting-started/)
Quick start guides to get you up and running fast.

- **[Quick Start Guide](getting-started/QUICK_START.md)** - Get started in 5 minutes
- **[Installation](getting-started/INSTALLATION.md)** - Installation and setup
- **[First Rules](getting-started/FIRST_RULES.md)** - Write your first rules
- **[Basic Concepts](getting-started/CONCEPTS.md)** - Core concepts explained

### 🎯 [Core Features](core-features/)
Essential features and capabilities.

- **[Forward Chaining (RETE)](core-features/FORWARD_CHAINING.md)** - RETE algorithm and forward reasoning
- **[Backward Chaining](core-features/BACKWARD_CHAINING.md)** - Goal-driven inference
- **[GRL Syntax](core-features/GRL_SYNTAX.md)** - Grule Rule Language
- **[Pattern Matching](core-features/PATTERN_MATCHING.md)** - Advanced pattern matching
- **[Facts & Working Memory](core-features/FACTS.md)** - Managing facts and state

### ⚡ [Advanced Features](advanced-features/)
Advanced capabilities for production use.

- **[RETE Optimization](advanced-features/RETE_OPTIMIZATION.md)** - 1,235x join speedup & memory optimizations
- **[ProofGraph Caching](advanced-features/PROOF_GRAPH_CACHING.md)** 🆕 - 100-1000x speedup for backward chaining
- **[Streaming & CEP](advanced-features/STREAMING.md)** - Synchronous stream processing with optional drivers
- **[Modules & Imports](advanced-features/MODULES.md)** - Modular rule organization
- **[Plugins & Extensions](advanced-features/PLUGINS.md)** - Custom plugins and functions
- **[Performance Tuning](advanced-features/PERFORMANCE.md)** - Optimization techniques
- **[Redis State Backend](advanced-features/REDIS_STATE_BACKEND.md)** - Optional synchronous Redis-backed state

### 📖 [API Reference](api-reference/)
Complete API documentation.

- **[Public API](api-reference/API_REFERENCE.md)** - Public API reference
- **[GRL Query Syntax](api-reference/GRL_QUERY_SYNTAX.md)** - Query language reference
- **[Parser Cheat Sheet](api-reference/PARSER_CHEAT_SHEET.md)** - Parser quick reference
- **[Error Handling](api-reference/ERROR_HANDLING.md)** - Error types and handling

### 📝 [Guides](guides/)
Step-by-step tutorials and best practices.

- **[Backward Chaining Integration](guides/BACKWARD_CHAINING_RETE_INTEGRATION.md)** - Combine forward + backward
- **[Module Management](guides/MODULE_PARSING_GUIDE.md)** - Working with modules
- **[Troubleshooting](guides/TROUBLESHOOTING.md)** - Common issues and solutions
- **[Best Practices](guides/BEST_PRACTICES.md)** - Production-ready patterns
- **[Migration Guide](guides/MIGRATION.md)** - Upgrade between versions

### 💡 [Examples](examples/)
Real-world examples and use cases.

- **[E-commerce Rules](examples/ECOMMERCE.md)** - Shopping cart, discounts, loyalty
- **[Healthcare](examples/HEALTHCARE.md)** - Patient diagnosis, treatment authorization
- **[Finance](examples/FINANCE.md)** - Loan approval, fraud detection
- **[AI Integration](examples/AI_INTEGRATION.md)** - Combine with ML models
- **[Stream Processing](examples/STREAM_PROCESSING.md)** - Real-time event processing

---

## 🎯 Quick Navigation by Task

### I want to...

#### Get Started
- ✅ **Install the library** → [Installation Guide](getting-started/INSTALLATION.md)
- ✅ **Write my first rule** → [First Rules](getting-started/FIRST_RULES.md)
- ✅ **Understand core concepts** → [Basic Concepts](getting-started/CONCEPTS.md)

#### Use Forward Chaining (RETE)
- ✅ **Write forward rules** → [Forward Chaining](core-features/FORWARD_CHAINING.md)
- ✅ **Optimize RETE performance** → [Performance Tuning](advanced-features/PERFORMANCE.md)
- ✅ **Pattern matching** → [Pattern Matching](core-features/PATTERN_MATCHING.md)

#### Use Backward Chaining
- ✅ **Quick start** → [Backward Chaining Quick Start](BACKWARD_CHAINING_QUICK_START.md)
- ✅ **Write queries** → [GRL Query Syntax](api-reference/GRL_QUERY_SYNTAX.md)
- ✅ **Combine with RETE** → [Integration Guide](guides/BACKWARD_CHAINING_RETE_INTEGRATION.md)
- ✅ **Troubleshoot issues** → [Troubleshooting](BACKWARD_CHAINING_TROUBLESHOOTING.md)

#### Advanced Features
- ✅ **Stream processing** → [Streaming Architecture](advanced-features/STREAMING_ARCHITECTURE.md)
- ✅ **Use modules** → [Module Guide](guides/MODULE_PARSING_GUIDE.md)
- ✅ **Add plugins** → [Plugins](advanced-features/PLUGINS.md)
- ✅ **Redis backend** → [Redis State](advanced-features/REDIS_STATE_BACKEND.md)

#### Integration & Production
- ✅ **Integrate with AI/ML** → [AI Integration](examples/AI_INTEGRATION.md)
- ✅ **Production deployment** → [Best Practices](guides/BEST_PRACTICES.md)
- ✅ **Performance optimization** → [Performance Guide](advanced-features/PERFORMANCE.md)
- ✅ **Error handling** → [Error Reference](api-reference/ERROR_HANDLING.md)

---

## 📊 Feature Matrix

| Feature | Forward Chaining | Backward Chaining | Version |
|---------|-----------------|-------------------|---------|
| **RETE Algorithm** | ✅ | ➖ | 1.0.0+ |
| **Pattern Matching** | ✅ | ✅ | 1.0.0+ |
| **GRL Syntax** | ✅ | ✅ | 1.0.0+ |
| **Aggregation (COUNT, SUM, AVG)** | ➖ | ✅ | 1.7.0+ |
| **Negation (NOT)** | ✅ | ✅ | 1.8.0+ |
| **Explanation System** | ➖ | ✅ | 1.9.0+ |
| **Disjunction (OR)** | ✅ | ✅ | 1.10.0+ |
| **Nested Queries** | ➖ | ✅ | 1.11.0+ |
| **Query Optimization** | ➖ | ✅ | 1.11.0+ |
| **Streaming/CEP** | ✅ | ➖ | 1.3.0+ |
| **Modules & Imports** | ✅ | ✅ | 1.5.0+ |
| **Plugins** | ✅ | ✅ | 1.4.0+ |
| **Redis State Backend** | ✅ | ➖ | 1.6.0+ |

---

## 🔍 Search by Version

### Latest (v1.11.0)
- ⭐ **[Nested Queries](api-reference/GRL_QUERY_SYNTAX.md#nested-queries-subqueries)** - Multi-level reasoning
- ⭐ **[Query Optimization](api-reference/GRL_QUERY_SYNTAX.md#query-optimization)** - 10-100x speedup

### Previous Versions
- **v1.10.0** - [Disjunction (OR)](api-reference/GRL_QUERY_SYNTAX.md#disjunction)
- **v1.9.0** - [Explanation System](api-reference/GRL_QUERY_SYNTAX.md#explanation-system-v190)
- **v1.8.0** - [Negation (NOT)](api-reference/GRL_QUERY_SYNTAX.md#negation-not-keyword)
- **v1.7.0** - [Aggregation Functions](api-reference/GRL_QUERY_SYNTAX.md#aggregation-functions)

---

## 📦 File Organization

```
docs/
├── README.md                          # This file - documentation index
│
├── getting-started/                   # Quick start guides
│   ├── QUICK_START.md
│   ├── INSTALLATION.md
│   ├── FIRST_RULES.md
│   └── CONCEPTS.md
│
├── core-features/                     # Core functionality
│   ├── FORWARD_CHAINING.md
│   ├── BACKWARD_CHAINING.md
│   ├── GRL_SYNTAX.md
│   ├── PATTERN_MATCHING.md
│   └── FACTS.md
│
├── advanced-features/                 # Advanced capabilities
│   ├── STREAMING.md
│   ├── STREAMING_ARCHITECTURE.md
│   ├── STREAM_OPERATORS.md
│   ├── MODULES.md
│   ├── PLUGINS.md
│   ├── PERFORMANCE.md
│   └── REDIS_STATE_BACKEND.md
│
├── api-reference/                     # API documentation
│   ├── API_REFERENCE.md
│   ├── GRL_QUERY_SYNTAX.md
│   ├── PARSER_CHEAT_SHEET.md
│   └── ERROR_HANDLING.md
│
├── guides/                            # Step-by-step guides
│   ├── BACKWARD_CHAINING_RETE_INTEGRATION.md
│   ├── MODULE_PARSING_GUIDE.md
│   ├── TROUBLESHOOTING.md
│   ├── BEST_PRACTICES.md
│   └── MIGRATION.md
│
└── examples/                          # Real-world examples
    ├── ECOMMERCE.md
    ├── HEALTHCARE.md
    ├── FINANCE.md
    ├── AI_INTEGRATION.md
    └── STREAM_PROCESSING.md
```

---

## 🚦 Documentation Status

| Document | Status | Last Updated | Version |
|----------|--------|--------------|---------|
| GRL Query Syntax | ✅ Complete | 2024-12-10 | 1.11.0 |
| GRL Syntax | ✅ Complete | 2024-11-15 | 1.10.0 |
| Backward Chaining Quick Start | ✅ Complete | 2024-11-01 | 1.9.0 |
| Streaming Architecture | ✅ Complete | 2024-10-20 | 1.6.0 |
| Module Parsing Guide | ✅ Complete | 2024-10-15 | 1.5.0 |
| API Reference | ✅ Complete | 2024-10-10 | 1.5.0 |
| Performance Guide | ✅ Complete | 2024-09-25 | 1.4.0 |
| Plugins | ✅ Complete | 2024-09-20 | 1.4.0 |

---

## 🤝 Contributing to Documentation

We welcome documentation improvements! Please:

1. Follow the existing structure
2. Use clear, concise language
3. Include code examples
4. Add cross-references to related docs
5. Update the version and date in headers
6. Test all code snippets

See [CONTRIBUTING.md](../CONTRIBUTING.md) for details.

---

## 📞 Support & Community

- **GitHub Issues:** [Report bugs & request features](https://github.com/KSD-CO/rust-rule-engine/issues)
- **Discussions:** [Ask questions & share ideas](https://github.com/KSD-CO/rust-rule-engine/discussions)
- **Documentation:** [Read the docs](https://docs.rs/rust-rule-engine)

---

## 📄 License

MIT License - See [LICENSE](../LICENSE) for details.

---

**Made with ❤️ by the Rust Rule Engine Team**
