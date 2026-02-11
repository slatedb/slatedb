# SlateDB RFC Title

<!-- Replace "RFC Title" with your RFC's short, descriptive title. -->

Table of Contents:

<!-- TOC start (generate with https://bitdowntoc.derlin.ch) -->

<!-- TOC end -->

Status: Draft

Authors:

* [Your Name](https://github.com/your_github_profile)

## Summary

<!-- Briefly explain what this RFC changes and why. Prefer 3–6 sentences. -->

## Motivation

<!-- What problem are we solving? What user or system pain exists today? Include concrete examples and why “do nothing” is insufficient. -->

## Goals

- Goal 1
- Goal 2

## Non-Goals

- Non-goal 1
- Non-goal 2

## Design

<!-- A detailed description of the proposed change. Include diagrams, examples, schemas, and pseudo-code as appropriate. -->

## Impact Analysis

SlateDB features and components that this RFC interacts with. Check all that apply.

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [ ] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [ ] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [ ] Block cache
- [ ] Object store cache
- [ ] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [ ] Language bindings (Go/Python/etc)
- [ ] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

<!-- Describe performance and cost implications of this change. -->

- Latency (reads/writes/compactions)
- Throughput (reads/writes/compactions)
- Object-store request (GET/LIST/PUT) and cost profile
- Space, read, and write amplification

### Observability

<!-- Describe any operational changes required to support this change. -->

- Configuration changes
- New components/services
- Metrics
- Logging

### Compatibility

<!-- Describe compatibility considerations with existing versions of SlateDB. -->

- Existing data on object storage / on-disk formats
- Existing public APIs (including bindings)
- Rolling upgrades / mixed-version behavior (if applicable)

## Testing

<!-- Describe the testing plan for this change. -->

- Unit tests:
- Integration tests:
- Fault-injection/chaos tests:
- Deterministic simulation tests:
- Formal methods verification:
- Performance tests:

## Rollout

<!-- Describe the plan for rolling out this change to production. -->

- Milestones / phases:
- Feature flags / opt-in:
- Docs updates:

## Alternatives

List the serious alternatives and why they were rejected (including “status quo”). Include trade-offs and risks.

## Open Questions

- Question 1
- Question 2

## References

<!-- Bullet list of related issues, PRs, RFCs, papers, docs, discord discussions, etc. -->

- Reference 1
- Reference 2

## Updates

Log major changes to this RFC over time (optional).
