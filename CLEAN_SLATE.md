# Clean Slate

We want to build SlateDB with a single ethos that helps the community work together in a positive and collaborative way. This document describes Clean Slate, SlateDB's style guide. The guide is meant to clarify why and how we make changes to the project. This includes:

- How we decide which features should be added to SlateDB and which should be left out.
- How new features should be designed and implemented.
- How we evaluate changes to the project governance and decision-making processes.

## Vision

> We believe that the future of object storage are multi-region, low latency buckets that support atomic CAS operations. Inspired by [The Cloud Storage Triad: Latency, Cost, Durability](https://materializedview.io/p/cloud-storage-triad-latency-cost-durability), we set out to build a storage engine built for the cloud. SlateDB is that storage engine. --[Building a Cloud Native LSM on Object Storage by Chris Riccomini & Rohan Desai](https://www.youtube.com/watch?v=8L_4kWhdzNc), P99 CONF 2024

Our vision of, "a storage engine built for the cloud," is an embedded key-value store that...

- uses object storage as the source of truth,
- caches efficiently across all tiers,
- provides transactions and snapshots,
- and is easy to run in the cloud.

## Features

This vision has led us to support the following features in SlateDB:

- **Zero-disk architecture**: SlateDB should be capable of running withhout any disks. Object storage is always the only source of truth.
- **Single-writer**: SlateDB only needs to support one writer process at a time. SlateDB should enforce this property. Applications that need multiple writers should use multiple SlateDB databases as partitions.
- **Multi-reader**: SlateDB allows multiple processes, potentially across multiple machines, to read from a single SlateDB database.
- **Read caching**: SlateDB should cache reads to improve performance and reduce cloud API costs.
- **Snapshot isolation**: Readers should be able to read from a consistent view of the database at a point in time.
- **Transactions**: Writers should be able to write transactions atomically.
- **Pluggable compaction**: Compaction is a never-ending research project with different options suitable for different workloads. SlateDB should have a pluggable compaction layer capable of running as a separate process on a separate machine.

We have deliberately chosen to avoid the following features:

- **A server or network protocol**: SlateDB is an embeddable library only.
- **Online analytics processing (OLAP) and columnar storage**: SlateDB is row based.

## Style

Clean Slate lives in service of our vision. SlateDB is a transactional database, so consistency and durability are paramount. To achieve this, Clean Slate is simple, incremental, and empirical. We make changes incrementally. Changes that add complexity must justify the change with empirical evidence.

Clean Slate manifests itself in many ways. Here are a few examples:

- **We will usually not implement the latest research ideas**. If we do, we will have a strong reason to do so.
- **We will not always prioritize performance**. SlateDB must be fast enough to service the use cases we envision for it. SlateDB does not need to be as fast as possible, especially when it comes at the cost of simplicity.
- **We prioritize readable code** rather than leveraging all language features.
- **We will hard-code some configurations** to keep the number of user-facing options low.
- **We default to copying RocksDB, LevelDB, Badger, or Pebble** whenever possible. They are excellent databases that work empirically well.
- **Our governance model and release process are simple** and can be changed as we see fit.
- **We have a separate Go project** where Go developers are building a Go implementation of SlateDB. This project is part of SlateDB but not goverened by SlateDB's [GOVERNANCE.md] and [CLEAN_SLATE.md].

## References

- [Define use cases, features, and requirements for slatedb](https://github.com/slatedb/slatedb/issues/20)
- [What Istio Got Wrong: Learnings from the Last Seven Years of Service Mesh](https://youtu.be/XW10IpsTmH8?si=8DCg3BdFdGdfbmTe)
- [Tiger Style](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)