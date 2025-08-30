---
title: Introduction
description: Learn about SlateDB, an embedded database built on object storage
---

SlateDB is an embedded storage engine built as a [log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). Unlike traditional LSM-tree storage engines, SlateDB writes all data to object storage.

## Vision

Object storage is an amazing technology. It provides highly-durable, highly-scalable, highly-available storage at a great cost. And recent advancements have made it even more attractive:

* Google Cloud Storage supports multi-region and dual-region buckets for high availability.
* All object stores support compare-and-swap (CAS) operations.
* Amazon Web Service's S3 Express One Zone has single-digit millisecond latency.

We believe that the future of object storage are multi-region, low latency buckets that support atomic CAS operations. Inspired by [The Cloud Storage Triad: Latency, Cost, Durability](https://materializedview.io/p/cloud-storage-triad-latency-cost-durability), we set out to build a storage engine built for the cloud. SlateDB is that storage engine.

## Features

* **Zero-Disk architecture**: SlateDB is easy to operate. It runs as an in-process storage engine with no local state, no control plane, and no replication protocol.
* **Single-writer**: SlateDB is designed for a single writer. Partitioning can easily be built on top of SlateDB since fencing is supported.
* **Multiple-readers**: Multiple readers on different nodes can all read the same SlateDB database.
* **Read caching**: SlateDB supports in-memory and (optional) on-disk read caching to reduce latency and API cost.
* **Snapshot isolation**: SlateDB supports snapshot isolation, which allows readers and writers to see a consistent view of the database.
* **Transactions**: Transactional writes are supported.
* **Object store persistence**: SlateDB writes all data to object storage, which means SlateDB has the same durability, scalability, and availability as your object store.
* **Writer fencing**: SlateDB enforces writer fencing. Zombie writer processes are detected and prevented from writing to the database.
* **Pluggable compaction**: SlateDB supports pluggable compaction, so you can use the compaction strategy that fits your needs.

:::note

Snapshot isolation and transactions are planned but not yet implemented.

:::

## Use Cases

SlateDB is a great fit for use cases that are tolerant to 50-100ms write latency, are tolerant to data loss during failure, or are willing to pay for frequent API PUT calls. Such use cases include:

* Stream processing
* Serverless functions
* Durable execution
* Workflow orchestration
* Durable caches
* Data lakes
