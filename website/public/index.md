# SlateDB

> SlateDB is an embedded key-value storage engine built on object storage.

SlateDB is an open-source (Apache-2.0) embedded key-value database, implemented
as an LSM tree, that depends on object storage alone for durability — Amazon S3,
Google Cloud Storage, Azure Blob Storage, MinIO, and Cloudflare R2. It is built
in async Rust with bindings for Rust, Go, Java, Node, and Python.

It is the natural, object-native successor to RocksDB. "Diskless" systems that
delegate durability to object storage are the future of database systems: the
economics are phenomenal, object storage provides 99.999999999% durability and
handles replication for you, and data written once can be read by an arbitrary
number of readers without additional ETL. SlateDB brings those properties to
online, low-latency workloads.

## When to use SlateDB

SlateDB is a **library**, not a server or a hosted service. It ships as an
embedded engine with no HTTP server; you link it into your own application and
it communicates directly with the object store you configure.

You can use it as:

- The storage core inside any data system (e.g.  database, cache, stream
  processor, or workflow engine) you are building.
- A cheap, elastic, object-native replacement for local-disk LSM engines like
  RocksDB, WiredTiger, or Pebble.
- Durable state for stateless or serverless compute where attaching and
  replicating local disks is impractical.
- Single-writer, multi-reader deployments that scale reads independently of
  writes over a shared bucket.

## When not to use SlateDB

- You need single-digit-microsecond reads/writes. Object storage request
  latencies are an order of magnitude higher (~50–100ms per request); SlateDB
  batches writes and caches reads to amortize this, but it does not match a
  local NVMe engine on raw latency.
- You need SQL, a query planner, or a relational schema — SlateDB is a
  key-value / LSM engine that exposes a small byte-oriented API and does not
  enforce schemas or serialization.

## Key features

- **Object-store native** — durability comes from object storage alone; no
  disk of record and no replication for you to manage. A local disk is
  optional and used only as cache.
- **Cheap and durable** — inherits object storage's ~11 nines of durability at
  a fraction of the cost of replicated block storage or NVMe.
- **Transactions & snapshot isolation.**
- **Single-writer, multi-reader** — open as many readers as you want against
  the same bucket, isolated from the writer.
- **Checkpoints & forks** — O(1) branching by marking a manifest as retained.
- **Rescaling via views** — split a database by key range as an O(1) manifest
  view instead of copying data.
- **Pluggable, distributed compaction** — run compaction on separate machines
  so it never contends with production traffic.
- **Multi-language** — async Rust core with Go, Java, Node, and Python bindings.

## Who uses it

SlateDB is used in production by Dropbox, ZeroFS, HelixDB, Opendata, and others.
It is nearing its 1.0 release.

## Get started

- Introduction: https://slatedb.io/docs/get-started/introduction/
- Quick Start: https://slatedb.io/docs/get-started/quickstart/
- FAQ: https://slatedb.io/docs/get-started/faq/
- Introducing SlateDB (blog): https://slatedb.io/blog/introducing-slatedb/

## Documentation for LLMs and agents

- `/llms.txt` — index of the documentation with a project summary and use cases.
- `/llms-full.txt` — the full documentation concatenated as markdown.
- `/agents.md` — guidance for AI coding agents on how to use SlateDB.
- Every docs page is also available as clean markdown at a sibling `.md` URL,
  e.g. `/docs/get-started/quickstart.md`.

## Links

- Website: https://slatedb.io
- GitHub: https://github.com/slatedb/slatedb
- Rust API docs: https://docs.rs/slatedb
- crates.io: https://crates.io/crates/slatedb
- Python docs: https://slatedb.readthedocs.io/
- Discord: https://discord.gg/mHYmGy5MgA
