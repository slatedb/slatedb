# Tracking Timestamps for Sequence Numbers

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Motivation](#motivation)
- [Goals](#goals)
- [Public API](#public-api)
- [Implementation](#implementation)
- [Rejected Alternatives](#rejected-alternatives)

<!-- TOC end -->

Status: Draft

Authors:

- [Almog Gavra](https://github.com/agavra)

## Motivation

Standardizing on sequence numbers for tracking progress in SlateDB makes it
straightforward to reason about "when" something happened. Users, however, may
want to perform certain operations based on time instead of sequence number. For
example, they may want to fetch a snapshot associated with a particular system
time or issue a scan for all records committed before a certain time.

This RFC proposes tracking timestamps for sequence numbers so users can obtain a
rough approximation of the mapping between sequence numbers and (system)
timestamps.

## Goals

- Use a predictable, fixed amount of memory for the sequence tracker (therefore bounded storage space)
- Expose an API for determining the timestamp for a given sequence number
- Expose an API for determining the sequence number for a given timestamp

To reduce storage overhead, this RFC does not propose an exact mapping between sequence numbers and timestamps.

## Public API

The API will be exposed through the CLI and involve changes to the manifest (which is implicitly a public API).

### CLI

```rs
/// Rounding behavior for non-exact matches in sequence-timestamp lookups.
#[allow(dead_code)]
#[derive(PartialEq)]
pub(crate) enum FindOption {
    /// Round up to the next higher value when no exact match is found.
    RoundUp,
    /// Round down to the next lower value when no exact match is found.
    RoundDown,
}

pub(crate) enum CliCommands {
    // ...

    /// Fetch an approximate (system) timestamp for a given sequence number.
    /// SlateDB tracks a mapping between sequence numbers and timestamps and
    /// maintains it with a lossy mechanism (over time the granularity of the
    /// tracked mapping degrades).
    GetTimestampForSeq {
        /// The sequence number to fetch the timestamp for.
        seq: u64,

        /// The find option to use when fetching the timestamp.
        find_opt: FindOption,
    }

    /// Fetch an approximate sequence number for a given timestamp.
    /// SlateDB tracks a mapping between sequence numbers and timestamps and
    /// maintains it with a lossy mechanism (over time the granularity of the
    /// tracked mapping degrades).
    GetSeqForTimestamp {
        /// The timestamp to fetch the sequence number for.
        ts: DateTime<Utc>,

        /// The find option to use when fetching the sequence number.
        find_opt: FindOption,
    }
}
```

### Manifest

```fbs
table ManifestV1 {
    // ...

    // The sequence tracker is a custom serialized type that is used to track
    // a (lossy) mapping between sequence numbers and timestamps. The serialization
    // format is described in RFC-0012.
    seq_tracker: [ubyte];
}
```

We choose to represent the sequence tracker as a custom serialized type instead
of a flatbuffer type to reduce manifest storage overhead. It uses Gorilla
encoding (delta-of-deltas) for both keys (sequence numbers) and values
(timestamps).

## Implementation

The high-level approach for implementing the tracker is to store the mapping in
the manifest and update it whenever a memtable is flushed or SlateDB cleanly
shuts down.

To reduce stored data we use two tactics:

1. The encoding uses [Gorilla Encoding](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf),
    which is an efficient encoding for timeseries data.
2. The granularity is decimated over time (older seq-ts pairs are downsampled)

### Serialization

Physically, the sequence tracker is serialized as two Gorilla-encoded arrays of
equal length (one for sequence numbers and one for timestamps). The arrays are
broken up logically (though not physically) into tiers, and within a tier the
timestamps expected to be evenly spaced (though this assumption is not
enforced).

The Gorilla encoding scheme means sequence numbers and timestamps will take up
between 1 and 32 bits depending on SlateDB insertion regularity (typically
closer to 7 bits each).

The ultimate serialization format is:
```
+-------------------------------------+
| Version (u8) | Length (u32)         |
|-------------------------------------|
| Sequence Numbers (Gorilla Encoding) |
|-------------------------------------|
| Timestamps (Gorilla Encoding)       |
+-------------------------------------+
```

Since both sequence numbers and timestamps are monotonically increasing, we can
represent this in memory as two sorted arrays and use simple binary search on
either array for bi-directional lookup:

```rs
struct SequenceTracker {
    sequence_numbers: Vec<u64>, // sorted
    timestamps: Vec<DateTime<Utc>>, // sorted
    capacity: u32, 
}
```

### Tiering & Downsampling Strategy

To meet the requirement of predictable, fixed storage for the sequence tracker,
we need to downsample data over time.

When the sequence tracker is initialized, it will be configured with a fixed
number of mappings to store (measured in key-value pairs). Once that number is
reached, the sequence tracker will downsample by removing every other entry. To
avoid aggressive downsampling, the sequence tracker will only track a timestamp
mapping no more than every 30 seconds.

The downsampling strategy will cause exponentially decreasing granularity over
time. For example, if the sequence tracker is configured with 1024 mappings, the
first time it fills up the data will represent a timeframe of `1024 * 30 seconds
= 512 minutes`. When downsampling kicks in, this time window will cover the
first 512 entries. The next time the sequence tracker fills up, this same data
will be covered by the first 256 entries (and so on).

## Rejected Alternatives

- **Storing timestamps in each row**: It is possible to store the system
    timestamp with each row in SlateDB alongside the logical timestamp. This would
    allow exact lookup of timestamps for sequence numbers. However, this would
    require significant additional storage space and would not be easy to retrieve
    using an admin API, limiting applicable use cases.