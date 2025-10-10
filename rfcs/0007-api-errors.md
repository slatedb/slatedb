# SlateDB Error API

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [SlateDB Error API](#slatedb-error-api)
   * [Motivation](#motivation)
   * [Goals](#goals)
   * [Principles](#principles)
   * [Public API](#public-api)

<!-- TOC end -->

Status: Accepted

Authors:

* [Jason Gustafson](https://github.com/hachikuji)

## Motivation

Errors exposed through a public API are part of the public API. Users depend on clearly
documented errors to drive failure handling logic and we cannot change them without considering
how it affects compatibility. It is important to have common agreement among contributors
about the guidelines to expose new public errors.

## Goals

The goal of this RFC is to establish a set of principles for exposing public errors
and a framework for maintaining them.

## Principles

- **Errors should not expose implementation.** The intent is to reserve 
enough space for the implementation to change without affecting the public API.
- **Errors should be prescriptive.** Ideally it should be clear to the user what they need to do
to resolve an issue. For example, we should indicate whether a failure may be transient and
an operation can be retried. It may not always be possible to have clear guidance for 
unexpected errors, but ideally we can at least state whether the error is fatal.
- **Prefer coarse error types.** A useful criteria to decide whether a new error type is needed 
is whether the user would handle it differently than one of the existing types. 
- **Use rich error messages.** Pack as much information as is needed to diagnose a problem
into the error message, which will not be considered part of the public API.

## Public API

All public APIs will document the complete set of errors that may be returned.

All public errors are exposed through the `slatedb::Error` struct. This struct includes an
enum describing the kind of error that it represents, `slatedb::ErrorKind`. New error
representations can be added to `slatedb::ErrorKind` only when the current representations
don't cover a general problem in the database.

Each exposed error type will expose documentation which contains a general description
of the error and contains any prescriptive guidance (such as indicating whether a failed 
operation can  be retried or is fatal).

Each error type will also expose a custom message field which is used to convey details about the specific
instance of the error that was encountered. Unlike the error type, the message is not part of the 
public API and can be changed without notice. The intent is to pack as much detail into the message
for someone to understand the root cause, and if possible, what they should do to resolve it.

New errors can be added by updating this RFC. Existing errors can be removed through semantic 
versioning. Typically, the need to remove an error suggests that some part of the internal 
implementation has been inadvertently leaked, so such cases should be rare if the exposed 
errors follow the principles above.

These are the currently supported `slatedb::ErrorKind`:

```rust
/// Represents the reason that a database instance has been closed.
#[derive(Debug, Clone, Copy)]
pub enum CloseReason {
    /// The database has been shutdown cleanly.
    Clean,

    /// The current instance has been fenced and is no longer usable.
    Fenced,

    /// One or more background tasks panicked.
    Panic,
}

/// Represents the kind of public errors that can be returned to the user.
///
/// These are less specific and more prescriptive. Application developers or operators must
/// decide how to proceed. Adding new [ErrorKind]s requires an RFC, and should happen very
/// infrequently.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum ErrorKind {
    /// A transaction conflict occurred. The transaction must be retried or dropped.
    Transaction,

    /// The database has been shutdown. The instance is no longer usable. The user must
    /// create a new instance to continue using the database.
    Closed(CloseReason),

    /// A storage or network service is unavailable. The user must retry or drop the
    /// operation.
    Unavailable,

    /// User attempted an invalid request. This might be:
    ///
    /// - An invalid configuration on initialization
    /// - An invalid argument to a method
    /// - An invalid method call
    /// - A user-supplied plugin such as the compaction schedule supplier or logical clock
    ///   failed.
    ///
    /// The user must correct the code, configuration, or argument and retry the operation.
    Invalid,

    /// Persisted data is in an unexpected state. This could be caused by:
    ///
    /// - Temporary or permanent machine or object storage corruption
    /// - Incompatible file format versions between clients
    /// - An eventual consistency issue in object storage
    ///
    /// The user must fix the data, use a compatible client version, retry the operation,
    /// or drop the operation.
    Data,

    /// An unexpected internal error occurred. Users should not expect to see this error.
    /// Please [open a Github issue](https://github.com/slatedb/slatedb/issues/new?template=bug_report.md&title=Internal+error+returned)
    /// if you receive this error.
    Internal,
}
```

### Constructing public errors

The `slatedb::Error` struct includes functions to construct public errors. Each one of those
functions is identified by the name variant that they represent. `slatedb::Error::system` creates
`slatedb::ErrorKind::System` errors and so on.

If you want to propagate another error into a public error, you have to use the `with_source` function
after constructing an error. For example if you want to propagate an `std::io::IOError`:

```rust
let error = slatedb::Error::unavailable("failed connection".to_string()).with_source(my_ioerror)
```

In general, you should not construct public errors directly. It's recommended to use the internal `slatedb::SlateDBError`
which is then transformed into a public error. This gives you more flexibility to construct your errors.

## Internal errors vs External errors

Internal errors can continue being described through `slatedb::SlateDBError`. This error
type can be transformed into an `slatedb::Error` through the `From` trait.

### Internal errors message format

`SlateDbError` uses [thiserror](https://docs.rs/thiserror) to define internal errors. This crate gives you flexibility
in the formatting of error messages, as well as other errors that are bubbling up from the database.

We follow some conventions to generate message to ensure they are all consistent. This is the list of conventions:

- Try to be specific when you create an internal error. Instead of creating an error that can take multiple messages, create different errors that can be formatted following these rules.
- Write messages in lowercase: `read channel error` instead of `Read Channel Error`.
- If an error message includes variables, they're added at the end of the message in KEY=`VALUE` format. `unexpected range key. key=\`foo\`, range=\`[..]\`` instead of `unexpected key \`foo\` in range \`[..]\``.
- Errors that propagate other errors don't include the propagated errors in the message. They will be included automatically when the internal error is translated into a public error. See the example below:

```rust
// DO THIS
enum SlateDBError {
  #[error("io error")]
  IOError(#[from] std::io::Error)
}

// **DO NOT** DO THIS
enum SlateDBError {
  #[error("io error: #{0}")]
  IOError(#[from] std::io::Error)
}
```
