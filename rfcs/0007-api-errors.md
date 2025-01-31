<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [SlateDB Error API](#slatedb-error-api)
   * [Motivation](#motivation)
   * [Goals](#goals)
   * [Principles](#principles)
   * [Public API](#public-api)

<!-- TOC end -->

# SlateDB Error API

Status: Draft

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

Publicly exposed errors will be maintained in `slatedb::Error`. Each new error type must be documented
in this RFC. All public APIs will document the complete set of errors that may be returned. 

Each exposed error type will expose a generic documentation string which contains a general description
of the error and contains any prescriptive guidance (such as indicating whether a failed operation can
be retried or is fatal).

Each error  type will also expose a custom message field which is used to convey details about the specific
instance of the error that was encountered. Unlike the error type, the message is not part of the 
public API and can be changed without notice. The intent is to pack as much detail into the message
for someone to understand the root cause, and if possible, what they should do to resolve it.

```rust
mod slatedb {
    pub trait Error {
        /// General description of the error and prescriptive actions
        fn doc(&self) -> &str;
        /// Custom message for specific instance details to identify the root cause 
        fn message(&self) -> String;
    }
}
```

The complete set of public error types are listed below:

- `Error::InvalidArgument`: One of the arguments passed by the user is invalid. This must be raised 
before an operation is allowed to produce any side effects. The operation can be retried
once the error has been addressed.
- `Error::IoError`: There was an error accessing storage or other IO resources. The operation
can be safely retried.
- `Error::InternalError`: Reserved for unexpected cases such as invalid internal database states. 
This error should be considered fatal (i.e. the database must be closed).

Public errors can be removed through semantic versioning. Typically, the need to remove an error
suggests that some part of the internal implementation has been inadvertently leaked, so such
cases should be rare if the exposed errors follow the principles above.