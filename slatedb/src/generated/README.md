# FlatBuffers Generated Code

This folder contains the generated [flatbuffer](https://flatbuffers.dev/) code. Flat buffer schemas are in the root `schemas` folder.

## How to Generate FlatBuffers Rust Code

To generate `.rs` files from `.fbs` files:

1. Install [flatc](https://github.com/google/flatbuffers).
2. Run `flatc -o slatedb/src/generated --rust --gen-all schemas/manifest.fbs` from the project root.
    - `--gen-all` is required because including other schemas [does not work well](https://github.com/google/flatbuffers/issues/5275).
_NOTE: You can install it with `brew install flatbuffers` if you're using Homebrew._