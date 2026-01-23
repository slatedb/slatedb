# FlatBuffers Generated Code

This folder contains the generated [flatbuffer](https://flatbuffers.dev/) code. Flat buffer schemas are in the root `schemas` folder.

## How to Generate FlatBuffers Rust Code

To generate `.rs` files from `.fbs` files:

1. Install [flatc](https://github.com/google/flatbuffers) version 24.3.25.
2. Run `flatc -o slatedb/src/generated --rust --gen-all schemas/root.fbs` from the project root.
    - `--gen-all` is required because including other schemas [does not work well](https://github.com/google/flatbuffers/issues/5275).

**NOTE:**
You can install it with `brew install flatbuffers` if you're using Homebrew.
If the installed version is not 24.3.25, might need to do the following:
1. Create a local homebrew tap directory: `mkdir -p <tap dir>/Formula`
2. Make the local homebrew tap directory a git repo:
   3. `cd <tap dir>`
   3. `git init`
2. Copy the formula for the 24.3.25 version from https://github.com/Homebrew/homebrew-core/blob/4089fb01c8b18caa34d29bbeafe561af5fe36aa5/Formula/f/flatbuffers.rb to `<tap-dir>/Formula/flatbuffers.rb`
3. Add the file to the git repo:
   1. `git add .`
   2. `git commit -m "Initial commit"`
4. Create the homebrew tap: `brew tap local/tap  <tap dir>`
5. Install `flatc` from the local formula: `brew install local/tap/flatbuffers`