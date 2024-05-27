This folder contains the generated [flatbuffer](https://flatbuffers.dev/) code.</br>
Flat buffer schemas are in `../fbs/` folder.</br>

Steps to generate .rs files from .fbs files.

1. Build and get the code generation tool [flatc](https://github.com/google/flatbuffers)
2. Run `flatc --rust ../fbs/sst.fbs` from this folder.
3. Add `#![allow(warnings)]` to the top of the generated file. Generated .rs file has warnings about unused imports and dead code.
4. Run `cargo fmt`. Ignoring the file with rustfmt.toml does not work.