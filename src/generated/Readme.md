This folder contains the generated [flatbuffer](https://flatbuffers.dev/) code.</br>
Flat buffer schemas are in `../../schemas` folder.</br>

Steps to generate .rs files from .fbs files.

1. Install [flatc](https://github.com/google/flatbuffers). If you're using Homebrew, you can install it with `brew install flatbuffers`.
2. Run `flatc --rust ../../schemas/sst.fbs` from this folder.
