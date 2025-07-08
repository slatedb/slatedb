# Contributing

SlateDB is an open source project and we welcome contributions. Please open Github issues for bugs, feature requests, or other issues. We also welcome pull requests.

Join our [Discord server](https://discord.gg/mHYmGy5MgA) to chat with the developers.

Please follow the instructions below before making a PR:

- Run `cargo clippy --all-targets --all-features` and `cargo fmt` on your patch to fix lints and formatting issues.
- Run `cargo nextest run --all-features` to check that your patch doesn't break any tests.

## Contributor License Agreement

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

## Project Ideas

Here are some fun projects that would be fun (and useful) to work on:

- Integrate SlateDB into [Kine](https://github.com/k3s-io/kine)
- A Redis-compatible server on SlateDB.
- A `pg_slatedb` PostgreSQL extension.
- A [TiKV](https://github.com/tikv/tikv)-compatible [RawKV](https://tikv.org/docs/dev/develop/rawkv/introduction/) or TxnKV storage layer on SlateDB.
- A multi-writer storage service on SlateDB using [Chitchat](https://github.com/quickwit-oss/chitchat).
- SlateDB bindings in other languages.