# Contributing

SlateDB is an open source project and we welcome contributions. Please open Github issues for bugs, feature requests, or other issues. We also welcome pull requests.

Join our [Discord server](https://discord.gg/mHYmGy5MgA) to chat with the developers.

Please follow the instructions in the [pull request template](.github/PULL_REQUEST_TEMPLATE.md).

If you plan on making a substantial change, please follow these steps:

1. Open a Github issue
2. If requested, write an RFC
3. Submit incremental PRs that add functionality while moving us toward the goal of the RFC/issue
4. If you feel the need to submit a large PR, submit it as a draft with a bullet-point list of steps to break it into smaller pieces. We will not accept large PRs.

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