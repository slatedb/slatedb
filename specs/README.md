# SlateDB Design Specifications

This directory contains the formal specs for SlateDB protocols using the
[FizzBee model checker](https://fizzbee.io).

## FizzBee

You can try specs without installing anything in the
[FizzBee playground](https://fizzbee.io/play).

For local runs on macOS:

```
brew tap fizzbee-io/fizzbee
brew install fizzbee
```

Prebuilt binaries are also available from the
[latest FizzBee release](https://github.com/fizzbee-io/fizzbee/releases/latest)
for macOS and Linux. After extracting a release archive, run `./fizz` with the
path to a spec. Build-from-source instructions are in the
[FizzBee repository](https://github.com/fizzbee-io/fizzbee).

## Running Specs

With `fizz` on your `PATH`, run:

```
fizz specs/fizzbee/KeyValueStore.fizz
fizz specs/fizzbee/SequencedMetadataBoundary.fizz
```

## Specs

- `fizzbee/KeyValueStore.fizz`: basic write, flush, and read behavior.
- `fizzbee/SequencedMetadataBoundary.fizz`: proof model for the
  bounded sequenced storage GC boundary protocol.

## FizzBee Agent Instructions

Recent FizzBee releases include Agent Skills for writing, checking, debugging,
and model-based testing FizzBee specs. If you installed FizzBee with Homebrew,
install the skills with:

```
fizz install-skills
```

To preview without making changes:

```
fizz install-skills --check
```

Re-run `fizz install-skills` after upgrading FizzBee to refresh the installed
skills and reference docs.
