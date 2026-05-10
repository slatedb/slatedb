# SlateDB Design Specifications

This directory contains the formal specs for SlateDB protocols using the
[FizzBee model checker](https://fizzbee.io).

More about the tool and the instructions:

- [Run the model checker][fizz]
- [FizzBee Homebrew package](https://github.com/fizzbee-io/homebrew-fizzbee)
- [FizzBee playground](https://fizzbee.io/play)

Once installed from source and set the PATH, you can run with

```
fizz specs/kvstore/KeyValueStore.fizz
fizz specs/gc-boundary/SequencedMetadataBoundary.fizz
```

## Specs

- `kvstore/KeyValueStore.fizz`: basic write, flush, and read behavior.
- `gc-boundary/SequencedMetadataBoundary.fizz`: proof model for the
  bounded sequenced storage GC boundary protocol.

## FizzBee Agent Instructions

FizzBee agent instructions are located in
`.github/instructions/fizzbee.instructions.md`. If you wish, you may include
them in your IDE's instruction set as well.

[fizz]: https://github.com/fizzbee-io/fizzbee#run-a-model-checker
