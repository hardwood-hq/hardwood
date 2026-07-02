<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

-->
# Reader configuration

**Status: Implemented.**

## Summary

Read-time behaviour knobs live on a dedicated immutable `ReaderConfig` value,
mirroring the writer side's `WriterConfig`. `HardwoodContext` holds only shared,
stateful, lifecycle-bound runtime resources — the decode executor, the libdeflate
pool, and the decompressor factory. The two concerns are separate objects because
they have different natures and lifetimes.

## Boundary

| Object | Nature | Lifetime | Examples |
|--------|--------|----------|----------|
| `HardwoodContext` | Stateful, `AutoCloseable`, expensive to build, meant to be shared | Spans many reads; owns a thread pool | executor, libdeflate pool, decompressor factory |
| `ReaderConfig` | Immutable value, cheap, no lifecycle | Per read | fixed-size-list fast path toggle |
| `WriterConfig` | Immutable value, cheap, no lifecycle | Per write | page/row-group targets, `created_by` |

The rule: **runtime resources on the context, per-operation behaviour on the
config value.** Sizing the executor (`HardwoodContext.create(int threads)`) is a
resource concern and stays on the context; toggling a decode path is behaviour and
lives on `ReaderConfig`.

### Why not on the context

Welding a behaviour knob to the resource holder forces the two to co-vary. To read
one file with the fast path on and another with it off in the same process, a knob
on the context would require a *second context* — i.e. a second thread pool — to
flip one boolean. A `ReaderConfig` passed per read lets a single shared context
serve both.

### Why string-keyed

`ReaderConfig` knobs are set through a string-keyed option map
(`option(String, String)`), not typed setters. The knobs it currently carries are
transitional escape hatches: a flag exists only until the fast path it gates is
unconditionally trusted, then it is retired. A string key can be retired without
changing the type — old callers passing the key still compile; the key is simply
no longer consulted. A typed setter, by contrast, is a compile-time break to
remove.

The cost of string keys — a mistyped key silently taking the default — is the
fail-early concern the project's conventions forbid. It is handled when the reader
consumes the config (`ParquetFileReader.open`): an unrecognised key is ignored but
logged at `WARNING`, so a typo surfaces instead of silently no-op-ing.

`ReaderConfig` itself is a generic string bag — it exposes only the option map
([#options]) and carries no per-flag API. The option keys, their defaults,
resolution, and the set of recognised keys are private to `ParquetFileReader`,
which reads the map once at `open` time, resolves each recognised key to a plain
value, and threads that value to the decode workers. Keeping that knowledge
private is what lets a flag be retired without any change to a shared or public
surface.

## API

`ReaderConfig` is an immutable value, obtained via `ReaderConfig.defaults()` or
`ReaderConfig.builder()`:

```java
ReaderConfig config = ReaderConfig.builder()
        .option("hardwood.fixed-list-fast-path", "false")
        .build();

try (HardwoodContext context = HardwoodContext.create();
     ParquetFileReader reader = ParquetFileReader.open(inputFile, context, config)) {
    // one context (one thread pool) can back reads with different ReaderConfigs
}
```

`ParquetFileReader.open` / `openAll` gain an overload taking a `ReaderConfig`; the
existing overloads use `ReaderConfig.defaults()` (fast path on). The reader stores
the config and threads it to the column/row reader factories, which resolve the
concrete flags when constructing decode workers.

## Fixed-size-list fast path flag

The `hardwood.fixed-list-fast-path` option (default `"true"`) gates the
fixed-size-list read fast path (see
[FIXED_SIZE_LIST_FASTPATH.md](FIXED_SIZE_LIST_FASTPATH.md)), resolved privately in
`ParquetFileReader`. Setting it to `"false"` forces the general nested-decode path
for every column. It is a transitional escape hatch: once the fast path is
unconditionally trusted, the key drops out of the recognised set (callers still
passing it get the unknown-key warning) and the private resolver and decode branch
are removed — the public `ReaderConfig` surface never changes.

## Scope

Only transitional reader flags belong in the option map. Permanent, stable read
knobs that need a typed contract — for example the records-per-batch size,
presently exposed as `ColumnReaderBuilder.batchSize(int)` — stay typed and are not
folded into the string map. `ReaderConfig` may grow such typed setters alongside
`option(...)`, the same way `HardwoodContext` pairs the typed `threads(int)` with
shared runtime state; that pairing is out of scope here.
