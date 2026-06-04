<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# The Concurrency Model

Hardwood is multi-threaded at its core: a single reader, driven by a single consumer thread,
keeps a pool of worker threads busy decoding ahead of where the consumer is reading.

For the knobs themselves — pool sizing and the JFR events that expose pipeline behavior — see
[Configuration](../reference/configuration.md) and
[Read Multiple Files as One Dataset](../how-to/multi-file.md).

## Why concurrency is built in

The Parquet layout makes parallelism natural: a column chunk is a sequence of independently
compressed pages (see [How a Parquet File Is Laid Out](parquet-layout.md)). Decompressing and
decoding a page is CPU-bound work that doesn't depend on its neighbors, so pages can be worked
on concurrently. Hardwood exploits this without asking the caller to manage threads — you write
an ordinary single-threaded read loop, and the parallelism happens underneath it.

## The shared pool: `Hardwood` and `HardwoodContext`

Worker threads live in a pool owned by a context, not by individual readers. There are two ways
that context comes into being:

- **Implicit.** `ParquetFileReader.open(InputFile)` creates and owns a context sized to the
  available processors. Closing the reader shuts it down.
- **Explicit.** Create a `Hardwood` (via `Hardwood.create()`) or a `HardwoodContext` (via
  `HardwoodContext.create(threadCount)`) yourself and pass it to the reader. The pool then
  outlives any one reader and is shared across all readers opened against it.

Share one context across many reads. The pool is the expensive, reusable resource; readers are
cheap and short-lived. This matters most when reading many files — see below.

## The assembly pipeline

For a single read, the reader runs an **assembly pipeline** ahead of the consumer. While
your loop is processing the current batch (or row), pool threads are already decompressing and
decoding the pages that feed the next batches, queuing the results. The consumer thread
generally pulls ready-to-use decoded data instead of blocking on decode.

Two [JFR events](../reference/configuration.md#jfr-java-flight-recorder-events) make the pipeline visible
when you profile:

- **`BatchWait`** — the consumer blocked waiting for the pipeline. Frequent or long waits mean
  decode (or I/O) isn't keeping up with consumption.
- **`PrefetchMiss`** — a needed page wasn't prefetched in time and had to be decoded
  synchronously on the consumer's path.

## Cross-file prefetching

When a reader spans multiple files (via `Hardwood.openAll(...)`), prefetching crosses file
boundaries: as the pages of file _N_ run low, pages of file _N+1_ are already being fetched and
decoded. The transition between files doesn't stall the consumer. This is the main reason to open
a multi-file reader rather than looping over single-file readers yourself — and the reason the
shared pool matters, since all the files draw on the same workers. See
[Read Multiple Files as One Dataset](../how-to/multi-file.md).

## What this means for your code

- **Drive one reader from one thread.** A `RowReader` / `ColumnReader` / `ColumnReaders` instance
  is a stateful cursor meant to be advanced by a single consumer thread. The concurrency is
  *internal* — you do not, and should not, call `next()` / `nextBatch()` on the same reader from
  multiple threads. This restricts advancing the cursor, not the data it returns: a `ColumnReader`
  allocates fresh batch arrays on every `nextBatch()` and never reuses them later, so you can fan a
  batch's arrays out to other threads for processing while the consumer thread moves on (see
  [Column-Oriented Reading](../how-to/column-reader.md#retaining-and-handing-off-batch-arrays)).
- **For your own parallelism, read in parallel at the file or row-group grain.** To use more
  than one consumer thread, give each its own reader over a different file, or partition one file
  across readers by byte range with
  [split-aware reading](../how-to/query-controls.md#split-aware-reading). Have them share a single
  `HardwoodContext` so they draw worker threads from one pool rather than each spinning up its
  own.
- **Size the pool to the workload, not the file.** `HardwoodContext.create(n)` sets the number of
  threads that decompress and decode pages — the bulk of the CPU — so `n` is the main throughput
  and CPU dial. The default (available processors) suits a process whose main job is reading
  Parquet; lower it when Hardwood shares the machine with other heavy work. Two lighter stages —
  fetching page bytes and assembling decoded values into batches — run on the JVM's virtual-thread
  carriers rather than this pool, so `n` does not bound them; for strict CPU isolation on a shared
  machine, also cap the carriers with `-Djdk.virtualThreadScheduler.parallelism`.

## Further reading

- [How a Parquet File Is Laid Out](parquet-layout.md) — why pages are independent units of work.
- [Configuration](../reference/configuration.md) — JFR events, plus SIMD and libdeflate acceleration of the
  decode itself.
- [Read Multiple Files as One Dataset](../how-to/multi-file.md) — the cross-file API and pool
  sharing in practice.
