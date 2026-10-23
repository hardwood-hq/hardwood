<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# The Concurrency Model

Hardwood is multi-threaded at its core: a single reader, driven by a single consumer thread,
keeps a pool of worker threads busy decoding ahead of where the consumer is reading, and a
writer encodes the column chunks of each row group on such a pool.

## Why concurrency is built in

The Parquet layout makes parallelism natural: a column chunk is a sequence of independently
compressed pages (see [How a Parquet File Is Laid Out](parquet-layout.md)). Decompressing and
decoding a page is CPU-bound work that doesn't depend on its neighbors, so pages can be worked
on concurrently. Hardwood exploits this without asking the caller to manage threads: you write
an ordinary single-threaded read loop, and the parallelism happens underneath it.

## The shared pool: `Hardwood` and `HardwoodContext`

Worker threads live in a pool owned by a context, not by individual readers.
`ParquetFileReader.open(InputFile)` creates and owns a context sized to the available processors,
and closing the reader shuts it down. An explicitly created `HardwoodContext` outlives any one
reader and is shared across all readers opened against it; the caller closes it. Closing a
context lets the tasks already submitted to its pool finish, waiting up to five seconds for them. See
[Read Multiple Files as One Dataset](../how-to/multi-file.md) for creating and passing one.

Share one context across many reads. The pool is the expensive, reusable resource; readers are
cheap and short-lived. This matters most when reading many files, as described below.

## The assembly pipeline

For a single read, the reader runs an **assembly pipeline** ahead of the consumer. While
your loop is processing the current batch (or row), pool threads are already decompressing the
pages that feed the next batches and decoding their values straight into those batches. The consumer thread
generally pulls ready-to-use decoded data instead of blocking on decode.

Frequent or long **`BatchWait`** [JFR events](../reference/configuration.md#jfr-java-flight-recorder-events)
mean decode (or I/O) isn't keeping up with consumption.

## Cross-file prefetching

When a reader spans multiple files (via `Hardwood.openAll(...)`), prefetching crosses file
boundaries: as the pages of file _N_ run low, pages of file _N+1_ are already being fetched and
decoded. A read of files not in memory, such as files on S3, also opens several files beyond
_N+1_: their footers and the first data of each are fetched while the read works on earlier files,
within a share of the heap the context holds for reading ahead. A read with a row limit opens only
file _N+1_ ahead. This is the main reason to open
a multi-file reader rather than looping over single-file readers yourself, and the reason the
shared pool matters, since all the files draw on the same workers. See
[Read Multiple Files as One Dataset](../how-to/multi-file.md).

## Writing

A writer copies a batch's values into the open row group in parallel: the pool threads of a `HardwoodContext` and the calling thread each take the next column not yet claimed, and `writeBatch` returns once every column holds its values. When the row group is flushed, its column chunks are encoded and compressed in parallel the same way, a chunk at a time, and written in schema order. The file is the same as one written on a single thread. A writer opened without a context creates its own for the duration of the write; `ParquetFileWriter.create(out, schema, config, context)` uses yours, so writers and readers can draw on one pool.

A schema of many columns spreads both the copying and the flush across all the threads. A schema of a few columns is bounded by its most expensive column while values are copied, since a column is copied by one thread. At flush, a chunk is encoded by one thread, but under a compressing codec such as `ZSTD` a pool thread with no chunk left compresses pages of the chunks still being encoded, so a schema of a few columns, or of one, spreads its flush across the threads too, and those threads stay with the flush until its last chunk is encoded; uncompressed, the flush of a few columns is bounded by its most expensive column chunk. Between two batches, the pool threads that copied values wait for the next batch for at most a millisecond before returning to the pool, and they return at once when a row group is flushed or the writer is closed. While batches keep arriving, each such thread goes to the back of the pool's queue every millisecond, so other work queued on a shared context runs in between.

## A context of zero threads

`HardwoodContext.create(0)` creates a context without a pool. A read on it runs on the thread that calls the reader: when `hasNext()` or `nextBatch()` finds no batch ready, that call walks the page headers, decompresses and decodes pages until the batch is complete, and returns it. Nothing decodes while your loop works on a batch, so a read uses one core, the one your loop runs on.

This suits a process that runs one pipeline per core: N readers, each driven by its own thread, use N cores, with no worker threads competing with them for the cores, caches and memory bandwidth. It also suits an environment that cannot start threads. One context of zero threads serves readers on several threads at once, each read running on the thread that drives it.

A read of local files, or of files in memory, starts no thread. A read of other files, such as files on S3, fetches bytes ahead of the read on virtual threads, which wait for the requests and do no other work. These virtual threads run on the carrier threads of the JVM's virtual-thread scheduler, which the whole process shares and the context does not bound: the CPU a request takes inside the `InputFile`, such as decrypting TLS or copying the response, runs on those carriers. While a page's bytes are in flight, the read's thread waits for them in `hasNext()` or `nextBatch()`, which a `BatchWait` event records. The reads that plan a row group (the next file's footer, its page index, Bloom filters and dictionaries) run on the read's thread, and no `BatchWait` event records them. Drive such a read from a platform thread: a virtual thread holds its carrier for the length of each decode. A writer on such a context copies, encodes and compresses on the thread that calls it.

## Guidelines for your code

- **Drive one reader from one thread.** A `RowReader` / `ColumnReader` / `ColumnReaders` instance
  is a stateful cursor meant to be advanced by a single consumer thread. The concurrency is
  *internal*: do not call `next()` / `nextBatch()` on the same reader from
  multiple threads. The batch arrays a `ColumnReader` returns may be handed to other threads while
  the consumer moves on (see
  [Column-Oriented Reading](../how-to/column-reader.md#retaining-and-handing-off-batch-arrays)).
  A `ParquetFileWriter` and its views are likewise used from one thread at a time.
- **Read on your own threads, never on the pool's.** A read waits for work only the pool's
  threads run, so a reader built in a task submitted to its context's `executor()` would wait
  for the thread it holds; building one there fails with an `IllegalStateException`.
- **For your own parallelism, read in parallel at the file or row-group grain.** To use more
  than one consumer thread, give each its own reader over a different file, or partition one file
  across readers by byte range with
  [split-aware reading](../how-to/query-controls.md#split-aware-reading). Have them share a single
  `HardwoodContext` so they draw worker threads from one pool rather than each spinning up its
  own.
- **Size the pool to the workload.** `HardwoodContext.create(n)` sets the number of
  threads that decompress and decode pages, and that encode and compress column chunks on write, which is the bulk of the CPU, so `n` is the main throughput
  and CPU dial. The default (available processors) suits a process whose main job is reading
  Parquet; lower it when Hardwood shares the machine with other heavy work. Lower it as well when
  your read loop spends longer per batch than the pool takes to decode it: the read then goes at
  the pace of your loop, and the threads beyond what it can take in compete with it for cores,
  shared caches and memory bandwidth, so such a read can finish sooner, and with less CPU, on 2 to 4
  threads than on the default. Measure the read at a few values of `n` to find its best. Every page a read
  decodes, dictionary pages included, is decoded on these `n` threads. The same threads walk each
  column's pages, parse the footer of every file after the first and the page indexes of each row
  group, and scan a row group's dictionaries for a filter's values. The bytes of a file not in
  memory are fetched on the JVM's virtual threads. The first file's footer is parsed on the thread
  that opens the reader. With `n = 0`, all of that work runs on your loop's thread instead
  ([A context of zero threads](#a-context-of-zero-threads)).

## Further reading

- [How a Parquet File Is Laid Out](parquet-layout.md) — why pages are independent units of work.
- [Configuration](../reference/configuration.md) — JFR events, plus SIMD and libdeflate acceleration of the
  decode itself.
- [Read Multiple Files as One Dataset](../how-to/multi-file.md) — the cross-file API and pool
  sharing in practice.
- [Concurrent Column Consumer](https://github.com/hardwood-hq/hardwood-examples/tree/main/concurrent-column-consumer)
  — a runnable example that fans each batch's per-value work across a thread pool, with a
  single-threaded baseline to show the gains.
