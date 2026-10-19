# Plan: reads on the calling thread alone

Tracking issue: #1022.

A read today runs on three kinds of thread besides its consumer: a virtual-thread fetcher per decoded column, page tasks on the context's decode pool, and speculative tasks on the common `ForkJoinPool` ([READ_PIPELINE.md](../_designs/READ_PIPELINE.md#thread-inventory)). `HardwoodContext.create(1)` bounds the decode pool, not the threads: a one-thread read still hands every page across two threads and runs the consumer, a fetcher per column and a decode thread at once. #1022 asks for a mode that does all of a read's work on the thread iterating it, so that N independent single-threaded pipelines scale with N cores and no background work competes with them. The WASM build of `dive` needs the same thing, since a browser gives the read one thread (`801-wasm-webimage-spike` removed the async paths by hand).

This plan describes what an inline read takes, the prototype on branch `1022-single-threaded-mode`, its measurements, the proposed public shape and the stages.

## What an inline read is

The consumer drives the read from its poll. When `BatchExchange.poll()` finds no batch, it takes one step of the read (`ReadScheduler.step()`) and looks again, until a batch is published or the stream ends. A step is one of:

| Step | When | Runs |
|---|---|---|
| Run a page task | the scheduler's own queue holds one | the oldest task, in the order the scheduler submitted them |
| Fetch a page | no task is queued | `ScheduledColumn.fetchStep()` for the column, among those that may fetch, whose next row is lowest |
| Fail | neither is possible | `IllegalStateException("Inline read stalled ...")`: the read cannot progress, which the threaded read would show as a hang |

`ReadScheduler.submit` is unchanged except for the hand-off: a page it would `execute` on the pool joins its queue instead. Row order, the read-ahead depth, pages in flight, placement (`Placer`), cells (`Cell`, `NestedCell`), the fills, measuring and sizing, publishing and the room listener all run unchanged on the one thread. The scheduler submits pages in row order across columns, so running its queue in arrival order keeps the guarantee the pool's FIFO gives: the batch the consumer polls for is decoded before any read-ahead.

## Where the threaded assumptions sit

| Mechanism | Threaded read | Inline read in the prototype |
|---|---|---|
| Fetcher per column | virtual thread looping `PageSource.next()`, parking for room in its queue (`runFetcher`) | the loop body is `fetchStep()`, shared by both; the threaded loop parks between steps, the inline driver calls a step when the column `mayFetch()` |
| Room for the fetcher | timed park (`WAKE_CHECK_NANOS`), unparked by `takeReady` | none: a column without room is not stepped |
| Consumer's wait | timed poll, `BatchWait` JFR event | `driveUntilReady()`; no wait, so no `BatchWait` event |
| Page tasks | context pool, FIFO | scheduler's `ArrayDeque`, FIFO |
| `submitting` counter | serializes submission across threads | still correct: a step never runs inside a submission, so no call nests |
| Room listener | consumer publishes cells that found no room | unchanged, same thread |
| `Cell.allocate` lock, `Cell.park`, `BinaryFill.unpark` | contended, a parked segment is filled by the sizing thread | uncontended; a binary segment can still park when its page is measured before the cell is sealed, and the sizing fills it, on the same thread |
| Nested placement by predecessor task | a page measured before its predecessor waits; the predecessor's task places both | never taken: tasks of one column run in page order, so each page is placed by its own task |
| Scratch | `ThreadLocal` per pool thread (flat), column pools | the consumer thread's `ThreadLocal`; one per pipeline thread |
| Close | `ReadScheduler.stop()`, join the fetcher, timed wait for the column's tasks | no fetcher to join; `stop()` runs the queued tasks, which return at once since the columns are done |
| Speculative work | next row group's plans and first chunk, one-ahead chunk and region prefetch, next footer, on the common pool | dropped: `PrefetchTasks` takes none, `FileMetadataCache.prefetch` does nothing and a footer loads on the asking thread |
| Error propagation | `signalError` ends the exchange; an `Error` is rethrown on the pool thread | unchanged; an `Error` in a step propagates out of the consumer's poll |
| JFR events | page, fetch and filter events on the threads doing the work | the same events, all on the consumer's thread; `BatchWait` never fires |
| Closed context | the pool refuses a task (`RejectedExecutionException`) | submission checks `HardwoodContextImpl.isClosed()` and fails the read the same way |

Nothing in `Placer`, `Cell`, `NestedCell`, `FlatFill`, `BinaryFill`, `NestedFill` or the readers changed. The fetcher loop was split into `fetchStep()` without changing what it does, so the threaded read runs the same code.

**Still on other threads.** An `InputFile` may run its own threads (an S3 client's event loop). The context still owns a fixed thread pool for the writer; `ThreadPoolExecutor` starts its threads on the first task, so a context used only for inline reads starts none.

## The prototype

The internal system property `hardwood.internal.inlineReads=true` makes every context created while it is set run its reads inline; `HardwoodContextImpl.create(int threads, boolean inlineReads)` creates one explicitly, for benchmarks and tests. No public API changed.

| Class | Change |
|---|---|
| `HardwoodContextImpl` | `inlineReads()` and `isClosed()`; the property and the factory |
| `ReadScheduler` | `step()`, the task queue, `stop()` drains it, `start()` starts no fetcher |
| `ScheduledColumn` | `fetchStep()` / `mayFetch()`; `runFetcher` loops over `fetchStep()`; registers the scheduler as its exchange's driver |
| `BatchExchange` | `Driver`, `setDriver`, `driveUntilReady()` |
| `PrefetchTasks`, `FileMetadataCache`, `RowGroupIterator`, `ParquetFileReader` | the flag that drops speculative work and loads footers on demand |

**Verification.** `InlineReadTest` checks that every `InputFile` access of an inline flat, nested, multi-file and column read happens on the consumer's thread, that row counts match the threaded read, that a read closed part way closes its file, and that a transport failure reaches the consumer. The `small-batches` profile runs the result-comparing suites once more with inline reads at a batch capacity of 7 (`inline-reads-7`). With the property forced on for the whole core suite, the only failures are tests of threaded behaviour: the prefetch tests (`NextRowGroupPrefetchTest`, `SequentialNextRowGroupPrefetchTest`, `IndexWindowLifecycleTest`), the fetcher-close tests (`ScheduledColumnCloseTest`, `ParentCloseTest`, which gate a read held on another thread), and `ReadAheadDepthTest`, which waits for the pool to settle without polling and never returns.

## Measurements

Intel N300 (8 E-cores), clock pinned at 1.50 GHz, JDK 25.0.4, `-Xms2g -Xmx2g -XX:+AlwaysPreTouch`. A plain timing harness (`SingleThreadBench`, not committed) writes its files with Hardwood's writer and reads each workload on a one-thread context, threaded (`HardwoodContextImpl.create(1, false)`) and inline (`create(1, true)`), in one JVM: three warm-up rounds, then 25 rounds alternating the order of the two modes. Each figure is the median per operation; wall time from `System.nanoTime()`, CPU time from the process's CPU time (`OperatingSystemMXBean.getProcessCpuTime()`, all threads, GC and JIT included, in 10 ms ticks: the tiny cases average 500 operations per sample). Row counts are checked on every read.

| Workload | File | Repeats per sample |
|---|---|---|
| `firstBatch-tiny` | open, build a row reader, read the first row, close; 1,000 rows, four flat columns (`int64`, `int32`, optional `double`, dictionary string), 7 KB | 500 |
| `rows-tiny` | every row of the same file | 500 |
| `rows-small` | every row; 100,000 rows, 301 KB | 20 |
| `rows-nested` | every row; 100,000 rows, `int64` and an optional list of `int32`, 723 KB | 20 |
| `rows-200files` | every row of 200 files of 1,000 rows, each opened on its own | 1 |
| `cols-medium` | `ColumnReaders` over the four columns; 10 million rows, 32 MB | 1 |

**One core** (`taskset -c 3 -XX:ActiveProcessorCount=1`), the requester's setup of one pipeline per core:

| Workload | Threaded wall | Inline wall | Δ wall | Threaded CPU | Inline CPU | Δ CPU |
|---|---|---|---|---|---|---|
| `firstBatch-tiny` | 323 µs | 192 µs | −41 % | 320 µs | 200 µs | −38 % |
| `rows-tiny` | 359 µs | 257 µs | −28 % | 360 µs | 260 µs | −28 % |
| `rows-small` | 13.7 ms | 13.4 ms | −2 % | 13.5 ms | 13.5 ms | 0 % |
| `rows-nested` | 15.5 ms | 14.6 ms | −6 % | 15.0 ms | 14.5 ms | −3 % |
| `rows-200files` | 74.6 ms | 52.8 ms | −29 % | 80 ms | 50 ms | −38 % |
| `cols-medium` | 626 ms | 542 ms | −13 % | 620 ms | 540 ms | −13 % |

Interquartile ranges, wall: threaded 315–388 µs, 356–391 µs, 13.6–14.4 ms, 14.9–18.5 ms, 73–109 ms, 619–670 ms; inline 190–271 µs, 255–381 µs, 13.0–14.0 ms, 14.3–15.0 ms, 52.5–56.9 ms, 534–551 ms.

**Eight cores, unpinned:**

| Workload | Threaded wall | Inline wall | Δ wall | Threaded CPU | Inline CPU | Δ CPU |
|---|---|---|---|---|---|---|
| `firstBatch-tiny` | 514 µs | 239 µs | −54 % | 580 µs | 240 µs | −59 % |
| `rows-tiny` | 517 µs | 321 µs | −38 % | 600 µs | 320 µs | −47 % |
| `rows-small` | 16.4 ms | 15.1 ms | −8 % | 16.5 ms | 15.5 ms | −6 % |
| `rows-nested` | 19.9 ms | 18.5 ms | −7 % | 20.0 ms | 19.0 ms | −5 % |
| `rows-200files` | 219 ms | 84.6 ms | −61 % | 180 ms | 80 ms | −56 % |
| `cols-medium` | 735 ms | 745 ms | +1 % | 870 ms | 760 ms | −13 % |

The threaded figures on eight cores spread widely (interquartile `firstBatch-tiny` 316–876 µs, `rows-200files` 84–259 ms; inline 215–286 µs and 64–97 ms), and are slower than the same read confined to one core. The cost sits in the hand-offs between the consumer, the fetchers and the decode thread when they run on different cores; this is inferred from the one-core numbers, not profiled.

What the numbers say:

- **Per-read overhead is where inline wins.** Opening a small file and reaching its first row costs 130–270 µs less, and a sequence of small files reads 30–60 % faster, on less CPU. Starting a fetcher per column and handing each page to the pool and back costs a fixed amount per read.
- **Decode-bound reads are even.** On the 100,000-row files the per-page work dominates and the two modes are within the noise of each other.
- **Inline never costs more CPU**, and 13 % less on the 32 MB scan, since no thread parks, wakes or polls on a timer. Threaded keeps a wall-time advantage only where a second core can overlap decoding with consuming, which the medium scan on eight cores shows as a draw: one decode thread is not enough to hide the consumer.
- Container runs (aarch64, eight cores shared with concurrent builds) were too noisy to report.

## Proposed public shape

| Option | Shape | For | Against |
|---|---|---|---|
| A | `HardwoodContext.builder().threads(0)` and `HardwoodContext.create(0)`: no threads, every read and write runs on the calling thread | one knob, reads as "zero background threads"; the builder rejects 0 today, so accepting it breaks no caller; a context is the documented home of runtime resources | the writer must honour it too (stage 3) |
| B | a separate builder flag (`callerRunsReads()`) | reads only, no writer work | two knobs that interact (`threads(8).callerRunsReads()`), and a context name for what is a thread count of zero |
| C | a `ReaderConfig` option | per read | `ReaderConfig` holds behaviour, the context holds threads ([READ_PIPELINE.md](../_designs/READ_PIPELINE.md#configuration-readerconfig)); one context could not serve both kinds of read |

Recommended: A. `threads(0)` states what the caller wants, and `HardwoodContext.executor()` returns an executor that runs tasks on the calling thread, which keeps the method's contract without a pool. The writer already has a path with no helpers (`RowGroupBuffer.flushTo(out, null, 0, false)` when a schema has one column, and no `ParallelAppend`), so a zero-thread writer takes that path for every schema.

## Stages

| # | Stage | Done when |
|---|---|---|
| 1 | Inline reads behind the internal switch (this branch) | result-comparing suites green with the switch on, in the PR build |
| 2 | Tests that pin threaded behaviour run in both modes or say which mode they pin; the threaded-only tests above opt out of the inline run explicitly; `step()` picks the column to fetch from the scheduler's index instead of a pass over the columns, so a wide read is not quadratic | the whole core suite passes with the switch forced on, minus the named threaded-only tests |
| 3 | Writer on the calling thread for a zero-thread context | writer suites green on a zero-thread context |
| 4 | Public `threads(0)` (option A), user docs in `concepts/concurrency-model.md` and `reference/`, `READ_PIPELINE.md` thread inventory and close protocol for both modes; the switch is removed | API review |

## Open questions

- **Remote reads.** An inline read drops every prefetch, so a read over S3 waits for each chunk in turn. Prefetch only overlaps when another thread does it. A zero-thread context could still let the `InputFile` fetch asynchronously if it offers that, which needs an asynchronous range read on `InputFile`. Out of scope until someone asks for inline reads over S3.
- **Read-ahead in inline mode.** The depth and the exchange's two ready batches let an inline read decode up to about two batches per column ahead of the consumer, with no thread to overlap it. Decoding only the batch polled for (depth 0, one ready batch) would cut memory and keep the decoded batch in cache. Not measured.
- **`BatchWait`.** An inline read never waits, so the event says nothing. A per-step event or none at all?
- **Thread confinement.** An inline reader may be handed from one thread to another between calls, as a threaded one may; it keeps its locks and volatiles. A reader used from two threads at once was never supported and stays so.
- **WASM.** The browser build has no virtual threads and no `ForkJoinPool` threads. The inline read starts neither, but the context's `Executors.newFixedThreadPool` and the `ThreadLocal` scratch must also work there; `threads(0)` creating no pool covers the first.
