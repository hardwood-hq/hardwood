# Plan: the threading model of a read

Tracking issue: #1587. Stage issues: #1560, #1589, #1564, #1590, #1022. Related: #1397
(remote read scheduling), #1573 (decode concurrency for a consumer-bound read).

A read uses two resources of its context: CPU time on the context's pool, and requests in
flight against the input files. This plan places every kind of work a read does on the
thread that matches the resource it uses, and orders the changes that get it there.

## Target model

| Resource | Carrier | Bound | Owner |
| --- | --- | --- | --- |
| CPU | the context's pool of n platform threads | `threads(n)` | this plan |
| I/O | a virtual thread per request | requests and bytes in flight per context | #1397 (#1264) |
| The read loop | the caller's thread | the caller | the caller |

| Work | Runs on |
| --- | --- |
| Waiting for I/O, coordination, per-page and per-request bookkeeping (page headers, plan lookups) | a virtual thread |
| Work that grows with the data: decompression, dictionaries, page decode, encode | the pool; a virtual thread that needs it hands it over and waits |
| Materialization, the consumer's own work | the caller's thread |

Invariants of the end state:

- No read or write hands work to the JVM's common pool.
- A task of the pool never waits for work only the pool runs (`requireOffPool`).
- CPU work that parses or scans what a read fetched runs on the pool, without exceptions for work
  that looks small; a virtual thread does I/O and per-request bookkeeping only (stage 2).
- The reads sharing a context share its pool fairly: no read's read-ahead queues ahead of
  another read's head-of-line page.
- Within a read, work a reader waits for runs before read-ahead, and read-ahead before
  speculative work.

## Today

| Work | Thread | Gap |
| --- | --- | --- |
| Page decode, decompression, dictionary decode (#1568) | pool | none |
| Fetchers: page headers, page filters, demand-path planning | virtual | none |
| Next row group's planning and first chunk, next chunk, next region (`PrefetchTasks`) | common pool | #1560 |
| Later files' footers (`FileMetadataCache`) | common pool | #1560 |
| A planning prefetch's dictionary decode | pool, ahead of read-ahead pages | #1590 |
| Page tasks of reads sharing a context | pool, one FIFO across all reads | #1564 |
| Writer copy, encode and compression | pool and the caller, submitted to the raw `ExecutorService` | #1564 |
| S3 part uploads | virtual | none |

## Stages

| # | Stage | Issue | Gate before the next stage |
| --- | --- | --- | --- |
| 1 | Speculative I/O and footer loads on virtual threads | #1560 | done |
| 2 | Parsing and scans on the pool | #1589 | done |
| 3 | The pool's queue: fairness across reads, priority within a read | #1564, #1590 | see below |
| 4 | Single-threaded mode as a configuration of the two carriers | #1022 | after #1264 |

Stages 1 and 2 are independent. Stage 3 lands before #1395 and #1396 read further ahead,
since those put more speculative work on the pool. Stage 4 waits for #1264, which puts every
request of a read behind one scheduler; before that, single-threaded I/O would rework the
fetch paths #1264 replaces.

### Stage 1: off the common pool

Done (#1560, PR #1592). Prefetches and later files' footer loads run on a virtual thread each
(`HardwoodContextImpl.ioExecutor`); a footer the caller is about to wait for loads on the
caller's thread. A speculative failure no caller waits for is reported (`PrefetchTasks.report`),
an `Error` thrown by the close that waits for the work, and a task not started at close skips
its work. On JDK 21 a virtual thread blocked in or on a monitor holds its carrier, so every lock
the read path holds across a fetch or a wait for another thread became a `ReentrantLock`, and
the per-row-group caches an `OnceCache`, which holds no lock while it computes; the N300's
pinned stacks under `-Djdk.tracePinnedThreads` went from 143 to none in Hardwood. `run-s3`
showed no change. The one-ahead shape of prefetch stays: #1395 replaces it.

### Stage 2: parsing and scans on the pool

Done (#1589, PR #1596). Measured on the container and the N300: a footer costs about a
microsecond per column chunk to parse, the page indexes of a 1,000-column row group about
0.6 ms, a dictionary scan up to milliseconds per filter leaf, a hand-off to the pool 10–30 µs
idle. Every parse and scan of fetched bytes now runs on the pool through
`HardwoodContextImpl.compute`: footers of later files, the page filter, the fetch plans, the
dictionary checks. The cost is one wait for a busy pool per row-group boundary, about 3 % on a
selective row read on the N300, which #1395's deeper read-ahead would hide. The `IN` scan's
per-entry cost is #1594.

### Stage 3: the pool's queue

Both issues change the one queue of `DecodeExecutor`, so they share a design.

**Across reads (#1564).** The queue is FIFO across every read of the context, so one read's
read-ahead queues ahead of another read's head-of-line page; `SharedContextLatencyBenchmarkTest`
measured a small read beside a full scan at up to 15× its idle latency. Page tasks reach the
pool as dispatch tokens, and a token, when it runs, picks the next read round-robin among those
with queued work and runs that read's oldest task. Each read's queue stays in scheduler order.
The gate is #1564's: that benchmark on the taxi files at 4 and 8 threads, and the narrow, flat
and 1BRC regression contenders on the container and the N300.

**Within a read (#1590).** A speculative planning task decodes dictionaries through
`HardwoodContextImpl.decode`, which puts the decode at the head of the queue, ahead of
read-ahead page tasks the consumer needs sooner. Measure a filtered read over many row groups
whose planning decodes dictionaries: the consumer's batch waits with and without the
speculative decodes queued at the tail. If the waits differ measurably, each read's queue gets
three classes (awaited by a reader, read-ahead, speculative) and planning's decodes run as
speculative. If not, the result is recorded and the measurement repeated when #1396 lands.

#1564 lands first: its per-read queues are where the classes go, and a waiting decode of one
read jumping ahead of every read's work is itself a fairness question.

**A caller-supplied executor** (#1564's open design point) is decided here too. A read waits
for pool work, so on a caller's bounded executor reads issued from its tasks starve, and the
fail-early check `requireOffPool` needs the pool's threads, which only Hardwood's own thread
factory records. The options are helping (a waiting pool thread runs queued work, taken from
the per-read queues), compensation (a `ForkJoinPool` with every wait a managed block, at the
cost of n no longer bounding decode) or leaving it unsupported. Until one is chosen, the pool
of n stays Hardwood's own.

**The writer** submits its append and flush tasks to the pool's raw `ExecutorService`
(`ParquetFileWriter` passes `executor()` to `flushTo` and `ParallelAppend`), past the queue that
reads go through. Under per-read dispatch it would bypass the fairness, so a write reaches the
pool through the context, as reads do, and takes its share of the dispatch as one more user of
the pool; its shutdown check becomes a context method. After that, the raw pool is touched only
by the context's `close()` and the deprecated `HardwoodContext.executor()` (#1581). Once that
accessor is removed in the next major release, the pool becomes private to `DecodeExecutor`,
and the context holds one handle per resource: the pool and the I/O executor.

### Stage 4: single-threaded mode

#1022, scoped as a configuration: the CPU carrier runs tasks on the caller's thread, the I/O
carrier runs requests inline, and the fetchers become steps the caller drives. Its design is
written against #1264's scheduler.

## Boundary with #1397

#1397 decides which ranges a read fetches, when, how far ahead and how many at once, within
the memory budget. This plan decides which threads run which work and bounds CPU parallelism.
#1264's scheduler issues its requests on the virtual-thread carrier from stage 1; the I/O
budget (requests and bytes in flight per context) is #1397's vision item 6.

## Open questions

- Whether the end state goes into the "Components and threading" section of
  `READ_PIPELINE.md` or into a design document of its own, since the writer follows the same
  rules.
- Whether the default n should leave a processor to the consumer; that is #1573's question
  and stays there.

## End state

The PR that completes stage 4 folds the target model and its invariants into the design
documents and deletes this plan.
