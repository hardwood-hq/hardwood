# Synchronous read mode

## Status

Proposed. Enables `RowReader` to run with no threads, for runtimes that have none
(a GraalVM Web Image / WebAssembly build, where `Thread.ofVirtual()` continuations and
`ForkJoinPool` are unsupported), and as a deterministic, low-overhead path for small reads.

## Problem

The read path is built for throughput and assumes real concurrency in three places:

1. **Per-column pipeline.** Each `ColumnWorker` runs two virtual threads — a *retriever*
   (pulls `PageInfo` from a `PageSource`, submits decode tasks) and a *drain* (assembles
   decoded pages into batches, publishes to a `BatchExchange`) — coordinated with
   `LockSupport.park`/`unpark` and a circular reorder buffer.
2. **Decode tasks.** The retriever submits each page decode via
   `CompletableFuture.runAsync(task, decodeExecutor)`.
3. **Prefetch.** `ChunkHandle` / `IndexedFetchPlan` / `RowGroupIterator` issue
   `CompletableFuture.runAsync(...)` on the common `ForkJoinPool`.

A single-threaded runtime cannot run any of these. Injecting a single-thread `ExecutorService`
is not enough: the drain/retriever threads and the common-pool prefetch bypass it entirely.

## Design

Add a **synchronous mode** carried on `HardwoodContext`. The decode logic — `PageDecoder`,
page assembly, batch publication — is unchanged and reused; only the *scheduling* changes.

### 1. Same-thread executor

`DirectExecutorService` (an `AbstractExecutorService`) runs every submitted task inline on the
caller. `CompletableFuture.runAsync(task, direct)` then completes synchronously, so decode tasks
(#2) and any prefetch routed through the context executor (#3) run inline with no threads.

`HardwoodContextImpl.synchronous(codecOverrides)` builds a context whose `executor()` is a
`DirectExecutorService` and whose new `synchronous()` accessor returns `true`.

### 2. Pull-based `ColumnWorker`

`ColumnWorker.start()` branches on `synchronous()`:

- **Parallel (unchanged):** start the drain and retriever virtual threads.
- **Synchronous:** start no threads. The retriever loop body is factored into
  `produceOnePage()` (pull one `PageInfo`, submit its decode — which runs inline — into the
  reorder buffer) and a sentinel writer. A new `pump()` drives production and draining on the
  calling thread:

  ```
  pump():                       // returns once one batch is published, or the worker is done
    published = batchesPublished
    while !done && batchesPublished == published:
      while !done && !sourceExhausted && inFlight < MAX_INFLIGHT_PAGES:
        if !produceOnePage(): sourceExhausted = true
      if sourceExhausted && !sentinelWritten: writeSentinel()
      if !drainReadyPages(): break        // nothing left to assemble
  ```

  `drainReadyPages()` gains one synchronous-mode guard: after a page whose assembly published a
  batch, it returns instead of continuing, so a single `pump()` yields roughly one batch (a large
  page that fills several batches is spread across successive `pump()` calls). The `park`/`unpark`
  calls are skipped when there is no peer thread.

### 3. Sync-aware `BatchExchange`

The consumer already pulls through `poll()`. In synchronous mode the exchange holds a `pump`
callback (registered by its worker) and an **unbounded** ready queue (publish never blocks):

```
poll():
  b = readyQueue.poll();  if b != null: return b
  if finished: return readyQueue.poll()
  if pump != null:                         // synchronous mode
    while readyQueue empty && !finished: pump.run()
    return readyQueue.poll()
  ... existing timed-poll loop (parallel mode) ...
```

The recycling free-queue is unchanged: `pump()` yields ~one batch at a time, the consumer reads
and `recycle()`s it before the next `poll()`, so at most one or two batches are outstanding.

### 4. Prefetch

`ChunkHandle` / `IndexedFetchPlan` / `RowGroupIterator` take the context executor (or an
`isSynchronous` flag) and, in synchronous mode, fetch inline (`CompletableFuture.completedFuture`)
instead of `runAsync` on the common pool. For an in-memory `ByteBufferInputFile` the fetch is a
buffer slice, so this is a no-op beyond removing the thread hop.

## Selection

Callers opt in by passing a synchronous context, e.g. `ParquetFileReader.open(inputFile,
HardwoodContextImpl.synchronous(overrides))`. The default `open` paths are unchanged, so the
parallel hot path is untouched. The dive WebAssembly build (`DiveSession`) uses a synchronous,
pure-Java-codec context so the data-preview screen can read rows.

## Testing

- A correctness test reads representative fixtures (flat and nested, compressed and not, with and
  without `head`/filter) through both the parallel and the synchronous context and asserts
  identical row output.
- The existing reader suite runs unchanged on the parallel path.

## Non-goals

- Making the synchronous path match parallel throughput — it is for threadless runtimes and small
  reads, not bulk scans.
- Running JVM virtual-thread continuations under Web Image — that is a platform gap, not addressed
  here.
