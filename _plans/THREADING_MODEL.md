# Plan: the threading model of a read

Tracking issue: #1587. Stage issues: #1560, #1589, #1597, #1598, #1022,
#1264, #1564, #1590. Related: #1397 (remote read scheduling), #500 (late materialization),
#1573 (decode concurrency for a consumer-bound read).

A read uses two resources of its context: CPU time on the context's pool, and requests in
flight against the input files. This plan places every kind of work a read does on the
thread that matches the resource it uses, and orders the changes that get it there.

## Target model

A read is a state machine that never blocks. Its control flow (which page to walk next, which
range to fetch, which row group to plan) runs as short steps; whichever thread is at hand runs
them: a thread of the pool after its task, the consumer when it polls, or the completion of a
request.

| Resource | Carrier | Bound | Owner |
| --- | --- | --- | --- |
| CPU, control steps included | the context's pool of n platform threads; with `threads(0)`, the thread driving the read | `threads(n)` | this plan |
| I/O | a virtual thread per request, which does nothing but the request | requests and bytes in flight per context | #1397 (#1264) |
| The read loop | the caller's thread | the caller | the caller |

| Work | Runs on |
| --- | --- |
| Control: walking page headers over fetched bytes, choosing the next page or range, per-request bookkeeping | a step on the pool |
| Work that grows with the data: decompression, dictionaries, page decode, parsing footers and indexes, planning, encode | the pool |
| Waiting for a request | a virtual thread; its completion makes the step that waits for the bytes runnable again |
| Materialization, the consumer's own work | the caller's thread |

Invariants of the end state:

- No read or write hands work to the JVM's common pool.
- A task of the pool never waits for work only the pool runs (`requireOffPool`), nor for a
  request: a step whose bytes are not present issues the request and yields.
- A virtual thread does I/O and nothing else; work that needs the bytes it fetched runs on the
  pool, where the read needs it.
- The reads sharing a context share its pool fairly: no read's read-ahead queues ahead of
  another read's head-of-line page.
- Within a read, control steps and work a reader waits for run before read-ahead, and
  read-ahead before speculative work.

Single-threaded mode is a configuration of this model: with `threads(0)` the consumer's poll
runs the read's queued steps and tasks, and the I/O carrier stays virtual threads, so a remote
read keeps its prefetches.

## Today

| Work | Thread | Gap |
| --- | --- | --- |
| Page decode, decompression, dictionary decode (#1568) | pool | none |
| Parsing and scans of fetched bytes (#1589) | pool, handed over by the thread that fetched them | stage 3 removes the hand-off |
| Fetchers: page headers, page filters, demand-path planning, demand fetches | a virtual thread per decoded column, blocking | stages 3 and 4 |
| Next row group's planning and first chunk, next chunk, next region, later files' footers | a virtual thread per task, which hands its CPU work to the pool and waits | stage 3 |
| A planning prefetch's dictionary decode | pool, ahead of read-ahead pages | #1590 |
| Page tasks of reads sharing a context | pool, one FIFO across all reads | #1564 |
| Writer copy, encode and compression | pool and the caller, submitted to the raw `ExecutorService` | #1564 |
| S3 part uploads | virtual | none |

## Stages

| # | Stage | Issue | Gate before the next stage |
| --- | --- | --- | --- |
| 1 | Speculative I/O and footer loads on virtual threads | #1560 | done |
| 2 | Parsing and scans on the pool | #1589 | done |
| 3 | Fetchers as steps on the pool | #1597 | see below |
| 4 | Steps never wait on I/O | #1598 | see below |
| 5 | Single-threaded mode: `threads(0)` | #1022 | see below |
| 6 | One I/O scheduler per context | #1264 | #1397's plan |
| 7 | The pool's queue: fairness across reads, priority within a read | #1564, #1590 | see below |

| # | What it changes | What it buys |
| --- | --- | --- |
| 3 | The per-column fetcher threads go: fetch steps run on the pool at the head of its queue; the speculative tasks split into a read on a virtual thread and CPU work on the pool; a column closes without joining a thread | Less cost per read (measured: 200 small files −19 to −24 %, open and first row −19 to −27 % on one thread) and less CPU (−6 to −40 %); no hand-off of parsing on the fetch path; no parking and waking of fetchers, and with them most of the wake and pinning rules; one way of driving a read, which stages 5 and 6 build on |
| 4 | A step whose bytes are not present issues their request and yields; the completion queues it again | A thread of the pool does CPU work only; a request can wait for stage 6's budget without holding a thread; the pool stays free for the other reads of a shared context. No throughput change on the S3 scans alone, which are bound by I/O |
| 5 | `threads(0)`: the consumer's poll runs the read's queue; the writer takes its path without helpers; `executor()` throws on such a context | #1022: a pipeline on one core with no background CPU, so N pipelines scale with N cores (prototype, one core: −28 to −41 % on small reads, −13 % on a 32 MB scan); reads in an environment without threads, such as the browser build of `dive` |
| 6 | One scheduler issues every request of a context, within request and byte budgets; a read-ahead depth replaces the one-ahead rules of chunks, regions, row groups and files | Remote throughput within a memory budget; requests merged, split and skipped by #1397's cost model |
| 7 | Each read keeps its own queue, dispatched round-robin; four classes order a read's work; the writer reaches the pool through the context | A small read beside a full scan keeps close to its idle latency (up to 15× it today); speculative work never delays work a reader waits for; the writer shares the pool fairly |

Stage 4 lands before stage 6: a request that waits for the scheduler's budget must not hold a
thread of the pool, and only a step that yields lets it wait without one. Stage 7 lands before
#1395 and #1396 read further ahead, since those put more speculative work on the pool.

### Stage 1: off the common pool

Done (#1560, PR #1592). Prefetches and later files' footer loads run on a virtual thread each
(`HardwoodContextImpl.ioExecutor`); a footer the caller is about to wait for loads on the
caller's thread. On JDK 21 a virtual thread blocked in or on a monitor holds its carrier, so
every lock the read path holds across a fetch or a wait for another thread is a
`ReentrantLock`, and the per-row-group caches are `OnceCache`s, which hold no lock while they
compute.

### Stage 2: parsing and scans on the pool

Done (#1589, PR #1596). Every parse and scan of fetched bytes runs on the pool through
`HardwoodContextImpl.compute`: footers of later files, the page filter, the fetch plans, the
dictionary checks. The cost is one wait for a busy pool per row-group boundary, about 3 % on a
selective row read on the N300.

### Stage 3: fetchers as steps on the pool

**Changes.** A column's fetcher stops being a virtual thread. Its loop body becomes a fetch step
(`ScheduledColumn`), which queues pages while the column has room and returns when it has
none; a change that makes room, which today unparks the fetcher, queues the column's next fetch
step instead. A fetch step runs on the pool, at the head of the queue: the read waits for what
it fetches. On a thread of the pool, `compute` and `decode` run in place, so the fetch path has
no hand-off.

A step on the pool must never wait for work only the pool runs, so the speculative tasks split
along the target model. A virtual thread reads what is known before any parsing: a later
file's footer bytes, the next row group's index window and Bloom filters, its first chunk, the
next chunk and region. The footer is parsed by the load that needs it; the next row group is
planned by a speculative task at the tail of the pool, whose waits are on running threads
only (`OnceCache` waits on the thread computing).

Closing a column waits for its queued fetch step instead of joining a
thread; the threaded-only tests (the fetcher-close tests, the tests that hold a fetcher inside
`PageSource.next()`, `ShuffledTaskOrderTest`'s executor, which holds back every task of the
pool) become tests of the step; the thread inventory, the back-pressure chain and the close
protocol in `READ_PIPELINE.md` are rewritten for steps.

**Buys.** The fixed cost of a read and the CPU of its control flow, measured in the spike below; one way of driving a read instead of two, which stages 5 and 6 need.

**Spike.** Branch `1022-fetch-steps-spike` runs the fetchers as steps behind
`-Dhardwood.internal.fetchSteps`. Against main, one JVM per arm, interleaved:

| Workload | Container | N300, 1.5 GHz pinned |
| --- | --- | --- |
| narrow, flat, 1BRC regression presets | −2 to −9 % | −7 to +4 % |
| 200 files of 1,000 rows | −24 % | −19 % |
| open and first row, one thread | −19 % | −27 % |
| `ColumnReaders`, 10 million rows | −7 to −8 % | 0 to −20 % |
| S3 scans at 30 ms and 235 ms latency | even, identical requests and bytes | not run |

Fetch steps at the tail of the queue lost 10–15 % on small reads, which the head of the queue
recovers. Flat columnar on the N300 is 4 % slower in the regression preset with 2 % fewer
instructions, and even under perfnorm's longer iterations; the fetchers on today's carriers
run beside the n threads of the pool, and steps put them inside the bound. Measured once more
on the PR, with `threads(n + 1)` as the check.

**Gate.** The core suite passes with steps the only mode; the narrow, flat and 1BRC regression
contenders and `run-s3` on the container and the N300 against main; any regression beyond the
flat columnar case above is raised before the PR merges.

### Stage 4: steps never wait on I/O

**Changes.** A step that needs bytes not yet present issues their request on the I/O carrier and yields; the
completion queues the step again. The yield points are few, since walking page headers over
fetched bytes needs no further I/O: a column's next chunk or region (a sequential plan's next
window, or the rest of a page that straddles one), an indexed plan's next range, and a row
group's planning inputs (its dictionaries, the footer of the next file). A step that yields on a
range marks it as demand, the priority #1397's scheduler gives a range a reader waits for.

**Buys.** At 235 ms latency the S3 scans show no cost of pool threads waiting on I/O: the reads are bound
by I/O, and no decode waits for the thread. The stage is structural: it is what lets stage 6 hold
a request back for its budget, it frees the pool for the other reads of a shared context, and it
leaves a thread of the pool doing CPU work only.

**Gate.** A test that every `InputFile` access of a read happens on a virtual thread; `run-s3`
at both latencies against stage 3; a shared-context read (a local scan beside a remote one) on
the container and the N300.

### Stage 5: single-threaded mode

**Changes.** #1022. `HardwoodContext.create(0)` and `builder().threads(0)` create a context without a pool:
the work a pool thread would run runs on the thread that drives the read, which is the thread
calling the reader. The builder rejects 0 today, so no caller changes meaning. When the poll
finds no batch, it runs the read's oldest queued step or task, else waits for a request in
flight. The writer takes its path without helpers for every schema. `HardwoodContext.executor()`
throws `UnsupportedOperationException` on such a context. One such context serves reads on
several threads at once, each driven by its own caller.

**Buys.** What #1022 asks for: N independent pipelines, each on one core with no background CPU, scale with N cores. The prototype on branch `1022-single-threaded-mode` measured, on one core, −41 % to open a small file and read its first row, −29 % on 200 small files and −13 % on a 32 MB scan. A read also runs where no thread can be started, such as the browser build of `dive`.

**Gate.** The core suite passes on a zero-thread context, minus the tests that name a pool; a
test that no parse or decode of a local read leaves the consumer's thread; writer suites on a
zero-thread context; user docs (`concepts/concurrency-model.md`, `reference/`).

### Stage 6: one I/O scheduler per context

**Changes.** #1264, designed in #1397's plan. The requests come from the steps' yields and the speculative
tasks' reads, the places stages 3 and 4 leave, so the scheduler replaces `ioExecutor()` and the
one-ahead rules of `ChunkHandle`, `SharedRegion`, the next row group and the next file with its
read-ahead depth.

**Buys.** Remote throughput within a memory budget, and one place where requests are merged, split and skipped by #1397's cost model.

### Stage 7: the pool's queue

**Across reads (#1564).** Each read keeps a queue of its steps and tasks. Work reaches the pool
as dispatch tokens, and a token, when it runs, picks the next read round-robin among those with
queued work and runs that read's oldest item. The gate is `SharedContextLatencyBenchmarkTest`
on the taxi files at 4 and 8 threads, and the narrow, flat and 1BRC regression contenders on the
container and the N300.

**Within a read (#1590).** Four classes: control steps, work a reader waits for, read-ahead,
speculative. Stage 3 already needs the first and the last (fetch steps at the head, planning
ahead at the tail); the dictionary decodes of a speculative planning task run as speculative.

**The writer** submits its append and flush tasks through the context and takes its share of
the dispatch as one more user of the pool. After that, the raw pool is touched only by the
context's `close()` and the deprecated `HardwoodContext.executor()` (#1581).

**Buys.** Fairness between the reads of a context, which today measures a small read beside a full scan at up to 15× its idle latency; a read's speculative work never delays the work its reader waits for.

**A caller-supplied executor** stays unsupported until a design for a pool Hardwood does not
own is chosen: a read waits for pool work, so on a caller's bounded executor reads issued from
its tasks starve.

## Boundary with #1397 and #500

#1397 decides which ranges a read fetches, when, how far ahead and how many at once, within the
memory budget; its three request priorities (demand, read-ahead, speculative) mirror the
classes of stage 7. This plan decides which threads run which work and bounds CPU parallelism.
Under steps, a reader waiting on a range is a step that yielded on it, which settles how its
demand is raised.

Late materialization (#500) adds a dependency between columns: a payload page waits for the
selection its predicate columns resolve. In the step model the resolution is one more event
that makes a page runnable, or lets its range go unfetched.

## Open questions

- Whether the end state goes into the "Components and threading" section of
  `READ_PIPELINE.md` or into a design document of its own, since the writer follows the same
  rules.
- Whether the default n should leave a processor to the consumer; that is #1573's question
  and stays there.

## End state

The PR that completes stage 7 folds the target model and its invariants into the design
documents and deletes this plan.
