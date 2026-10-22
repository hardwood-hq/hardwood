# Plan: the threading model of a read

Tracking issue: #1587. Stage issues: #1560, #1589, #1597, #1598, #1022,
#1264, #1564, #1590. Related: #1397 (remote read scheduling), #500 (late materialization),
#1573 (decode concurrency for a consumer-bound read).

A read uses two resources of its context: CPU time on the context's pool, and requests in
flight against the input files. This plan places every kind of work a read does on the
thread that matches the resource it uses, and orders the changes that get it there.

## Target model

A thread of the pool never waits. Code that runs on the pool is a state machine over what is
present: where it needs bytes, or a result, that is not, it returns the need, and the step that
runs it yields on the need and is queued again once the need is met. Code that waits runs on a
virtual thread: a request, and the planning of a row group, which reads the next file's footer,
the row group's Bloom filters, dictionaries and page index one after another. Such code hands
its CPU work to the pool (`compute`), where the context bounds it.

Which of the two a piece of control flow is follows from how often it runs. The walk over a
column's pages runs once per page, where a hand-off to a virtual thread and back would cost on
every page, so it is a state machine whose steps run on the pool. A row group is planned once,
mostly waiting for its requests, so its planning is plain blocking code on a virtual thread, and
a step that reaches the row group before its plans are ready yields on them.

| Resource | Carrier | Bound | Owner |
| --- | --- | --- | --- |
| CPU, control steps included | the context's pool of n platform threads; with `threads(0)`, the thread driving the read | `threads(n)` | this plan |
| I/O | a virtual thread per request, which does nothing but the request | requests and bytes in flight per context | #1397 (#1264) |
| Planning a row group | a virtual thread per row group, which waits for the planning's requests | one row group ahead of the read | this plan |
| The read loop | the caller's thread | the caller | the caller |

| Work | Runs on |
| --- | --- |
| Control: walking page headers over fetched bytes, choosing the next page or range, per-request bookkeeping | a step on the pool |
| Planning a row group: the next file's footer, statistics, Bloom filters, dictionaries, the index window, the mask probe, the plans | a virtual thread; its parsing, checks and plan building run on the pool |
| Work that grows with the data: decompression, dictionaries, page decode, parsing footers and indexes, building plans, encode | the pool |
| Waiting for a request | a virtual thread; its completion makes the step that waits for the bytes runnable again |
| Materialization, the consumer's own work | the caller's thread |

Invariants of the end state:

- No read or write hands work to the JVM's common pool.
- A task of the pool never waits for work only the pool runs (`requireOffPool`), nor for a
  request, nor for code that waits for one: a step whose bytes are not present issues the
  request and yields, and a step whose row group is not yet planned yields on its planning.
- A virtual thread runs a request, or code that waits for requests (a row group's planning), and
  no work that grows with the data: that runs on the pool (`compute`), where the read needs it.
- Every read of an `InputFile` goes through the context's I/O scheduler, the reads a caller
  waits for on its own thread included; a check fails the build on a read issued past it.
- The reads sharing a context share its pool fairly: no read's read-ahead queues ahead of
  another read's head-of-line page.
- Within a read, control steps and work a reader waits for run before read-ahead, and
  read-ahead before speculative work.

Single-threaded mode is a configuration of this model: with `threads(0)` the consumer's poll
runs the read's queued steps and tasks, and the I/O carrier stays virtual threads, so a remote
read keeps its prefetches. Without a pool, a row group's planning runs on the consumer's thread
when the read reaches the row group, waiting for its requests there, as it does today for a file
whose bytes are in memory.

## Today

| Work | Thread | Gap |
| --- | --- | --- |
| Page decode, decompression, dictionary decode (#1568) | pool | none |
| Parsing and scans of fetched bytes (#1589) | pool: in place on fetch steps and planning tasks, handed over by the calling thread | none |
| Fetch steps (#1597): page headers, page filters, demand fetches | pool, head of the queue; a page's bytes not yet read are read on a virtual thread, which the step yields on (4a) | none |
| Demand-path planning: the next file's footer, index windows, Bloom filters, dictionaries, mask probes | a virtual thread per work item (4b), its CPU work on the pool; for a read whose files are all in memory, the fetch step that reaches the work item | none |
| Next row group's index window, Bloom filters and first chunk, next chunk, next region, later files' footer reads | a virtual thread per request, which only reads | none |
| Next row group's planning | the virtual thread of its prefetch, its CPU work at the head of the pool's queue; for a read whose files are all in memory, a task at the tail | #1590 |
| A planning prefetch's parsing and dictionary decode | pool, ahead of read-ahead pages | #1590 |
| Page tasks of reads sharing a context | pool, one FIFO across all reads | #1564 |
| Writer copy, encode and compression | pool and the caller, submitted to the raw `ExecutorService` | #1564 |
| S3 part uploads | virtual | none |

## Stages

| # | Stage | Issue | Gate before the next stage |
| --- | --- | --- | --- |
| 1 | Speculative I/O and footer loads on virtual threads | #1560 | done |
| 2 | Parsing and scans on the pool | #1589 | done |
| 3 | Fetchers as steps on the pool | #1597 | done |
| 4 | Steps never wait on I/O | #1598 | done |
| 5 | Single-threaded mode: `threads(0)` | #1022 | see below |
| 6 | One I/O scheduler per context | #1264 | see below |
| 7 | The pool's queue: fairness across reads, priority within a read | #1564, #1590 | see below |

| # | What it changes | What it buys |
| --- | --- | --- |
| 3 | The per-column fetcher threads go: fetch steps run on the pool at the head of its queue; the speculative tasks split into a read on a virtual thread and CPU work on the pool; a column closes without joining a thread | Less cost per read (measured: 200 small files −19 to −24 %, open and first row −19 to −27 % on one thread) and less CPU (−6 to −40 %); no hand-off of parsing on the fetch path; no parking and waking of fetchers, and with them most of the wake and pinning rules; one way of driving a read, which stages 5 and 6 build on |
| 4 | The walks over a column's pages become state machines that return the bytes they need instead of reading them; a row group's planning runs on a virtual thread; a step whose bytes or plans are not ready yields, and the completion queues it again | A thread of the pool does CPU work only; a request can wait for stage 6's budget without holding a thread; the pool stays free for the other reads of a shared context. No throughput change on the S3 scans alone, which are bound by I/O |
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

Done (#1597). A column's fetcher runs as fetch steps on the pool, at the head of its queue
(`ScheduledColumn.runFetchStep`); a change that makes room queues the next step, and a close
waits for the steps as for page tasks. On a thread of the pool, `compute` and `decode` run in
place. A later file's footer is read on a virtual thread and parsed by the load that takes it;
a load on a thread off the pool hands its parse over as a task a thread of the pool waiting for
the load runs itself (`FileMetadataCache.Load`). The next row group's index window, Bloom
filters and first chunk are read on virtual threads, and its planning runs as a task at the
tail of the pool.

Gate against main, three interleaved rounds per machine: on the container, nothing slower,
1BRC fan-out −4 % and ids −8 %, everything else within −3 to +1 %; on the N300 at 1.50 GHz,
1BRC −1 to −5 %, narrow and S3 even, flat +3 % in that session and even (−0.2 %) in the next,
where `threads(n + 1)` changed neither flat contender beyond the spread of the runs.

### Stage 4: steps never wait on I/O

**Changes.** The code that walks a column's pages does no I/O: it is a state machine over the
bytes present, and where it needs bytes that are not, it returns the need instead of reading
(sans-I/O). Its state lives in fields between calls, so a call that returns a need loses
nothing, and the next call resumes where it stopped. The step that drives it turns a need into a
request; the one-ahead prefetches the chunk handles and regions start stay as they are until
stage 6 replaces them with its read-ahead depth. Every request of a walk goes through one entry
(`RangeRead.start`), which returns the request's future: for a file whose bytes are in memory (`MappedInputFile`,
`ByteBufferInputFile`) a completed one, read in place, and otherwise one a virtual thread
completes. A need whose future is complete is met at once, so a local read never yields; a step
whose need is not met registers a resume on the future and returns, and the completion queues
the step again. A step that yields on a request marks it as demand, the priority #1397's
scheduler gives a range a reader waits for. A thread off the pool, such as the caller's before
the read starts, waits for the future.

| Machine | States | Needs |
| --- | --- | --- |
| `SequentialFetchPlan`'s walk | page header, repetition levels, page body, per page | the windows holding the bytes of the state; a header that does not fit its peek widens it |
| `IndexedFetchPlan`'s walk | dictionary, then each page | the dictionary range, then each page group |
| `PageSource` | waiting for a work item's plans, walking its plan | the work item's planning, then what the plan returns |

A row group is planned on a virtual thread, as plain blocking code: the next file's footer and
statistics when the work list needs the next file, then the row group's Bloom filters and
dictionaries, its index window, the page filter, the mask probe and the plans, each reading what
it needs and waiting for it. What grows with the data (parsing the footer and indexes, the
dictionary checks, building the plans) it runs on the pool through `compute`, ahead of the queued
tasks, since the read waits for it. The planning of a work item starts once, by the first
column to reach it or by the previous row group's prefetch, and every column's step that reaches
the work item before it is planned yields on it. A read whose files are all in memory plans
on the step that reaches the work item: no read can wait, so a hand-off would only cost.

The planning stays blocking because it runs per row group, mostly waiting for requests that
follow one another: a state machine of its states (next footer, filters, dictionaries leaf by
leaf, index window, probe, plans) would buy no throughput, and the predicate evaluation that
reads dictionaries as it goes would have to become resumable too. The walks, which run per page,
stay state machines, since a hand-off per page costs: moving the per-column fetchers off virtual
threads in stage 3 made small reads 19–27 % faster.

**Parts.** Two PRs. 4a (#1640): the request entry, the steps' yield and resume, and the column
walks (`SequentialFetchPlan`, `IndexedFetchPlan`, `PageSource`), which hold every read of a scan
in its steady state. 4b: the planning on a virtual thread, the steps' yield on it, and the gate's
thread test, which holds only once 4b is in.

**Buys.** At 235 ms latency the S3 scans show no cost of pool threads waiting on I/O: the reads are bound
by I/O, and no decode waits for the thread. The stage is structural: it is what lets stage 6 hold
a request back for its budget and see a read's needs before they are issued, it frees the pool
for the other reads of a shared context, it leaves a thread of the pool doing CPU work only, and
it gives stage 5 a read the consumer's poll can drive.

**Gate.** A test that no thread of the pool waits while the reads of a file whose bytes are not
in memory are held back, neither in a read nor for planning; the reads the caller makes on its
own thread before the read starts (the first footer, the initial planning probes) are exempt,
and stage 6 takes them over. `run-s3` at 30 ms and 235 ms latency against stage 3; a shared-context read (a local scan beside a remote one) on the
container and the N300; the narrow, flat and 1BRC regression presets against stage 3. 4a gates
on the benchmarks alone.

4a against stage 3, three interleaved rounds per machine. The presets and the S3 scans: nothing
slower on the container or the N300 at 1.50 GHz. A remote scan beside a remote scan of every
column on one context, at 235 ms latency: 2.4× faster on both machines (container 2,050 → 850 ms,
N300 2,230 → 925 ms), where stage 3's fetch steps held a pool thread per column in flight; a
local scan beside it 5–8× faster on average, its worst operation from one round trip (1.5 s) to
27 ms (container) and 129 ms (N300). A local scan beside a narrow remote scan: 9 % faster on
the container, 6–9 % slower on the N300, and 6 % slower there at a pool of 6, its best size.
Stage 3's fetch steps capped the pool's decoding while the remote read waited, close to the
N300's best concurrency for that read (about 4 decoders, where a narrow scan needs 6 or more);
4a removes the cap, and choosing decode concurrency per read is #1573's.

4b against 4a: `PoolWaitTest` holds back every read of a file not in memory and finds the pool
idle, where 4a's fetch steps waited in `rowGroupPlans` and in `workItemAt` for the next file's
footer. The S3 scans at 30 ms latency, two rounds per machine: within ±2.5 % of 4a on every
contender on the container and the N300 at 1.50 GHz, the Bloom-filter, filtered, multi-file and
shared-context scans included; at 235 ms on the container, the Bloom-filter, filtered,
multi-file and shared-context scans within ±3 %. The narrow, flat and 1BRC presets read local files, whose
planning stays on the fetch step as in 4a, so 4b changes nothing they run.

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
read-ahead depth. The reads a caller makes on its own thread (the first footer, the initial
planning probes) become requests to the scheduler too, whose completion the caller waits for, so
they count against the context's budgets. Once every read goes through the scheduler, it is the
only code that calls `InputFile.readRange`; a check enforces that at build time, such as an
Error Prone check like `NoThreadLocal`, since a test sees only the reads its fixtures reach.

**Buys.** Remote throughput within a memory budget, and one place where requests are merged, split and skipped by #1397's cost model.

**Gate.** The build-time check that only the scheduler calls `InputFile.readRange`; the
benchmarks of #1397's plan.

### Stage 7: the pool's queue

**Across reads (#1564).** Each read keeps a queue of its steps and tasks. Work reaches the pool
as dispatch tokens, and a token, when it runs, picks the next read round-robin among those with
queued work and runs that read's oldest item. The gate is `SharedContextLatencyBenchmarkTest`
on the taxi files at 4 and 8 threads, and the narrow, flat and 1BRC regression contenders on the
container and the N300.

**Within a read (#1590).** Four classes: control steps, work a reader waits for, read-ahead,
speculative. Stage 3 uses the first and the last (fetch steps at the head, planning
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
- Whether stage 6 needs a row group's planning requests ahead of time, as a list it can merge
  and order, where the blocking planning issues them one after another as it reaches them. They
  are few per row group and mostly depend on each other (the footer before the filters, the
  filters before the dictionaries a filter leaves undecided).
- The default n and the decode concurrency within it, which is #1573's question and stays
  there. On the N300 a pool of 6 is level with or faster than 8 on every regression contender
  (up to 10 %), and a local scan beside a remote one is fastest at about 4; the container is
  fastest at 8 on every contender, and its narrow scan is 14 % slower at 6.

## End state

The PR that completes stage 7 folds the target model and its invariants into the design
documents and deletes this plan.
