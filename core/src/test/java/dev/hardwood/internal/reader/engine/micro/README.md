# Micro reader

The read engine's mechanism in about 1,350 lines, its test included, with no dependencies and no Parquet. It is a smaller cut of `_tmp/mini-reader`, which reads real files through Hardwood's format classes; this one keeps only what makes the engine work.

From the repository root:

```
M=core/src/test/java/dev/hardwood/internal/reader/engine/micro/MicroReader.java
java $M 4          # four decode threads
java $M 2 trace    # every update, the thread that applied it, what the poll emitted
java $M 0 trace    # zero threads: the consumer's thread decodes
```

It also runs from the IDE: `MicroReader.main`, with the same arguments.

On a machine of eight cores, one thread takes about 310 ms, two 160 ms, four 80 ms, eight 80 ms. The read is then bound by its I/O and its read-ahead cap. The timed read follows an untimed one, which loads the classes (the source launcher compiles the other files on first use) and warms the JIT; a traced run is timed with both, and with its printing.

| File | Holds |
|---|---|
| `MicroReader` | `main`: reads every batch and checks the order and the sum |
| `SimulatedFile` | the "file": its columns' pages, a fetch that sleeps, a decode that spins |
| `Page`, `Batch` | a page of one column; the rows the consumer takes |
| `Engine` | the read decided in one `poll()`, which calls each stage's rule with what it takes from the others |
| `Fetching`, `Planning`, `Decoding`, `Output`, `Demand` | `Engine`'s state, one object per stage of the read, each with its stage's rule; and the consumer's demand, which the poll hands to the rule that needs it |
| `Request`, `PageCursor`, `DecodedRows`, `Slots` | the parts the stages use: a read of a byte range, a column's next page to walk, a column's decoded rows, a bound on work in flight |
| `Command`, `Fact` | what the engine asks for (fetch, decode) and what happened (fetched, decoded, failed): both data |
| `EngineTest` | the engine driven by hand, without a thread: facts posted in a chosen order, the polls' commands checked |
| `ReadInterpreter` | how the read's commands run: a fetch is I/O, a decode is CPU, both call the file |
| `Reader` | the read as the consumer sees it: the engine run by a driver, and `next()` |
| `runtime/Machine`, `runtime/Interpreter`, `runtime/Driver` | the generic part, which knows nothing of reading: a machine is facts in, commands out; an interpreter says where a command runs and what running it takes; the driver holds the lock and the threads |

## The model

A "file" is three columns cut into pages of 100, 250 and 40 rows, so page boundaries differ between columns, in row groups of 2,000 rows, 8 bytes a row. Within a row group, column 0's chunk comes first, then column 1's, then column 2's. Fetching a request sleeps 2 ms (I/O), whatever its size. Decoding a page spins 10 µs per row (CPU) and yields its values. The consumer takes batches of 500 rows, in row order, each holding all three columns.

Before a row group's bytes may be fetched, the row group is planned: its index is fetched (I/O, sharing the fetch budget with the data) and parsed (CPU, one at a time, in row-group order), as Hardwood's planning reads a row group's page index before its walks. Planning then cuts the row group's chunks into requests of at most 8 KiB: a request may hold the pages of several columns, and a page may straddle two requests. A column's walk takes a page once every request holding its bytes has arrived, as Hardwood's walks parse page headers from the bytes present, and stops on the first request missing.

A request starts in one of three ways, as Hardwood's do: at once and outside the budget, when a stopped walk waits for it (demanded); at once and outside the budget, when the consumer's next batch needs it (a need); or while the bytes in flight leave room, when it lies within the read-ahead cap (a want).

## The three parts

**`Engine`: the state machine.** It knows where everything in progress stands and runs nothing itself. Its state is grouped by the stage of the read it belongs to, one object each:

- `Planning`: the fetched indexes waiting for their parse, the one parse slot, and the row groups planned with their requests (coalescing);
- `Walking`: every column's next page (`PageCursor`), the requests that have arrived, the pages walked, and the requests the walks stopped on;
- `Fetching`: the next row group's index, the planned requests not started, ranked by their lowest row, and the bytes in flight for wants;
- `Decoding`: the walked pages waiting for a decode, lowest row first across columns, the decode slots, and every column's decoded rows (`DecodedRows`);
- `Output`: the consumer's side: the next batch to build, the batches built and not taken, and from them the consumer's position and `Demand`.

A stage's object holds that stage's state and its rule, its share of the engine's poll (`poll`), records the facts that concern it, and reads no other stage. The engine's `poll()` is the only caller of the stages' polls, in one pass and a fixed order, and passes each rule what it takes from the others as values:

```java
Demand demand = output.demand();
planning.poll(out);
walking.poll(demand, planning.plannedRows(), planning.plannedRequests());
fetching.poll(demand, planning.plannedRequests(), walking.stoppedOn(), out);
decoding.poll(walking.takeWalked(), out);
buildBatches();   // decoding's rows into the consumer's batches
```

So which stage works with which is the argument list of a call in `poll()`: walking takes the demand and what planning planned; fetching takes the demand, the requests planned and the requests the walks stopped on, so a dependency running back from a later stage to an earlier one is an argument like any other; decoding takes the pages walked; planning takes nothing from the others. The walks run before fetching, so a request a walk stops on starts in the same poll. `Demand` is the consumer's reach in rows, `need` (its next batch) and `want` (as far as the read may run ahead), computed once per poll; the read-ahead cap and the priority of needs over wants are this one value.

`poll()` is the only method that decides anything. It reads the state and returns a list of `Command`s: fetch this page, decode that one. `post(fact)` is the only way in: a switch over the facts that records each one. Nothing in the engine knows about threads, locks, time, the file or the decoder.

**`Command` and `Fact`: work out, results in, both data.** A command says what to do (`Fetch(request)`, `Decode(page)`), not how; a fact says what happened (`Fetched(request)`, `Decoded(page, values)`, `Failed(command, cause)`). Both are sealed, so the complete list of what a read asks for and what can happen to it is two short files, and the switches over them are exhaustive. Since a fact is data, posting one can only record it: there is no code in it that could decide anything. And since the engine is facts in, commands out, a test can drive it without a thread (`EngineTest`).

**`runtime.Driver`: one lock and the threads, for any machine.** It knows nothing of reading: it runs a `Machine<F, C>` with an `Interpreter<C, F>`. `postAndPoll(fact)` takes the lock, posts the fact, polls, signals waiters, releases the lock, and dispatches the commands by the resource the interpreter names: I/O to virtual threads, CPU to the pool, or, with zero threads, to the consumer's queue. A command's result comes back as a fact, which the thread that ran it posts. It also counts the commands in flight, which is mechanism, not policy. `await(ready, action)` is the consumer's side: it waits until `ready` holds for the machine (running queued CPU commands meanwhile on zero threads), applies `action`, and polls, since an action such as taking a batch changes the state.

**`ReadInterpreter` and `Reader`: the read's two adapters.** `ReadInterpreter` is the only code that calls the file: `Fetch` is I/O and sleeps, `Decode` is CPU and decodes. `Reader.next()` is one `await`: ready once a batch is built, the read is done or it failed; the action takes the batch or throws the failure.

That is the whole cycle:

```
   poll() ─▶ commands ─▶ executed on some thread ─▶ fact ─▶ (lock) post + poll() ─▶ commands ─▶ …
```

## Reading the trace

```
main             start           -> [fetch index 0]
io (virtual)     fetched index 0 -> [parse index 0]
pool-1-thread-1  parsed index 0  -> [fetch r0.0, fetch r0.1, fetch r0.3, fetch r0.2, fetch r0.4, fetch r0.5]
io (virtual)     fetched r0.0    -> [decode c0p0, decode c0p1, decode c0p2, decode c0p3]
```

- The first poll fetches row group 0's index: nothing else is planned. Its arrival lets planning parse it.
- The parse plans row group 0 and cuts its six requests. In the same poll, the walks stop on the requests holding each column's first page (`r0.0`, `r0.1`, `r0.3`), which start at once; the rest follow as needs and wants, lowest row first.
- A request's arrival lets the walks take every page whose bytes are all present, and the poll starts their decodes, lowest row first, while decode slots are free.
- A decode's result frees a decode slot: the poll starts the walked page of the lowest row.
- Once every column's decoded rows reach the end of the next batch, the poll builds it and the consumer wakes.

With `0`, the decodes run on `main`, the consumer's thread, inside `next()`. Same engine, same polls; only the driver's dispatch differs.

Two things to watch in the trace:

- **Order holds among the pages present.** The first decodes start in the order requests arrive, since a decode only starts from what has been walked: row order holds among the walked pages, per poll. That is the invariant the Hardwood engine states too.
- **Every line is one lock acquisition.** The work between them runs unlocked and in parallel.

## Why it is correct

- **No lost wake-up.** Every change to the state happens in a post, and every post is followed by a poll that re-reads the whole state. No code has to remember to tell another component that something changed.
- **No thread waits for work.** A fetch occupies a virtual thread that only sleeps; a decode occupies a pool thread that only computes. Nothing ever waits inside a job for another job.
- **Bounds are conditions in the poll.** Fetches in flight, decodes in flight (twice the threads), and the read-ahead cap (`AHEAD_ROWS` past the first row the consumer has not taken) are each one `if`. Pages held in memory are therefore bounded by the cap, whatever the consumer's speed.
- **The consumer gets batches in order** because batches are built only in order, from columns whose decoded rows are contiguous.

## Why not stages joined by edges

An earlier version of this reader also decided the read with stages that each decide alone and talk only through edges: items flowing down, demand ("needed now", "wanted ahead") flowing up, every stage polled until a pass changes nothing, the fused-stage model of Akka Streams. Both engines made the same decisions. The comparison, with planning in both, showed:

- **Ownership was clearer in the stage model, and is kept here.** One object per stage, holding its state and its rule, came from it; so did the consumer's demand as the one source of the read-ahead cap (`Demand`).
- **A rule spanning two stages became an edge.** "A page waits for its row group's plan" is one value here (`planning.plannedRows()`, passed to `fetching.start`); there it was a third edge and a cycle between IO and planning.
- **Demand travelled in passes.** The consumer's demand reached the first index fetch on the third pass. In Hardwood each edge would change units on the way (batches, rows, pages, byte ranges, files), and a demand translated too small stalls the read without a trace.
- **IO serving two downstreams was not a problem in itself.** Its priority across the two edges was one method; in Hardwood it would take a common position key, which the engine already has (file, work item, offset).
- **It took more code**, about 565 lines against 490 for the same decisions, and a question such as why a page was not fetched was followed across three edges instead of one `poll()`.

## What Hardwood's engine adds

| Micro reader | Hardwood (`internal.reader.engine.RowGroupIterator` and around) |
|---|---|
| `Engine` + `runtime.Driver` + `ReadInterpreter` | one class, `RowGroupIterator`, holding the state, the lock, the dispatch and the jobs' code; a generic runtime is open for it |
| requests cut from a row group's chunks, a byte budget for wants | the same, coalesced within a gap policy, and the bytes held charged to the context's R until every column has passed them |
| walks inside the poll, pages' extents known from the index | walk advances as jobs on the pool, which parse page headers from the bytes present, also where no offset index gives the pages; a planning stage per work item (Bloom filters, page index, mask probe, fetch plans) that stops on bytes the same way |
| batches built by copying rows | cells: a flat page is placed into the batch's arrays before it decodes, so the decode writes each value once (`Placer`, `FlatColumn`); nested pages are measured, then placed |
| `built` deque | one `BatchExchange` per column, consumed in lockstep |
| `postAndPoll` takes the lock | with several decode threads, a thread that finds the lock held queues its result and the holder applies it |
| `Command`, `Fact` as data | results as a sealed `Fact` type taken in by one switch (`take`); jobs as code (`emit(Kind, buffers -> …)`), not command data |
| nothing | multiple files and footers, filters, row limits, failures placed at their page, retries, close, the stall check |

## Exercises

Each one adds a piece of the real engine and shows what it costs.

1. **Queue results for the holder.** Replace `lock.lock()` in `postAndPoll` with `tryLock()`, and on failure put the fact in a `ConcurrentLinkedQueue` that the holder drains before it unlocks. This is the flat-combining of #1712's commit 13. Careful: the holder must re-check the queue after `unlock()`, or a result queued in between waits forever.
2. **Fail a page.** Make `decode` throw for one page. The driver already posts it as `Fact.Failed`, which ends the read. Write the interleaving as an `EngineTest` first. Then make it fail only that column, and watch the other columns stall: the batch builder waits for the failed column's rows. That is the review's first finding of #1712, in miniature.
3. **Hold and release bytes.** Keep a request's bytes until every page it holds is decoded, charged to a second budget (Hardwood's R), and see how far ahead the read gets with R small.
4. **Add a planning step that needs bytes.** Before a column's pages can be fetched, a "footer" fact must arrive. Make it a state the poll checks rather than a wait.
