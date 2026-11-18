# Pico engine

`PicoEngine` is a read engine stripped to its scheduling: the jobs, priority, bounds, need and lifetimes of [READ_PIPELINE.md "Scheduling"](../../../../../../../../../../_designs/READ_PIPELINE.md#scheduling) in about 200 lines, with no Parquet. It reads N rows in pages of fixed rows and bytes; pages are grouped into ranges, one request each. A fetch parks for a latency plus its bytes over a per-request bandwidth, a decode parks for a CPU time per page, and a consumer takes batches of rows in order.

It shows, each in a few lines:

- Jobs of two kinds, `FETCH` on virtual threads and `DECODE` on a fixed pool. A job is a key (the first row it serves), a cost in bytes, work run outside the lock, and a callback applied under the lock as it ends.
- Outputs: a range's bytes are an `Output` from the start; each page waits on its range's output and becomes ready to decode when the fetch's callback ends it.
- One priority: ready jobs lowest key first, across both kinds.
- Three bounds: F (a fetch ahead starts while less than F is fetched ahead), the decode look-ahead in batches past the consumer, and the CPU slots. A refusal holds back only the later jobs of its own kind.
- Need: while no job keyed below the end of the consumer's next batch is in flight, the lowest such job starts past the bounds: a fetch at once, uncounted against F; a decode at the next free CPU slot.
- Lifetimes: a range's bytes are held until its last page is decoded, and only then count no more against F.

## Running it

It lives in test scope and has no `@Test`, so the build compiles it but never runs it. It needs only the JDK:

```shell
javac -d /tmp/pico core/src/test/java/dev/hardwood/internal/reader/engine/pico/PicoEngine.java
java -cp /tmp/pico dev.hardwood.internal.reader.engine.pico.PicoEngine
java -Dtrace=true -cp /tmp/pico dev.hardwood.internal.reader.engine.pico.PicoEngine
```

`main` prints one row per setting: latency, F, look-ahead and CPU slots, with the wall time and the peak range bytes held. `-Dtrace=true` adds one line per job start (time, kind, key, and `forced by need` when need started it past the bounds), showing the order the priority produces.

## Left out

- Footers, planning and their kinds of job (`FOOTER`, `PLAN`, and the rule that planning takes a free CPU slot first): every range and page is known at the start.
- Files ahead, the bound on footers.
- R: with one read, and decoded values outside R, R would count the same bytes as F. It matters only across reads that share it.
- Header scans and their `CPU` jobs: every page has a known size and row count.
- Placement into cells, dictionaries and their outputs: a page lies in exactly one batch and counts it down.
- Job values and failures: no work produces a value or fails, so a job has one callback where the engine has `onValue` and `onFailure`; nothing retries.
- The batch exchange: the consumer takes a batch as soon as its last page is decoded, so the look-ahead counts from the consumer.
- Close: the consumer always reads to the end.
- The lock hand-off between threads: each finishing job waits for the lock and applies its own callback.
