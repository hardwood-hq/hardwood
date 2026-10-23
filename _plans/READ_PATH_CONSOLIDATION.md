# Plan: one fetch sequence per read

Tracking issue: #1662. Related: #1663 (the 1.1.0.CR1 read-path plan, which this plan precedes),
#1399, #1659 and #100 (folded into stage 2), #1629 and #1393 (S3 transport), #1354 (the mask
probe), #1042 (`PrefetchMissEvent`).

This plan reduces the read path's fetching and planning to one mechanism that issues every byte
request of a read, without making any read slower. It precedes 1.1.0.CR1. Building out remote
reads (split and parallel requests, deeper read-ahead) follows it, as changes to the numbers that
size the mechanism.

## Goals

1. **One mechanism.** One fetch sequence per read issues every byte request a read makes. The six
   read-ahead mechanisms, four coalescing passes and eight request issuers of today go.
2. **Efficient use of I/O and CPU.**
   - Read ahead far enough that decode never waits for bytes it could have had, and no further:
     bytes ahead are bounded per read (F) and per context (R), and released as the read passes
     them.
   - Organize requests well: needed ranges coalesced across columns under the gap G, every
     request at most S, no range the sequence holds requested again. A read fetches no byte it
     does not use beyond the gaps G bridges and, when it stops early, the bytes it held ahead.
     The organization admits parallel requests later as a change of S and F.
   - No thread of the pool, and no thread driving a read on a context of zero threads, waits for
     I/O. The waits that remain are listed under [Waits that remain](#waits-that-remain).
3. **No read gets slower,** local or remote, under the [gates](#gates).
4. **Every capability and guarantee is kept:**
   - correctness, and robustness against crafted files (W, R);
   - zero-thread contexts, close safety, failure reporting, JFR events;
   - pruning by statistics, Bloom filters, dictionaries and page index;
   - row limits (`head`, `tail`, skip), nested and flat reads, row and column readers;
   - multi-file reads with schema checks, footers a `MetadataSource` supplies;
   - files in memory or mapped, S3, range-backed files, and the CLI's direct reads.
5. **No design decision is left to an implementing PR.** Each stage states the rules its PRs
   implement. A question an implementation raises that the plan does not answer goes back into the
   plan before the PR continues.
6. **Iteration stays fast:** a gate of the build plus seconds per PR, minutes per stage.
7. **Building out remote reads changes parameters, not mechanisms.**
8. **Complexity measurably drops:** see [Measures](#measures).

## Where the complexity is

Data reads go through `RangeRead`, whose future a fetch step yields on. Planning reads do not:
the footer, the page-index window, Bloom filters, pruning dictionaries and the page-format probe
call `InputFile.readRange` directly (11 call sites), so planning blocks the thread it runs on. The
following exist to work around that:

- three planning modes (`RowGroupIterator.PlanningMode`), planning on virtual threads, and
  `WorkItemPlanning` as the need a fetch step yields on;
- a second `PrefetchTasks` per read and `ReadExecutor.executePlanning`;
- `HardwoodContextImpl.compute` hops, which ship planning CPU from a blocking thread to the pool
  and wait;
- `FileMetadataCache`'s load hand-off and `DecodeExecutor.awaitFirst`, so a pool thread waiting for
  a footer can run its parse;
- `OnceCache`, and `ReentrantLock`s where monitors would do, so a virtual thread blocked in
  planning does not hold its carrier on JDK 21.

With no single place that issues bytes ahead, each read-ahead feature brought its own mechanism:

| Concern | Implementations today |
| --- | --- |
| Issue a byte request | `ChunkHandle`, `SharedRegion`, the `FileOpenAhead` span, `CoalescedRanges`, `DictionaryParser`, `RowGroupBloomFilterSource`, `PageFormatProbe`, `ParquetMetadataReader` |
| Read ahead | the `ChunkHandle` chain, the `SharedRegion` chain, `prefetchNextRowGroup` (index window, planning, first chunk), `FileMetadataCache.prefetch`, `FileOpenAhead` footers, `FileOpenAhead` spans through `FetchedAhead` |
| Coalesce | `coalescePages`, `coalesceAcrossColumns` (with `CoalescableFirstChunk`), `plannedSpan`, `CoalescedRanges` |
| Per-row-group state evicted in `releaseWorkItem` | three `OnceCache`s, reference counts, `nextRowGroupPrefetched`, `plannings`, `IndexWindow` releases, `FetchedAhead` spans |
| Report a speculative failure | `PrefetchTasks.report`, untaken `Error`s of `RangeRead` and `WorkItemPlanning`, `FileMetadataCache.close`, the sticky planning failure |

`RowGroupIterator` (2.5k lines) holds multi-file planning, schema compatibility, pruning, row
limits, page selection and coalescing, read-ahead, its caches and close, and the planning modes.

The scheduler side (`ReadScheduler`, the scheduled columns, `ReadQueue`, `WorkingSet`,
`ReadAheadBuffer`, `BatchExchange`) is essential to the decode, zero-thread and untrusted-input
designs. Stage 1 tidies it; no stage restructures it.

## Target model

A read has one fetch sequence, one planning chain, and its columns' fetch steps.

- **The fetch sequence** holds the read's byte requests (its entries) at positions in the order
  the read consumes them. It starts entries by the [rules](#the-sequences-rules) below, runs them
  on virtual threads of the context's I/O executor, and completes a future per entry. It never
  parses.
- **The planning chain** parses what the sequence delivered and inserts the entries that follow
  from it: a footer gives the file's work items and its windows; Bloom filters and dictionaries
  decide which row groups survive; the page index gives each column's pages; a row group's needed
  ranges, from every column, are coalesced once into its data entries.
- **Fetch steps** take a page's bytes as a slice of a completed entry, located by offset through an
  index fixed when the row group is planned.

### Positions

Positions are ordered by file, then row group, then kind:

| Kind | Per | Inserted when |
| --- | --- | --- |
| Footer | file | the read starts, or the file comes within the [footer share](#the-sequences-rules) |
| Bloom window | window of row groups | the file is planned |
| Pruning dictionaries | row group | its Bloom filters leave a leaf undecided that a dictionary can decide |
| Page-index window | window of row groups | a member survives Bloom filters and dictionaries |
| Mask probe | row group | its plans would apply a page mask and a column needs the probe (#1354 stays open) |
| Data | row group | the row group is planned |

This keeps today's order of a row group's planning reads: Bloom filters, dictionaries, page
index. A row group whose row ranges are all rows, as without a filter, has its data entries derived
from the footer's chunk ranges, which are the bytes its pages fill; they need no page index, so a
file opened ahead has its first data in flight before its index arrives, as `plannedSpan` does
today.

A file whose chunk lies in another file (`file_path`) gets no entries for that row group, and its
failure is raised when the read reaches it, as today.

### The sequence's rules

1. **Demand.** An entry is demanded once a step yields on it. A demanded entry starts at once,
   whatever F and R hold, and is never charged to either.
2. **The read's next unit.** For each column, the entry after the one it reads; at the end of a
   row group, the next row group's first entries; at the end of a file, the next file's footer and
   first row group. These start without F and are not charged to R, which is today's rule for a
   read's own task and its next file.
3. **Further ahead.** Every other entry is speculative. It starts, in position order, while the
   read's started speculative bytes stay within F and the context's R has room for them. Nothing
   waits for R: an entry that does not fit starts on a later release.
4. **Footers ahead.** Footers of files beyond the next one are speculative entries whose bytes may
   take at most a quarter of F, so data keeps the rest. Under a row limit, no footer beyond the
   next file is inserted, and F holds nothing beyond the next unit, as a read with a row limit
   opens only the next file today.
5. **Requests found during a parse.** A step may insert a demanded entry at its position: the
   header of a Bloom filter without a length and its rest, a dictionary longer than its first
   read, a page-format probe's longer peek, the rest of a column chunk a row limit left out. A
   range the sequence holds is sliced, never requested again.
6. **Release.** An entry is released once every column that slices it has passed it, and its
   charges go back to F and R. `PageInfo` slices may keep its buffer alive past release; W
   charges those already.
7. **Planning depth.** The planning chain plans the next unit, and plans further while the
   speculative bytes planned but not started are below F, so deeper read-ahead has entries to
   start. It plans row groups once each, in work-list order; a footer that arrives early waits its
   turn. `BatchPlan`, the row budget, the skip residue and the sticky planning failure stay with
   the chain.
8. **Files in memory.** Entries of a file whose bytes are in memory are created when demanded,
   complete at once, and are charged to neither F nor R. No entry of such a file is inserted
   ahead. A page costs one indexed slice, with no search, lock or atomic.
9. **Failure.** A speculative entry that fails is dropped and started again when demanded; the
   demanded entry's failure goes to the step. An `Error` no step took is raised once, by the
   read's close. A footer's failure is cached per file and raised when the read reaches that file,
   as THREADING invariant 7 requires today, so a schema mismatch in a file ahead fails the read
   only there.
10. **Close.** Close drops unstarted entries, waits for those in flight without interrupting them,
    and releases every charge. Requests in flight are bounded by the next unit plus F.
11. **Validation.** Before coalescing, each needed range is checked against the file's length and
    its column chunk's extent, and a range that fails is a `ParquetReadException` naming the
    column. A shared entry then never fails for a column whose ranges were valid.
12. **Fetch reason.** An entry carries the `FetchReason` its planner composed: the row group, the
    kind, and for data the columns it serves.
13. **Shared footers.** `FileMetadataCache` stays per `ParquetFileReader` and owns footer entries.
    A read's sequence references the entry; it is charged to the read that started it, and the
    cache's close awaits it, as today.

### Numbers

| | Meaning | Default | Takes over |
| --- | --- | --- | --- |
| G | the largest gap one request bridges | 1 MiB | `CoalescingPolicy.GAP_BYTES` |
| S | the largest request | 128 MiB | `CoalescingPolicy.MAX_SPAN_BYTES`, `sequentialChunkSize`, the open-ahead span limit |
| F | a read's speculative bytes beyond its next unit | set in stage 2, at least S | `maxFilesAhead`, `FOOTER_RESERVATION` |
| R | the speculative bytes of all reads of a context | 25 % of the heap | unchanged |
| W | the working set of one read, a hard limit | the heap | unchanged |

F ≥ S, so a speculative request of any size can start. The page-index window budget
(`indexWindowBytes`, 16 MiB) stays its own number: it bounds index bytes held across row groups,
which a read that stops early wastes, and a window's requests are capped at S. The footer request
size, the probe sizes and the row-limit estimate stay constants.

Today's request shapes are kept. In particular, whole column chunks of a row group already share
requests of up to S through `SharedRegion`; one coalescing pass keeps those shapes.

### Waits that remain

- The thread that opens a reader reads the first file's footer.
- A `tail` read's probe, `getFileMetaData(int)` and the other accessors that answer before a read
  starts wait on their caller.
- A `MetadataSource` call and S3's `InputFile.open()` (whose suffix GET reads the footer) block;
  they run as footer entries on virtual threads of the I/O executor.
- `PageIterator` and the CLI's direct calls (`DictionaryParser.readPage`,
  `RowGroupIndexBuffers.fetch`, `PageFormatProbe`) keep blocking variants for callers outside a
  read.
- Until stage 3, planning runs on a virtual thread and waits there for its entries.
- A range-backed file (`RangeBackedInputFile`) serializes its misses under one lock, which limits
  parallel requests on that backend.

## Measures

The fetch and planning classes (`RowGroupIterator`, `SequentialFetchPlan`, `IndexedFetchPlan`,
`ChunkHandle`, `SharedRegion`, `RangeRead`, `PrefetchTasks`, `FileMetadataCache`,
`FileOpenAhead`, `FetchedAhead`, `IndexWindow`, `CoalescedRanges`, `OnceCache`,
`WorkItemPlanning`, `PageSource`, `ParquetMetadataReader`, and the classes the stages move code
into or add) measure, on `4fbba14e7`:

- 4,325 lines of code, comments and blank lines excluded;
- 88 concurrency constructs: locks, `synchronized`, atomics, futures and `volatile` fields.

Each stage's last PR reports both. Expected after stage 2: about −20 % code and −40 % constructs;
after stage 3, about −25 % and −55 %.
A stage that falls well short says why in its PR before the next one starts.

## Gates

Against a baseline jar of `4fbba14e7`, with `run-gate.sh` in hardwood-benchmarks:

| When | Gate | Time |
| --- | --- | --- |
| Every PR | `./mvnw verify -DskipITs` and the `-Pzero-threads` core suite; the request-shape tests; `run-gate.sh --counts-only`: requests of every S3 read the benchmark checks no more than the baseline's, and bytes no more unless requests fell within the I/O budget's bridging; the full `./mvnw verify` before push when the PR touches the S3 path | the build plus about 20 s |
| The last PR of stages 2 and 3 | `run-gate.sh` on the container: the projected, filtered, many-small-files, multi-file and remote-beside-wide-remote contenders at 30 ms, base and head interleaved over two rounds, none slower beyond a band of at least 5 % | about 4.5 min |
| The end of stages 2 and 3, and before 1.1.0.CR1 | `run-regression.sh`, narrow and flat presets, on the N300 pinned at 1.50 GHz, two rounds, once on a pool and once with `-Dhardwood.internal.zeroThreads=true`: 0 rows slower | |
| Before 1.1.0.CR1 | `run-s3.sh` at 235 ms, many-small-files included | |

The counts are exact, and they are what an I/O restructuring changes first. Base against base,
the timed S3 contenders move by at most 2 % between rounds. Local reads are not timed on the
container, where short runs of the same jar move by 20 % and more between rounds; the N300 runs
cover them at the end of each stage, the zero-thread path included. `head`, `tail`, nested reads
and chunks without an offset index are held by their request-shape tests and the counts. A stage
changes no default these gates do not cover. Each PR updates the design documents its change
alters (`READ_PIPELINE.md`, `FETCH_PLANNING.md`, `THREADING.md`, `FILE_METADATA.md`).

## Stages

| # | Stage | Removes | Risk |
| --- | --- | --- | --- |
| 0 | Gate script and baseline | — | low |
| 1 | Deletions and moves | dead code, hand-written waits, `RowGroupIterator` down to its work list | low |
| 2 | The fetch sequence | `SharedRegion`, `CoalescableFirstChunk`, the chunk chains, `FetchedAhead`, `FileOpenAhead`, `plannedSpan`, `prefetchNextRowGroup`, `CoalescedRanges`, `RangeRead.servedBy`, the `FileMetadataCache` prefetch maps | medium |
| 3 | Planning as steps (go/no-go) | `PlanningMode`, `WorkItemPlanning`, the planning `PrefetchTasks`, `executePlanning`, the planning `compute` hops, the load hand-off, `awaitFirst` | high |
| 4 | Fold into the design documents | this plan | low |

Stages run in order, one PR each or more, every commit keyed to #1662. Stopping after stage 2 is
an accepted end state: stage 3 starts only if stage 2's measures and the effort it would take
justify it.

### Stage 0: baseline

- `run-gate.sh` in hardwood-benchmarks, calibrated base against base, and the baseline jar of
  `4fbba14e7`. The multi-file and remote-beside-wide-remote contenders join the timed set and the
  calibration is repeated for them.
- #1630, #1629 and #1393 continue alongside; they touch fuzz targets and `S3Fetcher`'s body
  handling. #1399 lands in stage 2, after #1629.

### Stage 1: deletions and moves

No behaviour changes. Nothing a later stage deletes is reworked here.

- Remove `ScheduledColumn.unparkScheduler` (no caller), `ScheduledColumn.fail` (an alias of
  `signalError`) and `ColumnPipeline` (one implementor). `RangeRead.Priority` goes too; the
  sequence carries the demanded and speculative distinction from stage 2 on.
- Move schema compatibility (`validateSchemaCompatibility`, `ChunkPathCheck`, the touched columns),
  pruning (statistics, Bloom and dictionary probes and their events) and page selection with
  coalescing out of `RowGroupIterator`, and reduce its five `initialize` overloads and four
  constructors to one each. `RowGroupIterator` keeps the work list and its lifecycle.
- Scheduler side:
  - one waiter for a release of R, in place of the copies in `ReadScheduler` and
    `WorkingSet.Column`, which the sequence then uses as well;
  - one take-use-return helper in `DecodeExecutor`;
  - one ordered set in `ReadScheduler` for queued and pending columns;
  - one per-read count of tasks and yielded steps, in place of `ScheduledColumn.tasks`, the
    column-level `CloseClaim` and `ReadQueue`'s `outstanding`, `yielded` and `resumed` (a
    yielded step is counted twice today);
  - one per-read wait primitive, in place of the park loops in `BatchExchange`,
    `ScheduledColumn.close` and `ReadQueue.runUntil`.

### Stage 2: the fetch sequence

The sequence replaces every request issuer and read-ahead mechanism of a read, with the
[rules](#the-sequences-rules) above. Planning still runs on its virtual thread or its step as
today, and waits there for the entries it needs; it no longer reads the file itself. Its PRs, in
order:

1. **The sequence for data, with one coalescing pass.** A row group's data entries are built
   from every column's needed ranges at once (page groups, the dictionary page, a column without
   an offset index whole or, under a row limit, the first part today's estimate sizes), coalesced
   under G and S. This replaces the chunk and region chains, cross-column coalescing,
   `prefetchNextRowGroup`'s first chunk, `FileOpenAhead`'s spans with `FetchedAhead`, and
   `plannedSpan`. The planning of files ahead (`planFilesAhead`) moves to the planning chain's
   depth rule. F's default is set here, the smallest value at least S at which no gate contender
   is slower than the baseline. A column without an offset index scans page headers across the
   entries that hold its chunk, as `SequentialFetchPlan`'s walk does today: a page straddling the
   cut between two entries is copied from both into one buffer, at most once per cut, and under a
   row limit the rest of the chunk is inserted as a demanded entry only when the walk reaches it.
2. **Footers.** Footer reads, `MetadataSource` calls and S3's open become footer entries, with the
   footer share. This replaces `FileOpenAhead`'s footers and `FileMetadataCache`'s prefetch maps.
   #1399 lands here, after #1629: the S3 suffix GET's size becomes an internal setting the core
   passes to the S3 module (64 KiB when a `MetadataSource` supplies the footers). The suffix is the
   footer entry's bytes: a data entry inside it is sliced from it (rule 5), so a file up to that
   size costs one request, and it is released by rule 6, right after planning when no data entry
   slices it. That settles #100's memory concern without closing files early; #100's early close
   stays out of this plan. The size is tuned on real S3 (#1671).
3. **Planning reads.** The Bloom window, pruning dictionaries, page-index window and mask probe
   become entries, at the positions above. This replaces `CoalescedRanges` and the direct
   `readRange` calls of `IndexWindow`, `DictionaryParser`, `RowGroupBloomFilterSource` and
   `PageFormatProbe` within a read; their blocking variants stay for callers outside a read.
   #1659 lands here: a page-index window is inserted once a member survives its Bloom filters and
   dictionaries, so a window whose members they all drop costs no index read.

Drop events (`RowGroupBloomFilterEvent`, `RowGroupDictionaryFilterEvent`) are emitted when the
read reaches the row group, as today, though planning may decide earlier.

### Stage 3: planning as steps (go/no-go)

The planning chain becomes steps that yield on entries, on the pool or the read's queue, so
planning no longer needs a virtual thread. This removes `PlanningMode`, `WorkItemPlanning`, the
planning `PrefetchTasks`, `executePlanning`, the planning `compute` hops,
`FileMetadataCache`'s load hand-off and `DecodeExecutor.awaitFirst`, and checks whether
`OnceCache` and the `ReentrantLock`s still need their JDK 21 reasons. The probes' adaptive
re-reads become steps that insert their longer entries (rule 5).

The decision to start it is taken after stage 2, on its measures: stage 3 is expected to remove
about 450 lines and add a comparable amount, and pays only if the constructs and the modes it
removes outweigh that. Weighed with the lines: after stage 3, virtual threads run only the
blocking `InputFile.readRange` of an entry in flight, which holds no lock and does no CPU work.
Planning on virtual threads is what needs today's workarounds: the planning modes, the `compute`
hops that send planning CPU back to the pool, the footer-parse hand-off, and `OnceCache` and
`ReentrantLock`s in place of monitors so that JDK 21 does not pin a carrier. Two questions are settled in it if it runs:

- Whether a dictionary parse whose buffers exceed `DecodeBuffers.RETAINED_BYTES` still needs
  admission to R beside the read's own task, or W alone bounds it.
- Whether the bookkeeping that raises an untaken `Error` exactly once can go.

### Stage 4: fold into the design documents

The end state goes into `READ_PIPELINE.md`, `FETCH_PLANNING.md`, `THREADING.md` and
`FILE_METADATA.md`, and this plan is deleted. #1663 then continues to 1.1.0.CR1.

## After this plan

The remote work superseded by the target model was closed. It continues as measured changes to S,
F and the sequence's rules, each with its own issue when it starts:

- split and parallel requests: a smaller S under a larger F, up to F/S speculative requests in
  flight (the range-backed backend serializes them);
- deeper cross-file read-ahead: F and the footer share;
- overlapped planning reads: their entries started together within F;
- cancellation of requests in flight on close, which deeper read-ahead makes worth having;
- an observable for a step that waits on a fetch (#1042).

Their gates are the ones above, plus one run on real S3 from a distant region (#1671).
