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
     I/O. Planning meets this only with stage 3; the waits that remain are listed under
     [Waits that remain](#waits-that-remain).
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
8. **Complexity measurably drops:** one mechanism per concern (stage 3's decision rule), with the
   lines and constructs of [Measures](#measures) reported alongside.

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
  parses, with one exception: a `MetadataSource` that has no footer for a file lets the file's
  footer entry parse it there, as today's prefetch does.
- **The planning chain** parses what the sequence delivered and inserts the entries that follow
  from it: a footer gives the file's work items and its windows; Bloom filters and dictionaries
  decide which row groups survive; the page index gives each column's pages; a row group's needed
  ranges, from every column, are coalesced once into its data entries.
- **Fetch steps** take a page's bytes as a slice of a completed entry, located by offset through an
  index fixed when the row group is planned.

The sequence is created when the reader is built. Planning a reader does before any read starts
(the first footer, a fast `tail`'s probe, `hasLiveWorkItem`) inserts demanded entries and waits
for them on its caller, so the read later slices what that planning fetched.

A file whose bytes are in memory gets no entries: its planning reads the mapping directly and its
columns slice the mapping by their plans' offsets, as today.

### Positions

Positions are ordered by file, then row group, then kind:

| Kind | Per | Inserted when |
| --- | --- | --- |
| Footer | file | the read starts, or the file comes within the files ahead (rule 4) |
| Bloom window | window of row groups | the file is planned |
| Pruning dictionaries | row group | its Bloom filters leave a leaf undecided that a dictionary can decide |
| Page-index window | window of row groups | a member survives Bloom filters and dictionaries; the window holds every member's slices, as today |
| Mask probe | row group | its plans would apply a page mask and a column needs the probe (#1354 stays open) |
| Data | row group | the row group is planned |

This keeps today's order of a row group's planning reads: Bloom filters, dictionaries, page
index. A row group whose rows are all read and that no Bloom filter or dictionary can drop has its
data entries derived from the footer's chunk ranges, cut into requests of at most S, so they can
start before its page index arrives, as `plannedSpan` does for files opened ahead today; its page
index is still read for the columns that have one, which walk their pages by it. A page that
straddles a cut is copied from both entries into one buffer, at most once per cut, whichever walk
reads it.

A row group with a chunk in another file (`file_path`) gets no entries, and its failure is raised
when the read reaches it, as today.

### The sequence's rules

1. **Demand.** An entry is demanded when a fetch step yields on it, when a step or a caller waits
   on planning or a footer load that needs it, and always when planning runs on the thread
   driving a read of a context of zero threads. A demanded entry starts at once, whatever F and R
   hold, and is never charged to either.
2. **The read's next unit.** When the first column enters row group k, row group k+1's planning
   entries and its first data entry in position order are the next unit; at the last row group
   of a file, the next file's footer and its first row group's are. A row group a column asks to
   plan before any column has entered it (the read's first row group, or one whose next-unit
   planning failed) starts its first data entry the same way. These start without F and are not
   charged to R, which is today's rule for a read's own task and its next file. The next unit's
   planning reads (their I/O; the parse stays with the chain) start when the first column enters
   k, whatever the planning chain is planning: until planning reads are entries (stage 3)
   on a virtual thread of the read's own, and from then on as entries the sequence starts itself.
3. **Data further ahead.** Every other data entry is speculative. It starts, in position order,
   while the read's started speculative data bytes stay within F and the context's R has room for
   them. Nothing waits for R: an entry that does not fit starts on a later release. A release of
   R wakes its waiters in registration order, decode tasks and entries alike, as today. An idle
   read holds what it has started, at most F, until it is released or closed.
4. **Files ahead.** The next file's footer entry starts with the next unit. A footer past it starts
   only while the planning chain has room to plan its file (the entries planned and not started are
   below F), for at most `maxFilesAhead` (64) files after the one the read has reached, the next
   file included, a cap on the footer requests in flight rather than on how far planning goes. Each
   past the next file is charged to R at the tail's size from its start until the read is done with
   the file, which drops the tail. Footers are outside F. Under a row limit, no footer beyond the
   next file is inserted, and no data beyond the next unit starts, as a read with a row limit opens
   only the next file today.
5. **Requests found during a parse.** A step may insert a demanded entry at its position: the
   header of a Bloom filter without a length and its rest, a dictionary longer than its first
   read, a page-format probe's longer peek, the rest of a column chunk a row limit left out. A
   range the sequence holds is sliced, never requested again.
6. **Release.** An entry is released once every column whose chunk extent overlaps it has passed
   it; a column with no pages in the row group, or one skipped or stopped at its limit, does not
   hold it. Its charges then go back to F and R. A `PageInfo` slice pins the whole entry past
   release, while W charges only the page; that remainder is uncharged and at most S per column,
   as a chunk handle's buffer is today.
7. **Planning depth.** The planning chain plans the next unit, and further while the speculative
   data bytes planned but not started are below F, and never more than one row group past the
   last row group with a started data entry, which bounds the dictionaries and index windows held
   for planned row groups. For a read whose files are all in memory, depth stays one row group on
   a pool and none on a context of zero threads, as today. The chain starts when a column first
   asks for a work item, and plans row groups once each, in work-list order: it plans row group j
   only once j-1 has its plans, so a planning that waits for a read holds the chain behind it. A
   footer that arrives early waits its turn. `BatchPlan`, the row budget,
   the skip residue and the sticky planning failure stay with the chain.
8. **Concurrency.** Each sequence has one `ReentrantLock`, which guards its entries and positions
   and is never held across I/O, a waiter or a future's completion. Planning, demanding steps,
   completions, R's waiters and close take it briefly. A fetch step's slice reads an index fixed
   when the row group was planned, without the lock.
9. **Failure.** A speculative data entry that fails is dropped and started again when demanded;
   the demanded entry's failure goes to the step. An `Error` no step took is raised once, by the
   read's close. A footer entry is the exception: its failure is cached per file and raised when
   the read reaches that file, as THREADING invariant 7 requires today, so a schema mismatch in a
   file ahead fails the read only there.
10. **Close.** Close drops unstarted entries, waits for those in flight without interrupting them,
    and releases every charge. Requests in flight are bounded by the next unit, F and the files
    ahead. A footer entry the reader's `FileMetadataCache` shares keeps its charge until it
    completes, whichever read started it.
11. **Validation.** Before coalescing, each needed range is checked against the file's length and
    its column chunk's extent. A range that fails, or chunk extents that overlap, is a
    `ParquetReadException` naming the column, so a shared entry never fails for a column whose
    ranges were valid.
12. **Fetch reason.** An entry carries today's `FetchReason` strings: `rg=N indexes`,
    `rg=N pruning`, and for data `rg=N col=a col=b`, one `col=` for each column it serves, by name,
    in file order.
13. **Events.** `RowGroupFilterEvent`, `PageFilterEvent`, `RowGroupBloomFilterEvent` and
    `RowGroupDictionaryFilterEvent` are emitted when the read reaches the file or row group they
    describe, as today, though planning may decide earlier. `FileOpenedEvent` begins when the
    footer entry starts and is committed by the parse.

### Numbers

| | Meaning | Default | Property | Takes over |
| --- | --- | --- | --- | --- |
| G | the largest gap one request bridges | 1 MiB | — | `CoalescingPolicy.GAP_BYTES` |
| S | the largest request | 128 MiB | `hardwood.internal.maxCoalescedBytes` | `MAX_SPAN_BYTES`, `sequentialChunkSize`, the open-ahead span limit |
| F | a read's speculative data bytes beyond its next unit | 2 S, confirmed by stage 2's gate | `hardwood.internal.fetchAheadBytes` | the depth of today's open-ahead spans |
| R | the speculative bytes of all reads of a context | 25 % of the heap | `hardwood.internal.readAheadBytes` | unchanged |
| W | the working set of one read, a hard limit | the heap | `hardwood.internal.maxWorkingSetBytes` | unchanged |

Also kept as they are: the files ahead (64 since stage 2, `hardwood.internal.maxFilesAhead`), the page-index
window budget (16 MiB, `hardwood.internal.indexWindowBytes`, which bounds index bytes held across
row groups and caps a window's requests at S), the footer request size, the probe sizes and the
row-limit estimate. Tests that shrink `sequentialChunkSize` to force small pieces shrink S
instead; where that also changes their coalescing, their expectations are restated in the PR
that moves them.

Request shapes change in one respect: page groups, unfolded dictionary pages and row-limited
first parts of different columns may now share a request across a gap of at most G, which today
only whole column chunks do (`isCoalesceSafe`). The PR that does it restates the expectations of
`CrossColumnCoalesceTest` and `PageRangeIoTest` with fewer requests, within `IoBudget`. Every
other shape is kept.

### Waits that remain

- The thread that opens a reader reads the first file's footer.
- A `tail` read's probe, `getFileMetaData(int)` and the other accessors that answer before a read
  starts wait on their caller.
- A `MetadataSource` call and S3's `InputFile.open()` (whose suffix GET reads the footer) block;
  they run as footer entries on virtual threads of the I/O executor.
- The CLI's direct calls (`DictionaryParser.readPage`, `RowGroupIndexBuffers.fetch`,
  `PageFormatProbe`) keep blocking variants for callers outside a read.
- Until stage 3, planning waits for its entries: on a virtual thread on a pool, and on the thread
  driving the read on a context of zero threads, as today (#1649). Goal 2's last point holds for
  planning only after stage 3.
- A range-backed file (`RangeBackedInputFile`) serializes its misses under one lock, which limits
  parallel requests on that backend.

## Measures

`tools/read-path-measures.sh`, committed in stage 0, counts in the fetch and planning classes
(`RowGroupIterator`, `SequentialFetchPlan`, `IndexedFetchPlan`, `ChunkHandle`, `SharedRegion`,
`RangeRead`, `PrefetchTasks`, `FileMetadataCache`, `FileOpenAhead`, `FetchedAhead`, `IndexWindow`,
`CoalescedRanges`, `OnceCache`, `WorkItemPlanning`, `PageSource`, `ParquetMetadataReader`,
`DictionaryParser`, `RowGroupBloomFilterSource`, `PageFormatProbe`, `RowGroupIndexBuffers`,
`ReadAheadBuffer`, `ReadExecutor`, `DecodeExecutor`, `HardwoodContextImpl`, and every class the
stages move code into or add):

- lines of code: lines that are neither blank nor comments, imports included;
- concurrency constructs: lines containing `ReentrantLock`, `synchronized`, `Atomic*`,
  `CompletableFuture`, `volatile` and `tryLock`.

The baseline on `4fbba14e7` is 5,314 lines of code and 96 concurrency constructs. Each stage's last PR reports both. Stage 2 ends near
line-neutral (5,393 lines and 86 constructs after its two items): the fetch sequence implements
the whole rule set, F, R, release by overlapping extent, the re-read of a failed read and the
footers ahead, which each mechanism it replaced did only in part, and stage 2 keeps planning as
blocking code, with its two carriers and the transitional next-unit read. The structural
deletions are stage 3's; its spike states the reduction it expects, measured, before it starts. A
stage that falls well short says why in its PR before the next one starts.

## Gates

Against a baseline jar of `4fbba14e7`, with `run-gate.sh` in hardwood-benchmarks:

| When | Gate | Time |
| --- | --- | --- |
| Every PR | `./mvnw verify -DskipITs` and the `-Pzero-threads` core suite; the request-shape tests; `run-gate.sh --counts-only` (the full `./mvnw verify` before push when the PR touches the S3 path) | the build, the second core suite and about 20 s |
| The last PR of stages 2 and 3 | `run-gate.sh` on the container: the projected, many-small-files, multi-file and remote-beside-wide-remote contenders at 30 ms, base and head interleaved over two rounds, none slower beyond a band of at least 5 % | under 5 min |
| The end of stages 2 and 3, and before 1.1.0.CR1 | `run-regression.sh`, narrow and flat presets, on the N300 pinned at 1.50 GHz, two rounds, once on a pool and once with `-Dhardwood.internal.zeroThreads=true`: 0 rows slower | |
| Before 1.1.0.CR1 | `run-s3.sh` at 235 ms, many-small-files included | |

The counts check passes when, for every S3 read the benchmark checks, requests are no more than
the baseline's, and bytes no more than the baseline's plus G for every request saved plus, per
file, the S3 suffix's growth over 64 KiB. Base against base, the timed S3 contenders moved by at
most 0.8 % between rounds; the filtered scan's three requests are held by the counts. Local reads are not timed on the container, where short runs of the
same jar move by 20 % and more between rounds; the N300 runs cover them at the end of each stage,
the zero-thread path included. `head`, `tail`, nested reads and chunks without an offset index
are held by their request-shape tests and the counts. A stage changes no default these gates do
not cover. Each PR updates the design documents its change alters (`READ_PIPELINE.md`,
`FETCH_PLANNING.md`, `THREADING.md`, `FILE_METADATA.md`).

## Stages

| # | Stage | Removes | Risk |
| --- | --- | --- | --- |
| 0 | Gate script, measures script, baseline | — | low |
| 1 | Deletions and moves | dead code, hand-written waits, `RowGroupIterator` down to its work list | low |
| 2 | The fetch sequence | `SharedRegion`, `CoalescableFirstChunk`, the chunk chains, `FetchedAhead`, `FileOpenAhead`, `plannedSpan`, `prefetchNextRowGroup`, `RangeRead.servedBy`, the `FileMetadataCache` prefetch maps | medium |
| 3 | Planning as steps, with planning reads as entries (go/no-go) | `PlanningMode`, `WorkItemPlanning`, `OnceCache`, `LazyMaskCapability`, `planLock`, the planning `PrefetchTasks`, the planning `compute` hops, the load hand-off, `awaitFirst`, the next unit's read on a virtual thread of its own | high |
| 4 | Fold into the design documents | this plan | low |

Stages run in order, one PR each or more, every commit keyed to #1662. Stopping after stage 2 is
an accepted end state: stage 3 starts only if its decision rule below says so.

### Stage 0: baseline

- `run-gate.sh` in hardwood-benchmarks (hardwood-benchmarks#35), calibrated base against base,
  and the baseline jar of `4fbba14e7` (`1.1.0-m1665-SNAPSHOT`).
- `tools/read-path-measures.sh` and the baseline it records.
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
today, and waits there for the entries it needs; it no longer reads the file itself.

The first PR starts as a spike: the sequence for data on a branch, run against the full test
suite, the request-shape tests and the counts gate. The rules above are the spike's
specification. A decision the spike forces that they do not settle is written into this plan,
and reviewed there, before the PR continues.

Its PRs, in order:

1. **The sequence for data, with one coalescing pass.** A row group's data entries are built
   from every column's needed ranges at once (page groups, the dictionary page, a column without
   an offset index whole or, under a row limit, the first part today's estimate sizes), coalesced
   under G and S. This replaces the chunk and region chains, cross-column coalescing,
   `prefetchNextRowGroup`'s first chunk, `FileOpenAhead`'s spans with `FetchedAhead`, and
   `plannedSpan`. The planning of files ahead (`planFilesAhead`) moves to the planning chain's
   depth rule. A column without an offset index scans page headers across the entries that hold
   its chunk, as `SequentialFetchPlan`'s walk does today: a page straddling the cut between two
   entries is copied from both into one buffer, at most once per cut, and under a row limit the
   rest of the chunk is inserted as a demanded entry only when the walk reaches it.
   `FetchPlan.pages()` and `PageIterator` go with the walk they wrap: no main code calls them,
   and the tests and benchmarks that walk a plan with them move to driving the sequence.
2. **Footers.** Footer reads, `MetadataSource` calls and S3's open become footer entries, with the
   files-ahead rule. This replaces `FileOpenAhead`'s footers and `FileMetadataCache`'s prefetch
   maps. #1399 lands here, after #1629. A footer entry reads the file's tail, its last 256 KiB
   (an internal property), in one request, and every read inside the tail is sliced from it: the
   footer, the page index, and data, whose ranges coalescing cuts at the tail's start (rule 5), so
   a file up to that size costs one request. `S3InputFile` is opened by the engine alone and
   needs no hook into the core: its open fetches a suffix of that same size, and the footer
   entry's read of the whole tail takes it, so S3 holds no tail from then on. The read drops the
   tail when it is done with the file. That settles #100's memory concern without closing files
   early; #100's early close stays out of this plan. Reads whose footers a `MetadataSource`
   supplies take the same tail. Core's tests read an eight-byte tail, so a fixture of a few
   kilobytes still reads as a file larger than its tail. The size is tuned on real S3 (#1671).
F's default is confirmed at stage 2's last PR, once footers are in the sequence: 2 S unless a gate contender is slower than the baseline there, in which case the
smallest of 2 S, 3 S and 4 S that is not.

### Stage 3: planning as steps (go/no-go)

The planning chain becomes one step on the read's executor, and planning stops reading the file
itself: every byte planning needs is an entry of the sequence, and every parse of file structure is
a function of the bytes the read holds. Planning then needs no thread of its own, and the
mechanisms that work around a blocking planner go. #1659 lands here.

The rules below are the spike's specification, as settled with the maintainer during the spike. A
decision a later PR forces that they do not settle is written here before it continues.

**Planning reads as entries.**

- **S1. Kinds and starts.** A row group's planning entries are its Bloom window's, its pruning
  dictionaries', its page-index window's and its mask probe's. The chain inserts each when a stage
  needs it and demands it at once; planning entries are never speculative, so they are outside F
  and uncharged to R. The chain's depth (rule 7) and the index-window budget bound what they hold.
  A row group's pruning dictionaries are read one at a time, as the filter's fold reaches each
  leaf a dictionary can decide, so a dictionary the fold never reaches is never read.
- **S2. One holder.** `PlanningReads` holds every planning entry of one file: a window's requests,
  inserted as the file is planned and held by every member, and a work item's dictionary pages,
  filters without a length and probes, held by the work item. A range inside the file's tail while
  the read holds it, or of a file in memory, is sliced in place and needs no entry; so is a data
  entry inside the tail, which then starts no request and is charged to neither F nor R, since the
  tail is charged already. Entries are released with the work
  items holding them, or on close. A window's slices are merged into requests by
  `CoalescedRanges`, which only plans them.
- **S3. Page-index windows (#1659).** A window's page-index requests start when the chain plans
  the first member its Bloom filters and dictionaries keep. A window whose members are all dropped
  costs no index read.
- **S4. Sans-I/O parses.** The Bloom filter, dictionary page and page-format probe parses read
  through a `ByteSource` and never read the file: a range the source does not hold is `null`, and
  the parse returns the range it needs (`Parsed`). Within a read the source is the read's
  `PlanningReads`; outside one (the CLI's calls) it reads the file and holds every range, so one
  parser serves both. A parse that finds its bytes short (a Bloom filter without a length, a
  dictionary page longer than its opening read, a probe's longer peek) returns the longer range,
  which is rule 5.

**Planning as a step.**

- **S5. One chain.** Each read has one planning chain, run as a step under one claim. It plans
  work items once each, in work-list order, files included, through stages: the file's footer and
  plan for a file's first work item, Bloom filters and dictionaries, page index and page filter,
  the mask probe when its plans would apply a mask, and the plans. Each work item's planned state
  is written once, by the chain, before it is published planned, and read without a lock after.
- **S6. Need and resume.** A stage that needs what the read does not hold returns it: a footer's
  read or another read's load of it, a window's entries, a range of the file. The chain demands it,
  registers to run again once it is met, and lets its thread go, keeping the claim. It resumes by
  running the stage again over what the read now holds. The dictionary fold keeps each leaf's
  decision once final, so no dictionary check runs twice. A filter or dictionary not held yet
  counts as absent for one evaluation: a drop stands, a keep is evaluated again once the first
  range the evaluation missed has arrived; the fold reaches the same leaves in the same order as a
  blocking read would, so it reads the same dictionaries.
- **S7. Where it runs.** On the read's executor: at the tail while it plans ahead, at the head
  while a column or caller waits for it, and in place by a fetch step that needs a work item
  planned and finds the chain idle. A read of files in memory, whose chain never waits, therefore
  plans on the step, as it did with planning modes. Planning CPU runs where the chain runs, so the
  planning `compute` hops go; a footer a caller outside a read loads keeps its hop.
- **S8. Depth, not modes.** `PlanningMode` goes. The chain plans the work items the columns asked
  for, the read's next unit, and further as rule 7 allows: past the next unit by F for a read of
  files not all in memory without a row limit, and no next unit at all for a read of files in
  memory on a context of zero threads.
- **S9. Columns and callers wait for one cursor per stage.** A column waits for "work item i
  planned", a need the chain keeps; the sequence only answers whether the chain may plan a work
  item ahead (rule 7). A caller outside the read that needs a work item taken only
  part of the way before the read starts (`workItemAt`, `getWorkItems`, `hasLiveWorkItem`,
  `canFastSkipAllRowGroups`) asks for it to be taken through a stage (listed, arrival, mask) and
  waits for that stage's published cursor: parked on a pool, running the read's queue on a
  context of zero threads.
- **S10. Reaching a file.** The chain decides when the read has reached a file, from the furthest
  work item the read has reached (a column's request, the next unit, a caller's) and that work
  item's file, or, for a work item not listed yet, the file listed next. Reaching a file moves the
  footers ahead (rule 4), which the sequence starts, and commits the file's `RowGroupFilterEvent`
  once the file is planned.
- **S11. Footers.** A footer read is a task of the read that starts it, like every other read: it
  runs on the read's `PrefetchTasks`, whose close waits for it, and an `Error` it failed with that
  no load took is raised by that close. It opens the file rather than reading a known range, so it
  is the one read that is not a `RangeRead`. The chain takes a footer without waiting: it yields
  on the footer's read, or on another thread's load of it, and once the footer has been read loads
  it in place. A caller outside a read registers the load itself and waits for it, so a close waits
  for that load. A footer read a read's close dropped before it started is no read: the next
  request starts it again.
- **S14. What a read holds.** Each listed work item has one state from its listing until every
  column has released it, when the work list drops it: the work list holds the work items the
  read has not passed. A read drops a file's tail and footer once it is done with the file, the
  first file's footer excepted, which the reader keeps; a later read, or the reader's metadata
  accessor, reads a dropped footer again. A read of many files therefore holds the footers and
  work items of the files it is reading, not of every file it has read.
- **S12. Priority.** A demanded entry starts at once (rule 1); the next unit's planning entries
  start only when the first column enters the row group before it (rule 2), after that row group's
  data entries are demanded or started; planning past the next unit is bounded by rule 7. Nothing
  preempts a request in flight, and a fetch step's work runs at the head of the executor while the
  chain planning ahead runs at its tail.
- **S13. Failure and close.** A failure planning a file or a work item ends planning there: the
  work item it hit and every one after it take the same instance, and so do the columns that reach
  them. A planning entry fails once, ahead of demand or not; it is not read again (a transient
  failure of a planning read ahead of demand fails the read at that row group, where stage 2 read
  it again). An `Error` that ended planning and that no column took is raised once, by close. A
  pruning dictionary keeps its admission to R beside the read's own task. Close ends the chain,
  waits for a step of it that runs, and discards the planning entries.

**Decision rule.** Stage 3 starts if the spike reduces every concern below to one mechanism, or
to two where the table says why, with every gate passing. Lines and constructs are reported, not
gated.

| Concern | Stage 2 | After stage 3 |
| --- | --- | --- |
| Issue a byte request | sequence entries, `CoalescedRanges`, `readRange` in `DictionaryParser`, `RowGroupBloomFilterSource`, `PageFormatProbe` and the footer read | sequence entries, run with the footer reads as tasks of the read (S11) |
| Plan a row group | on demand on a virtual thread or the step, ahead, the next unit's read, two caller paths | the chain |
| Keep a row group's state | three `OnceCache`s, the plannings map, the next-unit set, the reference counts, the entered and reported sets, the work list | one `Planning` per work item, which the work list holds (S14) |
| Wait for planning | `WorkItemPlanning`, blocking `OnceCache`, the load's join and hand-off, `IndexWindow`'s lock | a need for steps, `ReadTasks` for callers |
| Run planning CPU on the pool | four `compute` hops, `awaitFirst`, the hand-off | where the chain runs |
| Fail planning | sticky for files, re-planned for row groups, swallowed ahead, untaken `Error`s | sticky, one instance |
| Order planning | `planLock`, `IndexWindow`'s lock, `OnceCache`, the chain's claim | the chain's claim and its cursor |
| Get a footer | the load, the read ahead, the hand-off, `awaitFirst` | the chain's need and a caller's load (a caller may block, the chain may not) |
| Decide the read has reached a file | the planning lock's callers | the chain (S10) |

The spike measured 5,719 lines of code and 76 concurrency constructs, against stage 2's 5,401 and
86. Stage 3 measures 5,742 and 80: moving the cursor and the reached file into the
chain, footer reads onto the read's tasks and the per-work-item state into one object left the
size where it was, and dropped what a read holds per file (S14) and slicing data inside the tail
(S2) added code.

### Stage 4: fold into the design documents

The end state goes into `READ_PIPELINE.md`, `FETCH_PLANNING.md`, `THREADING.md` and
`FILE_METADATA.md`, and this plan is deleted. #1663 then continues to 1.1.0.CR1.

## After this plan

The remote work superseded by the target model was closed. It continues as measured changes to S,
F and the sequence's rules, each with its own issue when it starts:

- split and parallel requests: a smaller S under a larger F, up to F/S speculative requests in
  flight (the range-backed backend serializes them);
- deeper cross-file read-ahead: F and the files ahead;
- overlapped planning reads: their entries started together within F;
- cancellation of requests in flight on close, which deeper read-ahead makes worth having;
- an observable for a step that waits on a fetch (#1042).

Their gates are the ones above, plus one run on real S3 from a distant region (#1671).
