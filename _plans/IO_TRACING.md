# Plan: trace a read's fetch sequence

Tracking issue: #827. Stage issues: one per PR from 2 to 9, filed once they are agreed on #827.

Related:

- #828: the earlier design (Perflab), which predates the read engine. This plan replaces it.
- #1753: the read engine of parts (`ReadEngine`, `RangeFetcher`, `JobScheduler`, `FooterReader`),
  which this plan's sites are named on.
- #1734: gives a read's data buffers back to the `InputFile` once the read is done with them.
- #1754: derives how far a read fetches ahead from its rates, the first consumer of PRs 2 and 3.
- #1671 (real-S3 latency run), #763 (latency model, sleep benchmark), #1688 (scorer).
- #1401 (hedging, held in #1663), #1687 (parallel part GETs), #1661 (page faults).
- #1042 (`PrefetchMissEvent`), #1464 (a test fix PR 6 needs).

This plan adds JFR events that record what a read planned, what it ran and when it gave the bytes
back, and the `hardwood iotrace` command.

## Why trace a read's I/O

Hardwood's fetch planning decides which byte ranges a read fetches, which nearby ranges it merges
into one request, and how far ahead it starts requests. Its gap limit G is 1 MiB, roughly the bytes
that take as long to transfer as one round trip at an assumed 30 ms and 50 MiB/s, and its largest
request S is 16 MiB. Other readers set the merge gap to constants a thousand times apart, from
4 KiB (Hadoop S3A) to 4 MiB (ClickHouse), and DuckDB 2.0 leads with runtime-derived gap based
on probed latency × bandwidth.

Pushing the Hardwood planner to the next levels of sophistication requires observability
instrumentation: proposing cost models and checking predictions against controlled execution.
This plan adds JFR events that capture both the planner's intent and the actual execution,
down to S3 API requests - with a single id joining them. The new `hardwood iotrace` CLI command
collects and renders event traces in both human- and machine-friendly formats.

## Terms

A request is one of two kinds:

1. A *footer* request reads a file's footer.
2. A *section* request reads one range of a file that a read fetches as one (a `RangeFetch`):
   a batched unit's data, a window's Bloom filters or page index, a dictionary that
   pruning reads, or the next piece of a chunk without an OffsetIndex. Its purpose says
   what the bytes are for (`data`, `pruning`, or `indexes`), and a data request's holders
   are the decoded columns whose pages it holds.

When the S3 backend opens a file it fetches the last 256 KiB, the tail. A section request
answered from the tail, or from bytes the file already holds in memory, is in place and
runs no read; a footer read is always an attempt, one that reads nothing for a file in memory,
so a trace of a local file shows its open. F is a read's bound on bytes fetched ahead of the
consumer's need, and R is the context's read-ahead buffer, shared by every read on the context
([`READ_PIPELINE.md`, "Scheduling"](../_designs/READ_PIPELINE.md#scheduling)). A request is held
from its fetch's start until its last reader is done, and released then. A start rule names
the branch of the engine's start decision that started a read.

## Target model

| Layer | Event | One event per | Emitted at |
| --- | --- | --- | --- |
| Intent | `dev.hardwood.FetchRequest` | request the read planned | `RangeFetch.of` (three sites), `FooterReader.startFooter`, `openFirstFile`, a `FileMetadataCache.getFile` that loads |
| Reality | `dev.hardwood.FetchAttempt` | execution of a request against its `InputFile` | the work of a `RangeFetcher.fetch` job, `FileMetadataCache.readFooter` |
| Below the `InputFile` | `dev.hardwood.S3Request` | HTTP request the S3 backend sends | `S3Api.sendWithRetry` |
| Link | `dev.hardwood.FetchLink` | request served by a load it did not start | `FileMetadataCache.claim` |
| Release | `dev.hardwood.FetchRelease` | request whose bytes the read gives back | `JobScheduler.release`, `FooterReader.returnCharge` |

The event classes sit in `dev.hardwood.jfr` beside the existing events. `S3RequestEvent` sits in
`dev.hardwood.s3`, because a package cannot span the core and s3 jars.

A request has two attempts when its read fails with an exception and the job runs it once more
([`READ_PIPELINE.md`, "Scheduling"](../_designs/READ_PIPELINE.md#scheduling), "Retries"), and
later more under hedging (#1401). An attempt serves several requests when reads of one reader
share a footer load ([`READ_PIPELINE.md`, "Units and
files"](../_designs/READ_PIPELINE.md#units-and-files), "Opening files ahead").
`FetchAttempt.requestId` names the request that started the attempt, and each other request it
serves emits one `FetchLink`. A link arises only where readers share a file set: two reads of one
reader that need a later file's footer at overlapping times, or a lookup outside a read
(`getFileMetaData(i)`) whose footer a read then claims before it is dropped. A read never requests
the first file's footer, which the open loaded, a footer of a file in memory runs no attempt to
link to, and a footer requested after the read dropped it is read again, so a trace of one read
has no link. Retries and resumes of a GET happen inside one `readRange`, and
`S3Request` joins its attempt on `attemptId` and `part`, never on thread nesting.

```
FetchRequest            FetchAttempt                       S3Request          FetchRelease

1. one request, one attempt
   R1 ──requestId─────► A1                     ──────────► G1                 R1 by page
   counts: 1 request, 1 attempt, 0 links, 1 GET, 1 release

2. a retry: two attempts of one request
   R1 ◄─requestId────── A1  outcome io-error   ──────────► G1 G2 G3 (S3's retries)
      ◄─requestId────── A2  attempt 2, retry   ──────────► G4                 R1 by batch
   counts: 1 request, 2 attempts, 0 links, 4 GETs, 1 release

3. no attempt
   R1  inPlace          sliced from the tail or memory
   R2                   released before it started, or never started
   counts: 2 requests, 0 attempts, 0 links, 0 GETs, 0 releases

4. a shared footer load: requests of one attempt
   R1 ──requestId─────► A1                     ──────────► G1 open-tail, G2 footer-body
   R2 ──FetchLink(R2, A1)┘                                                    R1 by file
   counts: 2 requests, 1 attempt, 1 link, 2 GETs, 1 release
```

An attempt has no `S3Request` when its bytes come from the tail the S3 backend fetched on open.

All five events are enabled by default, carry `@StackTrace(false)` and
`@Category({"Hardwood", "I/O"})`, and are named like `PageDecodedEvent` (`dev.hardwood.PageDecoded`).
`dev.hardwood.Experimental` means an event may change in any release without deprecation, as
`MetadataSource` may. `@jdk.jfr.Experimental` hides an event in JMC by default.

| Event | `dev.hardwood.Experimental` | `@jdk.jfr.Experimental` | Later |
| --- | --- | --- | --- |
| `FetchAttempt` | yes | no | promoted by dropping `dev.hardwood.Experimental` |
| `S3Request` | yes | no | |
| `FetchRequest` | yes | yes | stays experimental, its fields are mechanism-shaped (rule 6) |
| `FetchLink` | yes | yes | |
| `FetchRelease` | yes | yes | stays experimental, it records the engine's charges (rule 6) |

## What the events carry

| Event | Fields |
| --- | --- |
| `FetchAttempt` | `file`, `fileIndex`, `rgStart`, `rgEnd`, `attemptId`, `requestId`, `kind`, `purpose`, `startRule`, `waited`, `heldBy`, `offset`, `length`, `bytes`, `attempt`, `outcome`, `reason` |
| `FetchRequest` | `requestId`, `file`, `fileIndex`, `rgStart`, `rgEnd`, `kind`, `purpose`, `offset`, `length`, `holders`, `usefulBytes`, `inPlace`, `reason` |
| `FetchLink` | `requestId`, `attemptId` |
| `FetchRelease` | `requestId`, `chargedBytes`, `aheadBytes`, `releasedBy` |
| `S3Request` | `operation`, `attemptId`, `part`, `reason`, `offset`, `length`, `httpAttempt`, `resumeAt`, `status`, `outcome`, `bytes`, `timeToHeaders`, `timeToFirstByte`, `backoff`, `amzRequestId`, `amzId2`, `httpVersion`, `bytesReceived` |

`kind` is `section` or `footer`. `FetchAttempt.outcome` is `ok`, `io-error` or `error`. `S3Request.outcome` is `ok`,
`http-<status>`, `network`, `idle-timeout`, `body-failed`, `interrupted` or `error`. Each event's
Javadoc and the events table of
[`configuration.md`](../docs/content/reference/configuration.md) give the types and definitions.

An attempt's duration runs from the start of its read to the return of its last read of bytes the
file does not hold, so `MetadataSource.footerOf` and the footer's parse lie outside it. The job
commits the attempt whether it succeeds or fails, and a job that a closed context refuses emits
nothing. The job reads once more after any `Exception`, so an `error` attempt may be followed by a
`retry`. Over-fetch is a section's length less its `usefulBytes`. `usefulBytes` is -1 where the
request names no useful bytes when it is created: the next piece of a chunk without an OffsetIndex,
whose pages its scan finds only as the bytes arrive, and a pruning step's missing input, which the
next step parses.

**Waiting to start.** `waited` is how long the attempt's job sat in the read's queue, and
`heldBy` is the bound that held it back last: `none`, `read-ahead-budget` (R was full) or
`fetch-ahead-limit` (F was reached). A job queued behind one a bound refused counts as held by that
bound. The forced fetch records `none`, and a retry, which runs at once, records 0 and `none`. The
request's own event marks when it was created, which can be well before it was queued.

**Release.** `FetchRelease` is emitted when the read gives a request's bytes back: when the last
page, batch, scan or planner step reading a range is done (`releasedBy`), or when the consumer
passes a footer's file (`file`). It records what the request held of R and of F; a forced fetch
holds neither. Each request that ran ends in one release, unless the read closes first, which gives
everything back at once and records nothing. Decodes also hold room in R but make no request, so an
attempt can show `read-ahead-budget` with no release explaining it.

`S3Request` is one event type for both directions, keyed by `operation` (`GetObject` today), so
that latency by operation is one query. A field that does not apply holds its documented default.
`part` indexes a split of a GET's range by the client (#1687), never S3's multipart part number.
No bucket or key is recorded, as `amzRequestId` joins a request to its access log line. A write
trace records `offset`, `length` and `bytes` as 0, the body's length and bytes sent for
`PutObject`, as -1, 0, 0 for the create, complete and abort of a multipart upload, and as the
part's start, length and bytes sent for `UploadPart`. It adds `uploadId` to the upload's
requests, `partNumber` to `UploadPart` and `partCount` to the completion, and leaves
`timeToFirstByte`, `resumeAt` and `part` at 0. An upload's time to headers includes sending its
body, so a counting `BodyPublisher` times the send.

## Schema rules

1. Events and fields are named after concepts (request, attempt, purpose, kind), never classes.
2. Mechanism vocabularies (kind, purpose, start rule, held by, released by, outcome) are open sets:
   "values may be added".
3. `reason` is display text. A value a consumer filters on (row-group range, purpose, columns)
   is a field of its own.
4. A field states what the code observed at one point, not what a policy intends. `attempt`
   counts the attempts seen for this request, whatever the retry rule allows.
5. Requests and attempts link many-to-one in either direction (`requestId`, `FetchLink`).
6. Mechanism quantities go on `FetchRequest` and `FetchRelease`, never on `FetchAttempt`: holders,
   and the bytes charged against F and R. Mechanism vocabularies may go on `FetchAttempt` as open
   sets (`startRule`, `heldBy`).
7. Fields are only added. None is renamed or removed.
8. A snapshot test pins each event's JFR names, types and descriptions against a checked-in file
   (`dev.hardwood.Experimental` is class-retained and lies outside it). It cannot pin meaning, so
   each PR that changes which part of the engine creates, starts or releases a request checks by
   hand that each field still means what its description says, as #1753 and its follow-ups would
   have had to.
9. A count restarts only with its id. `attempt` numbers the attempts of one `requestId` from 1,
   with no gaps or repeats. If the code may drop and recreate the object that holds a count, it
   mints a new id with the new object.
10. A start-rule value names the branch of the start decision that ran, as `READ_PIPELINE.md`
    defines it in the release that recorded it. A new branch gets a new value, and a value never
    moves to a different trigger: a re-read after a failure is `retry`, not the `need` it
    reached the start through.

## Start rules

A start rule records which branch of the start decision started a request. Inside a read there is
one decision, `JobScheduler.tryStart`, for both kinds
([`READ_PIPELINE.md`, "Scheduling"](../_designs/READ_PIPELINE.md#scheduling)). When more than one
branch applies, the event records `retry` over the others.

| Kind | Value | Branch |
| --- | --- | --- |
| both | `need` | the lowest job keyed below the consumer's next batch while no other work of that batch is in flight, started whatever F and R hold, uncharged |
| both | `ahead` | any other start, while less than F is fetched ahead and R has room, charged to both |
| both | `retry` | a read run once more in its job after it failed with an exception |
| footer | `open` | the first file's footer, read by `openAll` on the caller's thread |
| footer | `load` | a footer that `FileMetadataCache.getFile` loads for a caller outside a read |

A read that waits on another read's footer load and finds no entry when it completes claims the
footer again and starts a footer job of its own, which starts by `need` or `ahead` like any other.

## Carrying the ids

`FetchAttempts.run` (PR 2) wraps the read at three sites: the work of a `RangeFetcher.fetch` job
around `ReadRanges.read`, `FileMetadataCache.readFooter` (which serves the read's footer jobs and
`getFile` alike), and `ParquetFileReader.openFirstFile`. It numbers the attempt, sets the ids, runs
the unchanged read and commits the event. `FetchReason.CURRENT` holds a record of the reason and
the ids instead of the reason alone, so code inside an `InputFile` sees both. It keeps its
`NoThreadLocal` suppression. `MetadataSource.footerOf` runs with the ids cleared. Request and
attempt ids come from one counter shared across both. A section's id and attempt count live on its
`RangeFetch`, which `JobScheduler.retried` runs again in the same job. A footer's live on the
read's `FooterReader.FooterLoad`, one per file per read, so a footer claimed again keeps its id
(rule 9). `openFirstFile` mints the first file's footer request, which the read itself never
starts. The cache's footer entry keeps that request's id and its last attempt's id, which
`FetchLink` names. `waited` and `heldBy` are stamped on the job by `JobScheduler.add`, by
`tryStart`'s refusals and by `start` for the jobs it holds back behind a refusal, and read by
`FetchAttempts.run` as the job starts.

## Sites on the engine

The hooks sit on `upstream/main` at the parts #1753 named, renamed since by its follow-ups.

| Site | Request | Knows at creation |
| --- | --- | --- |
| `RowGroupPlanner.ranges` (`RangeFetch.of`), for a window's Bloom filters | section, `pruning` | the window's row groups; readers are every unit of the window |
| the same, for a window's page index | section, `indexes` | the window's row groups; created at listing, queued once the first unit of the window waits for it, or never |
| the same, for a unit's pruning dictionaries (#1792) | section, `pruning` | one row group and one column; useful bytes equal the length |
| the same, for a pruning step's missing inputs (#1792) | section, `pruning` | one row group; useful bytes known only after the next step |
| `PageTracker.track` | section, `data` | one row group, its columns, and the planned pages each range holds, so `usefulBytes` is the sum of their extents within the range |
| `HeaderScanner.fetchRest` | section, `data` | one row group and one column; pages are found only as the scan walks them |
| `FooterReader.startFooter`, `openFirstFile`, `FileMetadataCache.getFile` | footer | the file; the footer's length is known only once the tail is read |

`RangeFetch` holds only a `FileRange(start, length, reason)`, so PR 4 passes each site's row
groups, purpose, columns and useful bytes to the request it creates. The 16 MiB cut of #1798 runs
at planning, before any request exists, so each piece is a request of its own, and nothing splits
a request after it is created.

## Delivery stages

The plan is delivered in small incremental stages, one PR per stage. Each PR also
updates any design document whose behaviour it changes, and Docs lists the known ones.
PRs 6 to 9 also update `docs/content/reference/cli.md`, and PR 6 adds a release note, a
"Tracing a read" section in `FETCH_PLANNING.md` and `skills/hardwood-cli/SKILL.md`.

| PR | Content | Waits for | Size / LOC | Docs |
| --- | --- | --- | --- | --- |
| 1 | This plan | none | doc | none |
| 2 | `FetchAttempt` with `requestId`, `attemptId`, `waited` and `heldBy`, the ids in `FetchReason`, `attemptId` on `S3Request` | 3 | 900 | event docs, `READ_PIPELINE.md` "Scheduling" (a new "Attempts" paragraph), the `attemptId` sentence of `S3_STORAGE.md` "Errors and retries", `PrefetchMissEvent` Javadoc, roadmap 7.2 Read Flow |
| 3 | `S3Request` with #1671's fields and the S3 request ids, HTTP version and bytes received, `s3` module only | none | 400 | event docs, `S3_STORAGE.md` "Errors and retries", the HTTP client row in `THREADING.md`, "I/O behavior" in `s3.md`, the "Fetch latency" bullet in `configuration.md`, roadmap 11.1 S3 Support |
| 4 | `FetchRequest`, `FetchLink` and `FetchRelease` | review of 2 | 550 | event docs, `FETCH_PLANNING.md` "Requests", `READ_PIPELINE.md` "Scheduling" ("Lifetimes") and "Units and files", roadmap 7.2 Read Flow |
| 5 | Fold of requests against attempts and releases in tests | review of 4, go/no-go | 150 | `FETCH_PLANNING.md` `CountingInputFile` paragraph, a "Conformance fold" paragraph and the "I/O budget" `Tests:` line |
| 6 | `hardwood iotrace`: split and tree views of a read of one file or many | review of 5, the #1464 fix | 2500 | roadmap 11.4 Command-line interface, `CLI_VALUE_RENDERING.md`, `CLI_LOGGING.md`, `NATIVE_BUILD.md`, `TESTING.md` |
| 7 | `iotrace -o x.jfr` keeps the recording, `--from` renders a recording of another run | 6 | 500 | the "Fetch latency" bullet in `configuration.md` |
| 8 | `iotrace -o x.json` writes a Chrome trace for the Perfetto UI | 7 | 350 | none beyond `cli.md` |
| 9 | `iotrace --repeat N --warmup M` reads one file repeatedly in one trace and tabulates the runs | 8 | 500 | `TESTING.md` |

### Notes by PR

**PR 2.** It covers the footer sites and the section site. `PrefetchMissEvent`, which nothing
emits, names `FetchAttempt` as its successor in its Javadoc, and its deprecation stays with #1042.
With the event enabled, an attempt costs about 140 ns in a microbenchmark, so about 1 µs for the 7
attempts of a 20 ms local read, below the whole-read noise. `waited` and `heldBy` add a timestamp
and a byte to each job. Done when each of these emits exactly one `FetchAttempt`: every run of a
`RangeFetcher.fetch` job's work, every footer read that runs, `getFile`'s load and the first file's
footer read; and when a test with F set below one request's length shows `fetch-ahead-limit`, one
with a full R shows `read-ahead-budget`, and the forced fetch shows `need` and `none`.

**PR 3.** One event per iteration of the loop in `S3Api.sendWithRetry` for a GET. The
writer's requests share the loop and emit nothing yet. PR 3 carries no `attemptId` and brings
the schema snapshot test (rule 8); PR 2 adds `attemptId` to `S3Request` (rule 7). PR 3's tests tell
their GETs apart by fetch reason. The test that the open's tail GET and footer-body GET carry the footer
attempt's `attemptId` goes in PR 2. PR 3 raises with #1687 that
part GETs sent off the attempt's thread must carry `attemptId` and `part`. Done when every GET
emits one event, carrying its `attemptId` once PR 2 is in.

**PR 4.** `FetchRequest` is emitted where a request is created, under the read's lock, so it
precedes every attempt. `FetchLink` is emitted at `FileMetadataCache.claim` and names the last
attempt of the load it hands over, a failed load included. `FetchRelease` is emitted under the
read's lock at the release. #1734, if it lands first, gives buffers back to the `InputFile` at the
same release, and the event's description says so. Done when each request has one `FetchRequest`,
a link if an attempt it did not start serves it, and one release for each request that ran an
attempt in a read that was not closed first.

**PR 5.** The fold checks, in tests and without JFR, the invariant `FETCH_PLANNING.md` "Requests"
states: a read issues no request it did not plan. It joins by id the requests PR 4's test
observer records, the attempts, the releases and the reads `CountingInputFile` saw. Every counted
read must run in an attempt at its request, a section's attempt reading at most once and within its
range. Every request must end in one way: an attempt followed by its release, a link, `inPlace`,
or a record that it was forgone: `released` (its last reader was done before its fetch started,
or it was never queued), `ended` (a footer past the read's end at `FooterReader.endAt`), or
`closed` (the read closed or failed first, which covers a job keyed past where the read stops). A
footer request outside a read, the open's or a lookup's, ends with its attempt. The production
changes are to the test observer, told of forgone requests, of each request's `InputFile` and of
the read that created it, and the bookkeeping behind the forgone records, kept only while a test
observes. Done when `ReadRequestsTest`, `CrossColumnCoalesceTest` and `PageRangeIoTest` pass under
the fold, run through subclasses of our own so the upstream classes stay unchanged, a test with two reads in flight folds each on its own, and
each forgone site has a test.

**PR 6.** `iotrace` reads one file, or several as one read, under a recording of the five events
above and `FileOpened`, and joins them by id. The default `split` view lists the plan (requests in
file order, never truncated, as coalescing is checked against it), the trace (attempts in start
order, with a timeline and the time each waited and what held it) and stats ending in a `Checks`
line of `OK`, `WARN` or `FAILED`. The `tree` view nests each request's attempts, GETs and release.
The text views may change between releases, so tools parse the recording or the Chrome trace. It
adds `--enable-monitoring=jfr` to the native build and reports its image-size and startup cost.
`iotrace` looks up the later files' footers for the file layouts of `--show-gaps` after the read,
when the read has dropped them, so each lookup reads its footer again; the recording stops once the
read is closed and before those lookups, so the trace holds no `load` attempt the read never made.
The #1464 test fix goes first as a PR of its own. PR 6 opens with two small commits that its code
uses: the column projection option `print` and `convert` share, and the count-with-a-noun helper
moved out of `dive`.

**PR 7.** `-o x.jfr` keeps the recording. `--from` renders one, and only the rendering options
apply to it, `--show-gaps` among them.

**PR 8.** The Chrome trace binds each request to its attempts and GETs as flows, runs each
request to its release, and shows the bytes the read's requests hold of R and F as a counter built
from the releases and attempts. It leaves out, until the events carry the fields they need (rules
6 and 7): the flow to the first `PageDecoded`, the decode and consumer tracks, the decodes' share
of R, and a flat JSONL export. Done when an export opens in the
Perfetto UI with its flows bound.

**PR 9.** `--repeat` repeats one read to look at its I/O, telling the runs apart by `FileOpened`,
and names each run's track groups in the Chrome trace, so it follows PR 8;
comparing builds stays with the benchmarks. Done when the runs table's median and spread match a
test's fixed recording.

## What the demo runs showed

A 7.5 MiB file read from three regions (2026-10-30, the engine before #1753) showed that no single
gap is right everywhere: in-region, one request per column chunk won; against us-west-2 and
ap-southeast-2 the merged request won by 29% and 42%. At a distance, concurrent GETs mostly wait as
long as a new connection would, which the events cannot yet tell apart from queueing, and the
256 KiB footer tail on a cold connection took about 1.65 s from ap-southeast-2.

The appendix's reruns on the #1753 engine (2026-11-17) add three points. Footer tails are still 96%
of what a read of three small files fetches. The 656 MiB, 188-column file reads in 7.2 s in
us-east-1, against 15.9 s before, at either request size and as fast as from local disk: the
speedup is the engine's decode, not the 16 MiB cut. From ap-southeast-2, where a stream carries
about 13 MiB/s, 16 MiB requests keep about seven in flight and halve the time against 128 MiB.

So bandwidth is not one number: it falls per stream with distance and grows with streams in flight.
A cost model for #763 has to price connection setup, per-stream bandwidth and concurrency, and the
gap and request size it tunes depend on the store and its distance.

## Open work

| Item | Issue | Waits for | What the events give it |
| --- | --- | --- | --- |
| Fetch-ahead derived from rates | #1754 | PRs 2 and 3 | per-attempt and per-GET latency and bytes, and `waited` and `heldBy` to show where F binds |
| A cost model for the gap and span | #763 | recordings like the above | per-attempt and per-GET latency, bodies in flight, per-stream rate |
| A scorer over captured sequences (F, R, need starts), or an estimator over `S3Request` | #763 or #1688 | an issue that asks | the recorded plan, its start rules, waits and releases, and each GET's times and bytes |
| Per-attempt latency on real S3 | #1671 | PRs 2 and 3 | latency split into time to first byte and transfer |
| A remote-latency simulator for benchmarks without S3 (a latency `InputFile` and a sleep benchmark) | #763 | a model that the recordings support: connection setup, per-stream bandwidth and concurrency, not a lognormal latency plus bytes over one bandwidth | recordings to fit it and check it against |
| Hedging | #1401, held in #1663 | #1671 showing a latency tail | per-GET time to headers, which shows the tail. A hedge is one more attempt, with an outcome for the loser |
| Parallel part GETs | #1687 | none | `part` and the attempt's id on each part GET. Its sweep should report connection reuse beside part size |
| The decodes' share of R | none yet | an issue that asks | the attempts R held back; a charge event for decodes and pruning evaluations would add the rest (rule 6) |
| A write trace | none yet | an issue that asks | the write operations above, rendered by `iotrace --from` and kept by `-o` on a command that writes |
| JDK events (`jdk.TLSHandshake`, socket and file I/O), eBPF or perf on Linux (retransmits, page faults for #1661) | none yet | an issue that asks | the attempt and GET to align them with, to tell new connections from reused ones |
| An internal gap override (`hardwood.internal.gapBytes`) for a sweep | none yet | an issue that asks | the plan and its GETs under each gap |

## End state

Once the last PR lands, the documents in the Docs column describe the events, the fold and
`iotrace`. The plan's content goes to these homes: the target model, the start rules, waiting and
release, and the carrying of ids to `READ_PIPELINE.md` "Scheduling" (its "Attempts" paragraph)
and `FETCH_PLANNING.md` "Requests" and "Tracing a read". `S3Request` and the write fields go to
[`S3_STORAGE.md`, "Errors and retries"](../_designs/S3_STORAGE.md#errors-and-retries). The field
lists and value sets go to the event Javadoc and `configuration.md`. The schema rules go to the
"Attempts" paragraph, which PR 2 adds. The last of PRs 6 to 9 to land deletes this plan. If the go/no-go stops the work
after PR 4, the last PR that does land deletes it, and PRs 5 to 9 move to #827 as open work.

## Appendix: demo runs

Runs of the whole stack, PR 9 included, on the engine of #1753 (`upstream/main` at `12e00e63`),
from an EC2 c7g.4xlarge in us-east-1, 2026-11-17. Bucket, file and most column names are
replaced by placeholders.

**ap-southeast-2, a 7.5 MiB file of one row group, repeated.** The warm-up run opens new
connections and takes 3.3 s, with a median time to headers of 580 ms. The measured runs reuse
them and take about 0.9 s, with a median time to headers of about 231 ms, as on the earlier
engine. The data request waits 0.3 ms in the read's queue. The last 254.5 KiB of data is sliced
from the 256 KiB tail fetched when the file was opened, so it has no attempt.

```
$ hardwood iotrace -f s3://<bucket>/profiling_uncompressed_dict.parquet --repeat 4 --warmup 1 s3://<bucket>/profiling_uncompressed_dict.parquet
5 reads: 1 warmup, 4 measured
showing run 3, the measured run of median wall time
7.5 MiB · 1 row group · 3 requests · 2 GETs · 889.3 ms

PLAN (requests in file order):

  ██████████████████████████████████████████████████████████████▒▒

      Serves                                        Range     Length  Started by  Outcome
  r2  rg0 id … description (7 cols)        [4 .. 7551245)    7.2 MiB  need        1 attempt, ok
  r1  footer                         [7551245 .. 7813389)  256.0 KiB  open        1 attempt, ok
  r3  rg0 description                [7551245 .. 7811814)  254.5 KiB              in place

TRACE (attempts in start order):

      Begin  Duration  Waited   Offset     Length  Request  GETs  Outcome  0 ms ──────────────────────────── 890 ms
  a1   <0.1     237.2          7551245  256.0 KiB  r1          1  ok       ░░░░░░░░░░█
  a2  240.9     648.4     0.3        4    7.2 MiB  r2          1  ok       ···········░░░░░░░░░░███████████████████

STATS:

  Wall    889.3 ms  footer 237.2 ms (26.7%), data 648.4 ms at 11.1 MiB/s
  Busy    885.6 ms  1.0× overlap, 8.4 MiB/s
  Bytes    7.5 MiB  returned by attempts; 7.5 MiB received by GETs, 254.5 KiB sliced in place, over-fetch 0 B (0.0% of fetched)
  Checks        OK  3 requests, each served once

RUNS:

  Run     Kind      Wall ms  Footer ms  Data ms  Headers p0  Headers p50  Headers p90  MiB/s
  1       warmup     3342.8     1770.0   1486.0       234.3        580.1        926.0    4.8
  2       measured    901.7      237.8    659.4       230.3        231.2        232.0   10.9
  3       measured    889.3      237.2    648.4       226.9        229.9        233.0   11.1
  4       measured    901.6      237.6    660.1       233.9        234.1        234.3   10.9
  5       measured    703.5      241.1    455.1       225.1        231.4        237.7   15.8
  median              895.4      237.7    653.9       228.6        231.3        233.6   11.0
  min                 703.5      237.2    455.1       225.1        229.9        232.0   10.9
  max                 901.7      241.1    660.1       233.9        234.1        237.7   15.8

LEGEND: █▓ sections, one shade per request   ▒ footer   ░ wait for headers   █ body   · pre-attempt
```

**us-east-1, a 656 MiB file of 188 columns written by parquet-rs, with `--show-gaps`.** The 16 MiB
cut of #1798 splits the five row groups into 59 data requests, where the earlier engine made 7 of
up to 128 MiB. The read takes 7.2 s, against 15.9 s before, and as long with 128 MiB requests or
from local disk: about four bodies are in flight on average, 289 MiB/s together and 85 MiB/s per
stream. 36 of the data requests wait behind the
fetch-ahead limit (`F`), for up to 6 s, as the read holds 256 MiB ahead of a consumer slower
than the network, and the
first wave waits about 0.5 s with no bound refusing it, from its row groups being batched to
their fetches starting. The gaps between row groups are parquet-rs's bloom filters, which the
read does not fetch. The trace below is an excerpt of the 62 attempts.

```
$ hardwood iotrace -f s3://<bucket>/<file>.parquet --show-gaps s3://<bucket>/<file>.parquet
656.3 MiB · 5 row groups · 63 requests · 62 GETs · 7239.8 ms

PLAN (requests in file order):

  ▓█▓▓▓▓█▓█▓██▓█████▓█▓█▓██▓██▓▓█▓█▓█▓█▓▓█▓▓███▓▓█▓█▓▓████▓██▓█▓██

       Serves                                                         Range     Length  Started by  Outcome
  r2   rg0 fieldx_id … fieldy_id (113 cols)                  [4 .. 1902750)    1.8 MiB  need        1 attempt, ok
  r3   rg0 col1                                       [1902750 .. 18679966)   16.0 MiB  ahead       1 attempt, ok
  r4   rg0 col1                                      [18679966 .. 27330847)    8.3 MiB  ahead       1 attempt, ok
  r5   rg0 col2                                      [27330847 .. 44108063)   16.0 MiB  ahead       1 attempt, ok
  r6   rg0 col2                                      [44108063 .. 53024997)    8.5 MiB  ahead       1 attempt, ok
  r7   rg0 col3                                      [53024997 .. 66860454)   13.2 MiB  ahead       1 attempt, ok
  r8   rg0 col4 … col5 (2 cols)                      [66860454 .. 80975122)   13.5 MiB  ahead       1 attempt, ok
  r9   rg0 col6 … col7 (2 cols)                      [80975122 .. 91219557)    9.8 MiB  ahead       1 attempt, ok
  r10  rg0 col8                                     [91219557 .. 101644960)    9.9 MiB  ahead       1 attempt, ok
  r11  rg0 col9 … col10 (3 cols)                   [101644960 .. 117232383)   14.9 MiB  ahead       1 attempt, ok
  r12  rg0 addr                                    [117232383 .. 134009599)   16.0 MiB  ahead       1 attempt, ok
  r13  rg0 addr … col11 (7 cols)                   [134009599 .. 149997881)   15.2 MiB  ahead       1 attempt, ok
  r14  rg0 col12 … foo_id (57 cols)                [149997881 .. 154277619)    4.1 MiB  ahead       1 attempt, ok
  …    not fetched: bloom filters                  [154277619 .. 154317798)   39.2 KiB
  r18  rg1 fieldx_id … fieldy_id (113 cols)        [154317798 .. 156962072)    2.5 MiB  ahead       1 attempt, ok
  r19  rg1 col1                                    [156962072 .. 173739288)   16.0 MiB  ahead       1 attempt, ok
  r20  rg1 col1                                    [173739288 .. 181959826)    7.8 MiB  ahead       1 attempt, ok
  r21  rg1 col2                                    [181959826 .. 198737042)   16.0 MiB  ahead       1 attempt, ok
  r22  rg1 col2                                    [198737042 .. 207239230)    8.1 MiB  ahead       1 attempt, ok
  r23  rg1 col3                                    [207239230 .. 221027496)   13.1 MiB  ahead       1 attempt, ok
  r24  rg1 col4 … col5 (2 cols)                    [221027496 .. 234884808)   13.2 MiB  ahead       1 attempt, ok
  r25  rg1 col6 … col7 (2 cols)                    [234884808 .. 244607974)    9.3 MiB  ahead       1 attempt, ok
  r26  rg1 col8                                    [244607974 .. 255447031)   10.3 MiB  ahead       1 attempt, ok
  r27  rg1 col9 … col10 (3 cols)                   [255447031 .. 270225651)   14.1 MiB  ahead       1 attempt, ok
  r28  rg1 addr                                    [270225651 .. 287002867)   16.0 MiB  ahead       1 attempt, ok
  r29  rg1 addr … col11 (7 cols)                   [287002867 .. 302215336)   14.5 MiB  ahead       1 attempt, ok
  r30  rg1 col12 … foo_id (57 cols)                [302215336 .. 306254454)    3.9 MiB  ahead       1 attempt, ok
  …    not fetched: bloom filters                  [306254454 .. 306306922)   51.2 KiB
  r31  rg2 fieldx_id … fieldy_id (113 cols)        [306306922 .. 309923539)    3.4 MiB  ahead       1 attempt, ok
  r32  rg2 col1                                    [309923539 .. 326700755)   16.0 MiB  ahead       1 attempt, ok
  r33  rg2 col1                                    [326700755 .. 334379052)    7.3 MiB  ahead       1 attempt, ok
  r34  rg2 col2                                    [334379052 .. 351156268)   16.0 MiB  ahead       1 attempt, ok
  r35  rg2 col2                                    [351156268 .. 358977491)    7.5 MiB  ahead       1 attempt, ok
  r36  rg2 col3                                    [358977491 .. 372397850)   12.8 MiB  ahead       1 attempt, ok
  r37  rg2 col4 … col5 (2 cols)                    [372397850 .. 386009662)   13.0 MiB  ahead       1 attempt, ok
  r38  rg2 col6 … col7 (2 cols)                    [386009662 .. 395174027)    8.7 MiB  ahead       1 attempt, ok
  r39  rg2 col8                                    [395174027 .. 407097613)   11.4 MiB  ahead       1 attempt, ok
  r40  rg2 col9 … col10 (3 cols)                   [407097613 .. 420735284)   13.0 MiB  ahead       1 attempt, ok
  r41  rg2 addr                                    [420735284 .. 437512500)   16.0 MiB  ahead       1 attempt, ok
  r42  rg2 addr … col13 (17 cols)                  [437512500 .. 454203428)   15.9 MiB  ahead       1 attempt, ok
  r43  rg2 col8 … foo_id (47 cols)                 [454203428 .. 455385392)    1.1 MiB  ahead       1 attempt, ok
  …    not fetched: bloom filters                  [455385392 .. 455462372)   75.2 KiB
  r44  rg3 fieldx_id … fieldy_id (113 cols)        [455462372 .. 458256240)    2.7 MiB  ahead       1 attempt, ok
  r45  rg3 col1                                    [458256240 .. 475033456)   16.0 MiB  ahead       1 attempt, ok
  r46  rg3 col1                                    [475033456 .. 477049554)    1.9 MiB  ahead       1 attempt, ok
  r47  rg3 col2                                    [477049554 .. 493826770)   16.0 MiB  ahead       1 attempt, ok
  r48  rg3 col2 … col3 (2 cols)                    [493826770 .. 506646670)   12.2 MiB  ahead       1 attempt, ok
  r49  rg3 col4 … col5 (2 cols)                    [506646670 .. 517231869)   10.1 MiB  ahead       1 attempt, ok
  r50  rg3 col6 … col8 (3 cols)                    [517231869 .. 533383239)   15.4 MiB  ahead       1 attempt, ok
  r51  rg3 col9 … col10 (3 cols)                   [533383239 .. 544655945)   10.8 MiB  ahead       1 attempt, ok
  r52  rg3 addr … gni (2 cols)                     [544655945 .. 561290675)   15.9 MiB  ahead       1 attempt, ok
  r53  rg3 foo_id … foo_id (62 cols)               [561290675 .. 573010498)   11.2 MiB  ahead       1 attempt, ok
  …    not fetched: bloom filters                  [573010498 .. 573062902)   51.2 KiB
  r54  rg4 fieldx_id … fieldy_id (113 cols)        [573062902 .. 576660320)    3.4 MiB  ahead       1 attempt, ok
  r55  rg4 col1                                    [576660320 .. 593437536)   16.0 MiB  ahead       1 attempt, ok
  r56  rg4 col1                                    [593437536 .. 595133854)    1.6 MiB  ahead       1 attempt, ok
  r57  rg4 col2                                    [595133854 .. 611911070)   16.0 MiB  ahead       1 attempt, ok
  r58  rg4 col2 … col3 (2 cols)                    [611911070 .. 623372098)   10.9 MiB  ahead       1 attempt, ok
  r59  rg4 col4 … col6 (3 cols)                    [623372098 .. 639891542)   15.8 MiB  ahead       1 attempt, ok
  r60  rg4 col7 … col8 (2 cols)                    [639891542 .. 649658726)    9.3 MiB  ahead       1 attempt, ok
  r61  rg4 col9 … col10 (3 cols)                   [649658726 .. 660169174)   10.0 MiB  ahead       1 attempt, ok
  r62  rg4 addr … gni (2 cols)                     [660169174 .. 675000963)   14.1 MiB  ahead       1 attempt, ok
  r63  rg4 foo_id … foo_id (62 cols)               [675000963 .. 685670952)   10.2 MiB  ahead       1 attempt, ok
  …    not fetched: bloom filters, column indexes  [685670952 .. 687005639)    1.3 MiB
  r15  rg=0-4 indexes                              [687005639 .. 687943370)  915.8 KiB  need        1 attempt, ok
  r16  rg=0-4 indexes                              [687943370 .. 687944556)    1.2 KiB  ahead       1 attempt, ok
  r1   footer                                      [687943445 .. 688205589)  256.0 KiB  open        1 attempt, ok
  r17  rg=0-4 indexes                              [687944556 .. 688088272)  140.3 KiB              in place

TRACE (attempts in start order):

        Begin  Duration    Waited     Offset     Length  Request  GETs  Outcome  1 ms ─────────────────────────── 7242 ms
  a1      1.6     421.4            687943445  256.0 KiB  r1          1  ok       ░█
  a15   533.1     116.8       4.8  687005639  915.8 KiB  r15         1  ok       ···█
  a16   533.1      39.5       4.7  687943370    1.2 KiB  r16         1  ok       ···█
  a2   1229.6     111.2     524.0          4    1.8 MiB  r2          1  ok       ·······█
  a3   1229.8     435.7     524.1    1902750   16.0 MiB  r3          1  ok       ·······██
  a12  1230.9     521.1     524.9  117232383   16.0 MiB  r12         1  ok       ·······░██
  a17  1232.8     317.5     375.8  154317798    2.5 MiB  r18         1  ok       ·······░█
  a13  1239.3     506.5     533.2  134009599   15.2 MiB  r13         1  ok       ·······░██
  a6   1242.0     437.8     536.1   44108063    8.5 MiB  r6          1  ok       ·······░█
  …
  a29  3351.7     128.5  2494.1 F  302215336    3.9 MiB  r30         1  ok       ···················█
  a21  3363.9     111.8  2506.6 F  198737042    8.1 MiB  r22         1  ok       ···················█
  a19  3363.9     179.5  2506.7 F  173739288    7.8 MiB  r20         1  ok       ···················█
  …
  a31  3469.8     364.5  2532.1 F  309923539   16.0 MiB  r32         1  ok       ···················██
  a54  6641.1     172.9  5509.2 F  576660320   16.0 MiB  r55         1  ok       ·····································█
  a62  7087.1     130.0  5955.1 F  675000963   10.2 MiB  r63         1  ok       ·······································█
  a55  7087.1      32.5  5955.2 F  593437536    1.6 MiB  r56         1  ok       ·······································█
  Waited: from the job queued to the attempt's start; R held back by the read-ahead buffer, F by the fetch-ahead limit

STATS:

  Wall          7239.8 ms  footer 421.4 ms (5.8%), data 6708.3 ms at 97.6 MiB/s
  Busy         17661.1 ms  2.4× overlap, 90.4 MiB/s
  Bytes         654.8 MiB  returned by attempts; 654.8 MiB received by GETs, 140.3 KiB sliced in place, over-fetch 0 B (0.0% of fetched)
  Checks               OK  63 requests, each served once
  Latency    p50 106.1 ms  to headers p0 21.9 ms, p50 106.1 ms, p90 240.3 ms, p100 370.6 ms over 62 GETs
  Bandwidth   288.6 MiB/s  across 3.9 bodies in flight on average; 85.3 MiB/s per stream over 50 GETs of 4 MiB or more

LEGEND: █▓ sections, one shade per request   ░ wait for headers   █ body   · pre-attempt
```

**Three small files as one read, one column.** Each file's 256 KiB tail is fetched to read 3 to
13 KiB of the one column: footers are 768 KiB of the 796 KiB the read fetched, the
many-small-files case #1671 measures. The footers of f2 and f3 start together at 435 ms, ahead of
need and before f1's data, after waiting about 5 ms in the read's queue. The page indexes are
sliced from the tails, so they have no attempt.

```
$ hardwood iotrace -f s3://<bucket>/<file1>.parquet -f s3://<bucket>/<file2>.parquet -f s3://<bucket>/<file3>.parquet -c fieldx_id
f1  s3://<bucket>/<file1>.parquet
f2  s3://<bucket>/<file2>.parquet
f3  s3://<bucket>/<file3>.parquet
3 files · 82.1 MiB · 3 row groups · 9 requests · 6 GETs · 692.3 ms

PLAN (requests in file order):

  f1  <file1>.parquet · 4.7 MiB · 1 row group

  ___█________________________________________________________▒▒▒▒

      Serves                        Range     Length  Started by  Outcome
  r2  rg0 fieldx_id    [251068 .. 254401)    3.3 KiB  need        1 attempt, ok
  r1  footer         [4695004 .. 4957148)  256.0 KiB  open        1 attempt, ok
  r3  rg=0 indexes   [4928949 .. 4928986)       37 B              in place

  f2  <file2>.parquet · 24.6 MiB · 1 row group

  __█____________________________________________________________▒

      Serves                          Range     Length  Started by  Outcome
  r5  rg0 fieldx_id    [1021315 .. 1034627)   13.0 KiB  need        1 attempt, ok
  r4  footer         [25573752 .. 25835896)  256.0 KiB  ahead       1 attempt, ok
  r6  rg=0 indexes   [25793676 .. 25793863)      187 B              in place

  f3  <file3>.parquet · 52.7 MiB · 1 row group

  █______________________________________________________________▒

      Serves                          Range     Length  Started by  Outcome
  r8  rg0 fieldx_id      [769418 .. 781137)   11.4 KiB  ahead       1 attempt, ok
  r7  footer         [55043880 .. 55306024)  256.0 KiB  ahead       1 attempt, ok
  r9  rg=0 indexes   [55238108 .. 55238615)      507 B              in place

TRACE (attempts in start order):

      Begin  Duration  Waited  File    Offset     Length  Request  GETs  Outcome  1 ms ──────────────────────────── 694 ms
  a1    1.4     344.3          f1     4695004  256.0 KiB  r1          1  ok       ░░░░░░░░░░░░░░░░░░██
  a3  435.5     189.7     4.9  f2    25573752  256.0 KiB  r4          1  ok       ·························░░░░░░░░░░█
  a5  435.5     191.1     4.8  f3    55043880  256.0 KiB  r7          1  ok       ·························░░░░░░░░░░█
  a2  443.2      62.3     1.8  f1      251068    3.3 KiB  r2          1  ok       ··························░░█
  a6  634.2      40.8     0.5  f3      769418   11.4 KiB  r8          1  ok       ·····································░█
  a4  634.2      59.5     1.0  f2     1021315   13.0 KiB  r5          1  ok       ·····································░░█

STATS:

            Wall    Footer      Data      Busy  Overlap      Bytes  Over-fetch  Checks       Latency   Bandwidth
  f1    504.2 ms  344.3 ms   62.3 ms  406.6 ms     0.8×  259.3 KiB         0 B  OK                 —           —
  f2    258.2 ms  189.7 ms   59.5 ms  249.2 ms     1.0×  269.0 KiB         0 B  OK                 —           —
  f3    239.5 ms  191.1 ms   40.8 ms  231.9 ms     1.0×  267.4 KiB         0 B  OK                 —           —
  read  692.3 ms  535.4 ms  250.5 ms  887.6 ms     1.3×  795.7 KiB         0 B  OK      p50 117.5 ms  24.9 MiB/s

  Latency    p50 117.5 ms  to headers p0 38.3 ms, p50 117.5 ms, p90 294.0 ms, p100 294.0 ms over 6 GETs
  Bandwidth    24.9 MiB/s  across 1.2 bodies in flight on average

LEGEND: █▓ sections, one shade per request   ▒ footer   _ not fetched   ░ wait for headers   █ body   · pre-attempt

NOTES:
  - Bandwidth has no rate per stream: 0 GETs are of 4 MiB or more, fewer than the 3 its median takes.
```
