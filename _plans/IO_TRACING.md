# Plan: trace a read's fetch sequence

Tracking issue: #827. Stage issues: one per PR from 2 to 6, filed once they are agreed on #827.

Related:

- #828: the earlier design (Perflab), which predates the read engine. This plan replaces it.
- #1662: the read engine, split by #1712 (the engine) and #1717 (facts, units and phase).
- #1720: moves the engine's state into one object per stage (#1719).
- #1671 (real-S3 latency run), #763 (latency model, sleep benchmark), #1688 (scorer).
- #1401 (hedging, held in #1663), #1687 (parallel part GETs), #1661 (page faults).
- #1042 (`PrefetchMissEvent`), #1464 (a test fix PR 6 needs).

This plan adds JFR events that record what a read planned and what it ran and the
`hardwood iotrace` command.

## Why trace a read's I/O

Hardwood's fetch planning decides which byte ranges a read fetches, which nearby ranges it merges
into one request, and how far ahead it starts requests. Its gap limit G is 1 MiB, roughly the bytes
that take as long to transfer as one round trip at an assumed 30 ms and 50 MiB/s, and its largest
request S is 128 MiB. Other readers set the merge gap to constants a thousand times apart, from
4 KiB (Hadoop S3A) to 4 MiB (ClickHouse), and DuckDB 2.0 leads with runtime-derived gap based
on probed latency × bandwidth.

Pushing the Hardwood planner to the next levels of sophistication requires observability
instrumentation: proposing cost models and checking predictions against controlled execution.
This plan adds JFR events that capture both the planner's intent and the actual execution,
down to S3 API requests - with a single id joining them. The new `hardwood iotrace` CLI command
collects and renders event traces in both human- and machine-friendly formats.

## Terms

A request is one of two kinds. A footer request reads a file's footer. A section request reads a
byte range that covers one or more column chunks or index structures. Its purpose says what the
bytes are for (`data`, `pruning`, `indexes` or `footer`), and its holders are the projected
columns waiting on its bytes. When the S3 backend opens a file it fetches the last 256 KiB, the
tail. A request answered from the tail, or from bytes the file already holds in memory, is in
place and runs no read. F is a read's bound on speculative bytes, and R is the context's shared
read-ahead buffer ([`FETCH_PLANNING.md`, "Requests"](../_designs/FETCH_PLANNING.md#requests)). A
start rule names the branch of the engine's start decision that started a read.

## Target model

| Layer | Event | One event per | Emitted at |
| --- | --- | --- | --- |
| Intent | `dev.hardwood.FetchRequest` | request the read planned | `Sections`, a footer's `FileState`, `openFirstFile`, a `FileMetadataCache.getFile` that loads |
| Reality | `dev.hardwood.FetchAttempt` | execution of a request against its `InputFile` | the I/O job of `RowGroupIterator.start(Request)`, each footer read |
| Below the `InputFile` | `dev.hardwood.S3Request` | HTTP request the S3 backend sends | `S3Api.sendWithRetry` |
| Link | `dev.hardwood.FetchLink` | request served by a load it did not start | `FileMetadataCache.claim` |

The event classes sit in `dev.hardwood.jfr` beside the existing events. `S3RequestEvent` sits in
`dev.hardwood.s3`, because a package cannot span the core and s3 jars.

A request has several attempts when its read fails and the engine reads it once more, which it
does for any failed request ([`FETCH_PLANNING.md`, "Release, failure and
close"](../_designs/FETCH_PLANNING.md#release-failure-and-close)), and later under hedging
(#1401). An attempt serves several requests when reads of one reader share a footer load
([`FETCH_PLANNING.md`, "Opening files ahead"](../_designs/FETCH_PLANNING.md#opening-files-ahead)).
`FetchAttempt.requestId` names the request that started the attempt, and each other request it
serves emits one `FetchLink`. Retries and resumes of a GET happen inside one `readRange`, and
`S3Request` joins its attempt on `attemptId` and `part`, never on thread nesting.

```
FetchRequest            FetchAttempt                       S3Request

1. one request, one attempt
   R1 ──requestId─────► A1                     ──────────► G1
   counts: 1 request, 1 attempt, 0 links, 1 GET

2. a retry: several attempts of one request
   R1 ◄─requestId────── A1  outcome io-error   ──────────► G1 G2 G3 (S3's retries)
      ◄─requestId────── A2  attempt 2, retry   ──────────► G4
   counts: 1 request, 2 attempts, 0 links, 4 GETs

3. no attempt
   R1  inPlace          sliced from the tail or memory
   R2                   released, closed, dropped or stopped before it ran
   counts: 2 requests, 0 attempts, 0 links, 0 GETs

4. a shared footer load: requests of one attempt
   R1 ──requestId─────► A1                     ──────────► G1 open-tail, G2 footer-body
   R2 ──FetchLink(R2, A1)┘
   counts: 2 requests, 1 attempt, 1 link, 2 GETs
```

An attempt has no `S3Request` when its bytes come from the tail the S3 backend fetched on open.

All four events are enabled by default, carry `@StackTrace(false)` and
`@Category({"Hardwood", "I/O"})`, and are named like `PageDecodedEvent` (`dev.hardwood.PageDecoded`).
`dev.hardwood.Experimental` means an event may change in any release without deprecation, as
`MetadataSource` may. `@jdk.jfr.Experimental` hides an event in JMC by default.

| Event | `dev.hardwood.Experimental` | `@jdk.jfr.Experimental` | Later |
| --- | --- | --- | --- |
| `FetchAttempt` | yes | no | promoted by dropping `dev.hardwood.Experimental` |
| `S3Request` | yes | no | |
| `FetchRequest` | yes | yes | stays experimental, its fields are mechanism-shaped (rule 6) |
| `FetchLink` | yes | yes | |

## What the events carry

| Event | Fields |
| --- | --- |
| `FetchAttempt` | `file`, `fileIndex`, `rgStart`, `rgEnd`, `attemptId`, `requestId`, `kind`, `purpose`, `startRule`, `offset`, `length`, `bytes`, `attempt`, `outcome`, `reason` |
| `FetchRequest` | `requestId`, `file`, `fileIndex`, `rgStart`, `rgEnd`, `kind`, `purpose`, `offset`, `length`, `holders`, `usefulBytes`, `inPlace`, `reason` |
| `FetchLink` | `requestId`, `attemptId` |
| `S3Request` | `operation`, `attemptId`, `part`, `reason`, `offset`, `length`, `httpAttempt`, `resumeAt`, `status`, `outcome`, `bytes`, `timeToHeaders`, `timeToFirstByte`, `backoff`, `amzRequestId`, `amzId2`, `httpVersion`, `bytesReceived` |

`kind` is `section` or `footer`. `FetchAttempt.outcome` is `ok`, `io-error` or `error`. `S3Request.outcome` is `ok`,
`http-<status>`, `network`, `idle-timeout`, `body-failed`, `interrupted` or `error`. Each event's
Javadoc and the events table of
[`configuration.md`](../docs/content/reference/configuration.md) give the types and definitions.

An attempt's duration runs from the job starting the execution to the return of its last read of
bytes the file does not hold, so queueing, `MetadataSource.footerOf` and the footer's parse lie
outside it. The job commits the attempt whether it succeeds or fails, and a job that a closed
context refuses emits nothing. The engine reads once more after any `Exception`, so an `error`
attempt may be followed by a `retry`. Over-fetch is a section's length less its `usefulBytes`.
Release is not recorded until a PR needs it.

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
2. Mechanism vocabularies (kind, purpose, start rule, outcome) are open sets: "values may be added".
3. `reason` is display text. A value a consumer filters on (row-group range, purpose, columns)
   is a field of its own.
4. A field states what the code observed at one point, not what a policy intends. `attempt`
   counts the attempts seen for this request, whatever the retry rule allows.
5. Requests and attempts link many-to-one in either direction (`requestId`, `FetchLink`).
6. Mechanism-shaped fields go on `FetchRequest`, never on `FetchAttempt`: holders, and the
   charges against F and R.
7. Fields are only added. None is renamed or removed.
8. A snapshot test pins each event's JFR names, types and descriptions against a checked-in file
   (`dev.hardwood.Experimental` is class-retained and lies outside it). It cannot pin meaning, so
   each PR that splits the read engine (each of the seven #1720 stages, and any later split, as
   #1712 and #1717 did before) checks by hand that each field still means what its description
   says.
9. A count restarts only with its id. `attempt` numbers the attempts of one `requestId` from 1,
   with no gaps or repeats. If the code may drop and recreate the object that holds a count, it
   mints a new id with the new object.
10. A start-rule value names the branch of the start decision that ran, as `FETCH_PLANNING.md`
    defines it in the release that recorded it. A new branch gets a new value, and a value never
    moves to a different trigger: a re-read after a failure is `retry`, not the `demand` it
    reached the start through.

## Start rules

A start rule records which branch of the start decision started a request, as `FETCH_PLANNING.md`
defines them in "Requests", "Release, failure and close" and "Opening files ahead". When more
than one branch applies, the event records `retry` over `planning`, and `planning` over `demand`.

| Kind | Value | Branch |
| --- | --- | --- |
| section | `demand` | a data request a walk stopped on, or the rest of a chunk a walk reached |
| section | `planning` | a planning request, which starts only when the read waits on it |
| section | `retry` | a request read once more after its read failed with an exception |
| section | `next-unit` | the first data request of the work item after the one the slowest column is in |
| section | `speculative` | any other data request, while F and R have room |
| footer | `next-file` | the footer of the file after the current one, and of any file the read skipped past, started at once and not charged to R |
| footer | `read-ahead` | a footer queued up to `maxFilesAhead` ahead, started while it fits R |
| footer | `retry` | a footer read once more after its read failed with an exception |
| footer | `demand` | a dropped read-ahead footer the listing now needs, the first file's footer on open, a `getFile` outside a read |
| footer | `reclaim` | a footer this read waited on while another read loaded it, started again because that load left no entry |

A dropped read-ahead footer is one whose read failed twice before the read reached its file, so
the read gave up its claim.

## Carrying the ids

`FetchAttempts.run` (PR 2) wraps the read at four sites: the I/O job of
`RowGroupIterator.start(Request)`, the engine's footer read through `FileMetadataCache`, the
cache's `loadFile` for a `getFile`, and `ParquetFileReader.openFirstFile`. It numbers the attempt,
sets the ids, runs the unchanged read and commits the event. `FetchReason.CURRENT` holds a record
of the reason and the ids instead of the reason alone, so code inside an `InputFile` sees both. It
keeps its `NoThreadLocal` suppression. `FetchReason.bind` never carries the ids, and
`MetadataSource.footerOf` runs with them cleared. Request and attempt ids come from one counter
shared across both. A section's id and attempt count live on the engine's `Request` and survive
its retry. A footer's live on the read's `FileState`, which is never recreated, so a dropped
read-ahead footer that is claimed again keeps its id (rule 9). `openFirstFile` mints the first
file's footer request, which the read itself never starts. The cache's footer entry keeps that
request's id and its last attempt's id, which `FetchLink` names.

## Moves under #1720

| #1720 stage | What it moves | Hooks on that code |
| --- | --- | --- |
| 4 (`Files`) | `FileState` and the footer methods (`requestFooters`, `startFooter`, `readFooter`, `parseFooter`, `footerReadFailed`, `footerPendingDone`, the charged start), which `list()` then reaches through `ListingFooters` | PR 2's, on `FileState`, `startFooter`, `readFooter` and the in-place read: every footer start rule, id and count |
| 5 (`Requests`) | the engine's `Request`, `startRequests`, `start(Request)`, `requestFailed`, `forget` and the `Sections` bodies | PR 2's, on `Request` and `start(Request)`: every section start rule, id and count, and PR 4's section `FetchRequest` |

PR 2 waits for neither #1720 stage. If a stage lands first, PR 2 rebases onto `Files` or
`Requests`. If PR 2 lands first, the stages move its hooks. `FileMetadataCache`,
`ReadPlanning` and `openFirstFile` stay where they are. PRs 4 and 5 wait for #1720 stage 5, naming the sites as they are then. The
`FetchAttempts.run` wrapper stays inside the I/O job whether or not the stage objects build their
own jobs, which #1720 leaves open.

## Delivery stages

The plan is delivered in small incremental stages, one PR per stage. Each PR also
updates any design document whose behaviour it changes, and Docs lists the known ones.
PR 6 also updates `docs/content/reference/cli.md`, adds a release note, updates
`FETCH_PLANNING.md` "Tracing a read" and `skills/hardwood-cli/SKILL.md`.

| PR | Content | Waits for | Size / LOC | Docs |
| --- | --- | --- | --- | --- |
| 1 | This plan | none | doc | none |
| fix | `requestFooters` stops restarting a dropped read-ahead footer (#1662, or a new issue) | none | 4 | one sentence of `FETCH_PLANNING.md` "Opening files ahead" |
| 2 | `FetchAttempt` with `requestId` and `attemptId`, the ids in `FetchReason`, the schema snapshot test | fix | 800 | event docs, `FETCH_PLANNING.md` "I/O budget" (a new "Attempts" subsection) and "Opening files ahead", `PrefetchMissEvent` Javadoc, roadmap 7.2 Read Flow |
| 3 | `S3Request` with #1671's fields and the S3 request ids, HTTP version and bytes received, `s3` module only | none | 300 | event docs, `S3_STORAGE.md` "Errors and retries", the HTTP client row in `THREADING.md`, "I/O behavior" in `s3.md`, the "Fetch latency" bullet in `configuration.md`, roadmap 11.1 S3 Support |
| 4 | `FetchRequest` and `FetchLink` | review of 2, #1720 stage 5 | 400 | event docs, `FETCH_PLANNING.md` "Requests" and "Opening files ahead", roadmap 7.2 Read Flow |
| 5 | Fold of requests against attempts in tests | review of 4, go/no-go | 100 | `FETCH_PLANNING.md` `CountingInputFile` paragraph, a "Conformance fold" paragraph and the "I/O budget" `Tests:` line |
| 6 | `hardwood iotrace`: split and tree views of a read of one file or many, `-o` (the recording, or a Chrome trace), `--from`, `--repeat` | review of 5 | 3000 | roadmap 11.4 Command-line interface, the "Fetch latency" bullet in `configuration.md`, `CLI_VALUE_RENDERING.md`, `CLI_LOGGING.md`, `NATIVE_BUILD.md`, `TESTING.md` |

### Notes by PR

**PR 2.** It covers the footer sites and the section site, widening the `Sections` methods
that create requests with the file index, row-group range and purpose. `reclaim` is tested once
#1720's stage 1 harness exists. `PrefetchMissEvent`, which nothing emits, names `FetchAttempt` as its
successor in its Javadoc, and its deprecation stays with #1042. With the event enabled, an
attempt costs about 140 ns in a microbenchmark, so about 1 µs for the 7 attempts of a 20 ms
local read, below the whole-read noise. Done when each of these emits exactly one `FetchAttempt`:
every I/O job that `start(Request)` submits, every footer read that runs, `getFile`'s load, and
the first file's footer read.

**PR 3.** One event per iteration of the loop in `S3Api.sendWithRetry` for a GET. The
writer's requests share the loop and emit nothing yet. If PR 3 merges before PR 2, PR 2 adds
`attemptId` (rule 7). The test that the open's tail GET and footer-body GET carry the footer
attempt's `attemptId` goes in whichever of PRs 2 and 3 merges second. PR 3 raises with #1687 that
part GETs sent off the attempt's thread must carry `attemptId` and `part`. Done when every GET
emits one event, carrying its `attemptId` once PR 2 is in.

**PR 4.** `FetchRequest` is emitted where a request is created, under the read's lock where a poll
could start it, so it precedes every attempt. `FetchLink` is emitted at `FileMetadataCache.claim`
and names the last attempt of the load it hands over, a failed load included. Done when each
request has one event, and a link if an attempt it did not start serves it.

**PR 5.** The fold checks, in tests and without JFR, the invariant `FETCH_PLANNING.md` "Requests"
states: a read issues no data request it did not plan. It joins by id the requests PR 4's test
observer records, the attempts, and the reads `CountingInputFile` saw. Every counted read must run
in an attempt at its request, a section's attempt reading at most once and within its range. Every
request must end in one way: an attempt, a link, `inPlace`, or a record that it was forgone
(`released`, `closed`, `dropped` or `stopped`). The only production changes are to the test
observer, which is told of forgone requests and of each request's `InputFile`. Done when `ReadRequestsTest`
(except its standalone `SequentialFetchPlan`, which runs no attempt), `CrossColumnCoalesceTest`
and `PageRangeIoTest` pass under the fold, a test with two reads in flight folds each on its own,
and each forgone site has a test.

**PR 6.** `iotrace` reads one file, or several as one read, under a recording of the four
events above and `FileOpened`, and joins them by id. The default `split` view lists the plan
(requests in file order, never truncated, as coalescing is checked against it), the trace
(attempts in start order, with a timeline) and stats ending in a `Checks` line of `OK`, `WARN` or
`FAILED`. The `tree` view nests each request's attempts and GETs. The text views may change
between releases, so tools parse the recording or the Chrome trace. It adds
`--enable-monitoring=jfr` to the native build and reports its image-size and startup cost. The
#1464 test fix rides the stack as its own commit and goes as a separate PR before it. The Chrome trace leaves
out, until the events carry the fields they need (rules 6 and 7): the flow to the first
`PageDecoded`, the decode and consumer tracks, F and R counters, and a flat JSONL export. `--repeat`
repeats one read to look at its I/O; comparing builds stays with the benchmarks. It is done when its
options are in `cli.md` and an export opens in the Perfetto UI with its flows bound.

## What the demo runs showed

A rerun of the old Perflab demo reported on #828, now on the read engine, read a 7.5 MiB file of one
row group from us-east-1 against buckets in 3 regions. In the same region, one request per column chunk
beat the default merged request, with a data phase of 225 ms against 310 ms. Against us-west-2
the merged request won by 29%, and against ap-southeast-2 by 42%, so no single gap is right for all three.

At a distance, concurrent GETs mostly wait as long as a new connection would: about three times a reused
connection's time to headers against ap-southeast-2, and more against us-west-2. The events cannot tell
connection setup from queueing. Requests of at most 1 MiB, the shape #1687 proposes, were slower still
against both distant regions. Against ap-southeast-2 the footer attempt took about 1.65 s,
half of it to transfer the 256 KiB tail on a cold connection, which bears on the tail size #1671
has yet to settle.

A read of three small files for one column (the third run in the appendix) shows the other side
of it: each footer request fetched 256 KiB to read 3 to 13 KiB of column data per file, so the
tails were 96% of the bytes the read fetched. Recordings of a 656 MiB file of 188 columns
showed that bandwidth is not one number. The rate per stream falls with distance, the aggregate
rate grows with the bodies in flight, and a per-GET fit of L + bytes / B is unusable under
concurrency. So a cost model for #763 has to price connection setup, per-stream bandwidth
and concurrency, and the gap it tunes depends on the store and its distance.

See Appendix for additional demo details.

## Open work

| Item | Issue | Waits for | What the events give it |
| --- | --- | --- | --- |
| A cost model for the gap and span | #763 | recordings like the above | per-attempt and per-GET latency, bodies in flight, per-stream rate |
| A scorer over captured sequences (F, R, next-unit starts), or an estimator over `S3Request` | #763 or #1688 | an issue that asks | the recorded plan, its start rules, and each GET's times and bytes |
| Per-attempt latency on real S3 | #1671 | PRs 2 and 3 | latency split into time to first byte and transfer |
| A remote-latency simulator for benchmarks without S3 (the reply's PRs 8 and 9) | #763 | a model that the recordings support: connection setup, per-stream bandwidth and concurrency, not a lognormal latency plus bytes over one bandwidth | recordings to fit it and check it against |
| Hedging | #1401, held in #1663 | #1671 showing a latency tail | per-GET time to headers, which shows the tail. A hedge is one more attempt, with an outcome for the loser |
| Parallel part GETs | #1687 | none | `part` and the attempt's id on each part GET. Its sweep should report connection reuse beside part size |
| A write trace | none yet | an issue that asks | the write operations above, rendered by `iotrace --from` and kept by `-o` on a command that writes |
| JDK events (`jdk.TLSHandshake`, socket and file I/O), eBPF or perf on Linux (retransmits, page faults for #1661) | none yet | an issue that asks | the attempt and GET to align them with, to tell new connections from reused ones |
| An internal gap override (`hardwood.internal.gapBytes`) for a sweep, a `queued` field (submit to start) on `FetchAttempt` | none yet | an issue that asks | the plan and its GETs under each gap, queueing apart from I/O |

## End state

Once the last PR lands, the documents in the Docs column describe the events, the fold and
`iotrace`. The plan's content goes to these homes: the target model, the start
rules and the carrying of ids to `FETCH_PLANNING.md` "Requests", "Opening files ahead", "Attempts"
and "Tracing a read". `S3Request` and the write fields go to [`S3_STORAGE.md`, "Errors and
retries"](../_designs/S3_STORAGE.md#errors-and-retries). The field lists and value sets go to the
event Javadoc and `configuration.md`. The schema rules go to the "Attempts" subsection of
`FETCH_PLANNING.md`, which PR 2 adds. PR 6 deletes this plan. If the
go/no-go stops the work after PR 4, the last PR that does land deletes it, and PRs 5 and 6
move to #827 as open work.

## Appendix: demo runs

Runs of the stack at PR 6 from an EC2 c7g.4xlarge in us-east-1, 2026-10-30. Bucket and file
names are replaced by placeholders.

**ap-southeast-2, a 7.5 MiB file of one row group, repeated.** The warm-up run opens new
connections and takes 3.2 s, with a median time to headers of 573 ms. The measured runs reuse
them and take about 0.9 s, with a median time to headers of about 232 ms. The last 254.5 KiB of
data is sliced from the 256 KiB tail fetched when the file was opened, so it has no attempt.

```
$ hardwood iotrace -f s3://<bucket>/profiling_uncompressed_dict.parquet --repeat 4 --warmup 1
s3://<bucket>/profiling_uncompressed_dict.parquet
5 reads: 1 warmup, 4 measured
showing run 4, the measured run of median wall time
7.5 MiB · 1 row group · 3 requests · 2 GETs · 889.5 ms

PLAN (requests in file order):

  ██████████████████████████████████████████████████████████████▒▒

      Serves                                        Range     Length  Started by   Outcome
  r2  rg0 id … description (7 cols)        [4 .. 7551245)    7.2 MiB  speculative  1 attempt, ok
  r1  footer                         [7551245 .. 7813389)  256.0 KiB  demand       1 attempt, ok
  r3  rg0 description                [7551245 .. 7811814)  254.5 KiB               in place

TRACE (attempts in start order):

      Begin  Duration   Offset     Length  Request  GETs  Outcome  0 ms ──────────────────────────── 890 ms
  a1    0.1     239.6  7551245  256.0 KiB  r1          1  ok       ░░░░░░░░░░█
  a2  242.3     647.3        4    7.2 MiB  r2          1  ok       ···········░░░░░░░░░░███████████████████

STATS:

  Wall    889.5 ms  footer 239.6 ms (26.9%), data 647.3 ms at 11.1 MiB/s
  Busy    886.9 ms  1.0× overlap, 8.4 MiB/s
  Bytes    7.5 MiB  returned by attempts; 7.5 MiB received by GETs, 254.5 KiB sliced in place, over-fetch 0 B (0.0% of fetched)
  Checks        OK  3 requests, each served once

RUNS:

  Run     Kind      Wall ms  Footer ms  Data ms  Headers p0  Headers p50  Headers p90  MiB/s
  1       warmup     3159.7     1740.8   1333.0       247.3        573.3        899.3    5.4
  2       measured    926.9      236.1    685.4       228.7        232.5        236.2   10.5
  3       measured    896.2      239.5    651.6       227.7        230.7        233.8   11.1
  4       measured    889.5      239.6    647.3       227.8        232.5        237.2   11.1
  5       measured    696.1      239.9    453.8       223.7        230.6        237.4   15.9
  median              892.9      239.5    649.5       227.8        231.6        236.7   11.1
  min                 696.1      236.1    453.8       223.7        230.6        233.8   10.5
  max                 926.9      239.9    685.4       228.7        232.5        237.4   15.9

LEGEND: █▓ sections, one shade per request   ▒ footer   ░ wait for headers   █ body   · pre-attempt
```

**us-east-1, a 656 MiB file of 188 columns written by parquet-rs, with `--show-gaps`.** The gaps
between row groups are parquet-rs's bloom filters for each row group, which the read does not
fetch. The attempts are busy for 8.4 s of the 15.9 s read (0.5× overlap). The idle stretches are
the consumer thread walking rows (`NestedBatchDataView.setRowIndex` over 188 nested columns), not
the network or the decode pool.

```
$ hardwood iotrace -f s3://<bucket>/<file>.parquet --show-gaps s3://<bucket>/<file>.parquet
656.3 MiB · 5 row groups · 12 requests · 11 GETs · 15880.4 ms

PLAN (requests in file order):

  ██████████▓▓▓▓███████████▓▓▓████████████▓▓███████████▓▓▓▓▓▓▓▓▓▓▓

       Serves                                                         Range     Length  Started by   Outcome
  r2   rg0 fieldx_id … fieldy_id (124 cols)                [4 .. 117232383)  111.8 MiB  speculative  1 attempt, ok
  r3   rg0 addr … foo_id (64 cols)                 [117232383 .. 154277619)   35.3 MiB  speculative  1 attempt, ok
  …    not fetched: bloom filters                  [154277619 .. 154317798)   39.2 KiB
  r7   rg1 fieldx_id … fieldy_id (124 cols)        [154317798 .. 270225651)  110.5 MiB  next-unit    1 attempt, ok
  r8   rg1 addr … foo_id (64 cols)                 [270225651 .. 306254454)   34.4 MiB  speculative  1 attempt, ok
  …    not fetched: bloom filters                  [306254454 .. 306306922)   51.2 KiB
  r9   rg2 fieldx_id … addr (125 cols)             [306306922 .. 440483038)  128.0 MiB  speculative  1 attempt, ok
  r10  rg2 gni … foo_id (63 cols)                  [440483038 .. 455385392)   14.2 MiB  speculative  1 attempt, ok
  …    not fetched: bloom filters                  [455385392 .. 455462372)   75.2 KiB
  r11  rg3 fieldx_id … foo_id (188 cols)           [455462372 .. 573010498)  112.1 MiB  speculative  1 attempt, ok
  …    not fetched: bloom filters                  [573010498 .. 573062902)   51.2 KiB
  r12  rg4 fieldx_id … foo_id (188 cols)           [573062902 .. 685670952)  107.4 MiB  speculative  1 attempt, ok
  …    not fetched: bloom filters, column indexes  [685670952 .. 687005639)    1.3 MiB
  r4   rg=0-4 indexes                              [687005639 .. 687943370)  915.8 KiB  planning     1 attempt, ok
  r5   rg=0-4 indexes                              [687943370 .. 687944556)    1.2 KiB  planning     1 attempt, ok
  r1   footer                                      [687943445 .. 688205589)  256.0 KiB  demand       1 attempt, ok
  r6   rg=0-4 indexes                              [687944556 .. 688088272)  140.3 KiB               in place

STATS:

  Wall        15880.4 ms  footer 335.9 ms (2.1%), data 15413.6 ms at 42.5 MiB/s
  Busy         8403.2 ms  0.5× overlap, 41.2 MiB/s
  Bytes        654.8 MiB  returned by attempts; 654.8 MiB received by GETs, 140.3 KiB sliced in place, over-fetch 0 B (0.0% of fetched)
  Checks              OK  12 requests, each served once
  Latency    p50 87.7 ms  to headers p0 58.0 ms, p50 87.7 ms, p90 134.5 ms, p100 290.9 ms over 11 GETs
  Bandwidth  130.5 MiB/s  across 1.4 bodies in flight on average; 101.8 MiB/s per stream over 8 GETs of 4 MiB or more
```

**Three small files as one read, one column.** Each file's 256 KiB tail is fetched to read 3 to
13 KiB of the one column: footers are 768 KiB of the 796 KiB the read fetched, the many-small-files
case #1671 measures. The footers of f2 and f3 (`next-file`) start together at 334 ms, before f1's
data, and the data of f2 and f3 starts once f2's footer has arrived. The page indexes are sliced
from the tails, so they have no attempt.

```
$ hardwood iotrace -f s3://<bucket>/<file1>.parquet -f s3://<bucket>/<file2>.parquet -f s3://<bucket>/<file3>.parquet -c fieldx_id
f1  s3://<bucket>/<file1>.parquet
f2  s3://<bucket>/<file2>.parquet
f3  s3://<bucket>/<file3>.parquet
3 files · 82.1 MiB · 3 row groups · 9 requests · 6 GETs · 637.5 ms

PLAN (requests in file order):

  f1  <file1>.parquet · 4.7 MiB · 1 row group

  ___█________________________________________________________▒▒▒▒

      Serves                             Range     Length  Started by   Outcome
  r2  rg0 fieldx_id         [251068 .. 254401)    3.3 KiB  speculative  1 attempt, ok
  r1  footer              [4695004 .. 4957148)  256.0 KiB  demand       1 attempt, ok
  r3  rg=0 indexes        [4928949 .. 4928986)       37 B               in place

  f2  <file2>.parquet · 24.6 MiB · 1 row group

  __█____________________________________________________________▒

      Serves                             Range     Length  Started by   Outcome
  r5  rg0 fieldx_id       [1021315 .. 1034627)   13.0 KiB  speculative  1 attempt, ok
  r4  footer            [25573752 .. 25835896)  256.0 KiB  next-file    1 attempt, ok
  r6  rg=0 indexes      [25793676 .. 25793863)      187 B               in place

  f3  <file3>.parquet · 52.7 MiB · 1 row group

  █______________________________________________________________▒

      Serves                             Range     Length  Started by   Outcome
  r8  rg0 fieldx_id         [769418 .. 781137)   11.4 KiB  next-unit    1 attempt, ok
  r7  footer            [55043880 .. 55306024)  256.0 KiB  next-file    1 attempt, ok
  r9  rg=0 indexes      [55238108 .. 55238615)      507 B               in place

TRACE (attempts in start order):

      Begin  Duration  File    Offset     Length  Request  GETs  Outcome  1 ms ──────────────────────────── 640 ms
  a1    2.0     241.2  f1     4695004  256.0 KiB  r1          1  ok       ░░░░░░░░░░░░░██
  a3  333.8     165.2  f2    25573752  256.0 KiB  r4          1  ok       ·····················░░░░░░░░░█
  a5  333.9      64.3  f3    55043880  256.0 KiB  r7          1  ok       ·····················░░░█
  a2  342.5      61.9  f1      251068    3.3 KiB  r2          1  ok       ·····················░░░█
  a4  505.0     134.4  f2     1021315   13.0 KiB  r5          1  ok       ································░░░░░░░█
  a6  505.8      97.4  f3      769418   11.4 KiB  r8          1  ok       ································░░░░░█

STATS:

            Wall    Footer      Data      Busy  Overlap      Bytes  Over-fetch  Checks       Latency   Bandwidth
  f1    402.5 ms  241.2 ms   61.9 ms  303.2 ms     0.8×  259.3 KiB         0 B  OK                 —           —
  f2    305.6 ms  165.2 ms  134.4 ms  299.7 ms     1.0×  269.0 KiB         0 B  OK                 —           —
  f3    269.4 ms   64.3 ms   97.4 ms  161.7 ms     0.6×  267.4 KiB         0 B  OK                 —           —
  read  637.5 ms  406.5 ms  296.9 ms  764.6 ms     1.2×  795.7 KiB         0 B  OK      p50 113.6 ms  22.3 MiB/s

  Latency    p50 113.6 ms  to headers p0 51.9 ms, p50 113.6 ms, p90 197.7 ms, p100 197.7 ms over 6 GETs
  Bandwidth    22.3 MiB/s  across 1.0 bodies in flight on average

LEGEND: █▓ sections, one shade per request   ▒ footer   _ not fetched   ░ wait for headers   █ body   · pre-attempt
```
