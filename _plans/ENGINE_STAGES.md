# Plan: the read engine's state in one object per stage

Tracking issue: #1719. Follows #1712 (the read engine) and #1717 (results as facts, per-unit state in `Unit`, the lifecycle in `Phase`), both under #1662.

`RowGroupIterator` holds a read's state in about 75 fields under one lock, and `poll()` decides what runs next. This plan moves that state into one object per stage of the read. Each stage object holds its stage's state and its share of the poll, and reads no other stage. `poll()` stays the only decision point: it calls the stages' polls in one pass and a fixed order, and passes each what it takes from the others as arguments. Facts reach the stages through `take`, which routes each fact to the stages it concerns. No stage of this plan changes behaviour: the same facts lead to the same state, and the same polls emit the same jobs in the same order.

The model is `core/src/test/java/dev/hardwood/internal/reader/engine/micro/` (`README.md`, `Engine`, `Planning`, `Walking`, `Fetching`, `Decoding`, `Output`, `Demand`, driven without a thread by `EngineTest`). Its README section "Why not stages joined by edges" records why the stages share one poll and do not exchange items and demand over edges: a rule spanning two stages became an edge, and demand took several passes to reach the stage that acts on it.

## Target structure

| Object | Holds | Its share of the poll | Its methods today |
|---|---|---|---|
| `Files` | each file's `FileState`, the file reached, the footers started, queued and dropped, the footers another read loads, the `Error` no listing raised | `filesReached` | `reach`, `requestFooters`, `startFooter`, `returnCharge`, `readFooter`, `parseFooter`, `footerParsed`, `footerReadFailed`, `footerReadErred`, `footerPendingDone`, `uncharge(FileState)`, `dropFilesBefore` |
| `Plans` | the units listed (file, planned), the next unit to plan, its need and running job, the listing position, the question asked and its answer, the planning failure | `plan`, `probe` | `plannedFarEnough`, `list`, `fileListed`, `planStepped`, `probed`, `masksOpen`, `awaitPlanNeed`, `planFailed`, `appendUnits`, `hasRows` |
| `Requests` | the position queue, F's accounting (`queuedBytes`, `speculativeBytes`), the requests demanded, R's wait, the requests that erred, each unit's data requests | `startRequests` | `charge`, `start`, `arrived`, `requestFailed`, `requestErred`, `uncharge(Request)`, `forget`, the `Sections` methods' bodies, `requestsHeld`, `dataBytesHeld` |
| `Walks` | each column's walk state, the runnable and cap-blocked columns, each unit's holders and the first unit not passed, the furthest unit entered, the columns still placing | `walk` | `walked` (both), `place`, `needMet`, `placementEnded`, `columnEnded`, `passAll`, `stopWalk`, `end`, `walkFailed`, `endAtWalkFailure`, `placedPlanFailure`, `markRunnable`, `wakeColumnsFrom`, `consumerUnit` |
| `Decodes` | the ready pages, the decodes in flight and their bound, the task admission and its wait | `decode` | `admit`, `decoded`, `decodeFailed` |
| `Publishing` | each column's first unpublished row and their multiset, the cap in rows, the batch capacity | none: it acts on facts, and hands the poll its values | `refresh`, `firstUnpublishedRow`, `roomMade`, `cellsFinished`, `sealed` |

The engine keeps the driver: the lock, the pending facts, `waitForLock`, `out`, `repoll`, `inFlight`, `phase`, `failure`, the release to R once the lock is let go of, `take`, `emit`, `fail`, `checkStall`, `describe`, `close`, the questions (`ask`, `hasLiveWorkItem`, `firstRowGroupSkip`, `canFastSkipAllRowGroups`) and `start`. `RowGroupIterator` keeps implementing `Sections`: each method takes the lock and delegates to `Requests`.

The stage names avoid `Planning` and `Planner`, which `internal.reader.plan` already uses. Each stage object is a package-private top-level class in `internal.reader.engine` (decision D1).

### Fields

Constants and collaborators a stage receives at construction (`fileCount`, `footerCharge`, `capRows`, `batchCapacity`, `maxDecodes`, the `ReadPlanning`, R) are listed under their main reader; a constant read by several stages is no design question. A row marked **two** holds mutable state that two stages write or read today; those rows are the design questions below.

| Field | Stage | Notes |
|---|---|---|
| `files`, `context`, `closeListener`, `executor`, `tasks`, `waitForLock` | driver | |
| `lock`, `pending`, `out`, `repoll`, `inFlight`, `phase`, `failure` | driver | |
| `toRelease` | **two** | written by `Files` (footer charges) and `Requests` (request charges); read by `releaseLock` |
| `referenceSchema`, `maxSpan`, `indexWindowBytes` | driver | used by `initialize` and `start` only |
| `planning` (`ReadPlanning`) | **two** | a collaborator of `Files` (claims, reads, parses, drops footers), `Plans` (lists, plans, probes, releases) and `Walks` (`plans`, `enterUnit`, `enter`, `workItem`, `batches`); decision D7 |
| `workingSet` | `Decodes` | `start` hands it to the columns; `close` cancels the wait through `Decodes` |
| `filtered`, `allInMemory` | `Plans` | |
| `nextPlan`, `planNeed`, `planRunning`, `planNeedMet` | `Plans` | `planNeed` is also read by `Requests.awaited` and `checkStall` |
| `units` (`Unit.file`, `Unit.planned`) | `Plans` | `Unit.file` is read by `filesReached` |
| `Unit.holders`, `heldFrom` | **two** | initialised from `placingColumns` by `appendUnits` (`Plans`), decremented by `Walks`; `consumerUnit` is read by `Files`, `Plans`, `Requests`; decision D2 |
| `Unit.requests` | **two** | inserted through `Sections`, forgotten when every column passed the unit; decision D2 |
| `nextList`, `listed` | `Plans` | read by `filesReached`, `footerReadFailed` (`Files`) and `walk` (`Walks`) |
| `probe`, `probeMode`, `probeAnswer` | `Plans` | |
| `furthestUnit` | **two** | written by `probe` before the columns start and by `walked` after; read by `filesReached` and `plannedFarEnough`; decision D2 |
| `planFailure`, `failedFrom`, `failedWorkItem` | `Plans` | read by `walk`, `reach`, `startRequests`, `ask` |
| `footers` (`FileState`), `filesAhead`, `fileCount`, `footerCharge` | `Files` | `FileState` also sits in `queued`; decision D5 |
| `reachedFile`, `nextFooter`, `startedThrough`, `doneBefore`, `pendingFooters` | `Files` | `reachedFile` is read by `fileListed` |
| `untakenError` | **two** | set by `Files`, cleared by `walk` where a column raises the planning failure, read by `close` |
| `readAhead` (R) | `Requests` | charged by `startRequests` for requests and queued footers; released by the driver |
| `fetchAheadBytes`, `queued`, `queuedBytes`, `speculativeBytes`, `waitingForRoom` | `Requests` | `queued` holds footers too (D5); `queuedBytes` against F is read by `plannedFarEnough` |
| `demands` | **two** | added to by `Walks` (`walked`), `Plans` (`awaitPlanNeed`) and `Request.demand` |
| `erred`, `requestErrors` | `Requests` | `requestErrors` is read by walk jobs outside the lock |
| `Request.waiting` | **two** | the columns stopped on a request: written by `Walks`, woken by `Requests`; decision D4 |
| `columns` (`Column.unit`, `walk`, `walking`, `waitingOn`, `needMet`, `candidate`, `unitTaken`, `walkFailure`, `sealAtFailure`, `placed`, `passedAll`, `ended`) | `Walks` | `ended` is also set by `fail` and `close`; decision D3 |
| `Column.side`, `Column.index` | **two** | the `DecodedColumn` is used by `Walks`, `Decodes` and `Publishing`; decision D3 |
| `Column.rowKey`, `unpublished` | `Publishing` | |
| `placingColumns` | `Walks` | gates `filesReached` and `plan`; initialises `Unit.holders` |
| `capBlocked`, `runnable` | `Walks` | `refresh` (`Publishing`) wakes `capBlocked` when the first unpublished row moves |
| `capRows`, `batchCapacity` | `Publishing` | read by `walk` and `decode` |
| `ready`, `readySeq` | `Decodes` | filled by `place` (`Walks`) |
| `decodesInFlight`, `maxDecodes`, `waitingForTask`, `taskEndedWake` | `Decodes` | |

## The poll as argument lists

The current `poll()` runs `filesReached`, `plan`, `walk`, `startRequests`, `decode` on a read whose columns started (the first two only while a column places), and `filesReached`, `probe`, `startRequests` while a question is asked. The poll keeps that order:

```java
private List<Runnable> poll() {
    if (phase == Phase.RUNNING) {
        if (walks.placingColumns() > 0) {
            files.poll(Math.max(walks.furthestUnit(), walks.consumerUnit() + 1), walks.consumerUnit(),
                    plans.workList(), out);
            plans.poll(walks.consumerUnit(), walks.furthestUnit(), requests.unstartedReachF(), files, out);
        }
        walks.poll(publishing.walkLimit(), plans.workList(), out);
        requests.poll(walks.consumerUnit(), plans.workList().failed(), files, out);
        decodes.poll(walks.takePlaced(), publishing.firstUnpublishedRow(), out);
    }
    else if (phase == Phase.ASKING && !plans.answered()) {
        files.poll(plans.probeReach(), walks.consumerUnit(), plans.workList(), out);
        plans.probe(files, out);
        requests.poll(walks.consumerUnit(), plans.workList().failed(), files, out);
    }
    tasks.signal();
    return out;
}
```

- `walks.consumerUnit()` is evaluated at each call. A walk that ends a column inside `walks.poll` passes units, so `requests.poll` sees a later consumer unit than `files.poll` did, as `startRequests` does today. A value computed once per poll, as the model's `Demand`, would change that order and is out of scope.
- `publishing.walkLimit()` is `firstUnpublishedRow() + capRows + batchCapacity`, the bound `walk` computes once today; `publishing.firstUnpublishedRow()` is the base of `decode`'s cap.
- `requests.unstartedReachF()` is `queuedBytes >= fetchAheadBytes`. Nothing between the start of `plan` and its end changes it.
- `plans.workList()` is the read-only view of the units (D2): their count, each unit's file and whether it is planned, `listed`, `nextList`, and the planning failure.
- `walks.takePlaced()` hands `Decodes` the pages placed since the last poll, in placement order, from a list `Walks` reuses. `Decodes` keys them with `readySeq` as `ready` does today, so the ready set `decode` iterates is unchanged.
- `files` is passed to `Plans` as `ListingFooters` (whether a file's footer is loaded; claim again a footer that failed ahead) and to `Requests` as `FooterStart` (start a queued footer, charged).
- The next-unit rule (FETCH_PLANNING.md, "Planning ahead": the work item the slowest column is in and the next, whatever F holds) lives in `Plans.plannedFarEnough(consumer, furthest, fReached)`: `nextPlan > consumer + 1 && fReached` for files not all in memory. Its request-side counterpart, the next unit's first request started uncharged, lives in `Requests.poll(consumer, ...)`.

### Facts routed to stages

`take` keeps its one switch. Each case calls the stages a fact concerns, in the order the current handler changes their state:

| Fact | Stages, in order |
|---|---|
| `PlanStepped` | `Plans`; a `Request` need to `Requests.demand`; for a planned unit every column has passed, `Plans.release` and `Requests.forget`; `Walks.unitPlanned` |
| `Probed` | `Plans`; a `Request` need to `Requests.demand` |
| `StageFailed`, `ListingFailed` | `Plans`, then `Walks.wakeFrom(from)` |
| `FileListed` | `Plans` (with `files.reachedFile()`), `Walks.unitsAppended(count)`, `Walks.wakeFrom(before)` |
| footer facts, `PendingFooterDone` | `Files`, with `phase` and the listing position as values |
| `Walked`, `WalkFailed` | `Walks` (placement, with `Publishing` for sealed cells and the first unpublished row); a stop on a request to `Requests.demand`; for each unit passed, `Plans.release` and `Requests.forget` |
| `Decoded` | `Decodes`, `Publishing` (sealed cells, refresh), `Walks` (placement ended, walk failure, runnable) |
| `DecodeFailed`, `ColumnFailed` | `Decodes` for the first; `Walks.columnEnded`, the side's failure, `Publishing.refresh` |
| `RequestRead`, `RequestErred` | `Requests`, then `Walks.arrived(request)` |
| `RequestFailed` | `Requests` with `awaited` as a value (`Walks.waitsOn(r)` or `Plans.need() == r`); on the second failure `Walks.arrived(request)` |
| `NeedMet` | `Walks` |
| `CellsFinished`, `RoomMade` | `Publishing`; a moved first unpublished row wakes `Walks`' cap-blocked columns |
| `ReadAheadReleased`, `TaskEnded` | `Requests`, `Decodes` |

`fail` sets the driver's failure and calls `Walks.failAll`. `close` calls each stage's close in today's order. `checkStall` ORs each stage's `waitsOutside()` (R's wait, the task admission, footers another read loads, a planning need other than a request, a column waiting for room or another need), and `describe` joins the stages' fragments into today's text.

## Cross-stage reads that are more than a value

| Read | Proposal | Decision |
|---|---|---|
| Walk jobs and planning jobs read requests' bytes outside the lock (`Section.poll`, `SectionCursor`); `Plans` and `Walks` test `Request.done()` under it | `Request` becomes a package-private class that `Requests` creates and changes; others hold it as a `Section` or a `Need` and call `done()` only | no |
| Planning stages and a walk past a row limit insert requests through `Sections` from their jobs; `Request.release`, `demand` and `discard` take the lock | `RowGroupIterator` keeps `Sections` and delegates under its lock; `Request` gets an `EngineLock` (`lock()`, and `unlock()` with the drain) from the driver | D6 |
| Planning asks whether F has room | the value `requests.unstartedReachF()` | no |
| `consumerUnit`, `heldFrom` and `Unit.holders` read by four stages, and the release rule (a unit is released once planned and passed by every column) spans `Plans` and `Walks` | see D2 | D2 |
| A walk that ends its column inside `walks.poll` seals, publishes and refreshes (`end`), and the refresh may wake cap-blocked columns | `Walks` receives `Publishing` as `ColumnOutput`: `boolean ended(Column)` seals, publishes, refreshes and answers whether the first unpublished row moved | D8 |
| A walk job tells a request's `Error` from its own (`requestErrors`) | `Walks` receives the set as a read-only `Predicate<Throwable>` | no |
| `walk` clears `untakenError` when a column raises the planning failure | `Walks` notes that a column raised it; the driver calls `files.raised(failure)` after `walks.poll`. Only `close` reads `untakenError`, and it takes the lock | no |
| `startRequests` starts queued footers in position order with the data | `Requests` keeps one position queue of `Positioned`; at a footer it calls `FooterStart.startCharged(f)` | D5 |
| Footer and request charges both return to R through `toRelease` | each of `Files` and `Requests` keeps its own counter; `releaseLock` sums and clears them | no |

### Decisions for the maintainer

- **D1. Where the stage classes live.** Example: `Walks` as a static nested class of `RowGroupIterator` compiles even when it reads `planFailure`, since nestmates see each other's private fields. Recommendation: package-private top-level classes in `internal.reader.engine`, so the compiler enforces that a stage reads no other stage. `Fact` stays private to the driver; `Request`, `FileState`, `Column`, `Unit` and `Positioned` move to package-private top-level classes.
- **D2. The units.** Example: `planStepped` marks a unit planned and, if `holders == 0`, releases it; `walked` decrements `holders` and releases a planned unit at zero. Recommendation: `Plans` owns `file` and `planned` and exposes them through `WorkList`; `Walks` owns the holders (an `int` per unit, appended on `unitsAppended`), `heldFrom` and `consumerUnit`, and takes the probe's reach as the starting value of `furthestUnit` when the columns start; `Requests` owns each unit's data requests. The release rule is the driver's: `Walks` reports each unit it passes through a `UnitsPassed` callback, so the release keeps its place inside the walk loop, and the driver releases the unit where `Plans` has planned it. The alternative is one `Units` table that `Plans`, `Walks` and `Requests` write through methods, which keeps the rule in one place and gives three stages a shared object.
- **D3. The column.** Example: `Column` carries walk state (`walk`, `waitingOn`), publishing state (`rowKey`) and the `DecodedColumn` every stage calls. Recommendation: `Walks` owns a `Column` holding the walk fields; `Publishing` keeps `rowKey` in a `long[]` by column index; the `DecodedColumn` sides are handed to `Walks`, `Decodes` and `Publishing` at `start`, as the column's component, which no stage owns. Facts that carry a `Column` keep doing so; `Decodes` and `Publishing` use only its index and side.
- **D4. Who waits on a request.** Example: `walked` adds a column to `r.waiting`, and `arrived` marks every waiting column runnable. Recommendation: `Walks` keeps the columns stopped on a request in an identity map, filled when a walk stops and emptied on `Walks.arrived(r)`; the cost is one lookup per request, not per page. The alternative keeps the list on `Request`, touched only by `Walks`, which saves the map and leaves walk state on an object `Requests` owns.
- **D5. Footers in the request queue.** Example: a file's queued footer must start after that file's earlier data and before later files' data, in one walk of the queue. Recommendation: `Requests` keeps the single queue and starts a footer through `FooterStart`, so the footer's I/O job is emitted at the same place in `out` as today. Collecting footers and starting them after `requests.poll` would move their jobs behind later data requests.
- **D6. The lock in `Request`.** Example: `Request.release` runs on a walk job and takes the read's lock. Recommendation: an `EngineLock` interface the driver implements, passed to `Requests` and from there to each `Request`; the alternative is an inner class of the driver, which defeats D1.
- **D7. `ReadPlanning` as a shared collaborator.** Example: `Files` calls `claimFooter` while `Walks` calls `plans(unit)` on the same object. Recommendation: each stage holds the `ReadPlanning` reference and calls only its own methods; narrowing it to one interface per stage is out of scope.
- **D8. Sealed-cell jobs.** Example: `place`, `end`, `endAtWalkFailure` and `decoded` each call `sealed`, which emits a decode job at that point in `out`. Recommendation: `Publishing.sealed(column, out)` is the one place, called from the routing and from `ColumnOutput`, so the jobs keep their order. Moving the emission into a poll would put the jobs behind the walk advances of the same poll.

## Stages

Each stage is one PR, behaviour-neutral, leaving `./mvnw verify` green. Every stage also runs `./mvnw verify -Pzero-threads` on core, since it changes how a read is driven, and `ShuffledTaskOrderTest`, whose seeded job orders cross every stage. A stage adds no allocation per poll or per page and no boxing: the hand-offs between stages reuse their lists.

### Stage 1: a threadless harness for the engine

A test harness drives a real `RowGroupIterator`, over in-memory files a read treats as remote, without a thread, as `EngineTest` drives the model. It needs from the engine:

- a package-private constructor that takes the dispatch: an executor that only collects each job with its kind (step, planning, decode, I/O) and what it reads, in place of `ReadExecutor.forContext` and `context.ioExecutor()`. `ReadExecutor` is sealed; the collecting executor is a third permitted type, or the dispatch moves behind a package-private interface that `emit` calls.
- package-private `post(Fact)` (today's `update`), with `Fact` package-private, for the callbacks a test plays: room made, R released, a task ended, a need met.
- the waits of `ask` and `close` served by the harness: `ReadTasks.await` runs the jobs the test scripted, as it runs a `ReadQueue`'s head.

A test runs a chosen job, or fails it with a chosen exception, and checks the jobs the following poll emitted. First tests: the next unit's first request starts with F full; a footer read ahead fails twice and fails the read only at its file; the cap stops the walks of a consumer that takes no batch. Later, the gated and timed waits in `ReadAheadTest`, `FilesAheadTest` (`GatedReads.awaitSettled`, `ReadGate.awaitInFlight`, `awaitTrue` loops) and `PoolWaitTest` can move onto the harness, and `ShuffledTaskOrderTest` can pick its seeded orders without `TaskOrderExecutor`'s runner thread and its wait for the caller to park. That move is a follow-up per test class.

### Stage 2: `Publishing`

Moves `unpublished`, `Column.rowKey`, `capRows`, `batchCapacity`, `refresh`, `firstUnpublishedRow`, `roomMade`, `cellsFinished` and `sealed` (D8). `refresh` answers whether the first unpublished row moved; the driver wakes `capBlocked`. Covered by `ReadAheadTest`, `ReaderEndOfStreamTest`, `BatchExchangeRowLimitTest`, `ColumnFailureTest`, `ReaderCloseMidReadTest`. Gate before merge.

### Stage 3: `Decodes`

Moves `ready`, `readySeq`, `decodesInFlight`, `maxDecodes`, `waitingForTask`, `taskEndedWake`, `admit`, `decode`, `decoded`'s and `decodeFailed`'s own part. Until stage 7, `place` calls `decodes.ready(column, page)` from the driver. Covered by `ShuffledTaskOrderTest`, `ColumnFailureTest`, `PoolWaitTest`, `ZeroThreadContextTest`. Gate before merge.

### Stage 4: `Files`

Moves `footers`, `reachedFile`, `nextFooter`, `startedThrough`, `doneBefore`, `pendingFooters`, `untakenError`, `filesAhead` and the footer methods. It gets `files.poll(reach, consumer, workList, out)` and implements `FooterStart` and `ListingFooters`, which the driver's `startRequests` and `list` call until stages 5 and 6. Until stage 6, `WorkList` is a view the driver implements over its own fields. Covered by `FilesAheadTest`, `FooterReadRetryTest`, `LaterFileFailureTest`, `MultiFilePlanningTest`, `ReadRetentionTest`, `EmptyFilesReadTest`, `FileMetadataCacheTest`.

### Stage 5: `Requests`

Moves `queued`, `queuedBytes`, `speculativeBytes`, `waitingForRoom`, `demands`, `erred`, `requestErrors`, `fetchAheadBytes`, `Unit.requests`, the `Request` class (D6) and `startRequests`, `forget`, the request facts, and the bodies of the `Sections` methods. Settles D4 and D5. Covered by `ReadRequestsTest`, `RequestRetentionTest`, `SpeculativeErrorTest`, `ReadYieldTest`, `SequentialNextRowGroupPrefetchTest`, `ReadAheadTest`, `SectionCursorTest`, `IndexWindowLifecycleTest`.

### Stage 6: `Plans`

Moves `units` (`file`, `planned`), `nextPlan`, `planNeed`, `planRunning`, `nextList`, `listed`, `probe`, `probeMode`, `probeAnswer`, `planFailure`, `failedFrom`, `failedWorkItem`, `filtered`, `allInMemory`, and `plan`, `probe`, `list` with their facts. Exposes `WorkList`. Covered by `LiveWorkItemTest`, `PlanningFailureTest`, `InMemoryPlanningAheadTest`, `DictionaryPushDownIoTest`, `BetweenWorkItemsFailureTest`, `SequentialNextRowGroupPrefetchTest`.

### Stage 7: `Walks`

Moves `columns`' walk state (D3), `runnable`, `capBlocked`, `Unit.holders`, `heldFrom`, `furthestUnit`, `placingColumns`, `walk` and the walk methods; settles D2 with `UnitsPassed`, and replaces `decodes.ready` with `takePlaced`. It is last because it reads the most: the walk limit, the work list, request errors, `ColumnOutput`, the units passed. Covered by `ColumnFailureTest`, `BetweenWorkItemsFailureTest`, `ReaderCloseMidReadTest`, `ShuffledTaskOrderTest`, `ZeroThreadContextTest`. Gate before merge. This PR also completes the plan (see Completion).

## Gate

The engine's bookkeeping per page runs serially under the lock, so stages 2, 3 and 7, which touch the per-page path (placement, refresh, the ready set, the walk loop), are gated on the N300 against main before they merge: pinned at 1.50 GHz, two rounds, base then head. Workloads, from the #1712 comparison: 2,000 `INT64` columns (`gen-wide.py`, 40k rows in 4 row groups) at 1 and 8 threads, and the peak page rate (`gen-rate.py`, 8 `INT64` columns of 8 KiB `PLAIN` pages, about 31k pages) at 1 and 8 threads, both through `mini.Bench` in `hardwood` mode in `_tmp/mini-reader`. A stage passes when its medians lie within the spread of main's two rounds. Stages 4 to 6 run per request, file or unit, and need the container's `./mvnw verify` only.

## Open questions

- The gate's workloads live in `_tmp/mini-reader`, outside the repository. Adding a wide-schema and a page-rate benchmark to `hardwood-benchmarks`, as regression-only, makes the gate repeatable by others; it would land before stage 2.
- Whether `Publishing` keeps `rowKey` in a `long[]` or in a small per-column object (D3); the array avoids an object per column on the page path.
- The stall message: `describe` keeps its text. Whether each stage's `toString` gives its fragment, as in the model, or the driver keeps building the text from stage accessors.

## Out of scope

- Any change of behaviour: the order of decisions in a poll, the jobs emitted and their order, the next-unit rule, F and R, the stall check.
- Walk advances stay jobs on the pool. The model runs walks inside the poll; the engine does not.
- Jobs stay code (`emit(Kind, Work)`). The model's commands as data and its generic runtime (`Machine`, `Driver`, `Interpreter`) stay in the model.
- A `Demand` value computed once per poll (see the poll's notes).
- Narrowing `ReadPlanning` to one interface per stage (D7), and the `Sections` interface.

## Completion

The PR of stage 7 folds the end state into `_designs/READ_PIPELINE.md` ("One decision point": the stage objects, what each holds, the poll's order and its arguments, and the routing of facts) and `_designs/THREADING.md` (the row of the read's state: one lock over the stage objects), adds the harness to `TESTING.md`, and deletes this plan. It also deletes the micro reader: the engine and `READ_PIPELINE.md` then carry its structure, the harness of stage 1 takes `EngineTest`'s role for the real engine, and git history keeps the model and its rationale. Keeping it would leave two descriptions of the engine's structure to keep in step.
