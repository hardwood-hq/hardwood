# Multi-File Incremental Planning (#1107)

**Status: Implemented**

## Problem

A read over N files opened N footers before yielding a row. `RowGroupIterator.initialize()`
built the entire work list in one pass: for every file it read the footer, cross-checked the
schema against the reference, and — with a filter — probed bloom filters and dictionaries to
decide each row group. Time to first row was therefore proportional to the number of files. On
object storage a footer costs roughly three round trips, so a 512-file dataset spent about
1,500 of them before the first row, most of them for files the caller may never reach.

## The planning cursor

Planning proceeds one file at a time, pulled by the read rather than pushed by construction.

`planNextFile()` plans exactly one file: it appends that file's surviving row groups to the work
list and returns `false` once every file is planned or the row budget is spent. The four values
the old single-pass loop carried in locals — the row budget, rows consumed, the physical-skip
residue, and the first row group's skip — are fields, because a call now handles one file and
the next call must resume where it left off.

Each projected column walks the work list through its own `PageSource`, which holds an integer
cursor rather than an `Iterator`. An iterator would have forced the whole plan at construction,
and would have thrown `ConcurrentModificationException` once planning began appending behind it.
Every step asks `workItemAt(i)`, which plans forward only when the cursor has run past what is
planned, and returns `null` when there is nothing left.

Reference counts live in a map keyed by work-item index rather than an array sized after
planning, because there is no final size until the read ends.

## The invariant

**Reader structure must never be derived from file contents.**

Anything the reader decides once, at construction, from facts belonging to individual row groups
inside individual files forces every file to be planned before the first row — and silently
returns the read to O(N). The laziness is only as good as this invariant.

Two questions about a filtered read used to violate it, both global ANDs over every row group:
whether `maxRows` may cap the drain, and whether the predicate needs evaluating at all. One
undecided row group anywhere flipped either answer, so answering either meant planning
everything.

Both are now decided per row group, where the fact belongs. Statistics decide a row group, not a
read, and that decision already reaches the pipeline per row group as
`WorkItem.filterAlwaysMatches` → `Batch.filterAlwaysMatches`. See
[ALWAYS_MATCH_STATISTICS.md](ALWAYS_MATCH_STATISTICS.md) for the three consumers that read it.

The same rule governs where a filter is evaluated: the drain-side/record-matcher choice is made
from the predicate's shape alone, by `BatchFilterCompiler.tryCompile`, never from what a file
turned out to hold.

## Questions that are genuinely about the whole read

Two remain, and both plan only as far as they must:

- `firstRowGroupSkip()` reports the rows to discard from the first kept row group. It answers `0`
  without planning when there is no physical skip, which is every read that does not use one, and
  otherwise plans through the first work item only — the value is written when that item is
  added.
- `canFastSkipAllRowGroups()` asks whether every row group supports per-page masking, which is a
  question about all of them. It is a single-file path, so a full plan is one footer.

`getWorkItems()` plans the whole read by construction and exists for tests and teardown. A caller
that only needs to know whether the read has any work asks `workItemAt(0) == null`.

## Concurrency

Planning is synchronized on the iterator. Every projected column walks the work list on its own
retriever thread, and a step past the planned end plans another file; without the lock the columns
race and half of them find an empty list where the others found row groups.

`workItemAt` and `getWorkItems` hold the monitor, and `getWorkItems` returns a copy so a caller
cannot iterate while another thread appends. Reference counts are decremented outside the lock,
on every column's retriever thread, which is why they are `AtomicInteger` values in a
`ConcurrentHashMap` rather than plain ints.

Planning blocks: it reads a footer and, under a filter, probes bloom filters and dictionaries.
That work must not run inside a `ConcurrentHashMap` mapping function, which holds a bin lock for
its whole duration — `getColumnPlan` therefore triggers the next row group's prefetch after its
`computeIfAbsent` returns, not inside it. The prefetch resolves the next work item on the calling
retriever thread rather than in its fire-and-forget task, so that a schema mismatch raised while
planning propagates to the reader instead of being swallowed with the file silently skipped.

## Error timing

A file's I/O errors and its disagreement with the reference schema are raised when the read
arrives at that file, from the reading loop, rather than from the call that built the reader.
They are still raised before any row of that file is returned, which is what keeps a mismatched
file from being read as data. The first file is opened eagerly by `openAll`, so its errors
surface immediately.

## What is not skipped

Reading every row still opens every file. Planning is deferred, not avoided. What remains after
the change is the retriever prefetching until backpressure stops it — bounded by in-flight pages,
not by the number of files.

A read whose data is smaller in total than one batch is a degenerate case of that bound: the
pipeline legitimately walks every file to fill the first batch, because that is what the read
asked for.
