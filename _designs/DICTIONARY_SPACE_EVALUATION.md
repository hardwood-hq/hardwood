# Dictionary-Space Predicate Evaluation

**Status: Implemented**

## Context

A dictionary-encoded Parquet data page stores one dictionary entry ID per
non-null value. The decoded binary batch already carries both forms needed by
the reader:

- packed value bytes and offsets, which preserve the ordinary value access
  contract; and
- the page dictionary plus each value's dictionary entry ID.

Drain-side binary matchers normally compare every row's packed byte slice with
the predicate literal. Repeated dictionary values therefore repeat the same
comparison. Dictionary-space evaluation compares each dictionary entry at most
once per predicate leaf and answers encoded rows from the cached outcome.

The optimization covers every binary predicate for which
`BatchFilterCompiler` has a slice matcher: equality, inequality, the four
orderings, and membership. Negated membership continues to use the resolved
conjunction of inequality leaves and gains dictionary evaluation through those
leaves.

## Eligibility

Dictionary-space evaluation is part of every compiled `BinaryBatchMatcher`.
The existing batch-filter eligibility rules remain authoritative:

- the predicate leaf names a projected top-level field;
- the binary comparison has a supported slice order; and
- the surrounding predicate tree can be represented by the per-column matcher
  and merge-plan model.

No separate byte-exact restriction applies. Each dictionary entry is decided
by the same per-value operation as an ordinary packed row, so variable-width
decimal equality uses sign-extending numeric comparison while strings and
fixed-width decimals use byte equality. Binary timestamp comparisons whose
logical order has no slice implementation remain on the record-filter
fallback, exactly as they do without dictionary encoding.

Nested leaves remain outside the compiled batch-filter path. Their leaf values
do not map one-to-one to top-level records, so they require a record-level
decision over repetition and definition levels rather than this flat entry-ID
lookup.

## Matcher contract

`BinaryBatchMatcher` exposes two operations with identical value semantics:

- whole-batch evaluation writes the result bitmap for an ordinary decoded
  batch; and
- per-value evaluation decides one half-open byte slice
  `[from, to)`.

Concrete binary matchers keep their specialized whole-batch loops. The
per-value operation is the semantic primitive used for a dictionary entry and
for a row that has no usable entry ID. Equality, ordering, decimal comparison,
short-value membership, and negation therefore have one definition each.

A dictionary-aware wrapper owns one `BinaryBatchMatcher` delegate:

- a batch without a dictionary goes directly to the delegate's whole-batch
  operation;
- a batch with a dictionary uses cached per-entry outcomes for encoded rows;
  and
- a row without an entry ID uses the delegate's per-value operation over its
  packed bytes.

The wrapper requires dictionary-index retention. `AndBatchMatcher` and
`OrBatchMatcher` propagate that requirement from either child, so a compound
on one column retains indices whenever any binary leaf needs them.

## Lazy per-entry outcomes

The wrapper caches state for the dictionary object currently being read. A
dictionary object is scoped to a column chunk and object identity is the cache
key. Each entry has one primitive byte state:

- `UNKNOWN` — the predicate has not been evaluated for this entry;
- `MATCH` — the entry satisfies the delegate; or
- `NO_MATCH` — the entry does not satisfy the delegate.

When a batch arrives with a new dictionary, the wrapper resets the active
state range to `UNKNOWN`. Before producing the row bitmap, it walks the batch's
present dictionary IDs while undecided entries remain. Each referenced
`UNKNOWN` entry is evaluated once through the delegate's per-value operation
and its outcome is retained for later rows and batches from the same chunk.
The discovery pass stops permanently for that dictionary once every entry has
an outcome.

This makes comparison work proportional to the distinct dictionary entries
the read actually touches. Page-index pruning, row masks, and early termination
can leave most entries undecided without paying to compare them. A full scan
still compares every entry once and pays an additional ID discovery pass only
until all entries are decided.

The output row pass observes validity first. A null row never reaches either
the cached-outcome lookup or the packed-value fallback and always has an unset
result bit.

## Batch representation and retention

`BinaryBatchValues` retains dictionary metadata when either string interning or
a dictionary-aware matcher needs it:

- `dictionary` is the first byte-array dictionary that contributes values to
  the batch;
- `dictIndices[i] >= 0` names the entry for value `i`; and
- `dictIndices[i] == -1` means value `i` must be tested from packed bytes.

The index array is allocated lazily when the first dictionary-encoded page
contributes to the batch. Reads without a dictionary-aware matcher and
non-string columns therefore keep the ordinary allocation behavior.

Dictionary retention is a property of the compiled matcher. The row-reader and
exact column-reader paths compile the filter before allocating their binary
batches and pass `requiresDictionaryIndices()` into the allocation. A nested
reader cannot honor this flat-batch requirement and rejects it; current
eligibility ensures a dictionary-aware matcher is never assigned there.

Recycled batches clear the dictionary reference before their next fill while
retaining the allocated index array. Active entries are overwritten as pages
are copied.

## Mixed encodings and chunk boundaries

Dictionary encoding is a data-page property. A column chunk may contain both
dictionary-encoded and plain pages, and a batch may cross from one column chunk
into another.

`BinaryBatchValues` adopts only the first dictionary contributing to a batch.
Rows from a plain page and rows belonging to a second dictionary carry `-1` and
use the packed-value fallback. The next batch that adopts the second
dictionary resets the wrapper's state by dictionary identity.

This preserves exact results without assuming that one dictionary covers a
whole batch or column chunk.

## Validation

Matcher tests cover every binary operator and membership comparison across:

- referenced and unreferenced dictionary entries;
- repeated IDs, proving one decision per entry per dictionary;
- null rows;
- mixed dictionary and packed-value rows;
- dictionary identity changes;
- small and large dictionaries; and
- row counts crossing a 64-bit output-word boundary.

Compiler tests prove that all eligible binary leaves request dictionary-index
retention, including leaves inside same-column `And` and `Or` composites.

End-to-end row-reader and exact column-reader tests compare string predicates
with an unfiltered reference across dictionary changes and verify non-string
`FIXED_LEN_BYTE_ARRAY` ID retention. Delegate-versus-wrapper oracle tests cover
all binary operators under byte-string, stored-byte, fixed-decimal, and
variable-decimal comparison, including packed fallback rows.

Performance validation has two layers:

- `RecordFilterBenchmarkTest` measures end-to-end binary equality, ordering,
  membership, and negated membership before and after dictionary evaluation;
  and
- a single-threaded JMH benchmark measures cold dictionary-state
  initialization and hot cross-batch reuse for equality, ordering, and
  membership, separating sparse/early-stop reads from full scans.

## Documentation and roadmap

This is an internal execution optimization and does not change the public
reader or predicate API. `ROADMAP.md` marks binary dictionary-space equality,
ranges, and membership complete when the implementation and performance
validation land. Fixed-width primitive dictionary evaluation remains tracked
separately under #859.
