# Dictionary-Space Predicate Evaluation

**Status: Implemented**

**Scope:** binary equality only. Dictionary-space evaluation for binary ranges
and membership, and for the fixed-width physical types, is not covered here and
remains open under #859.

## Context

A dictionary-encoded data page stores an integer dictionary ID for each
non-null value. Record filtering currently compares the materialized value of
every row with the predicate operand, repeating the same comparison for every
occurrence of a dictionary entry.

Dictionary-space evaluation resolves a predicate against the dictionary once
per column chunk and evaluates dictionary-encoded rows from their IDs. The
first supported shape is equality over top-level `BYTE_ARRAY` and
`FIXED_LEN_BYTE_ARRAY` columns. These columns already preserve the page
dictionary and per-row IDs through `Page.ByteArrayPage` and
`BinaryBatchValues`; the filter reuses that representation without changing
the value materialization contract of the row reader.

## Eligibility

A binary equality leaf is eligible for drain-side filtering when its column is
a projected top-level field. Both unsigned binary equality and equality for
signed fixed-length decimal values use byte equality, so the resolved
predicate's signed-order flag does not change this operation. This holds
because a `FIXED_LEN_BYTE_ARRAY` operand is required to be exactly as wide as
the column, which `FilterPredicateResolver` validates: a narrower operand would
make byte equality and signed comparison disagree.

Other binary operators and binary membership predicates remain on the
compiled per-row fallback path until their dictionary mask forms are
implemented. Existing fixed-width, boolean, membership, and null matchers keep
their current eligibility.

## Batch representation

`BinaryBatchValues` retains dictionary metadata only when either string
interning or a dictionary-aware filter needs it:

- `dictionary` is the first byte-array dictionary contributing values to the
  batch.
- `dictIndices[i]` is the entry ID for row `i` when that row uses
  `dictionary`.
- `dictIndices[i] == -1` means the matcher must inspect the materialized value.
  This covers nulls, plain-encoded pages, and values from a second column
  chunk when a batch crosses a chunk boundary.

The row-reader and exact column-reader allocation paths enable ID retention
for a matcher through the `ColumnBatchMatcher` contract. The exact
column-reader path compiles the batch filter before allocating predicate
readers and hands the same compiled filter to `SelectionEngine`, so allocation
and evaluation cannot disagree about this requirement. Reads without a
dictionary-aware filter keep the existing allocation behavior.

Retention is a property of the compiled predicate, not of the column, so —
unlike string interning — it cannot be derived inside `BatchExchange`'s
allocation helper and must be passed by the allocating reader. The nested read
path cannot honour it, because `NestedColumnWorker` fills its own accumulator
and publishes a trimmed copy rather than the batch the exchange allocated. Only
top-level non-repeated leaves are eligible for a batch matcher, and those always
take the flat path, so the nested path rejects a retention request instead of
dropping it silently.

The dictionary slot is cleared when a recycled batch is acquired for the next
fill. The index array remains allocated and is overwritten for the active
rows.

## Resolving the operand against a dictionary

The matcher resolves its operand by scanning the entries of the parsed
`Dictionary.ByteArrayDictionary` with `Arrays.equals`, exiting on the first hit.
This is the `byte[][]` that `Dictionary.parse` already builds to serve the read
path, so the scan allocates nothing and decodes no page: the entries exist
because the page under evaluation is being read.

The scan costs one pass per column chunk and is amortized over every batch drawn
from that chunk, so it is not on the per-row path this design optimizes. Two
neighbouring structures therefore sit outside this design:

- An allocation-free scan over the raw `PLAIN` dictionary buffer belongs to
  row-group pruning, where a chunk is dropped without ever being read and the
  entry arrays would be built only to be discarded. Once that path owns the
  primitive, this one can share it.
- A value-to-ID index over the dictionary turns each leaf after the first into
  an O(1) probe instead of a rescan. It pays for itself once one column carries
  several leaves, which is what binary membership support introduces, and its
  home is the memoized per-chunk dictionary source that pruning already keeps —
  so both paths share one index per chunk.

## Equality evaluation

The binary equality matcher caches the last dictionary object it observed and
the matching dictionary ID. Dictionary objects are scoped to a column chunk,
so object identity is the cache key. A new dictionary triggers one scan over
its entries:

- a hit stores the entry ID;
- an absent value stores `-1`.

If a malformed or non-canonical dictionary contains the same value under
multiple IDs, the matcher stores a mask containing every matching ID. The
single-ID representation remains the common fast path.

For each active row:

- a non-negative dictionary ID is compared with the cached matching ID or
  tested against the duplicate-entry mask;
- a `-1` ID is compared directly against the row's packed bytes and offsets;
- the resulting bitmap is intersected with the batch validity bitmap so nulls
  never definitely match.

The direct fallback compares the packed byte range without creating a
per-row `byte[]`. This keeps plain pages and mixed batches correct without
reintroducing the allocation behavior of the compiled row matcher.

The matcher writes one complete `long` word per 64 rows and leaves words
beyond `recordCount` untouched, matching the existing
`ColumnBatchMatcher` contract.

## Mixed encodings and chunk boundaries

Dictionary encoding is a page property. A column chunk may switch from
dictionary encoding to plain encoding, and a batch may span two column chunks
with distinct dictionaries. Dictionary-space evaluation therefore never
assumes that one dictionary covers every row in a batch.

`BinaryBatchValues` adopts only the first dictionary contributing to a batch.
Rows from plain pages and from another dictionary use the `-1` sentinel and
the packed-value fallback. This preserves exact results while allowing the
common single-dictionary case to use only integer comparisons.

## Comparison semantics

Equality compares byte sequences exactly. It is independent of the signed or
unsigned ordering used by range predicates. Nulls follow the existing
"definitely matches" contract: a null row has an unset result bit.

Future float and double dictionary-space matchers must use `Float.compare` and
`Double.compare` semantics, including distinct signed zeroes and canonical
NaN equality, as specified by
[_designs/DICTIONARY_PUSHDOWN.md](DICTIONARY_PUSHDOWN.md).

## Validation

Matcher tests cover:

- a dictionary hit and an absent operand;
- null rows;
- explicit packed-value fallback rows mixed with dictionary rows;
- duplicate dictionary entries;
- row counts crossing a 64-bit output-word boundary.

Compiler tests prove that binary equality enters the drain-side path while
other binary operators and membership predicates retain the full fallback.
End-to-end tests fully drain a dictionary-encoded `BYTE_ARRAY` column across
repeated files and a dictionary-encoded `FIXED_LEN_BYTE_ARRAY` column. These
cover batch recycling, dictionary changes, and the non-string index-retention
gate. A package-level integration test additionally verifies that a filtered
`ColumnReader` batch still carries its dictionary and entry IDs after
selection has compacted it.

Chunk-straddle parity has its own fixture: two row groups whose dictionaries
hold disjoint values, small enough that one batch spans both. Filtering it on
each stored value, through the row reader and the exact column reader, is
compared against the unfiltered read. Values from the second chunk exercise the
sentinel fallback, and the disjoint pools make a mis-resolved entry ordinal
observable rather than merely off by a count.

Performance validation uses a single-threaded JMH benchmark. It compares
dictionary-space equality with the packed-value comparison this design falls
back to, across dictionary cardinality and row-count parameters. That baseline
is deliberately stricter than the path being replaced: record filtering used to
materialise a `byte[]` per row before comparing, so the end-to-end saving is
larger than the benchmark reports. Multi-core wall-clock reader tests are not
used to attribute matcher CPU improvements.
