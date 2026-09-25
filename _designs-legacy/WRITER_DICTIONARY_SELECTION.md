# Row-group-global dictionary selection (#9, stage 18)

**Status: Complete.** Tracking issue: #975. Delivery stage 18
(Optimization) of [WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md).

**Superseded in part by stage 19** ([WRITER_CODECS_AND_ENCODINGS.md](WRITER_CODECS_AND_ENCODINGS.md),
complete): `WriterConfig.enableDictionary` is replaced by a per-leaf-column encoding policy,
under which the decision this document describes is the default policy `AUTO`, and a caller
declining a dictionary names the policy `PLAIN` instead — file-wide, which is what the
removed boolean said, or for one column, which it could not say. The mechanism itself — the
two passes, the size comparison, the uniform per-chunk encoding — is
unchanged by that stage.

## Context

Stage 9 chooses a column chunk's encoding while the values are still streaming past. A
dictionary is built optimistically, and when it crosses `WriterConfig.dictionaryPageLimitBytes`
the chunk falls back to `PLAIN` from that point on. Two costs follow from deciding without
seeing the data:

- A chunk that falls back still carries the dictionary page, because the pages written before
  the fallback reference it. The file pays for a dictionary it stopped using.
- A column that is entirely distinct but stays under the byte limit is dictionary-encoded at a
  dictionary page **plus** an index stream, where the values alone would have been smaller. The
  byte limit answers "will the dictionary fit", which is not the question; the question is
  "does the dictionary pay".

`FlatWriteBenchmark` measures the result. On its six-column, one-million-row fixture, files are
about a fifth larger than parquet-java's for the same data and matched settings, and the whole
divergence sits in the three high-cardinality columns. At 100 000 rows the all-distinct `id`
column alone is 982 KB against 781 KB — the latter being exactly `PLAIN`, with no dictionary
written at all.

### How other writers decide

| Writer | Decision point | Trigger | Result |
|---|---|---|---|
| DuckDB | An `Analyze` pass over the whole row group, then `FinalizeAnalyze` fixes each chunk's encoding before a byte is written | dictionary cardinality cap, or no values | uniform chunk; no dictionary page when the chunk is not dictionary-encoded |
| parquet-java | Streaming, with replay of the values buffered so far | a first-page probe comparing dictionary bytes plus encoded indices against raw value bytes, or the dictionary byte / entry cap | usually uniform; a dictionary page only if some page used it |
| Arrow C++ / PyArrow | Streaming, index pages buffered until fallback | dictionary encoded size reaches the dictionary page limit | mixed chunk: index pages, then plain pages, dictionary page retained |
| arrow-rs | Streaming, pages buffered | estimated dictionary page size reaches the limit | mixed chunk |
| parquet-go | Streaming | dictionary bytes exceed the configured maximum | mixed chunk, explicitly not re-encoded |

The pathology is common: PyArrow 24.0.0 writing 100 000 distinct `INT64` values uncompressed
produces 1 003 512 bytes with its default dictionary setting against 800 775 bytes without,
the same 25% penalty. What separates parquet-java is that its first trigger compares sizes
rather than consulting a limit.

## The decision

A row group is analyzed before it is written. Each leaf column chunk's encoding is chosen once,
from the values the whole chunk holds, and the chunk is then encoded as a whole:

1. **Pass 1 — accumulate and analyze.** Values arrive through `writeBatch` (or the row layer's
   staged batches) and are retained per leaf column, alongside their definition and repetition
   levels. A dictionary is built as they arrive, giving exact cardinality; it is abandoned for
   that column once the size probes described below find a dictionary losing, which decides
   the chunk against dictionary encoding.
2. **Pass 2 — encode and write.** When the row group's retained data reaches the row-group
   target, each column chunk is encoded whole in its chosen encoding, page by page, and written
   straight to the `OutputFile`.

### The rule

For a leaf column chunk holding `N` present values with exact cardinality `K`, compare the two
encodings' uncompressed sizes:

- `plainBytes` — the values as `PLAIN`: `N × width` for a fixed-width type, `Σ (4 + length)`
  for `BYTE_ARRAY`.
- `dictionaryBytes` — the dictionary page as `PLAIN` over `K` entries, plus the RLE index
  stream at `ceil(log2(K))` bits per value and its run headers.

The chunk is `RLE_DICTIONARY` when `dictionaryBytes < plainBytes`, and `PLAIN` otherwise. A
column whose dictionary was abandoned during pass 1 is `PLAIN` without further arithmetic. The
comparison is on uncompressed sizes: comparing compressed sizes would mean trial-compressing
both forms, whose cost is not repaid by the rare chunk where compression reverses the ranking.

`BOOLEAN` is never dictionary-encoded, as today. A chunk of a single distinct value — the
constant column — is the dictionary's best case and stays dictionary-encoded; its single-entry
dictionary and zero-bit-width index stream are the shape stage 14's interop gate holds to a
strict reader, and that coverage carries over unchanged.

### What follows from it

- No chunk mixes encodings, so no dictionary page is written for a chunk that ends up `PLAIN`.
- `Statistics.distinct_count` can be set from the exact cardinality, which the streaming design
  cannot know — for as long as the chunk still holds the dictionary it counted with, which stage
  26a's early abandonment ends the interning early for the columns it fires on.
- Dictionary indices are assigned after every value is known, so the dictionary can be written
  in sorted value order and flagged `DictionaryPageHeader.is_sorted` — a later increment, but
  only reachable from this shape.

## Value retention

Pass 2 needs the values pass 1 saw, so the writer retains them. It retains its own copy, which
costs no copy that is not already paid: every present value is copied out of the caller's array
as it arrives today too, into the dictionary or into the page's `PLAIN` buffer, because the
`ColumnBatch` contract frees the caller's arrays when `writeBatch` returns. What changes is the
copy's lifetime — until the row group flushes rather than until the page seals — and therefore
the memory held, not the work done. Pass 2 adds a read over the retained values rather than a
second write of them.

Values are retained per leaf column in one of two representations, and a column moves from the
first to the second exactly once:

- **Dictionary form** — the dictionary plus one index per value — while the column still has a
  dictionary. Every present value is interned as it arrives, which is what makes the cardinality
  behind the decision exact rather than estimated.
- **Raw form** — the values themselves — from the moment the column gives up its dictionary,
  which happens when the comparison at
  flush decides against it. Giving it up walks the indices accumulated so far back through the
  dictionary into the value store, so the chunk holds its values once rather than twice, and the
  dictionary's entries are released.

**Which representation the values are retained in never decides the encoding.** The two
questions look alike and are not: an in-memory index costs a machine word, while an encoded
index costs `ceil(log2 K)` bits. On a 1M-row `INT32` column with 10 000 distinct values the
dictionary form is no cheaper to hold than the values — a 4-byte index for a 4-byte value —
while dictionary *encoding* writes 1.8 MB against `PLAIN`'s 4.0 MB. A writer that abandoned its
dictionary whenever the raw form was the cheaper thing to hold would send every `INT32` column
to `PLAIN` and lose more than this stage gains. The retention choice is a memory optimization
and nothing else; the encoding decision is the size comparison above, computed from cardinality
whether or not the values are still held in dictionary form.

Levels are retained beside the values, because a page's boundaries are only known in pass 2 and
its level streams have to be cut to match. They are held one unsigned byte per entry, which
bounds the writer to the nesting depth a byte can express, and a flat `REQUIRED` column has none.
A byte holds a level the page will encode in one to a few bits, so a column whose values are
themselves narrow — a flat `OPTIONAL` `BOOLEAN`, one bit of value against one byte of level — is
the case where the level store rather than the value store dominates what a row group holds.

## Output streaming

Pass 2 removes the reason the writer buffers encoded output. Today pages are produced as
batches arrive, interleaved across columns, so a column chunk cannot be laid down contiguously
until the row group is complete and the encoded pages must be held until then. After the
analysis every value has already arrived, so pass 2 encodes and writes one column chunk at a
time directly to the sink, recording offsets as it goes. Chunk metadata is carried in the
footer, so nothing has to be revisited.

The row-group flush cadence is unchanged, and becomes more directly meaningful:
`rowGroupBufferTargetBytes` already counts buffered uncompressed data, which is now the size of the
retained values themselves rather than of a page stream derived from them. It counts what those
values occupy `PLAIN`-encoded, which is what the retention matches for every type; the level
store is the one structure it does not account for. It remains a bound on what the writer holds
rather than on what a row group occupies on disk — the two differ by whatever the chunk's chosen
encoding and codec achieve, by an order of magnitude on a dictionary-friendly column. A target on
the produced size is delivery stage 27 (#980).

## Memory

Buffering moves from the output side to the input side rather than being added to it:

| Configuration | Peak per row group in flight, at the 128 MiB target |
|---|---|
| Streaming (stage 9) | the encoded pages, ~0.4× at measured compression ratios |
| This stage, a chunk with dictionary encoding disabled | the values, ~1.0× |
| This stage, a chunk that builds a dictionary | ~3×: the values, an `int` index per value, the dictionary's entries, and the table behind them |
| Either, with column-parallel encode | one encoded chunk more, held so chunks can be written in schema order |

The ~3× is a peak rather than a constant, and it is the figure to size `rowGroupBufferTargetBytes`
against. A chunk the dictionary wins outright stays near 1×, its values held once as dictionary
entries with an index each and a table too small to matter. The peak belongs to the
high-cardinality chunk: it grows a dictionary and a table over the values it has seen, and then,
on losing the comparison, resolves them into a value store
alongside. Nothing is handed back at that point — `dropDictionary` empties the dictionary's
entries but keeps its value array and hash table at the capacity they reached, and `reset()` keeps
the index array, both deliberately, so that a file's second row group reuses what its first one
sized rather than rebuilding it. Giving up a dictionary stops the structures growing; it does not
release them.

At the 128 MiB target an all-distinct `INT32` column reaches the cap at about 16.7 million
distinct values, and from there holds an index array of 64 MiB, a dictionary value array of
64 MiB, a hash table of 128 MiB, and a value store growing to 128 MiB. What the cap bounds is how
far the first three of those grow, not how much is resident once they have.

Each value store holds its values at the width the flush trigger charges them, so that the
multipliers above are the whole story: a `BOOLEAN` chunk is charged the bit its value occupies
`PLAIN`-encoded and retains a bit, not the byte a `boolean[]` would take. The exception is the
level store, which is a byte per entry against the one to a few bits a level encodes to, so a
levelled column holds more than the target states by that difference — immaterial beside a
fixed-width value, and the dominant term for a flat `OPTIONAL` `BOOLEAN`.

Parallel encode (stage 23) applies inside a row group before it applies across row groups. Its
work units are the column chunks of one group, which after the analysis are independent — each
chunk's encoding is already fixed, no adaptive state is shared between them, and their output
sizes are predictable enough to schedule. That keeps peak memory at one row group's retention
regardless of how many cores are busy. Pipelining whole row groups multiplies retention by the
number in flight and only earns that on a schema too narrow to keep the cores fed.

## Scope

Delivered here:

- The analysis pass, the size-comparison rule, and whole-chunk encoding in pass 2.
- Per-column retention with the representation switch, and level retention.
- Direct-streamed chunk output in pass 2.
- `Statistics.distinct_count` from the exact cardinality, wherever the chunk reaches flush still
  holding what it counted with.
- **Removal of `WriterConfig.dictionaryPageLimitBytes`** and its `DEFAULT_` constant. The option
  is the streaming fallback threshold, and this stage removes the fallback it triggers; a
  caller who wants no dictionary keeps `enableDictionary(false)`, and one who wants the smaller
  file gets it from the comparison rather than by tuning a limit. Nothing has shipped that
  depends on it. Its call sites are `WriterConfig`, `ParquetFileWriter`, `WriterRoundTripTest`,
  `WriterDifferentialTest`, `WriterInteropTest` and `FlatWriteBenchmark`.

### What bounds the analysis

A dictionary needs no size limit of its own. It is part of what a chunk retains, and what a row
group retains is what `rowGroupBufferTargetBytes` is compared against, so a column building a
large dictionary spends the row group's budget and the row group is cut — the same bound that
applies to every other byte the writer holds.

That leaves the two mechanisms doing the jobs they are each suited to. The byte target bounds
memory, and it does so without deciding an encoding: a column whose dictionary is large but whose
values repeat enough to pay for it keeps its dictionary and simply reaches the target sooner. The
size probes bound *work*, giving up on a dictionary that is losing so that a high-cardinality
column stops hashing values into a table the flush comparison would reject.

A separate cap conflated the two: it was a memory bound that also returned an encoding verdict,
so a column just past it was written `PLAIN` however well a dictionary would have paid.

Deliberately not here, each reachable only from this shape and sequenced separately:

- **Sorted dictionaries** and `DictionaryPageHeader.is_sorted`.
- **Automatic selection beyond dictionary-or-plain** — `DELTA_*` and `BYTE_STREAM_SPLIT` are
  written on request as of stage 19 (#976), but choosing one without being asked needs a trial
  encode this comparison does not do.
- **Per-chunk codec choice**, which `ColumnMetaData.codec` allows, for a chunk the analysis
  finds incompressible.
- **Bloom filter sizing** from exact cardinality (stage 25), and page boundary choice and
  `ColumnIndex.boundary_order` for the page index (stage 24).

The dictionary page limit is parquet-java's alone, so `FlatWriteBenchmark` records it among the
asymmetries between the contenders rather than among the settings they match.

## Validation

- No produced chunk mixes `RLE_DICTIONARY` and `PLAIN` data pages, and no dictionary page is
  written for a chunk encoded `PLAIN`, asserted over the shapes the write-path test suites
  already sweep.
- The write-path interop gate and the byte-identical equivalence tests between the columnar and
  row APIs are re-baselined against the new output; every gate case still reads back through
  parquet-java with values agreeing.
- A column of one distinct value still round-trips through the gate's strict readers.
- `FlatWriteBenchmark` reports the size gap closed against parquet-java, and reports what the
  decision costs in encode time and allocation. Both numbers belong in the stage's result,
  because the size win is bought with them.

## What it costs

Interning every value to learn a column's exact cardinality is work a streaming writer does not
do. On the benchmark's fixture — half of whose columns are all-distinct — the columnar API
writes a million rows in about 210 ms against about 150 ms before the stage. Those columns are
hashed in full, into dictionaries that grow to a row group's worth of entries, and are then
resolved back into values when the comparison rejects them.

The cost is a property of *when* the comparison is made rather than of the design, and stage 26a
([WRITER_DICTIONARY_EARLY_ABANDONMENT.md](WRITER_DICTIONARY_EARLY_ABANDONMENT.md)) moves it
earlier: the same comparison runs over the chunk's prefix at 8192 present values and doubling,
and a dictionary losing two probes running is given up there. A column that stops interning
stops paying for the hash table, so the fixture writes in about 40% less time and produces the
same file, a column abandoned early being one the comparison would have rejected anyway.

What early abandonment risks is the column whose cardinality saturates late: it looks
all-distinct over its first values and would be written `PLAIN` on that evidence, losing a
dictionary that would have paid. Requiring two consecutive losing probes bounds that to a column
still minting distinct values past the second probe.
