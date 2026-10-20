# Plan: varlength values behind a per-value window

Tracking issue: #1578. Stage issues: #1578, #1572.

`ColumnReader` hands out a varlength leaf as one `byte[]` per batch with `getBinaryStarts()` and
`getBinaryEnds()`. The single buffer is the batch's storage layout made public, and it forces
three costs on the reader:

| Cost | Where |
| --- | --- |
| A plain batch's bytes in one array of 1–4 MB on the plain binary benchmark file, humongous at G1's region sizes and allocated per batch | `BinaryBatchValues.prepareBytes` (#1572) |
| A dictionary batch's values, or the whole dictionary once per batch, copied into that array | `BinaryBatchValues.buildViews` |
| Plain values copied from the page into that array, which a caller adopting it (Flink's `HeapBytesVector`) receives instead of the page's bytes | `BinaryPageValues.copyPlain` |

The plan moves the public API to a per-value window first, then changes the storage behind it.

## Stages

| # | Stage | Issue | Gate before the next stage |
| --- | --- | --- | --- |
| 1 | `BinaryValues` over the single buffer | #1578 | the window reads as fast as the raw arrays |
| 2 | Dictionary values in place, plain values in bounded chunks | #1572 | no batch array is humongous; `hardwoodRowStrings` and `hardwoodColumnViews` level or faster on the container and the N300 |
| 3 | Plain values in place in their pages | to file | measured first, see below |

### Stage 1: the API

`ColumnReader.getBinaryValues()` returns a `BinaryValues`, and `getBinaryStarts()` and
`getBinaryEnds()` are removed:

```java
@Experimental
public final class BinaryValues {
    public byte[] buffer(int i);   // the array value i is read from
    public int start(int i);       // inclusive, in buffer(i)
    public int end(int i);         // exclusive, in buffer(i)
    public int size();             // getValueCount()
    public int length(int i);
    public void copyTo(int i, byte[] dst, int dstOffset);
    public boolean contentEquals(int i, byte[] other, int offset, int length);
    public byte[] getBinary(int i);   // a copy, as BinaryDictionary.getBinary
}
```

- **Contract.** `buffer(i)` is an array value `i` can be read from, valid while the batch is, and
  not to be written. It promises nothing about values sharing an array, the same array on every
  call, or the array being where the value is stored. A later `MemorySegment` accessor (once the
  Java baseline allows FFM) is then an addition, and storage the window cannot point into is served
  from a copy without a change of contract.
- **No per-byte accessor.** A `byteAt(i, pos)` checked against the value's length costs +20–24% on
  a full-byte hash: the check is not hoisted out of the caller's loop. Unchecked, an index past the
  value reads its neighbour. The window costs nothing (measured in #1578).
- **Final class, index accessors.** One implementation keeps each call site in the caller's loop
  monomorphic. A callback API (`forEach(consumer)`) is not offered: its one call site inside the
  library goes megamorphic across callers.
- **No per-value string accessor.** Taking a batch's windows builds its dictionary values'
  views; `getStrings()` resolves strings through the dictionary without them, so it stays the
  string accessor.
- **Unchanged.** `getDictionaryIds()`, `getBinaryDictionary()`, `getBinaries()`, `getStrings()`,
  the row readers' accessors and the internal readers of `BinaryBatchValues` (matchers, logical
  type decoding).
- **Callers to port.** `docs/content/how-to/column-reader.md` (binary section, `list<string>` and
  map examples), `_designs/COLUMN_READER.md#variable-length`, release notes; hardwood-benchmarks'
  `BinaryScanBenchmark` and `NestedScanBenchmark`; the 1BRC harness's column-reader solutions; the
  Flink integration's `HardwoodColumnVectorFiller`.

Gate: the 1BRC column-reader solution and `hardwoodColumnViews` ported to the window, level with
the raw arrays on the container.

### Stage 2: storage behind the window (#1572)

Implemented in #1582; the N300 half of the gate is open. `BinaryBatchValues` holds a list of buffers and, per value, a buffer id beside its start
and end ([COLUMN_READER.md](../_designs/COLUMN_READER.md#variable-length)). Dictionary values
point into the dictionary's own bytes; plain values are copied into chunks of at most 256 KB,
each segment filling chunks of its own, so the byte sizing pass, segment parking and the flat
binary page's own scratch went with them. A flat row reader's recycled holder keeps its chunks
for its next fill, and the free queue keeps every holder returned. Not done: the Flink filler does
not yet hand Flink the dictionary and ids.

Gate: no batch allocation of 1 MB or more on the plain binary benchmark (JFR); GCs started by a
humongous allocation at the level before #1287; `hardwoodRowStrings` and `hardwoodColumnViews`
level or faster against `main` on the container and the N300.

### Stage 3: plain values in their pages

The window points into the decompressed page, so a plain value is never copied. Open before
building:

- A page's bytes live in the page's reused scratch; the window needs a buffer owned per page.
- A default page is 1 MB decompressed, humongous at 1 MB regions: the humongous allocation moves
  from batch to page unless pages are decompressed into bounded buffers.
- A batch holds its pages alive; a consumer keeping detached batches keeps their pages.
- A page stored uncompressed and read from a file's mapping is a direct buffer, which the window
  cannot point into before a `MemorySegment` accessor exists.

Gate to start: a measurement of what the copy into chunks costs after stage 2.
