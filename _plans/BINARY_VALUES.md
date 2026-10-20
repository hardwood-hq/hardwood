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

`BinaryBatchValues` holds a list of buffers and, per value, a buffer id beside its start and end.

- **Dictionary values** point into the dictionary's own bytes (`ByteArrayDictionary.entryBytes()`)
  at the entry's offsets. The batch copies no dictionary bytes, so the append-or-copy choice of
  `buildViews` goes away; the views stay deferred until a reader asks for them.
- **Plain values** are copied into chunks of a fixed size below half of G1's smallest region
  (1 MB), so no chunk is humongous at any heap size. A value that does not fit in the rest of a
  chunk starts the next; a value longer than a chunk gets a buffer of its own, the one humongous
  case left. Each segment fills chunks of its own and takes their ids from the batch under its
  monitor, once per chunk, so segments stay independent. Buffer ids follow no order.
- **No sizing pass for bytes.** A segment no longer waits for its cell's total to learn its offset
  in one buffer, so a parked segment's bytes can be written when its page is decoded. Whether
  parking can go entirely is checked while implementing.
- **Recycling.** A row reader's holder keeps its chunks for the next batch it fills. A column
  reader's batches are detached and their chunks are not reused, as their arrays are not today.
- **Internal readers.** Matchers, `stringAt`, the logical type decoders and `LeafCompaction` read
  through the value's buffer. The batch's 2 GB limit (`BinaryBatchValues.bufferOverflow`) goes.
- **Flink.** On plain columns the filler copies each value into its own buffer (one copy more than
  today until stage 3); on dictionary columns it can hand Flink the dictionary and ids
  (`WritableColumnVector.setDictionary`) and copy no bytes. The filler change is part of this
  stage.

Gate: JFR shows no allocation of 1 MB or more on the plain binary benchmark; GCs started by a
humongous allocation at the level before #1287; `hardwoodRowStrings` and `hardwoodColumnViews`
level or faster against `main` on both machines; the 1BRC column-reader contenders level.

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
