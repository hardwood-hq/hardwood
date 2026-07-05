# Dictionary String reuse on the row-reader path

Status: Implemented (flat and nested row-reader paths) — issue [#636](https://github.com/hardwood-hq/hardwood/issues/636)

## Problem

When the row reader materialises a `String` for a dictionary-encoded `BYTE_ARRAY`
column, it calls `new String(bytes, UTF-8)` for every value. A column chunk holds a few
distinct dictionary entries (cardinality `C`) referenced by many values (`N`), so the same
text is decoded and allocated up to `N / C` times. On dictionary-heavy text columns this
dominates allocation rate and adds materialisation cost.

## Design

Materialise each dictionary entry's `String` once per column chunk and hand back the cached
reference for every value that references it. The result is transparent: callers still
receive ordinary `String`s.

### Per-chunk interned cache

`Dictionary.ByteArrayDictionary` holds a lazily-allocated `String[]` parallel to its entry
bytes. `internedString(index)` decodes `values[index]` once and caches it:

```
String internedString(int i) {
    if (interned == null) interned = new String[values.length];
    String s = interned[i];
    if (s == null) { s = new String(values[i], UTF-8); interned[i] = s; }
    return s;
}
```

The dictionary instance already lives for the whole chunk (one per column chunk, reused
across all of the chunk's data pages), so the cache is chunk-scoped and is released with the
chunk.

### Carrying the entry index to the batch

Dictionary-encoded `Page.ByteArrayPage`s carry the source `ByteArrayDictionary` and the
per-value dictionary entry indices (`int[]`, `-1` at null positions); both are `null` for
`PLAIN` (dictionary-fallback) pages. The row-assembly value holder (`BinaryBatchValues`)
carries, for `UTF8` / `JSON` columns only (the `internStrings` flag, set at batch allocation), a
single chunk `dictionary` reference plus a parallel `int[] dictIndices`. `dictIndices[i]` is the
entry index, or `-1` for a value that decodes from the packed bytes (a plain value, a null,
or a value from a second chunk's dictionary — see below).
`BinaryBatchValues.stringAt(i)` resolves through the cache and falls back to the packed bytes
otherwise:

```
if (dictionary != null && dictIndices[i] >= 0)
    return dictionary.internedString(dictIndices[i]);
return new String(bytes, offsets[i], offsets[i+1]-offsets[i], UTF-8);  // unchanged fallback
```

The `dictionary` slot and `dictIndices` array are allocated lazily, when the first
dictionary-encoded page contributes to a batch; the worker resets the slot to `null` after
taking the next free batch (at the end of `publishCurrentBatch`). A non-string column allocates
neither, and a string column whose chunk is entirely `PLAIN`-encoded allocates neither — paying
only the single `dictionary != null` check in `stringAt`.

A batch flushes at file boundaries, not chunk boundaries, so one batch can span two column
chunks (two dictionaries). The holder keeps the **first** chunk's dictionary; values from the
second chunk record `-1` and decode through the packed bytes — correct, merely
un-deduplicated for that one straddling batch. The next batch resets the slot and adopts the
second chunk's dictionary.

### Scope

- `UTF8` / `JSON` `BYTE_ARRAY` columns on the row-reader path (flat and nested), across every
  string accessor — typed `getString`, generic `getValue` / `toString`, and the list and map
  collection views all resolve through the per-chunk cache. The per-value `dictIndices` array is
  allocated lazily and only for these columns; all other columns are unaffected.
- The packed-byte representation is retained, so `getBinary` / raw-byte access keep their
  existing contract.
- `UTF8` / `JSON` `BYTE_ARRAY` columns on the column-reader path (`ColumnReader.getStrings()`)
  resolve through the same per-chunk cache across every read — unfiltered-flat and
  struct-only-nested keep their dictionary directly, and filtered-flat and repeated (list/map)
  reads carry it through batch compaction (`compactBinary`).
- Out of scope: non-string dictionary leaves.

## Correctness

- **Lifetime.** The interned `String[]` lives on the per-chunk dictionary and is discarded
  when the chunk's page iterator is released. Returned `String`s are immutable, so handing the
  same instance to many rows — and letting callers keep them across `next()` — is safe; the
  flyweight "not safe across `next()`" contract concerns the reused mutable buffers, not
  immutable return values.
- **Mixed pages.** A chunk may mix dictionary and plain pages; plain-page values (and null
  values) record `dictIndices[i] = -1` and fall back to per-value `new String`.
- **Chunk-straddling batch.** A batch that spans two chunks keeps the first chunk's
  dictionary; the second chunk's values record `-1` and decode from the packed bytes for that
  one batch (correct, un-deduplicated). The byte buffer is written for every value regardless,
  so the fallback is always available.
- **Threading.** The dictionary (and its cache) is confined to one column's pipeline, and in
  the current readers the cache is filled only on the consumer thread (via `stringAt`).
  Entries are immutable, so even if a future caller filled it from another thread, the worst
  case is decoding an entry twice — never a wrong value.

## Notes

- **Generic-path interning** routes string leaves through `getString` *before* `convertValue`,
  because `convertValue` decodes a raw `byte[]` and cannot accept an already-interned `String`.
- **Column-reader interning ([#724](https://github.com/hardwood-hq/hardwood/issues/724)).**
  `ColumnReader` shares `FlatColumnWorker` / `NestedColumnWorker`, so a `UTF8` / `JSON` column
  records `dictIndices` and runs `ensureDictionary` every batch (`internStrings` gates by column
  type, not by consumer). `ColumnReader.getStrings()` consumes that bookkeeping through `stringAt`
  on every read, matching the row reader's dedup: unfiltered-flat and struct-only-nested reads
  keep the dictionary directly, and the filtered-flat and repeated reads gather the entry indices
  through `compactBinary` so the compacted batch still resolves against the chunk dictionary.
- **Deferred:** `Dictionary.decodePage` builds the per-value index array for every
  `ByteArrayDictionary` page, including `INT96` / `FIXED_LEN_BYTE_ARRAY` columns that never
  intern — a small, transient per-page allocation that a column-type hint could skip (the
  batch-level `dictIndices` is already limited to string columns via `internStrings`).
