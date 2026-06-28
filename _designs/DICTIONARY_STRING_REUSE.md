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
per-value dictionary entry indices (`int[]`); both are `null` for `PLAIN` (dictionary-fallback)
pages. The row-assembly value holder (`BinaryBatchValues`) records, for `UTF8` / `JSON`
columns, a per-value `(dictionary, index)` pair parallel to its offsets.
`BinaryBatchValues.stringAt(i)` resolves a dictionary-encoded value through the cache and
falls back to the packed bytes otherwise:

```
if (dictionaries != null && dictionaries[i] != null)
    return dictionaries[i].internedString(dictIndices[i]);
return new String(bytes, offsets[i], offsets[i+1]-offsets[i], UTF-8);  // unchanged fallback
```

Because the dictionary reference is per value, a batch that spans two column chunks (and
therefore two dictionaries) resolves each value against its own chunk's cache.

### Scope

- `UTF8` / `JSON` `BYTE_ARRAY` columns on the row-reader path (flat and nested), across every
  string accessor — typed `getString`, generic `getValue` / `toString`, and the list and map
  collection views all resolve through the per-chunk cache. The parallel per-value arrays are
  allocated only for these columns; all other columns are unaffected.
- The packed-byte representation is retained, so `getBinary` / raw-byte access keep their
  existing contract.
- Out of scope: non-string dictionary leaves, and the column reader's own `getStrings`
  materialisation (a separate API) — they can adopt the same per-chunk cache later.

## Correctness

- **Lifetime.** The interned `String[]` lives on the per-chunk dictionary and is discarded
  when the chunk's page iterator is released. Returned `String`s are immutable, so handing the
  same instance to many rows — and letting callers keep them across `next()` — is safe; the
  flyweight "not safe across `next()`" contract concerns the reused mutable buffers, not
  immutable return values.
- **Mixed pages.** A chunk may mix dictionary and plain pages; plain-page values (and null
  values) carry a `null` dictionary and fall back to per-value `new String`.
- **Threading.** The dictionary (and its cache) is confined to one column's pipeline, and in
  the current readers the cache is filled only on the consumer thread (via `stringAt`).
  Entries are immutable, so even if a future caller filled it from another thread, the worst
  case is decoding an entry twice — never a wrong value.

## Notes

- **Generic-path interning** routes string leaves through `getString` *before* `convertValue`,
  because `convertValue` decodes a raw `byte[]` and cannot accept an already-interned `String`.
- **Deferred:** `Dictionary.decodePage` builds the per-value index array for every
  `ByteArrayDictionary` page, including `INT96` / `FIXED_LEN_BYTE_ARRAY` columns that never
  intern — a small, transient per-page allocation that a column-type hint could skip. The
  column reader's `getStrings` and a typed `PqMap` key/value interning test are likewise
  left as follow-ups.
