<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# The Write Model

Reading a Parquet file is random access over bytes that already exist: the footer says where everything is, and the reader jumps to the parts it wants. Writing inverts that. The output size is unknown until the file is finished, the container is laid out for forward-only production, and every offset a reader will need is only known once the bytes it describes have been written.

That inversion explains most of what the write API does, and all of what it does not let you do.

## Forward-only, footer-last

A Parquet file is written front to back and never seeked backward:

```
PAR1 | <row group 0 pages> | <row group 1 pages> | … | FileMetaData | <footer length> | PAR1
```

The `FileMetaData` footer carries the schema and every page and column-chunk offset, and it can only be serialized once those offsets are known — so it goes last. The writer maintains a running byte position, records offsets as it streams pages out, and emits the accumulated metadata at the end.

Three things follow for a caller:

- **A file is valid only after `close()` returns.** Before that the destination holds pages with no footer, which is not a Parquet file at all. A writer abandoned mid-way leaves nothing readable.
- **A failure is not a partial file.** When the writer cannot finish, it discards its output rather than publishing it. The local backend writes to a temporary sibling path and renames atomically on close, so a reader never observes a half-written file at the target path.
- **The destination is a sequential sink.** `OutputFile` is `create` / `write` / `position` / `close` — no seeking, no size known ahead of time. That is what lets the same interface serve a file channel today and a multipart object upload later.

## Why memory is bounded by the row group, not by the file

A column chunk's metadata — its compressed and uncompressed sizes, its page offsets, its statistics — is only known once the chunk's bytes have been encoded. The writer therefore encodes and buffers a whole row group's columns in memory, then writes them out in schema order and records where each landed.

`rowGroupTargetBytes` is the knob that bounds that buffer, and it is the writer's only memory bound. It is not a bound on how much can be written: a file of any size is a sequence of row groups, each of which is buffered, flushed, and forgotten. Peak memory tracks the target, not the total.

The target counts the values buffered, and the writer holds a little more than that while a chunk is open — the value store, an index per value, and a dictionary's own table while it is deciding how to encode the chunk. Peak heap is therefore a small multiple of the target rather than the target exactly; the build pins it below three times, and it measures well under two. Budget accordingly when raising the target on a wide schema.

## Why the writer chooses the boundaries

Batches and records are *arrival* units. Pages, column chunks and row groups are *layout* units. The writer maps one to the other, and the caller does not see the seam: a batch's values are distributed into per-column page buffers, pages are cut at the page target, and a row group is flushed once its buffered data reaches the row-group target. A batch larger than the row group is split at the boundary rather than held whole.

This is why there is no explicit "end row group" call, and why submitting one large batch and streaming a thousand small ones produce the same file. The two targets are the whole vocabulary for layout, and they mean different things:

- **Page size** governs read granularity. A reader that skips pages by their statistics can only skip whole pages, so smaller pages prune finer and cost more metadata.
- **Row-group size** governs read parallelism and split sizing, and on the write side it is the memory bound above.

## How the writer picks an encoding

Under the default `AUTO` policy, the writer does not guess a column's encoding from its type or its first few values. It buffers the row group, and then — knowing what the chunk actually holds — compares the size of a dictionary page plus an index stream against the same values written `PLAIN`, and takes the smaller. The choice is per column chunk, so the same column may be dictionary-encoded in one row group and `PLAIN` in the next as its cardinality changes.

Two consequences are worth knowing. A chunk is encoded one way throughout — no chunk mixes a dictionary with `PLAIN` overflow pages, and no dictionary page is written for a chunk that ends up `PLAIN`. And naming an encoding explicitly opts out of the comparison entirely: that column builds no dictionary in any row group of the file.

The deciding is bounded, not free. A chunk whose dictionary grows past the writer's analysis budget stops interning, which is what keeps a high-cardinality column from hashing a whole row group's values into a table that is then thrown away — at the cost of that chunk no longer being able to state its `distinct_count`.

## Index addressing on the two sides

Both the reader and the writer let a field be addressed by position as well as by name, and the positions do not always mean the same thing:

- On the **reader**, a field index is a position among an accessor's **projected** children — what the read actually materializes, in projected schema order.
- On the **writer**, a field index is a position in the struct as **declared** in the schema being written.

Where a whole file is read into its own schema, the two orders coincide, and copying a record field by field — `getFieldName(i)` on one side, `setString(i, …)` on the other — is well defined. As soon as a projection drops or reorders fields, the reader's index `i` and the writer's index `i` name different fields, silently, because both are valid positions.

The rule that holds in every case: indices are positions within one schema, not identities that travel between two. When the read and write schemas are not identical, resolve the name and address by name — or map the names once and reuse the mapping.

## Further reading

- [Write Row by Row](../how-to/write-row-by-row.md) and [Write Column by Column](../how-to/write-column-by-column.md) — the two write APIs.
- [Writer Reference](../reference/writer.md) — options, encodings, codecs, and what the writer rejects.
- [How a Parquet File Is Laid Out](parquet-layout.md) — the container the writer produces.
- [The Layer Model](nested-columns.md) — the per-layer validity and offsets the columnar API takes for nested columns.
