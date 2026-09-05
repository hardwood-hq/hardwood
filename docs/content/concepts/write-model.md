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

- **A file is valid only after `close()` returns.** Before that, the destination holds pages without a footer, and no reader can open it. A writer abandoned mid-way leaves nothing readable.
- **A failure leaves nothing behind.** When the writer cannot finish — whether `create()`, a `writeBatch`, a `writeRow` flush, or `close()` itself fails — it discards what it has written. A writer that has thrown once refuses to publish; `close()` discards rather than committing a truncated prefix, and `try-with-resources` is safe by default. The local backend writes to a temporary sibling path and renames atomically on close, so a reader never observes a half-written file at the target path. A caller who wants the successfully-written prefix after a failure can configure `WriteFailurePolicy.COMMIT_PREFIX`, which makes `close()` finalize whatever rows were flushed before the failure rather than discarding them.
- **The destination is a sequential sink.** `OutputFile` is `create` / `write` / `position` / `close` — no seeking, no size known ahead of time. That is what lets the same interface serve a file channel today and a multipart object upload later.

## What bounds memory

A column chunk's metadata — its compressed and uncompressed sizes, its page offsets, its statistics — is only known once the chunk's bytes have been encoded. The writer therefore encodes and buffers a whole row group's columns in memory, then writes them out in schema order and records where each landed.

A file of any size is a sequence of row groups, each buffered, flushed and forgotten, so peak memory follows whichever target cuts a row group rather than the size of the file. `rowGroupTargetRows` is usually the one that cuts, since it binds for any record narrower than about 128 bytes; `rowGroupBufferTargetBytes` takes over above that and is what keeps records wider than expected from making a row group unboundedly large.

`rowGroupBufferTargetBytes` is the bytes the writer actually retains, measured rather than approximated: the level streams a byte per entry, the dictionary indices, each column's value store, and the dictionaries themselves. A row group passes it by at most one record, since a record cannot be split across row groups. So peak heap for one writer follows the target rather than the data or the size of the file. Two things sit on top of it. The buffers hold more than they are charged for while they grow: the value stores grow by half again, and the level streams, a `BYTE_ARRAY` column's packed content and every dictionary's value array and hash table double. And a column's buffers have a floor under them, so a schema with enough columns that each one's share of the target falls below that floor opens at a multiple of it — measured, 200 columns against a 1 MiB target hold about 2.4 MB before a record arrives, while a thousand columns against the default 128 MiB target stay inside it, their shares being far above the floor. Neither scales with how much you write.

Where the **row target** cuts first — which it does for any record retaining less than about 128 bytes, at the defaults — peak heap is instead the row count times what a record retains. That is worth knowing per schema, and it follows from what each column keeps:

| A column retains, per record | |
| --- | --- |
| Each level stream it has | one byte per entry |
| A present value, while the chunk is interning | 4 bytes of index, plus a dictionary entry if the value is new |
| A present value, once the chunk has stopped interning | its width in the value store |

A `list<int32>` column whose lists are empty therefore retains two bytes a record — a definition level and a repetition level, and no value at all. A flat `INT32` column of repeating values retains about four; the same column with every value distinct retains that index plus a dictionary entry and the table slots that find it, several times more, until the size probes give the dictionary up.

Multiply by your row target, and by the number of writers running in the same JVM.

## Why the writer chooses the boundaries

Batches and records are *arrival* units. Pages, column chunks and row groups are *layout* units. The writer maps one to the other, and the caller does not see the seam: a batch's values are distributed into per-column buffers, the row group is flushed once what those hold reaches the row-group target, and its pages are cut as it is written out. A batch larger than the row group is split at the boundary.

This is why there is no explicit "end row group" call, and why submitting one large batch and streaming a thousand small ones produce the same file. The targets are the whole vocabulary for layout, and they mean different things:

- **Page size** governs read granularity: a page is the unit a reader decompresses to reach any value in it, so smaller pages prune finer and cost more metadata. It is measured in *encoded* bytes — the values at the width the chunk's encoding gives them, so a dictionary column is measured in indices — and the page is cut before the value that would cross the target. Under a delta encoding, whose width is a property of the values rather than of the type, the cut charges the width the type would have taken `PLAIN`, so those pages land under the target rather than on it.
- **Row-group rows** governs split sizing and row-group-level pruning, and defaults to 1,048,576. A row group is self-contained, so it is the boundary a file partitions on across separate readers, and it is the granularity at which a reader skips on column-chunk statistics. It does not govern how much of the read runs in parallel: Hardwood decodes *pages* concurrently within a row group, so parallelism is bounded by pages and columns rather than by banding. Unlike a byte target it needs no estimate and does not vary with the data.
- **Row-group buffer bytes** is the memory bound above: the bytes the writer holds for the open row group. It also cuts a row group, so records wider than expected cannot make one unboundedly large.

A row group is cut at whichever of the two row-group targets is reached first. Neither is the size the row group takes on disk: that is smaller by whatever the encoding and the codec win, which is a property of the data — a dictionary-encoded column reaches the file as indices, and the codec then compresses those. Measure one file and scale the setting if a particular on-disk size is what you need.

## How the writer picks an encoding

Under the default `AUTO` policy, the writer does not guess a column's encoding from its type or its first few values. It buffers the row group, and then — knowing what the chunk actually holds — compares the size of a dictionary page plus an index stream against the same values written `PLAIN`, and takes the smaller. The choice is per column chunk, so the same column may be dictionary-encoded in one row group and `PLAIN` in the next as its cardinality changes.

Two consequences are worth knowing. A chunk is encoded one way throughout — no chunk mixes a dictionary with `PLAIN` overflow pages, and no dictionary page is written for a chunk that ends up `PLAIN`. And naming an encoding explicitly opts out of the comparison entirely: that column builds no dictionary in any row group of the file.

The deciding gives up early where it can. A chunk stops interning once repeated size probes find the dictionary losing to `PLAIN`, which keeps a high-cardinality column from hashing a whole row group's values into a table that is then thrown away. Such a chunk is written `PLAIN`, and is the only one under `AUTO` that cannot state its `distinct_count`. A dictionary needs no size limit of its own: it is part of what the chunk holds, and so is counted against `rowGroupBufferTargetBytes` like everything else the writer retains.

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
