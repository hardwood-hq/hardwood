<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# The Write Model

Reading a Parquet file is random access over bytes that already exist: the footer says where everything is, and the reader jumps to the parts it wants. Writing inverts that. The output size is unknown until the file is finished, the container is laid out for forward-only production, and every offset a reader will need is only known once the bytes it describes have been written.

## Forward-only, footer-last

A Parquet file is written front to back and never seeked backward:

```
PAR1 | <row group 0 pages> | <row group 1 pages> | … | <Bloom filters> | <page index> | FileMetaData | <footer length> | PAR1
```

The `FileMetaData` footer carries the schema and every page and column-chunk offset, and it can only be serialized once those offsets are known, so it goes last. The writer maintains a running byte position, records offsets as it streams pages out, and emits the accumulated metadata at the end. Each page's bounds and location go into the page index, which is written for the whole file just before the footer, after the Bloom filters of the columns configured to carry one.

- **Publication follows completion.** The footer is written last. A successful close confirms that the destination published the finished file. A lost remote publication response can leave the client uncertain even though the completed file exists; no incomplete prefix is published as a valid file.
- **Incomplete files are discarded.** When the writer cannot finish, it discards unpublished data; see [Handle Write Failures](../how-to/write-failures.md). `RowWriter.tryWriteRow` rejects a record without failing the writer. The local backend writes to a temporary sibling path and renames atomically on close, so a reader never observes a half-written file at the target path. For a remote destination, losing a response can leave publication or cleanup uncertain; the destination's contract determines what can be confirmed.
- **The destination is a sequential sink.** `OutputFile` is `create` / `write` / `position` / `close` / `discard`, with no seeking and no size known ahead of time. `close()` publishes the file and `discard()` abandons unpublished data; the writer calls exactly one of them.

## What bounds memory

A column chunk's metadata (its compressed and uncompressed sizes, its page offsets, its statistics) is only known once the chunk's bytes have been encoded. The writer therefore encodes and buffers a whole row group's columns in memory, then writes them out in schema order and records where each landed.

A file of any size is a sequence of row groups, each buffered, flushed and forgotten, so peak memory follows whichever target cuts a row group, whatever the size of the file, apart from the page index and Bloom filters described below. `rowGroupTargetRows` is usually the one that cuts, since it binds for any record narrower than about 128 bytes; `rowGroupBufferTargetBytes` takes over above that and is what keeps records wider than expected from making a row group unboundedly large.

A row group passes `rowGroupBufferTargetBytes` by at most one record, since a record cannot be split across row groups.

Two overheads sit on top of the target, and neither scales with how much you write:

- **Growth headroom.** The buffers hold more than they are charged for while they grow: the value stores grow by half again, and the level streams, a `BYTE_ARRAY` column's packed content and every dictionary's value array and hash table double.
- **A per-column floor.** A column's buffers have a floor under them, so a schema with enough columns that each one's share of the target falls below that floor opens at a multiple of it. Measured, 200 columns against a 1 MiB target hold about 2.4 MB before a record arrives, while a thousand columns against the default 128 MiB target stay inside it, their shares being far above the floor.

The page index is held until `close()` writes it, so it grows with the file: per data page, a location and two bounds. A bound is the type's width, or for a `BYTE_ARRAY` column usually at most `statisticsTruncationLength` bytes; a page holds at most `pageTargetRows` records and fewer where its bytes reach `pageTargetBytes` first. Bloom filters are held until `close()` too: per row group, each configured column's filter, about 1.2 bytes per distinct value of the chunk at the default false-positive probability, rounded up to a power of two. While a filter is built, a chunk that wrote no dictionary briefly holds up to about 2.4 bytes per non-null value for it.

Where the **row target** cuts first, peak heap is instead the row count times what a record retains, which follows from what each column keeps:

| A column retains, per record | |
| --- | --- |
| Each level stream it has | one byte per entry |
| A present value, while the chunk is interning | 4 bytes of index, plus a dictionary entry if the value is new |
| A present value, once the chunk has stopped interning | its width in the value store |

A `list<int32>` column whose lists are empty therefore retains two bytes a record: a definition level and a repetition level, and no value at all. A flat `INT32` column of repeating values retains about four; the same column with every value distinct retains that index plus a dictionary entry and the table slots that find it, several times more, until the size probes give the dictionary up.

Multiply by your row target, and by the number of writers running in the same JVM.

## Where the boundaries fall

Batches and records are *arrival* units. Pages, column chunks and row groups are *layout* units. The writer maps one to the other, and the caller does not see the seam: a batch's values are distributed into per-column buffers, the row group is flushed once what those hold reaches the row-group target, and its pages are cut as it is written out. A batch larger than the row group is split at the boundary.

Submitting one large batch and streaming a thousand small ones therefore produce the same file. Layout is set by the targets, and each governs something different:

- **Page size** governs read granularity: a page is the unit a reader decompresses to reach any value in it, so smaller pages prune finer and cost more metadata. A page also holds at most `pageTargetRows` records, so a column whose values encode small still has pages for the page index to prune by. Under a delta encoding, whose width depends on the values, the cut charges the width the type would have taken `PLAIN`, so those pages land below the target.
- **Row-group rows** governs split sizing and row-group-level pruning, and defaults to 1,048,576. A row group is self-contained, so it is the boundary a file partitions on across separate readers, and it is the granularity at which a reader skips on column-chunk statistics. It does not govern how much of the read runs in parallel: Hardwood decodes *pages* concurrently within a row group, so parallelism is bounded by pages and columns rather than by banding. Unlike a byte target it needs no estimate and does not vary with the data.
- **Row-group buffer bytes** is the memory bound above: the bytes the writer holds for the open row group.

A row group holds at most `Integer.MAX_VALUE - 9` records whatever the targets say. It is cut earlier where a repeated column's chunk would reach its structural ceiling of `Integer.MAX_VALUE - 8` values and nulls, or where any column chunk would pass `Integer.MAX_VALUE - 8` bytes of `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` values. These ceilings only cut under a `rowGroupBufferTargetBytes` above 2 GiB, where they yield more row groups rather than a failed write; a single record that passes one on its own is rejected. Neither is the size the row group takes on disk: that is smaller by whatever the encoding and the codec win, which is a property of the data. A dictionary-encoded column reaches the file as indices, and the codec then compresses those. Measure one file and scale the setting if a particular on-disk size is what you need.

### Boundaries the caller places

The targets cut on counts, so a row group's edges fall wherever the counts run out, regardless of what the records hold. `endRowGroup()` lets the caller cut where the data itself changes, which is what row-group pruning and splitting key on:

- **At a key change in sorted data.** A target cut mid-key leaves that key in two row groups. Cutting where the key changes gives each row group its own key values, so an equality on the key reads exactly the row groups holding that key.
- **At a partition value change.** Each row group then holds one value of the partition column: a filter on it skips every other row group by its statistics, and a reader that splits the file by row group processes one partition per split.
- **At an upstream unit.** Ending a row group at the end of a source file, a message batch or a checkpoint keeps each unit in its own row groups, so a consumer splitting by row group never sees two units mixed in one split.

The targets keep cutting as well, and a row group ends at whichever comes first.

## How the writer picks an encoding

Under the default `AUTO` policy, the writer buffers the row group and then, knowing what the chunk holds, compares the size of a dictionary page plus an index stream against the same values written `PLAIN`, and takes the smaller. The choice is per column chunk, so the same column may be dictionary-encoded in one row group and `PLAIN` in the next as its cardinality changes.

A chunk is encoded one way throughout: no chunk mixes a dictionary with `PLAIN` overflow pages, and no dictionary page is written for a chunk that ends up `PLAIN`. And naming an encoding explicitly opts out of the comparison entirely: that column builds no dictionary in any row group of the file.

The deciding gives up early where it can. A chunk stops interning once repeated size probes find the dictionary losing to `PLAIN`, which keeps a high-cardinality column from hashing a whole row group's values into a table that is then thrown away. A chunk also gives its dictionary up once it reaches 805,306,368 entries, the most its hash table can index. Either chunk is written `PLAIN`, and these are the only ones under `AUTO` that cannot state their `distinct_count`. A dictionary is otherwise limited only by what the chunk holds, which is counted against `rowGroupBufferTargetBytes` like everything else the writer retains; a dictionary whose page body would pass `Integer.MAX_VALUE - 8` bytes is not chosen.

## Further reading

- [Write Row by Row](../how-to/write-row-by-row.md) and [Write Column by Column](../how-to/write-column-by-column.md) — the two write APIs.
- [Writer Reference](../reference/writer.md) — options, encodings, codecs, and what the writer rejects.
- [How a Parquet File Is Laid Out](parquet-layout.md) — the container the writer produces.
- [The Layer Model](nested-columns.md) — the per-layer validity and offsets the columnar API takes for nested columns.
