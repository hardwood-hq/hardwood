<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Size the Heap for a Read

A read holds its working set in the heap: the decoded columns it is producing, plus the
buffers of the page being decoded. This guide says what that working set is, how to estimate
it for a file, and how to check a heap against it.

## What a read holds at once

Per projected column, the read holds:

- its dictionary's entries, and
- its largest page's decoded batches: one value width per value, plus the page's bytes for
  a binary column.

On top of that, counted once for the whole read, are the buffers of the largest decode task:
the page's decompressed bytes, its level arrays, and the scratch its encoding decodes
through. The working set is the sum of those parts.

Three properties of that sum matter when sizing a heap:

- **Nothing is released as the read advances.** A column stays charged for the largest page
  it has met, even after the read has moved past it.
- **The limit is per read.** By default it is the maximum heap (`-Xmx`); every read has its
  own. Reads running at once add up, and nothing caps their sum today. A heap quota shared
  by the reads of one context is planned for 1.2 (see the
  [roadmap](../index.md#roadmap)).
- **A read that would exceed the limit fails before allocating.** It throws
  `ParquetReadException` naming the bytes the page needs, the working set total, and the
  limit; see [Error Handling](../reference/error-handling.md). A read whose working set
  sits near or above the heap can still run out of memory instead.

## How large that is in practice

Measured on the default context (8 threads, every column read with `ColumnReaders`):

| File | Largest uncompressed page + dictionary, summed | Largest decoded page + dictionary, summed | Smallest heap that reads it |
| --- | --- | --- | --- |
| taxi 2016 (19 columns, 1 row group) | 12 MiB | 335 MiB | 412 MiB |
| taxi 2025 (20 columns, 4 row groups) | 12 MiB | 130 MiB | 205 MiB |
| 80 columns, one page per chunk (PyArrow) | 546 MiB | 851 MiB | 875 MiB |

The smallest heap sits 24-77 MiB above the decoded estimate on these three files. That gap
is the read's fixed overhead; it is not a rule to budget by, so allow headroom above the
estimate rather than sizing to it exactly.

Note the first column: the stored sizes understate the decoded sizes by an order of
magnitude. A dictionary-encoded column stores a small page of ids and reads back the full
values, so a file that looks small on disk can need a heap many times its size.

## Estimate it for a file

For each projected column, take its largest page and its dictionary:

- **Largest decoded page:** the page's value count times the value width below, plus the
  page's bytes for a binary column.
- **Dictionary:** its uncompressed size, plus 4 bytes per entry for a binary column.

| Physical type | Value width |
| --- | --- |
| `BOOLEAN` | 1 byte |
| `INT32`, `FLOAT` | 4 bytes |
| `INT64`, `DOUBLE` | 8 bytes |
| `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96` | 16 bytes (the value's view and its dictionary id) |

Add the columns up, then add the largest decode task's buffers (one page's decompressed
bytes plus its level and scratch arrays, counted once) and headroom for the fixed overhead.

`hardwood inspect pages` lists each page's value count, which is what the estimate needs.
Do not use the size it shows as the decoded size: that is the page's stored (compressed)
size, and for a dictionary-encoded column it understates the decoded values by an order of
magnitude.

## Levers when the heap is short

- **A narrower projection.** Reading fewer columns shrinks the sum directly: each column
  dropped removes its dictionary and its largest page. See
  [Filter, Project, Limit, and Split](query-controls.md).
- **Fewer reads at once.** The limit is per read, so two reads in parallel need roughly
  twice the heap of one.
- **A larger heap.** Raise `-Xmx` to at least the estimate plus headroom.

## Check a heap against a file

Read the widest projection the application uses, at the target `-Xmx`:

```java
try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path));
     ColumnReaders columns = reader.columnReaders(projection)) {
    while (columns.nextBatch()) {
        // consume the batch
    }
}
```

If the heap is short, the read fails with a `ParquetReadException` like this, naming the
bytes the page needs and the limit it would exceed:

```text
Data page needs 412000000 bytes to decode, which takes the read's working set to
900000000 bytes, more than its limit of 800000000 (hardwood.internal.maxWorkingSetBytes)
```

Take the "needs" figure as the size of the largest page the read met, add the other
columns' dictionaries and largest pages, and size the heap from that sum. The property
named in the message, `hardwood.internal.maxWorkingSetBytes`, is an internal override for
the per-read limit; the supported way to give a read more room is a larger `-Xmx`.
