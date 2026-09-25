# Per-File Metadata for Multi-File Readers

**Status:** Completed

## Context

A multi-file `ParquetFileReader` reads every input's footer while it scans, but a consumer that
needs whole-dataset facts — the total physical row count before allocating storage, per-file
on-disk sizes — can reach only the first file's `FileMetaData`. Getting the rest means reopening
every input and parsing footers Hardwood has already parsed.

## Public contract

- `getFileCount()` returns the number of physical inputs, in supplied order, without I/O.
- `getFileMetaData()` returns the eagerly parsed first footer.
- `getFileMetaData(int)` returns one physical input's footer; index `0` returns that same first
  `FileMetaData` instance. The accessor is indexed rather than an iterable or list of all
  footers: an index keeps the checked `IOException` on the single file access that can fail, and
  carries no suggestion that the whole set is loaded.
- Later footers are lazy. Metadata access is synchronous and may join an asynchronous load
  already started by a data reader.
- Each in-progress, successful, or failed load is retained for the parent reader's lifetime.
  Closing and reopening the parent is the retry boundary after failure and the refresh boundary
  for files changed on the underlying storage.
- Indexed metadata access after parent close fails. `getFileCount()` remains a pure property.
- Inputs must remain unchanged while the parent is open.

## Ownership and cache boundary

`ParquetFileReader` owns one `FileMetadataCache` and the input-file lifecycle. The cache keeps a
single future per physical file. Its `PreparedFile` contains only reusable file-wide state, every
component of it parsed from that file's own footer and therefore always present:

- the `InputFile`;
- parsed `FileMetaData`;
- derived `FileSchema`;
- the footer's row groups.

The reader seeds index `0` with the footer it read at open, so opening the reader and inspecting
that index never read the same footer twice. Both indexed metadata access and every
`RowGroupIterator` use this cache, so neither direction of access reparses a footer. A failed
future remains in the map, preventing an implicit retry in the same parent reader.

The reader tracks the iterators it hands to child readers, so that closing the parent tears down
an iterator whose child reader the caller never closed. An iterator drops itself from that list
when it is closed, so a reader used for many sequential single-column reads does not accumulate
the work lists of finished children. The row-reader and multi-column paths share one iterator
across sibling readers, so no individual child owns it and it stays tracked until parent close.

Closing a child row or column reader releases only iterator-local workers and caches. It neither
closes nor invalidates shared inputs. Parent close atomically prevents new cache admissions,
waits for every admitted footer load without holding the lifecycle lock, clears the cached
futures, then closes each owned input. Cache closure is idempotent and never closes inputs itself.

## Per-reader schema state

The cache deliberately does not store `FileColumnOrdinals`, touched-column validation, the
row-group subset one reader's filters admit, or any other projection/filter state — those belong
to a reader, not to a file. For every child reader, `RowGroupIterator` validates its touched columns
against each cached `FileSchema` and creates that iterator's own `FileColumnOrdinals` mapping.
The mapping is by field path and is used consistently for pruning, masks, indexes, bloom filters,
dictionaries, chunk-path checks, and fetch plans, preserving the cross-file ordering guarantees
from #906.

Consequently, `getFileMetaData(int)` can succeed for a file whose schema is incompatible with a
later projection. The incompatibility is reported when the row or column reader is planned,
because only then is the set of touched columns known.

## Concurrency and failure behavior

Reader cursors remain single-consumer objects. The cache's concurrency control exists to
coordinate Hardwood's footer prefetch with synchronous indexed access: one lifecycle boundary
serializes load admission against the start of cache closure, while atomic insertion of one
future per index makes all requesters observe the same in-progress result. Exception translation
differs by entry point: synchronous metadata access exposes the original footer `IOException`,
while iterator paths get the runtime wrapper. Parent close ignores cached load failures while
waiting so that every owned input can still be closed.
