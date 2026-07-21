# Parquet File Stitching

**Status: completed**

## Purpose

The `hardwood merge` command combines same-schema Parquet files without decoding,
re-encoding, or recompressing their pages. Each input row group remains unchanged.

## File assembly

For every input, the stitcher copies the complete file body between the leading
`PAR1` magic and the footer. Copying the whole body preserves column chunks, page
indexes, bloom filters, padding, and their relative layout. One relocation delta,
the destination body offset minus the source body offset, is applied to every
absolute offset from that input's footer.

The output consists of one leading magic, the copied input bodies in argument
order, and a newly serialized footer. Its row-group list is the concatenation of
the input row-group lists and its row count is their checked sum.

## Validation

All inputs are opened and validated before the output is created. Inputs must have
identical schema elements and column orders. File-level key/value metadata must be
identical because choosing one input's conflicting value would silently change the
meaning of another input. Encrypted files are rejected by the existing metadata
reader.

Offsets and row counts use checked arithmetic. A failure discards the temporary
output through `OutputFile`, so a partial Parquet file is never published.

## Metadata

The rebuilt footer preserves file and column key/value metadata, statistics,
column orders, page-index locations, and bloom-filter locations. Page encodings
and compression codecs may differ between row groups because Parquet describes
them per column chunk.

`ColumnChunk.file_offset` is written as zero. That field is deprecated and zero is
the specified value when column metadata exists only in the footer. Row-group
`file_offset`, when present, is relocated with the rest of the body pointers.

The output `created_by` value identifies Hardwood's stitcher. Source `created_by`
values are descriptive rather than semantic and are not combined.

## Scope

The first command accepts local input and output paths. It does not repack row
groups: changing row-group boundaries requires interpreting page-level records and
is incompatible with byte-for-byte page reuse.
