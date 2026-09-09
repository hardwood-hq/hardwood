/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.ExceptionContext.ReadContext.Region;
import dev.hardwood.internal.ReadScope;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.DictionaryParser;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Lazily reads and caches a row group's dictionary pages so predicate evaluation can use them to
/// prove a value absent from a column chunk.
///
/// Dictionaries are read on demand: a row group dropped by statistics or a bloom filter never pays
/// for the dictionary page I/O.
public final class RowGroupDictionaryFilterSource {

    /// Bytes read speculatively when the offsets give no gap to size the read — an absent
    /// `dictionary_page_offset`. Covers a page header plus the body of all but unusually large
    /// dictionaries, so the common case still resolves in a single read.
    private static final int DICTIONARY_PROBE_BYTES = 64 * 1024;

    private final InputFile inputFile;
    private final RowGroup rowGroup;
    /// The row group's own index in the file, for the failures that can name no byte.
    private final int rowGroupIndex;
    private final FileSchema fileSchema;
    private final HardwoodContextImpl context;
    private final Dictionary[] dictionaries;
    private final boolean[] read;

    public RowGroupDictionaryFilterSource(InputFile inputFile, RowGroup rowGroup, int rowGroupIndex,
                                          FileSchema fileSchema, HardwoodContextImpl context) {
        this.inputFile = inputFile;
        this.rowGroup = rowGroup;
        this.rowGroupIndex = rowGroupIndex;
        this.fileSchema = fileSchema;
        this.context = context;
        int columnCount = rowGroup.columns().size();
        this.dictionaries = new Dictionary[columnCount];
        this.read = new boolean[columnCount];
    }

    public Dictionary forColumn(int columnIndex) throws IOException {
        if (columnIndex < 0 || columnIndex >= dictionaries.length) {
            return null;
        }

        if (!read[columnIndex]) {
            dictionaries[columnIndex] = readDictionary(columnIndex);
            read[columnIndex] = true;
        }
        return dictionaries[columnIndex];
    }

    /// Whether every value in the column chunk is dictionary-encoded, so its dictionary page
    /// enumerates all of the chunk's non-null values — the precondition for proving a value absent
    /// from the chunk.
    ///
    /// True when [ColumnMetaData#encodingStats()] records a dictionary page, at least one
    /// dictionary-encoded data page, and no data page written with a non-dictionary encoding. A
    /// writer may start a chunk dictionary-encoded and fall back to plain pages once the dictionary
    /// grows too large; such a chunk reports `false`, since its dictionary covers only part of the
    /// data. Returns `false` when `encodingStats` is empty and likewise for a data page whose
    /// encoding isn't a dictionary.
    private static boolean isFullyDictionaryEncoded(ColumnMetaData metaData) {
        boolean hasDictionaryPage = false;
        boolean hasDataPage = false;

        for (PageEncodingStats stats : metaData.encodingStats()) {
            switch (stats.pageType()) {
                case DICTIONARY_PAGE -> hasDictionaryPage = true;
                case INDEX_PAGE -> { } // An index page holds no values, so it cannot contradict the dictionary.
                case UNKNOWN -> {
                    return false;
                }
                case DATA_PAGE, DATA_PAGE_V2 -> {
                    if (stats.encoding() != Encoding.PLAIN_DICTIONARY && stats.encoding() != Encoding.RLE_DICTIONARY) {
                        return false;
                    }
                    hasDataPage = true;
                }
            }
        }
        return hasDictionaryPage && hasDataPage;
    }

    private Dictionary readDictionary(int columnIndex) throws IOException {
        ColumnChunk columnChunk = rowGroup.columns().get(columnIndex);
        ColumnMetaData metaData = columnChunk.metaData();

        if (!isFullyDictionaryEncoded(metaData)) {
            return null;
        }

        // Reading this chunk's dictionary from the file being read would decode bytes belonging
        // to some other column, and prune row groups on them. Fail rather than degrade to
        // "no dictionary": the scan cannot read the chunk either.
        requireSameFile(columnChunk, columnIndex);

        try (ReadScope.Scope scope = ReadScope.file(inputFile.name())
                .rowGroup(rowGroupIndex)
                .column(columnPath(columnIndex))
                .region(Region.DICTIONARY_PAGE, columnChunk.chunkStartOffset())) {
            return readDictionaryPage(columnChunk, metaData, columnIndex);
        }
    }

    /// Locates and parses the chunk's dictionary page, inside the scope that
    /// names the file, the column and the page's own first byte. Every failure
    /// below says only what is wrong.
    private Dictionary readDictionaryPage(ColumnChunk columnChunk, ColumnMetaData metaData,
            int columnIndex) throws IOException {
        Long dictionaryOffset = metaData.dictionaryPageOffset();
        long dataPageOffset = metaData.dataPageOffset();

        // A first data page *preceding* the dictionary page cannot be read at all, so fail rather
        // than degrade to "no dictionary".
        if (dictionaryOffset != null && dataPageOffset < dictionaryOffset) {
            throw malformed("Malformed Parquet metadata: the dictionary page lies after the first"
                            + " data page at offset %d",
                            dataPageOffset);
        }

        // A dictionary page is always the chunk's first page, so one offset is both the page's
        // start and the chunk's, and `chunkStartOffset()` yields it: the declared
        // `dictionary_page_offset` verbatim when the file gives one, `data_page_offset` otherwise.
        //
        // The fallback is not a guess. `dictionary_page_offset` is optional in parquet.thrift, so
        // its absence is ordinary rather than corrupt: parquet-mr 1.12 omits it on every
        // PLAIN_DICTIONARY column of alltypes_tiny_pages.parquet in apache/parquet-testing, and
        // Trino did too before 427.
        long chunkStart = columnChunk.chunkStartOffset();
        long chunkEnd = chunkStart + metaData.totalCompressedSize();
        if (chunkEnd <= chunkStart) {
            throw malformed("Malformed Parquet metadata: the chunk holding the dictionary page"
                            + " ends at offset %d, so it holds no bytes at all", chunkEnd);
        }
        int availableBytes = Math.toIntExact(chunkEnd - chunkStart);

        ColumnSchema columnSchema = fileSchema.getColumn(columnIndex);
        try {
            ByteBuffer region = readDictionaryPage(columnIndex, chunkStart,
                    dataPageOffset > chunkStart
                            ? Math.toIntExact(dataPageOffset - chunkStart)
                            : DICTIONARY_PROBE_BYTES,
                    availableBytes);
            return region == null ? null : DictionaryParser.parse(region, columnSchema, metaData, context);
        }
        catch (IOException e) {
            throw new IOException(ReadScope.here() + "Failed to read the dictionary", e);
        }
    }

    /// What is wrong with the file. Where it is wrong is the scope's to say.
    private static ParquetReadException malformed(String message, Object... args) {
        return new ParquetReadException(String.format(message, args));
    }

    private FieldPath columnPath(int columnIndex) {
        return rowGroup.columns().get(columnIndex).metaData().pathInSchema();
    }

    /// Reads the bytes of the dictionary page beginning at `dictionaryStart`, or `null` when no
    /// dictionary page is there.
    ///
    /// The page's own header states its length; the offsets only size the opening read — the gap to
    /// the first data page where there is one, a bounded probe otherwise. For a well-formed chunk
    /// the gap equals the page's length, so the opening read is exact and no second one happens.
    ///
    /// A header-declared length is file data, so it is checked against the enclosing column chunk
    /// before being used to size a read. A page that claims to run past its own chunk is corrupt
    /// and is rejected here — truncating the read to the chunk instead would fail later and less
    /// clearly.
    private ByteBuffer readDictionaryPage(int columnIndex, long dictionaryStart, int gapBytes,
            int availableBytes) throws IOException {
        ByteBuffer region = inputFile.readRange(dictionaryStart, Math.min(gapBytes, availableBytes));
        int pageLength = DictionaryParser.pageLength(region);
        if (pageLength < 0) {
            return null;
        }

        if (pageLength > availableBytes) {
            throw malformed("Malformed Parquet metadata: the dictionary page header declares %d"
                            + " bytes but only %d remain in the chunk", pageLength, availableBytes);
        }

        return pageLength > region.remaining()
                ? inputFile.readRange(dictionaryStart, pageLength)
                : region;
    }


    /// Fails unless this chunk stores its data in the file being read.
    ///
    /// The split-file layout is legal Parquet that this reader does not read, so the failure
    /// leaves as the [UnsupportedOperationException] it arrived as, carrying the column it
    /// was raised for.
    private void requireSameFile(ColumnChunk columnChunk, int columnIndex) {
        try {
            columnChunk.requireSameFile();
        }
        catch (UnsupportedOperationException e) {
            // Named, not positioned. The file is correct, so there is nothing at
            // any byte for a caller to go and look at, and the remedy is the
            // same whichever column met it first.
            throw new UnsupportedOperationException(ExceptionContext.filePrefix(inputFile.name())
                    + "Cannot read column " + columnIndex + ": " + e.getMessage(), e);
        }
    }
}
