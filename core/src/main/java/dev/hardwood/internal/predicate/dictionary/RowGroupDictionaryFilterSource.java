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
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.DictionaryParser;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageEncodingStats;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Lazily reads and caches a row group's dictionary pages so predicate evaluation can use them to
/// prove a value absent from a column chunk.
///
/// Dictionaries are read on demand: a row group dropped by statistics or a bloom filter never pays
/// for the dictionary page I/O.
public final class RowGroupDictionaryFilterSource {

    private final InputFile inputFile;
    private final RowGroup rowGroup;
    private final FileSchema fileSchema;
    private final HardwoodContextImpl context;
    private final Dictionary[] dictionaries;
    private final boolean[] read;
    /// Where each read dictionary page ends, `0` for a column whose dictionary is not read.
    private final long[] pageEnds;

    public RowGroupDictionaryFilterSource(InputFile inputFile, RowGroup rowGroup,
                                          FileSchema fileSchema, HardwoodContextImpl context) {
        this.inputFile = inputFile;
        this.rowGroup = rowGroup;
        this.fileSchema = fileSchema;
        this.context = context;
        int columnCount = rowGroup.columns().size();
        this.dictionaries = new Dictionary[columnCount];
        this.read = new boolean[columnCount];
        this.pageEnds = new long[columnCount];
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

    /// The column's dictionary if [#forColumn] has read it, or `null` without reading anything.
    /// A fetch plan takes it from here so a dictionary pruning has read is not read again.
    public Dictionary loaded(int columnIndex) {
        return columnIndex >= 0 && columnIndex < dictionaries.length ? dictionaries[columnIndex] : null;
    }

    /// Where the column's dictionary page ends if [#forColumn] has read it — the file offset the
    /// chunk's first data page starts at — or `0`.
    public long loadedPageEnd(int columnIndex) {
        return columnIndex >= 0 && columnIndex < pageEnds.length ? pageEnds[columnIndex] : 0;
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
        requireSameFile(columnChunk);

        // A dictionary page is always the chunk's first page. Without the page index,
        // `data_page_offset` stands for the first data page; a file that omits
        // `dictionary_page_offset` points it at the dictionary page instead, which the page's own
        // header tells apart. A first data page preceding the dictionary page cannot be read at
        // all, so that fails rather than degrading to "no dictionary".
        ByteBuffer page = DictionaryParser.readPage(inputFile, columnChunk, columnPrefix(metaData));
        if (page == null) {
            return null;
        }
        ColumnSchema columnSchema = fileSchema.getColumn(columnIndex);
        long pageEnd = columnChunk.chunkStartOffset() + page.remaining();
        Dictionary dictionary = DictionaryParser.parse(page, columnSchema, metaData, context);
        pageEnds[columnIndex] = pageEnd;
        return dictionary;
    }

    /// Fails unless this chunk stores its data in the file being read.
    ///
    /// Checked, because reading a filter is a read like any other and every frame above this
    /// one says so; the cause is the [IOException] the metadata contract advertises for the
    /// split-file layout.
    private void requireSameFile(ColumnChunk columnChunk) {
        try {
            columnChunk.requireSameFile();
        }
        catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException(
                    columnPrefix(columnChunk.metaData()) + e.getMessage(), e);
        }
    }

    /// The `[file: column 'X'] ` prefix for a message about one column of this row group.
    ///
    /// A `RowGroup` does not carry its own ordinal, so the row group is the one part of the
    /// position this class cannot name.
    private String columnPrefix(ColumnMetaData metaData) {
        return ExceptionContext.readPrefix(inputFile.name(), ExceptionContext.UNKNOWN_ROW_GROUP,
                metaData.pathInSchema().toString());
    }
}
