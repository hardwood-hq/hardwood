/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Collections;
import java.util.List;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.reader.ParquetReadException;

/// Reader for OffsetIndex from Thrift Compact Protocol.
///
/// Parquet OffsetIndex struct fields:
///
/// - 1: page_locations (list<PageLocation>)
/// - 2: unencoded_byte_array_data_bytes (list<i64>, optional)
public class OffsetIndexReader {

    public static OffsetIndex read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.OFFSET_INDEX);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    /// Reads the OffsetIndex of a column chunk and checks it against the chunk ([#check]).
    ///
    /// @param chunk the chunk the index belongs to
    /// @param rowGroupRows the `num_rows` of the chunk's row group
    /// @throws ParquetReadException if the index contradicts its chunk or row group
    public static OffsetIndex read(ThriftCompactReader reader, ColumnChunk chunk, long rowGroupRows) {
        OffsetIndex offsetIndex = read(reader);
        check(offsetIndex, chunk, rowGroupRows);
        return offsetIndex;
    }

    /// Checks an OffsetIndex against the chunk it belongs to.
    ///
    /// An OffsetIndex locates every data page of its chunk, so one that lists no page for a
    /// chunk with values is malformed: a read planned from it would fetch none of the chunk.
    /// The struct alone cannot tell, as `page_locations` is a list that may be empty, so the
    /// check needs the chunk's `num_values`. Each page holds bytes, starts no earlier than the
    /// previous page ends and ends within the chunk, so the chunk's bytes bound the number of
    /// pages its index may list; and the first page starts at row 0 and each later page no
    /// earlier than the previous page's row and no later than the row group's `rowGroupRows`, so
    /// no page spans a negative count of rows. A page that begins inside a record may start at
    /// the row the previous page starts at, and the last page at the row group's end. Every read
    /// that plans pages or filters them from an OffsetIndex parses it through
    /// [#read(ThriftCompactReader, ColumnChunk, long)]; a tool showing an index as the file
    /// stores it reads it unchecked and calls this to flag what the read would refuse.
    ///
    /// @param chunk the chunk the index belongs to; one whose metadata is not inline is not
    ///        checked
    /// @param rowGroupRows the `num_rows` of the chunk's row group
    /// @throws ParquetReadException if the index breaks any of these
    public static void check(OffsetIndex offsetIndex, ColumnChunk chunk, long rowGroupRows) {
        ColumnMetaData chunkMetaData = chunk.metaData();
        if (chunkMetaData == null) {
            return;
        }
        if (chunkMetaData.numValues() > 0 && offsetIndex.pageLocations().isEmpty()) {
            throw new ParquetReadException("Malformed Parquet metadata: OffsetIndex.page_locations"
                    + " is empty but the column chunk has " + chunkMetaData.numValues() + " values");
        }
        long chunkStart = chunk.chunkStartOffset();
        if (chunkStart > Long.MAX_VALUE - chunkMetaData.totalCompressedSize()) {
            throw new ParquetReadException("Malformed Parquet metadata: column chunk at offset " + chunkStart
                    + " of " + chunkMetaData.totalCompressedSize() + " bytes ends past the largest file offset");
        }
        long chunkEnd = chunkStart + chunkMetaData.totalCompressedSize();
        List<PageLocation> pages = offsetIndex.pageLocations();
        for (int i = 0; i < pages.size(); i++) {
            checkBytes(pages, i, chunkStart, chunkEnd);
            checkFirstRow(pages, i, rowGroupRows);
        }
    }

    /// Checks that page `i` holds bytes, past the previous page's and within the chunk.
    private static void checkBytes(List<PageLocation> pages, int i, long chunkStart, long chunkEnd) {
        PageLocation page = pages.get(i);
        if (page.compressedPageSize() == 0) {
            throw malformed(i, "has a compressed_page_size of 0");
        }
        if (i == 0 && page.offset() < chunkStart) {
            throw malformed(i, "starts at offset " + page.offset() + ", before its column chunk starts at offset "
                    + chunkStart);
        }
        if (i > 0) {
            long previousEnd = end(pages.get(i - 1));
            if (page.offset() < previousEnd) {
                throw malformed(i, "starts at offset " + page.offset() + ", before page " + (i - 1)
                        + " ends at offset " + previousEnd);
            }
        }
        // compared by subtraction, as a declared offset near Long.MAX_VALUE would wrap its end
        if (page.offset() > chunkEnd - page.compressedPageSize()) {
            throw malformed(i, "of " + page.compressedPageSize() + " bytes at offset " + page.offset()
                    + " ends past the end of its column chunk at offset " + chunkEnd);
        }
    }

    /// Checks that page `i` starts at row 0 if it is the first, and otherwise no earlier than the
    /// previous page's row, and no later than `rowGroupRows`.
    private static void checkFirstRow(List<PageLocation> pages, int i, long rowGroupRows) {
        long firstRow = pages.get(i).firstRowIndex();
        if (i == 0 && firstRow != 0) {
            throw malformed(i, "has a first_row_index of " + firstRow + ", not 0");
        }
        if (i > 0 && firstRow < pages.get(i - 1).firstRowIndex()) {
            throw malformed(i, "has a first_row_index of " + firstRow + ", before page " + (i - 1) + "'s "
                    + pages.get(i - 1).firstRowIndex());
        }
        if (firstRow > rowGroupRows) {
            throw malformed(i, "has a first_row_index of " + firstRow + ", past the row group's "
                    + rowGroupRows + " rows");
        }
    }

    private static long end(PageLocation page) {
        return page.offset() + page.compressedPageSize();
    }

    private static ParquetReadException malformed(int page, String problem) {
        return new ParquetReadException("Malformed Parquet metadata: OffsetIndex page " + page + " " + problem);
    }

    private static OffsetIndex readInternal(ThriftCompactReader reader) {
        List<PageLocation> pageLocations = Collections.emptyList();
        long[] unencodedByteArrayDataBytes = null;
        boolean pageLocationsSeen = false;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // page_locations (required list<PageLocation>) — so a wrong wire type fails here
                    reader.requireField(header, Codes.LIST);
                    pageLocations = reader.readStructList(PageLocationReader::read);
                    pageLocationsSeen = true;
                    break;
                case 2: // unencoded_byte_array_data_bytes (list<i64>, optional)
                    if (reader.acceptField(header, Codes.LIST)) {
                        unencodedByteArrayDataBytes = reader.readOptionalI64Array();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        if (!pageLocationsSeen) {
            throw ThriftCompactReader.missingFields(ThriftStruct.OFFSET_INDEX, 1);
        }

        return new OffsetIndex(pageLocations, unencodedByteArrayDataBytes);
    }
}
