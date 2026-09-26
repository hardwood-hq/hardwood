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

    /// Reads the OffsetIndex of a column chunk and checks it against the chunk's metadata.
    ///
    /// An OffsetIndex locates every data page of its chunk, so one that lists no page for a
    /// chunk with values is malformed: a read planned from it would fetch none of the chunk.
    /// The struct alone cannot tell, as `page_locations` is a list that may be empty, so the
    /// check needs the chunk's `num_values`. Every read that plans pages or filters them from
    /// an OffsetIndex parses it here.
    ///
    /// @param chunkMetaData the metadata of the chunk the index belongs to, or `null` for a
    ///        chunk whose metadata is not inline, which is then not checked
    public static OffsetIndex read(ThriftCompactReader reader, ColumnMetaData chunkMetaData) {
        OffsetIndex offsetIndex = read(reader);
        if (chunkMetaData != null && chunkMetaData.numValues() > 0 && offsetIndex.pageLocations().isEmpty()) {
            throw new ParquetReadException("Malformed Parquet metadata: OffsetIndex.page_locations"
                    + " is empty but the column chunk has " + chunkMetaData.numValues() + " values");
        }
        return offsetIndex;
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
