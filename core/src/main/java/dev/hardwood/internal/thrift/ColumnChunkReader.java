/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.reader.ParquetReadException;

/// Reader for ColumnChunk from Thrift Compact Protocol.
public class ColumnChunkReader {

    public static ColumnChunk read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.COLUMN_CHUNK);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnChunk readInternal(ThriftCompactReader reader) {
        ColumnMetaData metaData = null;
        Long offsetIndexOffset = null;
        Integer offsetIndexLength = null;
        Long columnIndexOffset = null;
        Integer columnIndexLength = null;
        // Absent means this file, and so does the empty string the spec allows for it.
        String filePath = "";
        boolean fileOffsetSeen = false;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // file_path (optional string - deprecated)
                    if (reader.acceptField(header, Codes.BINARY)) {
                        filePath = reader.readString();
                    }
                    break;
                case 2: // file_offset (required i64, deprecated, so its value goes unused)
                    reader.requireField(header, Codes.I64);
                    reader.readI64();
                    fileOffsetSeen = true;
                    break;
                case 3: // meta_data (optional)
                    if (reader.acceptField(header, Codes.STRUCT)) {
                        metaData = ColumnMetaDataReader.read(reader);
                    }
                    break;
                case 4: // offset_index_offset (optional i64)
                    if (reader.acceptField(header, Codes.I64)) {
                        offsetIndexOffset = reader.readNonNegativeI64();
                    }
                    break;
                case 5: // offset_index_length (optional i32)
                    if (reader.acceptField(header, Codes.I32)) {
                        offsetIndexLength = reader.readNonNegativeI32();
                    }
                    break;
                case 6: // column_index_offset (optional i64)
                    if (reader.acceptField(header, Codes.I64)) {
                        columnIndexOffset = reader.readNonNegativeI64();
                    }
                    break;
                case 7: // column_index_length (optional i32)
                    if (reader.acceptField(header, Codes.I32)) {
                        columnIndexLength = reader.readNonNegativeI32();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        if (!fileOffsetSeen) {
            throw ThriftCompactReader.missingFields(ThriftStruct.COLUMN_CHUNK, 2);
        }
        requireBothOrNeither(offsetIndexOffset, offsetIndexLength, 4, 5);
        requireBothOrNeither(columnIndexOffset, columnIndexLength, 6, 7);

        return new ColumnChunk(metaData, offsetIndexOffset, offsetIndexLength, columnIndexOffset,
                columnIndexLength, filePath);
    }

    /// Fails when a page-index structure is located by only one of its two fields: an offset
    /// without a length, or a length without an offset, locates no bytes to read.
    private static void requireBothOrNeither(Long offset, Integer length, int offsetFieldId,
            int lengthFieldId) {
        if ((offset == null) != (length == null)) {
            ThriftStruct struct = ThriftStruct.COLUMN_CHUNK;
            throw new ParquetReadException(struct.structName() + " sets "
                    + struct.fieldName(offset == null ? lengthFieldId : offsetFieldId) + " without "
                    + struct.fieldName(offset == null ? offsetFieldId : lengthFieldId));
        }
    }
}
