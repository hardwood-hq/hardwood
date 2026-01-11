/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.thrift;

import java.io.IOException;

import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.ColumnMetaData;

/**
 * Reader for ColumnChunk from Thrift Compact Protocol.
 */
public class ColumnChunkReader {

    public static ColumnChunk read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnChunk readInternal(ThriftCompactReader reader) throws IOException {
        ColumnMetaData metaData = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // file_path (optional string - deprecated)
                    reader.skipField(header.type());
                    break;
                case 2: // file_offset (required i64)
                    reader.skipField(header.type());
                    break;
                case 3: // meta_data (required)
                    if (header.type() == 0x0C) { // STRUCT
                        metaData = ColumnMetaDataReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        return new ColumnChunk(metaData);
    }
}
