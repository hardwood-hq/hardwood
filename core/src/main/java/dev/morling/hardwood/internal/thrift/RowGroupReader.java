/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.RowGroup;

/**
 * Reader for RowGroup from Thrift Compact Protocol.
 */
public class RowGroupReader {

    public static RowGroup read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static RowGroup readInternal(ThriftCompactReader reader) throws IOException {
        List<ColumnChunk> columns = new ArrayList<>();
        long totalByteSize = 0;
        long numRows = 0;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // columns
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            columns.add(ColumnChunkReader.read(reader));
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // total_byte_size
                    if (header.type() == 0x06) {
                        totalByteSize = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // num_rows
                    if (header.type() == 0x06) {
                        numRows = reader.readI64();
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

        return new RowGroup(columns, totalByteSize, numRows);
    }
}
