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

import dev.morling.hardwood.metadata.ColumnMetaData;
import dev.morling.hardwood.metadata.CompressionCodec;
import dev.morling.hardwood.metadata.Encoding;
import dev.morling.hardwood.metadata.PhysicalType;

/**
 * Reader for ColumnMetaData from Thrift Compact Protocol.
 */
public class ColumnMetaDataReader {

    public static ColumnMetaData read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnMetaData readInternal(ThriftCompactReader reader) throws IOException {
        PhysicalType type = null;
        List<Encoding> encodings = new ArrayList<>();
        List<String> pathInSchema = new ArrayList<>();
        CompressionCodec codec = null;
        long numValues = 0;
        long totalUncompressedSize = 0;
        long totalCompressedSize = 0;
        long dataPageOffset = 0;
        Long dictionaryPageOffset = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // type
                    if (header.type() == 0x05) {
                        type = PhysicalType.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // encodings
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            encodings.add(Encoding.fromThriftValue(reader.readI32()));
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // path_in_schema
                    if (header.type() == 0x09) {
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            pathInSchema.add(reader.readString());
                        }
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // codec
                    if (header.type() == 0x05) {
                        codec = CompressionCodec.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 5: // num_values
                    if (header.type() == 0x06) {
                        numValues = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 6: // total_uncompressed_size
                    if (header.type() == 0x06) {
                        totalUncompressedSize = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 7: // total_compressed_size
                    if (header.type() == 0x06) {
                        totalCompressedSize = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 9: // data_page_offset
                    if (header.type() == 0x06) {
                        dataPageOffset = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 10: // index_page_offset (optional) - skipped for now
                    reader.skipField(header.type());
                    break;
                case 11: // dictionary_page_offset (optional)
                    if (header.type() == 0x06) {
                        dictionaryPageOffset = reader.readI64();
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

        return new ColumnMetaData(type, encodings, pathInSchema, codec, numValues,
                totalUncompressedSize, totalCompressedSize, dataPageOffset, dictionaryPageOffset);
    }
}
