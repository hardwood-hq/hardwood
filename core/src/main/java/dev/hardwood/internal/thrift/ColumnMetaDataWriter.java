/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;

/// Writer for ColumnMetaData to Thrift Compact Protocol, the inverse of
/// [ColumnMetaDataReader]. Serializes the required fields plus the optional
/// `dictionary_page_offset` and `statistics`; index offsets are written by later
/// increments.
public class ColumnMetaDataWriter {

    public static void write(ThriftCompactWriter writer, ColumnMetaData metaData) {
        short saved = writer.pushFieldIdContext();
        try {
            // 1: type
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(metaData.type()));

            // 2: encodings (list<Encoding>)
            writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(metaData.encodings().size(), ThriftCompactConstants.ElementType.I32);
            for (Encoding encoding : metaData.encodings()) {
                writer.writeI32(ThriftEnumLookup.thriftValue(encoding));
            }

            // 3: path_in_schema (list<string>)
            writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.LIST);
            writer.writeListBegin(metaData.pathInSchema().elements().size(), ThriftCompactConstants.ElementType.BINARY);
            for (String segment : metaData.pathInSchema().elements()) {
                writer.writeString(segment);
            }

            // 4: codec
            writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.I32);
            writer.writeI32(ThriftEnumLookup.thriftValue(metaData.codec()));

            // 5: num_values
            writer.writeFieldBegin(5, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(metaData.numValues());

            // 6: total_uncompressed_size
            writer.writeFieldBegin(6, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(metaData.totalUncompressedSize());

            // 7: total_compressed_size
            writer.writeFieldBegin(7, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(metaData.totalCompressedSize());

            // 9: data_page_offset
            writer.writeFieldBegin(9, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(metaData.dataPageOffset());

            // 11: dictionary_page_offset (present only for a dictionary-encoded chunk)
            if (metaData.dictionaryPageOffset() != null) {
                writer.writeFieldBegin(11, ThriftCompactConstants.FieldType.I64);
                writer.writeI64(metaData.dictionaryPageOffset());
            }

            // 12: statistics (min/max/null_count for reader-side predicate pushdown)
            if (metaData.statistics() != null) {
                writer.writeFieldBegin(12, ThriftCompactConstants.FieldType.STRUCT);
                StatisticsWriter.write(writer, metaData.statistics());
            }

            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }
}
