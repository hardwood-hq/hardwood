/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.SizeStatistics;

/// Writer for the Thrift SizeStatistics struct, the inverse of [SizeStatisticsReader]. Writes
/// each of its three optional fields where the [SizeStatistics] carries it.
public class SizeStatisticsWriter {

    public static void write(ThriftCompactWriter writer, SizeStatistics statistics) {
        short saved = writer.pushFieldIdContext();
        try {
            // 1: unencoded_byte_array_data_bytes (optional i64)
            if (statistics.unencodedByteArrayDataBytes() != null) {
                writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I64);
                writer.writeI64(statistics.unencodedByteArrayDataBytes());
            }

            // 2: repetition_level_histogram, 3: definition_level_histogram (optional list<i64>)
            if (statistics.repetitionLevelHistogram() != null) {
                writeLongs(writer, 2, statistics.repetitionLevelHistogram());
            }
            if (statistics.definitionLevelHistogram() != null) {
                writeLongs(writer, 3, statistics.definitionLevelHistogram());
            }

            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }

    private static void writeLongs(ThriftCompactWriter writer, int fieldId, long[] values) {
        writer.writeFieldBegin(fieldId, ThriftCompactConstants.FieldType.LIST);
        writer.writeListBegin(values.length, ThriftCompactConstants.ElementType.I64);
        for (long value : values) {
            writer.writeI64(value);
        }
    }
}
