/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.Statistics;

final class StatisticsWriter {
    private StatisticsWriter() {}

    static void write(ThriftCompactWriter writer, Statistics statistics) {
        short saved = writer.pushFieldIdContext();
        int minField = statistics.isMinMaxDeprecated() ? 2 : 6;
        int maxField = statistics.isMinMaxDeprecated() ? 1 : 5;
        if (statistics.maxValue() != null && maxField == 1) {
            writeBinary(writer, 1, statistics.maxValue());
        }
        if (statistics.minValue() != null && minField == 2) {
            writeBinary(writer, 2, statistics.minValue());
        }
        if (statistics.nullCount() != null) {
            writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(statistics.nullCount());
        }
        if (statistics.distinctCount() != null) {
            writer.writeFieldBegin(4, ThriftCompactConstants.FieldType.I64);
            writer.writeI64(statistics.distinctCount());
        }
        if (statistics.maxValue() != null && maxField == 5) {
            writeBinary(writer, 5, statistics.maxValue());
        }
        if (statistics.minValue() != null && minField == 6) {
            writeBinary(writer, 6, statistics.minValue());
        }
        writer.writeFieldStop();
        writer.popFieldIdContext(saved);
    }

    private static void writeBinary(ThriftCompactWriter writer, int field, byte[] value) {
        writer.writeFieldBegin(field, ThriftCompactConstants.FieldType.BINARY);
        writer.writeBinary(value);
    }
}
