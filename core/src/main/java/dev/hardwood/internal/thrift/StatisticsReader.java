/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.Statistics;

/**
 * Reader for Statistics from Thrift Compact Protocol.
 *
 * <p>Parquet statistics struct fields:</p>
 * <ul>
 *   <li>1: max (binary) — deprecated, use max_value</li>
 *   <li>2: min (binary) — deprecated, use min_value</li>
 *   <li>3: null_count (i64)</li>
 *   <li>4: distinct_count (i64)</li>
 *   <li>5: max_value (binary) — correct order per PARQUET-1025</li>
 *   <li>6: min_value (binary) — correct order per PARQUET-1025</li>
 * </ul>
 */
public class StatisticsReader {

    public static Statistics read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static Statistics readInternal(ThriftCompactReader reader) throws IOException {
        byte[] legacyMax = null;
        byte[] legacyMin = null;
        long nullCount = -1;
        long distinctCount = -1;
        byte[] maxValue = null;
        byte[] minValue = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // max (deprecated)
                    if (header.type() == 0x08) { // BINARY
                        legacyMax = reader.readBinary();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // min (deprecated)
                    if (header.type() == 0x08) { // BINARY
                        legacyMin = reader.readBinary();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // null_count
                    if (header.type() == 0x06) { // I64
                        nullCount = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // distinct_count
                    if (header.type() == 0x06) { // I64
                        distinctCount = reader.readI64();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 5: // max_value
                    if (header.type() == 0x08) { // BINARY
                        maxValue = reader.readBinary();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 6: // min_value
                    if (header.type() == 0x08) { // BINARY
                        minValue = reader.readBinary();
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

        // Prefer PARQUET-1025 min_value/max_value over legacy min/max
        boolean deprecated = (minValue == null && maxValue == null);
        byte[] finalMin = (minValue != null) ? minValue : legacyMin;
        byte[] finalMax = (maxValue != null) ? maxValue : legacyMax;

        return new Statistics(finalMin, finalMax, nullCount, distinctCount, deprecated);
    }
}
