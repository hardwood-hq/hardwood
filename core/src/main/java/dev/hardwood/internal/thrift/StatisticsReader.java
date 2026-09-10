/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.Statistics;

/// Reader for the Thrift Statistics struct from Parquet metadata.
///
/// Prefers fields 5/6 (`max_value`/`min_value` with correct sort order)
/// over deprecated fields 1/2 (`max`/`min`).
public class StatisticsReader {

    public static Statistics read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.STATISTICS);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static Statistics readInternal(ThriftCompactReader reader) {
        byte[] deprecatedMax = null;
        byte[] deprecatedMin = null;
        Long nullCount = null;
        Long distinctCount = null;
        byte[] maxValue = null;
        byte[] minValue = null;
        // is_max_value_exact / is_min_value_exact default to true when absent (parquet.thrift).
        boolean maxValueExact = true;
        boolean minValueExact = true;
        Long nanCount = null;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1: // max (deprecated)
                    if (reader.acceptField(header, Codes.BINARY)) {
                        deprecatedMax = reader.readBinary();
                    }
                    break;
                case 2: // min (deprecated)
                    if (reader.acceptField(header, Codes.BINARY)) {
                        deprecatedMin = reader.readBinary();
                    }
                    break;
                case 3: // null_count
                    if (reader.acceptField(header, Codes.I64)) {
                        nullCount = reader.readI64();
                    }
                    break;
                case 4: // distinct_count
                    if (reader.acceptField(header, Codes.I64)) {
                        distinctCount = reader.readI64();
                    }
                    break;
                case 5: // max_value (preferred)
                    if (reader.acceptField(header, Codes.BINARY)) {
                        maxValue = reader.readBinary();
                    }
                    break;
                case 6: // min_value (preferred)
                    if (reader.acceptField(header, Codes.BINARY)) {
                        minValue = reader.readBinary();
                    }
                    break;
                case 7: // is_max_value_exact
                    maxValueExact = reader.readBooleanField(header, maxValueExact);
                    break;
                case 8: // is_min_value_exact
                    minValueExact = reader.readBooleanField(header, minValueExact);
                    break;
                case 9: // nan_count (optional)
                    if (reader.acceptField(header, Codes.I64)) {
                        nanCount = reader.readI64();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        // Prefer fields 5/6 over deprecated 1/2. A struct carrying neither pair — a column
        // that wrote a null count and no bounds — took its bounds from nowhere, so it is not
        // reporting deprecated ones; saying otherwise labels an ordinary file's statistics
        // deprecated for everything that reads the flag.
        boolean deprecated = minValue == null && maxValue == null
                && (deprecatedMin != null || deprecatedMax != null);
        byte[] resolvedMin = minValue != null ? minValue : deprecatedMin;
        byte[] resolvedMax = maxValue != null ? maxValue : deprecatedMax;

        return new Statistics(resolvedMin, resolvedMax, nullCount, distinctCount, deprecated,
                minValueExact, maxValueExact, nanCount);
    }
}
