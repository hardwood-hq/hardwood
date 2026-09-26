/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.Statistics;

/// Reader for DataPageHeaderV2 from Thrift Compact Protocol.
public class DataPageHeaderV2Reader {

    /// Ids of the fields the format requires of a `DataPageHeaderV2`.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3, 4, 5, 6 };

    public static DataPageHeaderV2 read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.DATA_PAGE_HEADER_V2);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static DataPageHeaderV2 readInternal(ThriftCompactReader reader) {
        int numValues = 0;
        int numNulls = 0;
        int numRows = 0;
        Encoding encoding = null;
        int encodingValue = -1;
        int definitionLevelsByteLength = 0;
        int repetitionLevelsByteLength = 0;
        boolean isCompressed = true; // Default value per Parquet spec
        Statistics statistics = null;
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                // Fields 1-6 are required, so a wrong wire type fails here rather than being
                // reported as a field that never arrived.
                case 1: // num_values
                    reader.requireField(header, Codes.I32);
                    numValues = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 2: // num_nulls
                    reader.requireField(header, Codes.I32);
                    numNulls = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 3: // num_rows
                    reader.requireField(header, Codes.I32);
                    numRows = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 4: // encoding
                    reader.requireField(header, Codes.I32);
                    encodingValue = reader.readI32();
                    encoding = ThriftEnumLookup.encoding(encodingValue);
                    seen |= 1L << fieldId;
                    break;
                case 5: // definition_levels_byte_length
                    reader.requireField(header, Codes.I32);
                    definitionLevelsByteLength = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 6: // repetition_levels_byte_length
                    reader.requireField(header, Codes.I32);
                    repetitionLevelsByteLength = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 7: // is_compressed
                    isCompressed = reader.readBooleanField(header, isCompressed);
                    break;
                case 8: // statistics
                    if (reader.acceptField(header, Codes.STRUCT)) {
                        statistics = StatisticsReader.read(reader);
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.DATA_PAGE_HEADER_V2, seen, REQUIRED_FIELDS);

        return new DataPageHeaderV2(numValues, numNulls, numRows, encoding, encodingValue,
                definitionLevelsByteLength, repetitionLevelsByteLength, isCompressed, statistics);
    }
}
