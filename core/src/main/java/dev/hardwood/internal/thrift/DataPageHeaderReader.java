/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.Statistics;

/// Reader for DataPageHeader from Thrift Compact Protocol.
public class DataPageHeaderReader {

    /// Ids of the fields the format requires of a `DataPageHeader`.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3, 4 };

    public static DataPageHeader read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.DATA_PAGE_HEADER);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static DataPageHeader readInternal(ThriftCompactReader reader) {
        int numValues = 0;
        Encoding encoding = null;
        int encodingValue = -1;
        Encoding definitionLevelEncoding = null;
        Encoding repetitionLevelEncoding = null;
        Statistics statistics = null;
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                // Fields 1-4 are required, so a wrong wire type fails here rather than being
                // reported as a field that never arrived.
                case 1: // num_values
                    reader.requireField(header, Codes.I32);
                    numValues = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 2: // encoding
                    reader.requireField(header, Codes.I32);
                    encodingValue = reader.readI32();
                    encoding = ThriftEnumLookup.encoding(encodingValue);
                    seen |= 1L << fieldId;
                    break;
                case 3: // definition_level_encoding
                    reader.requireField(header, Codes.I32);
                    definitionLevelEncoding = ThriftEnumLookup.encoding(reader.readI32());
                    seen |= 1L << fieldId;
                    break;
                case 4: // repetition_level_encoding
                    reader.requireField(header, Codes.I32);
                    repetitionLevelEncoding = ThriftEnumLookup.encoding(reader.readI32());
                    seen |= 1L << fieldId;
                    break;
                case 5: // statistics
                    if (reader.acceptField(header, Codes.STRUCT)) {
                        statistics = StatisticsReader.read(reader);
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.DATA_PAGE_HEADER, seen, REQUIRED_FIELDS);

        return new DataPageHeader(numValues, encoding, encodingValue,
                definitionLevelEncoding, repetitionLevelEncoding, statistics);
    }
}
