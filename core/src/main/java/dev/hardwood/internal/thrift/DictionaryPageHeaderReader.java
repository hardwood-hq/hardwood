/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.metadata.DictionaryPageHeader;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.Encoding;

/// Reader for DictionaryPageHeader from Thrift Compact Protocol.
public class DictionaryPageHeaderReader {

    /// Ids of the fields the format requires of a `DictionaryPageHeader`.
    private static final int[] REQUIRED_FIELDS = { 1, 2 };

    public static DictionaryPageHeader read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.DICTIONARY_PAGE_HEADER);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static DictionaryPageHeader readInternal(ThriftCompactReader reader) {
        int numValues = 0;
        Encoding encoding = null;
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                // Both fields read here are required, so a wrong wire type fails here rather
                // than being reported as a field that never arrived.
                case 1: // num_values
                    reader.requireField(header, Codes.I32);
                    numValues = reader.readNonNegativeI32();
                    seen |= 1L << fieldId;
                    break;
                case 2: // encoding
                    reader.requireField(header, Codes.I32);
                    encoding = ThriftEnumLookup.encoding(reader.readI32());
                    seen |= 1L << fieldId;
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.DICTIONARY_PAGE_HEADER, seen, REQUIRED_FIELDS);

        return new DictionaryPageHeader(numValues, encoding);
    }
}
