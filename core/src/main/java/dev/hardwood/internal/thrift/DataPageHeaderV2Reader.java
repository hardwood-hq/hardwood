/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.metadata.Encoding;

/**
 * Reader for DataPageHeaderV2 from Thrift Compact Protocol.
 */
public class DataPageHeaderV2Reader {

    public static DataPageHeaderV2 read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static DataPageHeaderV2 readInternal(ThriftCompactReader reader) throws IOException {
        int numValues = 0;
        int numNulls = 0;
        int numRows = 0;
        Encoding encoding = null;
        int definitionLevelsByteLength = 0;
        int repetitionLevelsByteLength = 0;
        boolean isCompressed = true; // Default value per Parquet spec

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // num_values
                    numValues = reader.readI32();
                    break;
                case 2: // num_nulls
                    numNulls = reader.readI32();
                    break;
                case 3: // num_rows
                    numRows = reader.readI32();
                    break;
                case 4: // encoding
                    encoding = Encoding.fromThriftValue(reader.readI32());
                    break;
                case 5: // definition_levels_byte_length
                    definitionLevelsByteLength = reader.readI32();
                    break;
                case 6: // repetition_levels_byte_length
                    repetitionLevelsByteLength = reader.readI32();
                    break;
                case 7: // is_compressed (boolean encoded in type: 0x01=true, 0x02=false)
                    if (header.type() == 0x01) {
                        isCompressed = true;
                    }
                    else if (header.type() == 0x02) {
                        isCompressed = false;
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

        return new DataPageHeaderV2(numValues, numNulls, numRows, encoding,
                definitionLevelsByteLength, repetitionLevelsByteLength, isCompressed);
    }
}
