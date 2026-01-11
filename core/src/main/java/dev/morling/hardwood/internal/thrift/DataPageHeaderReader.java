/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.thrift;

import java.io.IOException;

import dev.morling.hardwood.metadata.DataPageHeader;
import dev.morling.hardwood.metadata.Encoding;

/**
 * Reader for DataPageHeader from Thrift Compact Protocol.
 */
public class DataPageHeaderReader {

    public static DataPageHeader read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static DataPageHeader readInternal(ThriftCompactReader reader) throws IOException {
        int numValues = 0;
        Encoding encoding = null;
        Encoding definitionLevelEncoding = null;
        Encoding repetitionLevelEncoding = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // num_values
                    if (header.type() == 0x05) {
                        numValues = reader.readI32();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // encoding
                    if (header.type() == 0x05) {
                        encoding = Encoding.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // definition_level_encoding
                    if (header.type() == 0x05) {
                        definitionLevelEncoding = Encoding.fromThriftValue(reader.readI32());
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4: // repetition_level_encoding
                    if (header.type() == 0x05) {
                        repetitionLevelEncoding = Encoding.fromThriftValue(reader.readI32());
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

        return new DataPageHeader(numValues, encoding, definitionLevelEncoding, repetitionLevelEncoding);
    }
}
