/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.AesGcmV1;

///  Reader for AesGcmV1 from Thrift Compact Protocol.
public class AesGcmV1Reader {

    public static AesGcmV1 read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static AesGcmV1 readInternal(ThriftCompactReader reader) throws IOException {
        byte[] aadPrefix = null;
        byte[] aadFileUnique = null;
        boolean supplyAadPrefix = false;

        while(true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch(header.fieldId()) {
                case 1: // aad_prefix
                    if (header.type() == 0x08) {
                        aadPrefix = reader.readBinary();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // aad_file_unique
                    if (header.type() == 0x08) {
                        aadFileUnique = reader.readBinary();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3: // supply_aad_prefix
                    if (header.type() == 0x01) {
                        supplyAadPrefix = true;
                    }
                    else if (header.type() == 0x02) {
                        supplyAadPrefix = false;
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

        return new AesGcmV1(aadPrefix, aadFileUnique, supplyAadPrefix);
    }
}
