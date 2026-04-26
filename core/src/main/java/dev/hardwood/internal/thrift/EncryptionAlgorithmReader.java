/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.AesGcmCtrV1;
import dev.hardwood.metadata.AesGcmV1;
import dev.hardwood.metadata.EncryptionAlgorithm;

///  Reader for EncryptionAlgorithm from Thrift Compact Protocol.
public class EncryptionAlgorithmReader {

    public static EncryptionAlgorithm read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static EncryptionAlgorithm readInternal(ThriftCompactReader reader) throws IOException {
        AesGcmV1 aesGcmV1 = null;
        AesGcmCtrV1 aesGcmCtrV1 = null;

        while(true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // AES_GCM_V1
                    if (header.type() == 0x0C) {
                        aesGcmV1 = AesGcmV1Reader.read(reader);
                    } else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // AES_GCM_CTR_V1
                    if (header.type() == 0x0C) {
                        aesGcmCtrV1 = AesGcmCtrV1Reader.read(reader);
                    } else {
                        reader.skipField(header.type());
                    }
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }

        if (aesGcmV1 != null) {
            return new EncryptionAlgorithm(aesGcmV1, null);
        } else if (aesGcmCtrV1 != null) {
            return new EncryptionAlgorithm(null, aesGcmCtrV1);
        } else {
            throw new IOException("EncryptionAlgorithm missing required field: one of AES_GCM_V1 or AES_GCM_CTR_V1 must be set");
        }
    }
}
