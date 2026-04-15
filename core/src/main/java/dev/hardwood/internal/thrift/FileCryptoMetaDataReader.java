/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.EncryptionAlgorithm;
import dev.hardwood.metadata.FileCryptoMetaData;

/// Reader for FileCryptoMetaData from Thrift Compact Protocol.
public class FileCryptoMetaDataReader {

    public static FileCryptoMetaData read(ThriftCompactReader reader) throws IOException {
        reader.resetLastFieldId();

        EncryptionAlgorithm encryptionAlgorithm = null;
        byte[] keyMetadata = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // encryption_algorithm
                    if (header.type() == 0x0C) {
                        encryptionAlgorithm = EncryptionAlgorithmReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // key_metadata(optional)
                    if (header.type() == 0x08) {
                        keyMetadata = reader.readBinary();
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

        if (encryptionAlgorithm == null) {
            throw new IOException("FileCryptoMetaData missing required field: encryption_algorithm");
        }

        return new FileCryptoMetaData(encryptionAlgorithm, keyMetadata);
    }
}
