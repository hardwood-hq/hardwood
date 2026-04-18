/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.ColumnCryptoMetaData;
import dev.hardwood.metadata.EncryptionWithColumnKey;
import dev.hardwood.metadata.EncryptionWithFooterKey;

/// Reader for ColumnCryptoMetaData from Thrift Compact Protocol.
public class ColumnCryptoMetaDataReader {

    public static ColumnCryptoMetaData read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnCryptoMetaData readInternal(ThriftCompactReader reader) throws IOException {
        boolean isFooterKey = false;
        EncryptionWithColumnKey columnKeyEncryptionMetaData = null;

        while(true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // encryption with footer key
                    if (header.type() == 0x0C) {
                        isFooterKey = true;
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2: // encryption with column key
                    if (header.type() == 0x0C) {
                        columnKeyEncryptionMetaData = EncryptionWithColumnKeyReader.read(reader);
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

        if (isFooterKey) {
            return new ColumnCryptoMetaData(new EncryptionWithFooterKey(), null);
        }
        else if (columnKeyEncryptionMetaData != null) {
            return new ColumnCryptoMetaData(null, columnKeyEncryptionMetaData);
        } else {
            throw new IOException("ColumnCryptoMetaData missing required field: one of ENCRYPTION_WITH_FOOTER_KEY or ENCRYPTION_WITH_COLUMN_KEY must be set");
        }
    }
}
