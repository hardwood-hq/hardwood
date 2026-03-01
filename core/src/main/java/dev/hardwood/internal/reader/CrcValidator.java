/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Validates CRC-32 checksums on Parquet page data.
 */
class CrcValidator {

    static void validate(int expectedCrc, ByteBuffer data, String columnName) throws IOException {
        CRC32 crc = new CRC32();
        byte[] buf = new byte[data.remaining()];
        data.get(data.position(), buf);
        crc.update(buf);
        int actualCrc = (int) crc.getValue();
        if (actualCrc != expectedCrc) {
            throw new IOException("CRC mismatch for column '" + columnName
                    + "': expected " + Integer.toHexString(expectedCrc)
                    + " but computed " + Integer.toHexString(actualCrc));
        }
    }
}
