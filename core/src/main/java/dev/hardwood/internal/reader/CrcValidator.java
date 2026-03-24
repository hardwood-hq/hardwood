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

class CrcValidator {

    /// Asserts that the CRC-32 checksum of the given page data matches the expected value
    /// from the page header. Throws if the checksum does not match.
    /// The buffer's position and limit are not modified.
    static void assertCorrectCrc(int expectedCrc, ByteBuffer pageData, String columnName) throws IOException {
        CRC32 crc = new CRC32();
        crc.update(pageData.duplicate());
        int actualCrc = (int) crc.getValue();
        if (actualCrc != expectedCrc) {
            throw new IOException("CRC mismatch for column " + columnName
                    + ": expected " + Integer.toHexString(expectedCrc)
                    + " but computed " + Integer.toHexString(actualCrc));
        }
    }
}
