/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.metadata.PhysicalType;

/// Builds dictionaries for tests the way the reader does: from a PLAIN dictionary page.
public final class TestDictionaries {

    private TestDictionaries() {
    }

    /// A `BYTE_ARRAY` dictionary holding `entries`, in order.
    public static Dictionary.ByteArrayDictionary byteArrays(byte[][] entries) {
        int size = 0;
        for (byte[] entry : entries) {
            size += Integer.BYTES + entry.length;
        }
        ByteBuffer page = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] entry : entries) {
            page.putInt(entry.length);
            page.put(entry);
        }
        return (Dictionary.ByteArrayDictionary) Dictionary.parse(
                page.array(), size, entries.length, PhysicalType.BYTE_ARRAY, null);
    }
}
