/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;

/// Validates the jzlib-based GZIP decoder against the JDK's gzip encoder.
class GzipInflateTest {

    @Test
    void inflatesJdkGzippedData() throws IOException {
        for (int size : new int[] { 0, 1, 100, 4096, 200_000 }) {
            byte[] original = new byte[size];
            new Random(size).nextBytes(original);
            // Mix in a compressible run so both literal and copy paths are exercised.
            for (int i = size / 2; i < size; i++) {
                original[i] = (byte) (i & 0x7);
            }
            byte[] gzipped = gzip(original);

            byte[] output = new byte[size];
            int produced = GzipInflate.decompress(gzipped, output);

            assertArrayEquals(original, output, "size=" + size);
            org.junit.jupiter.api.Assertions.assertEquals(size, produced, "produced size=" + size);
        }
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(baos)) {
            gz.write(data);
        }
        return baos.toByteArray();
    }
}
