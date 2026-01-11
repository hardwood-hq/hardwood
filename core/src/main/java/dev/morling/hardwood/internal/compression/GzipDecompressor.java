/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Decompressor for GZIP-compressed data using Java's standard library.
 */
public class GzipDecompressor implements Decompressor {

    @Override
    public byte[] decompress(byte[] compressed, int uncompressedSize) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(compressed);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(uncompressedSize);

        try (GZIPInputStream gzipStream = new GZIPInputStream(inputStream)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = gzipStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }

        byte[] result = outputStream.toByteArray();
        if (result.length != uncompressedSize) {
            throw new IOException("Decompressed size mismatch: expected " + uncompressedSize +
                    " but got " + result.length);
        }

        return result;
    }

    @Override
    public String getName() {
        return "GZIP";
    }
}
