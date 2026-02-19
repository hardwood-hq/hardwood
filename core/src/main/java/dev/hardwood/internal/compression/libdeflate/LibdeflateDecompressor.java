/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.libdeflate;

import java.io.IOException;
import java.nio.MappedByteBuffer;

import dev.hardwood.internal.compression.Decompressor;

/**
 * Stub implementation for Java 21 (no FFM API).
 * The real implementation using FFM is in META-INF/versions/22/ for Java 22+.
 */
public final class LibdeflateDecompressor implements Decompressor {

    public LibdeflateDecompressor(LibdeflatePool pool) {
        // Pool not used in stub - libdeflate not available on Java 21
    }

    @Override
    public byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException {
        throw new UnsupportedOperationException("libdeflate requires Java 22+");
    }

    @Override
    public String getName() {
        return "libdeflate (unavailable)";
    }
}
