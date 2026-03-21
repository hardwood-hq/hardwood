/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for decompressing compressed page data.
 */
public interface Decompressor {

    /**
     * Decompress the given compressed data from a buffer.
     * <p>
     * The returned array contains valid decompressed data in positions {@code [0, uncompressedSize)}.
     * The array length may be greater than {@code uncompressedSize} due to internal buffer reuse;
     * callers must use {@code uncompressedSize} as the effective length, not {@code array.length}.
     *
     * @param compressed the buffer slice containing compressed data
     * @param uncompressedSize the expected size of uncompressed data
     * @return an array containing the uncompressed data in the first {@code uncompressedSize} bytes
     * @throws IOException if decompression fails
     */
    byte[] decompress(ByteBuffer compressed, int uncompressedSize) throws IOException;

    /**
     * Get the name of this decompressor.
     */
    String getName();
}
