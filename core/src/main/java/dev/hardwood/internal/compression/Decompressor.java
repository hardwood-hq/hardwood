/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * Interface for decompressing compressed page data from memory-mapped files.
 */
public interface Decompressor {

    /**
     * Decompress the given compressed data from a memory-mapped buffer.
     *
     * @param compressed the memory-mapped buffer slice containing compressed data
     * @param uncompressedSize the expected size of uncompressed data
     * @return the uncompressed data
     * @throws IOException if decompression fails
     */
    byte[] decompress(MappedByteBuffer compressed, int uncompressedSize) throws IOException;

    /**
     * Get the name of this decompressor.
     */
    String getName();
}
