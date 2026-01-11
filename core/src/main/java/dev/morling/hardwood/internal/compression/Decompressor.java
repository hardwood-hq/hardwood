/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import java.io.IOException;

/**
 * Interface for decompressing compressed page data.
 */
public interface Decompressor {

    /**
     * Decompress the given compressed data.
     *
     * @param compressed the compressed data
     * @param uncompressedSize the expected size of uncompressed data
     * @return the uncompressed data
     * @throws IOException if decompression fails
     */
    byte[] decompress(byte[] compressed, int uncompressedSize) throws IOException;

    /**
     * Get the name of this decompressor.
     */
    String getName();
}
