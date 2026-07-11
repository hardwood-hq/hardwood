/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.util.Arrays;

/// Pass-through [Compressor] for the `UNCOMPRESSED` codec: it copies the requested slice out
/// so the compressed and uncompressed forms are the same bytes.
public class UncompressedCompressor implements Compressor {

    @Override
    public byte[] compress(byte[] data, int offset, int length) {
        return Arrays.copyOfRange(data, offset, offset + length);
    }

    @Override
    public String getName() {
        return "UNCOMPRESSED";
    }
}
