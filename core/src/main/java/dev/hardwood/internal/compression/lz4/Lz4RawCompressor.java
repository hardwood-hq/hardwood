/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.lz4;

import java.util.Arrays;

import dev.hardwood.internal.compression.Compressor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/// [Compressor] for the LZ4_RAW codec, the inverse of [Lz4RawDecompressor].
///
/// Produces a standard LZ4 block with no framing or header, which is what `LZ4_RAW` is and what
/// distinguishes it from the deprecated Hadoop-framed `LZ4`. Compresses through the fast
/// compressor, LZ4's default trade-off point and the one that makes the codec worth choosing
/// over a denser one, unless given a level: then through the high compressor (LZ4 HC) at that
/// level, which writes the same block format.
public class Lz4RawCompressor implements Compressor {

    private final LZ4Compressor compressor;

    /// Compresses through the fast compressor.
    public Lz4RawCompressor() {
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
    }

    /// Compresses through the high compressor.
    ///
    /// @param level a level within `CompressionLevels`' range for `LZ4_RAW`
    public Lz4RawCompressor(int level) {
        this.compressor = LZ4Factory.fastestInstance().highCompressor(level);
    }

    @Override
    public byte[] compress(byte[] data, int offset, int length) {
        byte[] output = new byte[compressor.maxCompressedLength(length)];
        int written = compressor.compress(data, offset, length, output, 0, output.length);
        return Arrays.copyOf(output, written);
    }

    @Override
    public String getName() {
        return "LZ4_RAW";
    }
}
