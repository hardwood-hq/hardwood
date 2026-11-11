/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A declared uncompressed size up to its codec's bound is accepted, one byte more refused.
class CodecExpansionTest {

    @ParameterizedTest
    @CsvSource({ "SNAPPY, 3, 64", "SNAPPY, 100, 2133", "LZ4, 100, 25500", "LZ4_RAW, 100, 25500", "GZIP, 100, 103200",
            "ZSTD, 100, 3276800" })
    void acceptsTheBoundAndRefusesOneByteMore(CompressionCodec codec, int stored, int bound) {
        assertThatCode(() -> CodecExpansion.requireWithinExpansion("Size", codec, stored, bound))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> CodecExpansion.requireWithinExpansion("Size", codec, stored, bound + 1))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Size of " + (bound + 1) + " bytes exceeds the " + bound + " bytes that " + stored
                        + " bytes of " + codec + " decompress to at most");
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = { "BROTLI" })
    void leavesACodecWithoutABoundToTheWorkingSetLimit(CompressionCodec codec) {
        assertThatCode(() -> CodecExpansion.requireWithinExpansion("Size", codec, 1, Integer.MAX_VALUE))
                .doesNotThrowAnyException();
    }
}
