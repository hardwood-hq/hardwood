/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.CompressionCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The [Compression] value: each factory's codec, the level ranges, and the codecs `of` refuses.
class CompressionTest {

    @Test
    void eachFactoryNamesItsCodecAtTheDefaultLevel() {
        assertThat(Stream.of(Compression.uncompressed(), Compression.snappy(), Compression.gzip(),
                Compression.zstd(), Compression.lz4Raw(), Compression.brotli()))
                .allSatisfy(compression -> assertThat(compression.level()).isEmpty())
                .extracting(Compression::codec)
                .containsExactly(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP,
                        CompressionCodec.ZSTD, CompressionCodec.LZ4_RAW, CompressionCodec.BROTLI);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = CompressionCodec.class, names = { "LZ4", "LZO" }, mode = EnumSource.Mode.EXCLUDE)
    void ofReturnsTheFactorysValue(CompressionCodec codec) {
        Compression compression = Compression.of(codec);
        assertThat(compression.codec()).isEqualTo(codec);
        assertThat(compression.level()).isEmpty();
    }

    @Test
    void ofRefusesTheCodecsThatAreNotWritten() {
        assertThatThrownBy(() -> Compression.of(CompressionCodec.LZ4))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("LZ4 uses the Hadoop framing the Parquet format has deprecated, so it is not written; "
                        + "use LZ4_RAW instead. Files already written with LZ4 are still read.");
        assertThatThrownBy(() -> Compression.of(CompressionCodec.LZO))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("LZO compression is not supported: there is no maintained JVM implementation under a "
                        + "licence this project can depend on. The read path refuses it for the same reason.");
        assertThatThrownBy(() -> Compression.of(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("codec must not be null");
    }

    static Stream<Arguments> rangeEnds() {
        return Stream.of(
                Arguments.of(Compression.zstd(), -131072), Arguments.of(Compression.zstd(), 0),
                Arguments.of(Compression.zstd(), 22),
                Arguments.of(Compression.gzip(), 0), Arguments.of(Compression.gzip(), 9),
                Arguments.of(Compression.brotli(), 0), Arguments.of(Compression.brotli(), 11),
                Arguments.of(Compression.lz4Raw(), 1), Arguments.of(Compression.lz4Raw(), 17));
    }

    @ParameterizedTest(name = "{0} level {1}")
    @MethodSource("rangeEnds")
    void acceptsEachCodecsLevelRangeEnds(Compression.Leveled codec, int level) {
        Compression.Leveled leveled = codec.level(level);
        assertThat(leveled.codec()).isEqualTo(codec.codec());
        assertThat(leveled.level()).isEqualTo(OptionalInt.of(level));
    }

    static Stream<Arguments> outsideRanges() {
        return Stream.of(
                Arguments.of(Compression.zstd(), -131073, "ZSTD's range -131072 to 22"),
                Arguments.of(Compression.zstd(), 23, "ZSTD's range -131072 to 22"),
                Arguments.of(Compression.gzip(), -1, "GZIP's range 0 to 9"),
                Arguments.of(Compression.gzip(), 10, "GZIP's range 0 to 9"),
                Arguments.of(Compression.brotli(), -1, "BROTLI's range 0 to 11"),
                Arguments.of(Compression.brotli(), 12, "BROTLI's range 0 to 11"),
                Arguments.of(Compression.lz4Raw(), 0, "LZ4_RAW's range 1 to 17"),
                Arguments.of(Compression.lz4Raw(), 18, "LZ4_RAW's range 1 to 17"));
    }

    @ParameterizedTest(name = "{0} level {1}")
    @MethodSource("outsideRanges")
    void rejectsALevelOutsideTheCodecsRange(Compression.Leveled codec, int level, String range) {
        assertThatThrownBy(() -> codec.level(level))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Compression level " + level + " is outside " + range);
    }

    @Test
    void aLevelReplacesAnEarlierOne() {
        assertThat(Compression.zstd().level(19).level(1)).isEqualTo(Compression.zstd().level(1));
    }

    @Test
    void equalsByCodecAndLevel() {
        assertThat(Compression.zstd().level(9)).isEqualTo(Compression.zstd().level(9))
                .hasSameHashCodeAs(Compression.zstd().level(9))
                .isNotEqualTo(Compression.zstd())
                .isNotEqualTo(Compression.brotli().level(9));
        assertThat(Compression.zstd().level(9)).hasToString("ZSTD level 9");
        assertThat(Compression.snappy()).hasToString("SNAPPY");
    }
}
