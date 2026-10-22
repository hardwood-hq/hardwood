/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.testing.InteropCase.Nullability;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.WriterConfig;

import static dev.hardwood.testing.FlatInteropGate.FEW_ROWS;
import static dev.hardwood.testing.FlatInteropGate.sweep;
import static dev.hardwood.testing.FlatInteropGate.verify;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The codec half of the flat write-path interop gate ([FlatInteropGate]): the codec axis over every
/// physical type, and the optional encodings crossed with the codecs. The other axes are in
/// [WriterInteropTest].
class WriterCodecInteropTest {

    /// The codec axis, over every codec the writer produces. The two it does not — `LZ4`'s
    /// deprecated Hadoop framing, and `LZO` — fail at writer creation, so they have no file for
    /// this gate to read back; `WriterRoundTripTest` holds them to failing there.
    ///
    /// What the axis is really checking is framing. Each of these codecs has a raw block form and
    /// a wrapped one, and the wrapped form round-trips perfectly against Hardwood's own reader
    /// while being the wrong bytes for everyone else — which is what makes an independent reader,
    /// rather than a round trip, the thing that settles it.
    ///
    /// `BROTLI` is the one codec the writer produces that is missing here, because the pinned
    /// parquet-java cannot read it at all — see [#parquetJavaHasNoBrotliCodec()].
    static Stream<InteropCase> codecs() {
        return sweep(PARQUET_JAVA_CODECS,
                (type, codec) -> InteropCase.of("codec " + codec, type, Nullability.OPTIONAL_SOME_NULL,
                        64, FEW_ROWS, WriterConfig.builder().codec(codec).build()));
    }

    /// Every codec the writer produces that the pinned parquet-java can also read, which is all
    /// of them but `BROTLI` — see [#parquetJavaHasNoBrotliCodec()].
    private static final List<CompressionCodec> PARQUET_JAVA_CODECS = List.of(
            CompressionCodec.UNCOMPRESSED, CompressionCodec.GZIP, CompressionCodec.SNAPPY,
            CompressionCodec.ZSTD, CompressionCodec.LZ4_RAW);

    /// The codec axis crossed with the optional encodings, which [WriterInteropTest#optionalEncodings] leaves at
    /// the default codec and [#codecs] leaves at the default encoding — so between them the four
    /// encodings stage 19b added meet no codec but one.
    ///
    /// The pair is what matters here rather than the type: a codec compresses a page body, and
    /// what these encodings change is the shape of that body. `BYTE_STREAM_SPLIT` is the case
    /// that makes the point — it changes no page's size on its own, and exists entirely for what
    /// the codec after it can then do — so each encoding is swept over the codecs on one type it
    /// is legal for rather than over all of them.
    ///
    /// Nullability is swept as it is in [WriterInteropTest#optionalEncodings], since the shape of a page body is
    /// what a codec sees: an `OPTIONAL_ALL_NULL` case hands the codec a body with no values in it
    /// at all, and a `REQUIRED` one a body the definition levels are absent from.
    static Stream<InteropCase> optionalEncodingCodecs() {
        return Stream.of(
                new EncodingCodecAxis(ColumnEncoding.DELTA_BINARY_PACKED, TypeFixture.INT64),
                new EncodingCodecAxis(ColumnEncoding.DELTA_LENGTH_BYTE_ARRAY, TypeFixture.BYTE_ARRAY),
                new EncodingCodecAxis(ColumnEncoding.DELTA_BYTE_ARRAY, TypeFixture.BYTE_ARRAY),
                new EncodingCodecAxis(ColumnEncoding.BYTE_STREAM_SPLIT, TypeFixture.DOUBLE))
                .flatMap(axis -> PARQUET_JAVA_CODECS.stream()
                        .flatMap(codec -> Stream.of(Nullability.values())
                                .map(nullability -> InteropCase.of(
                                        axis.encoding() + " under " + codec, axis.type(),
                                        nullability, 64, FEW_ROWS,
                                        WriterConfig.builder()
                                                .encoding(axis.encoding())
                                                .codec(codec)
                                                .build()))));
    }

    // ==================== Tests ====================

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void codecs(InteropCase testCase, @TempDir Path dir) throws IOException {
        verify(testCase, dir);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void optionalEncodingCodecs(InteropCase testCase, @TempDir Path dir) throws IOException {
        verify(testCase, dir);
    }

    /// Why `BROTLI` is absent from the codec axis, asserted rather than left as a comment.
    ///
    /// parquet-java resolves the codec by name through Hadoop's `CompressionCodec` registry, and
    /// for `BROTLI` the name is `org.apache.hadoop.io.compress.BrotliCodec` — a class that ships
    /// in neither parquet-java 1.18.1 nor Hadoop, but in `com.github.rdblue:brotli-codec`, an
    /// unmaintained third-party artifact whose bundled native binaries cover a few platforms
    /// only. Putting it on this module's classpath would make the gate's result depend on the
    /// architecture it runs on, which is the opposite of what a gate is for.
    ///
    /// So the codec is not unverified, only unverified *against this reader*:
    /// `WriterDifferentialTest` reads Hardwood's `BROTLI` files back through DuckDB, which
    /// decompresses them natively. The read direction has the same hole for the same reason —
    /// `large_string_map.brotli.parquet` is the corpus's only `BROTLI` file and sits in
    /// [Utils#SKIPPED_FILES] — so this is one gap in parquet-java, not a new one the write path
    /// introduced.
    ///
    /// This test pins the reason, so a parquet-java that gains the codec fails here and `BROTLI`
    /// rejoins the axis rather than staying out by inertia.
    @Test
    void parquetJavaHasNoBrotliCodec() {
        assertThatThrownBy(() -> Class.forName("org.apache.hadoop.io.compress.BrotliCodec"))
                .isInstanceOf(ClassNotFoundException.class);
    }

    private record EncodingCodecAxis(ColumnEncoding encoding, TypeFixture type) {
    }
}
