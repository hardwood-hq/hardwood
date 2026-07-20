/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.cli.command.NativeBinarySmokeIT.NativeResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/// Native-binary read coverage for every supported compression codec.
///
/// This has to run against the compiled binary, not the JVM. The compression
/// libraries (`lz4-java`, `zstd-jni`, `snappy-java`, `brotli4j`) load native
/// code and resolve their implementation classes reflectively, so a codec can
/// decode fine on the JVM yet crash in the native image when the reachability /
/// JNI config is missing — exactly the LZ4 failure that motivated this. A
/// JVM-only test proves nothing about the shipped artifact. See issue #804.
class NativeCompressionCodecIT {

    private final String nativeBinary = System.getProperty("native.image.path");

    static Stream<Arguments> codecs() {
        // uncompressed/snappy/gzip/zstd/brotli/lz4_raw are generated from the same
        // id/name/value table (tools/simple-datagen.py), so their decompressed
        // content is asserted directly. Hadoop-LZ4 exercises a different
        // decompressor (`Lz4Decompressor`) than LZ4_RAW (`Lz4RawDecompressor`) and
        // can't be produced by PyArrow, so it's the vendored apache/parquet-testing
        // file, asserted on a successful, non-empty read.
        return Stream.of(
                codec("UNCOMPRESSED", "uncompressed.parquet", "Alice"),
                codec("SNAPPY", "snappy.parquet", "Alice"),
                codec("GZIP", "gzip.parquet", "Alice"),
                codec("ZSTD", "zstd.parquet", "Alice"),
                codec("BROTLI", "brotli.parquet", "Alice"),
                codec("LZ4_RAW", "lz4_raw.parquet", "Alice"),
                codec("LZ4", "lz4_hadoop.parquet", null));
    }

    private static Arguments codec(String label, String resource, String expectedContent) {
        return arguments(Named.of(label, resource), expectedContent);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("codecs")
    void printsCompressedFile(String resource, String expectedContent) throws IOException, InterruptedException {
        String file = getClass().getResource("/compression/" + resource).getPath();

        NativeResult result = NativeBinarySmokeIT.exec(nativeBinary, "print", "-n", "3", "-f", file);

        assertThat(result.exitCode())
                .withFailMessage("print failed for %s: stdout=%s stderr=%s", resource, result.stdout(), result.stderr())
                .isZero();
        if (expectedContent != null) {
            assertThat(result.stdout()).contains(expectedContent);
        }
        else {
            assertThat(result.stdout()).isNotBlank();
        }
    }
}
