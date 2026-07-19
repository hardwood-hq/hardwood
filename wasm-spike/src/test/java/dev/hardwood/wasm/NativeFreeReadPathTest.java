/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.wasm;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/// Proves the Parquet read path can run with no native codec libraries on the
/// classpath — the prerequisite for a GraalVM Web Image (WebAssembly) build.
///
/// Fixtures are read from the `core` module's test resources; `hardwood-core`
/// declares the native codec jars as `optional`, so they are absent from this
/// module's classpath.
class NativeFreeReadPathTest {

    private static final Path FIXTURES = Path.of("..", "core", "src", "test", "resources");

    @Test
    void nativeCodecLibrariesAreAbsentFromTheClasspath() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("org.xerial.snappy.Snappy"));
        assertThrows(ClassNotFoundException.class, () -> Class.forName("com.github.luben.zstd.Zstd"));
        assertThrows(ClassNotFoundException.class, () -> Class.forName("net.jpountz.lz4.LZ4Factory"));
        assertThrows(ClassNotFoundException.class, () -> Class.forName("com.aayushatharva.brotli4j.Brotli4jLoader"));
    }

    @Test
    void readsUncompressedFileWithoutNativeCode() throws IOException {
        String summary = WebImageReadSpike.describe(fixture("plain_uncompressed.parquet"));
        assertTrue(summary.contains("rows: "), summary);
        assertTrue(rowCount(summary) > 0, summary);
    }

    @Test
    void readsSnappyFileViaPureJavaDecompressor() throws IOException {
        String summary = WebImageReadSpike.describe(fixture("plain_snappy.parquet"));
        assertTrue(rowCount(summary) > 0, summary);
    }

    private static byte[] fixture(String name) throws IOException {
        return Files.readAllBytes(FIXTURES.resolve(name));
    }

    private static long rowCount(String summary) {
        for (String line : summary.split("\n")) {
            if (line.startsWith("rows: ")) {
                return Long.parseLong(line.substring("rows: ".length()).trim());
            }
        }
        return -1;
    }
}
