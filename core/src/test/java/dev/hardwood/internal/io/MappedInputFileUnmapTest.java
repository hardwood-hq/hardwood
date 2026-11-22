/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.io;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// `close()` releases a file's mappings instead of leaving them for the GC (#199).
/// The release is observed through `/proc/self/maps`: a mapping of the file is listed
/// while it is open and gone once it is closed.
class MappedInputFileUnmapTest {

    private static final Path FILE = Path.of("src/test/resources/plain_uncompressed.parquet");

    @Test
    void closeUnmapsTheWholeFileMapping(@TempDir Path tempDir) throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        String absolute = path.toAbsolutePath().toString();
        assumeLinuxMaps();

        try (InputFile file = InputFile.of(path)) {
            file.open();
            assertThat(mappedPaths()).contains(absolute);

            file.close();
            assertThat(mappedPaths()).doesNotContain(absolute);
        }
    }

    @Test
    void closeUnmapsPerRegionMappings(@TempDir Path tempDir) throws Exception {
        // Sparse file over 2 GB, as in MappedInputFileLargeFileTest: only the pages
        // holding the sentinel bytes are materialized.
        Path path = tempDir.resolve("large.bin");
        long twoGb = (long) Integer.MAX_VALUE + 1;
        byte[] sentinel = "SENTINEL-16-BYTES".getBytes(StandardCharsets.US_ASCII);
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            raf.setLength(twoGb + 4096);
            raf.seek(0);
            raf.write(sentinel);
        }
        String absolute = path.toAbsolutePath().toString();
        assumeLinuxMaps();

        try (InputFile file = InputFile.of(path)) {
            file.open();
            ByteBuffer range = file.readRange(0, sentinel.length);
            byte[] bytes = new byte[sentinel.length];
            range.get(bytes);
            assertThat(bytes).isEqualTo(sentinel);
            assertThat(mappedPaths()).contains(absolute);

            file.close();
            assertThat(mappedPaths()).doesNotContain(absolute);
        }
    }

    @Test
    void aReadAfterCloseFails(@TempDir Path tempDir) throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        MappedInputFile file = new MappedInputFile(path);
        file.open();
        file.close();

        assertThatThrownBy(() -> file.readRange(0, 16))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("File not opened: file.parquet");
    }

    private static void assumeLinuxMaps() {
        assumeTrue(Files.exists(Path.of("/proc/self/maps")), "Linux-only: /proc/self/maps");
    }

    /// The pathnames of the process's current mappings, from `/proc/self/maps`.
    private static List<String> mappedPaths() throws Exception {
        return Files.readAllLines(Path.of("/proc/self/maps")).stream()
                .map(line -> {
                    String[] fields = line.split("\\s+", 6);
                    return fields.length == 6 ? fields[5] : "";
                })
                .toList();
    }
}
