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

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Verifies how Hardwood handles corrupted and intentionally-malformed Parquet files from
/// the `apache/parquet-testing` suite (the `bad_data/` directory plus a handful of files
/// in `data/` with corrupted CRCs). Covers both the single-file [ParquetFileReader] and
/// the [ParquetFileReader] paths, including failures that appear mid-sequence. These
/// tests do not compare against parquet-java — they assert Hardwood's own reject/accept
/// behavior — so they live here rather than in [ParquetComparisonTest].
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BadDataHandlingTest {

    private Path repoDir;

    @BeforeAll
    void setUp() throws IOException {
        repoDir = ParquetTestingRepoCloner.ensureCloned();
        Utils.ensureGoodCFile(repoDir);
    }

    // ==================== Single-file rejection ====================

    @Test
    void rejectParquet1481() throws IOException {
        // Corrupted schema Thrift value: physical type field is -7
        assertBadDataRejected("PARQUET-1481.parquet",
                "[PARQUET-1481.parquet] Invalid or corrupt physical type value: -7 (expected 0-7)."
                        + " File metadata may be corrupted");
    }

    @Test
    void rejectDictheader() throws IOException {
        // Dictionary page header has negative numValues.
        // All 4 columns are corrupted differently; parallel column scanning
        // means any column's error may surface first.
        assertBadDataRejected("ARROW-RS-GH-6229-DICTHEADER.parquet");
    }

    @Test
    void rejectLevels() throws IOException {
        // Page has insufficient repetition levels: the page header declares
        // 21 values but column metadata expects only 1. The v3 pipeline detects
        // this during level decoding ("Insufficient RLE/Bit-Packing data").
        assertBadDataRejected("ARROW-RS-GH-6229-LEVELS.parquet");
    }

    @Test
    void rejectArrowGH41317() throws IOException {
        // Columns do not have the same size: timestamp_us_no_tz has no data
        // pages (0 values vs 3 declared in metadata).
        assertBadDataRejected("ARROW-GH-41317.parquet");
    }

    @Test
    void rejectArrowGH41321() throws IOException {
        // Decoded rep/def levels less than num_values in page header.
        // Column 'value' also has negative dictionary numValues which is
        // caught during dictionary parsing or page decoding.
        assertBadDataRejected("ARROW-GH-41321.parquet");
    }

    @Test
    void rejectArrowGH45185() throws IOException {
        // Repetition levels start with 1 instead of the required 0
        assertBadDataRejected("ARROW-GH-45185.parquet",
                "[ARROW-GH-45185.parquet] Invalid column chunk for 'element':"
                        + " first repetition level must be 0 but was 1");
    }

    @Test
    void rejectCorruptChecksum() throws IOException {
        // Intentionally corrupted CRC checksums in data pages.
        // The CRC IOException is wrapped in UncheckedIOException with file context
        // by ColumnWorker before BatchExchange forwards it.
        assertCorruptChecksumRejected("data/datapage_v1-corrupt-checksum.parquet",
                "[datapage_v1-corrupt-checksum.parquet]"
                        + " CRC mismatch for column a: expected bbce3b9d but computed f4f6d0a",
                "CRC mismatch for column a: expected bbce3b9d but computed f4f6d0a");
    }

    @Test
    void rejectCorruptDictionaryChecksum() throws IOException {
        // Intentionally corrupted CRC checksum in dictionary page.
        // Caught and wrapped by IndexedFetchPlan as UncheckedIOException with
        // a `[fileName]` prefix.
        assertCorruptChecksumRejected("data/rle-dict-uncompressed-corrupt-checksum.parquet",
                "[rle-dict-uncompressed-corrupt-checksum.parquet]"
                        + " Failed to parse dictionary for column 'long_field'",
                "CRC mismatch for column long_field: expected 6522df6a but computed 6522df69");
    }

    @Test
    void acceptArrowGH43605() throws IOException {
        // Dictionary index page uses RLE encoding with bit-width 0.
        // This is valid for a single-entry dictionary (ceil(log2(1)) = 0);
        // parquet-java also accepts this file.
        Path testFile = repoDir.resolve("bad_data/ARROW-GH-43605.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile));
             RowReader rowReader = fileReader.rowReader()) {
            int count = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                count++;
            }
            assertThat(count).isGreaterThan(0);
        }
    }

    // ==================== Multi-file rejection (mid-sequence) ====================

    @Test
    void multiFileReaderRejectsBadFileMidSequence() throws IOException {
        Path good = repoDir.resolve("data/alltypes_plain.parquet");
        Path bad = repoDir.resolve("bad_data/PARQUET-1481.parquet");
        Utils.assertBadDataRejected("PARQUET-1481.parquet",
                "[PARQUET-1481.parquet] Invalid or corrupt physical type value: -7 (expected 0-7)."
                        + " File metadata may be corrupted",
                multiFileReadAction(good, bad));
    }

    @Test
    void multiFileReaderRejectsDictheaderMidSequence() throws IOException {
        Path good = repoDir.resolve("data/alltypes_plain.parquet");
        Path bad = repoDir.resolve("bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet");
        Utils.assertBadDataRejected("ARROW-RS-GH-6229-DICTHEADER.parquet",
                multiFileReadAction(good, bad));
    }

    @Test
    void multiFileReaderRejectsLevelsMidSequence() throws IOException {
        Path good = repoDir.resolve("data/good_c.parquet");
        Path bad = repoDir.resolve("bad_data/ARROW-RS-GH-6229-LEVELS.parquet");
        Utils.assertBadDataRejected("ARROW-RS-GH-6229-LEVELS.parquet",
                "[ARROW-RS-GH-6229-LEVELS.parquet] Column 'c' not found",
                multiFileReadAction(good, bad));
    }

    // ==================== Helpers ====================

    private void assertCorruptChecksumRejected(String relativePath, String expectedMessage,
                                                String expectedCauseMessage) throws IOException {
        Path testFile = repoDir.resolve(relativePath);

        assertThatThrownBy(() -> {
            try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile));
                 RowReader rowReader = fileReader.rowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
        }).as("Expected %s to be rejected due to corrupt checksum", relativePath)
          .hasMessage(expectedMessage)
          .hasRootCauseMessage(expectedCauseMessage);
    }

    private ThrowableAssert.ThrowingCallable singleFileReadAction(String fileName) {
        Path testFile = repoDir.resolve("bad_data/" + fileName);
        return () -> {
            try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(testFile));
                 RowReader rowReader = fileReader.rowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
        };
    }

    private void assertBadDataRejected(String fileName) throws IOException {
        Utils.assertBadDataRejected(fileName, singleFileReadAction(fileName));
    }

    private void assertBadDataRejected(String fileName, String expectedMessage) throws IOException {
        Utils.assertBadDataRejected(fileName, expectedMessage, singleFileReadAction(fileName));
    }

    private ThrowableAssert.ThrowingCallable multiFileReadAction(Path good, Path bad) {
        return () -> {
            try (HardwoodContextImpl context = HardwoodContextImpl.create();
                 ParquetFileReader mfReader = ParquetFileReader.openAll(
                         List.of(InputFile.of(good), InputFile.of(bad)), context);
                 RowReader rowReader = mfReader.rowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
        };
    }
}
