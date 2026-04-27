/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqVariant;

import static org.assertj.core.api.Assertions.assertThat;

/// Byte-level regression test for shredded Variant reassembly. Each
/// `shredded_variant/case-NNN.parquet` fixture has one `.variant.bin` file per
/// row, containing the canonical `metadata || value` bytes produced by
/// Iceberg's reference reader. Reassembling the file with Hardwood and
/// concatenating the two sides must match byte-for-byte.
///
/// Lives in `parquet-testing-runner` because the fixtures arrive via the
/// `apache/parquet-testing` clone this module manages.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VariantShredReassemblerTest {

    /// Fixtures deliberately flagged as invalid per the spec — cases.json
    /// notes "not valid according to the spec; implementations can choose to
    /// error or read the shredded value." Reader behaviour is implementation
    /// defined so we skip the byte-level oracle check.
    private static final Set<String> INVALID_FIXTURES = Set.of(
            "case-043-INVALID.parquet",
            "case-125-INVALID.parquet",
            "case-084-INVALID.parquet"
    );

    /// parquet-java cannot read these; cases.json pairs them with a
    /// variant_file anyway. Since we can't open them to fetch rows, skip.
    private static final Set<String> UNREADABLE = Set.of(
            "case-040.parquet",
            "case-041.parquet",
            "case-042.parquet",
            "case-087.parquet",
            "case-127.parquet",
            "case-128.parquet",
            "case-131.parquet",
            "case-137.parquet",
            "case-138.parquet"
    );

    private Path fixturesDir;

    @BeforeAll
    void setUp() throws IOException {
        Path repoDir = ParquetTestingRepoCloner.ensureCloned();
        fixturesDir = repoDir.resolve("shredded_variant");
    }

    @TestFactory
    List<DynamicTest> eachFixtureMatchesReassembled() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(fixturesDir, "case-*.parquet")) {
            List<Path> files = new ArrayList<>();
            stream.forEach(files::add);
            Collections.sort(files);
            for (Path file : files) {
                String name = file.getFileName().toString();
                if (INVALID_FIXTURES.contains(name) || UNREADABLE.contains(name)) {
                    continue;
                }
                tests.add(DynamicTest.dynamicTest(name, () -> verifyFile(file)));
            }
        }
        return tests;
    }

    private void verifyFile(Path parquetFile) throws IOException {
        String base = parquetFile.getFileName().toString().replaceFirst("\\.parquet$", "");
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(parquetFile));
                RowReader rowReader = fileReader.rowReader()) {
            int rowIndex = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                Path expectedFile = fixturesDir.resolve(base + "_row-" + rowIndex + ".variant.bin");
                PqVariant variant = rowReader.getVariant("var");
                if (!Files.exists(expectedFile)) {
                    // cases.json uses `null` to denote a missing expected payload for that row.
                    assertThat(variant).as("row %d should be SQL NULL variant", rowIndex).isNull();
                    rowIndex++;
                    continue;
                }
                byte[] expected = Files.readAllBytes(expectedFile);
                assertThat(variant).as("row %d", rowIndex).isNotNull();
                byte[] actual = concat(variant.metadata(), variant.value());
                assertThat(actual)
                        .as("%s row %d (metadata || value)", base, rowIndex)
                        .isEqualTo(expected);
                rowIndex++;
            }
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
