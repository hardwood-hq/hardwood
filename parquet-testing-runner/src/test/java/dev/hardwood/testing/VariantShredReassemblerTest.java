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
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqVariant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private static final String VALUE_ALONGSIDE_TYPED_VALUE =
            "Malformed shredded Variant: both value and typed_value are set for a non-object value";

    private static final String NON_OBJECT_VALUE_WITH_SHREDDED_FIELDS =
            "Malformed shredded Variant: typed_value holds shredded object fields but value is not an object";

    /// How Hardwood rejects each error case in `cases.json`, keyed by fixture.
    private static final Map<String, Rejection> ERROR_CASE_REJECTIONS = Map.of(
            "case-040.parquet", Rejection.onRead(VALUE_ALONGSIDE_TYPED_VALUE),
            "case-042.parquet", Rejection.onRead(VALUE_ALONGSIDE_TYPED_VALUE),
            "case-087.parquet", Rejection.onRead(NON_OBJECT_VALUE_WITH_SHREDDED_FIELDS),
            "case-127.parquet", Rejection.onBuild(unsupportedCarrier("INT32 UINT_32")),
            "case-128.parquet", Rejection.onRead(NON_OBJECT_VALUE_WITH_SHREDDED_FIELDS),
            "case-137.parquet", Rejection.onBuild(unsupportedCarrier("FIXED_LEN_BYTE_ARRAY")));

    /// Where an error case is rejected: when the row reader is built, before any row is
    /// read, or when the offending row's Variant is read.
    private enum Stage {
        BUILD,
        READ
    }

    /// An expected rejection; `message` excludes the file and column prefix.
    private record Rejection(Stage stage, String message) {

        static Rejection onBuild(String message) {
            return new Rejection(Stage.BUILD, message);
        }

        static Rejection onRead(String message) {
            return new Rejection(Stage.READ, message);
        }
    }

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
                // Two classes of fixture carry no byte-level oracle: spec-invalid
                // ones (`-INVALID`), whose reader behaviour is implementation
                // defined, and error cases, which eachErrorCaseIsRejected covers.
                if (Utils.isInvalidFixture(name) || Utils.isShreddedVariantErrorCase(name)) {
                    continue;
                }
                tests.add(DynamicTest.dynamicTest(name, () -> verifyFile(file)));
            }
        }
        return tests;
    }

    /// Every fixture `cases.json` classifies as an error case is rejected with a
    /// [ParquetReadException]. A carrier the shredding specification does not allow is
    /// rejected when the row reader is built, whether or not a row populates `typed_value`;
    /// a row whose `value` and `typed_value` conflict is rejected when its Variant is read.
    /// An error case added upstream fails here until its rejection is added to
    /// [#ERROR_CASE_REJECTIONS].
    @TestFactory
    List<DynamicTest> eachErrorCaseIsRejected() {
        List<DynamicTest> tests = new ArrayList<>();
        Utils.shreddedVariantErrorCases().forEach((name, errorMessage) ->
                tests.add(DynamicTest.dynamicTest(name + ": " + errorMessage, () -> {
                    assertThat(ERROR_CASE_REJECTIONS)
                            .as("expected rejection of cases.json error case %s (%s)", name, errorMessage)
                            .containsKey(name);
                    assertRejected(fixturesDir.resolve(name), ERROR_CASE_REJECTIONS.get(name));
                })));
        return tests;
    }

    /// A column read filtered on the Variant group assembles it to evaluate the predicate,
    /// and rejects the carrier when the reader is built.
    @Test
    void unsupportedTypedValueCarrierIsRejectedByAFilteredColumnRead() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(
                InputFile.of(fixturesDir.resolve("case-127.parquet")))) {
            assertThatThrownBy(() -> fileReader.buildColumnReader("id").filter(FilterPredicate.isNotNull("var")).build())
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("[case-127.parquet] Shredded Variant typed_value has type INT32 UINT_32, "
                            + "which the Variant shredding specification does not allow");
        }
    }

    private static void assertRejected(Path file, Rejection rejection) throws IOException {
        String fileName = file.getFileName().toString();
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(file))) {
            if (rejection.stage() == Stage.BUILD) {
                assertThatThrownBy(fileReader::rowReader)
                        .isExactlyInstanceOf(ParquetReadException.class)
                        .hasMessage("[" + fileName + "] " + rejection.message());
                return;
            }
            try (RowReader rowReader = fileReader.rowReader()) {
                assertThatThrownBy(() -> readAllVariants(rowReader))
                        .isExactlyInstanceOf(ParquetReadException.class)
                        .hasMessage("[" + fileName + ": column 'var'] " + rejection.message());
            }
        }
    }

    private static void readAllVariants(RowReader rowReader) throws IOException {
        while (rowReader.hasNext()) {
            rowReader.next();
            rowReader.getVariant("var").value();
        }
    }

    private static String unsupportedCarrier(String carrier) {
        return "Shredded Variant typed_value has type " + carrier
                + ", which the Variant shredding specification does not allow";
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
