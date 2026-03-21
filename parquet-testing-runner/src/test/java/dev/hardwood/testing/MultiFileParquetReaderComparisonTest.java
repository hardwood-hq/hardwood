package dev.hardwood.testing;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Comparison tests that validate Hardwood's output generated with MultiFileParquetReader
 * against the reference parquet-java implementation by comparing parsed results row-by-row, field-by-field.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiFileParquetReaderComparisonTest {

    private Path repoDir;

    @BeforeAll
    void setUp() throws IOException {
        repoDir = ParquetTestingRepoCloner.ensureCloned();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.hardwood.testing.Utils#parquetTestFiles")
    void compareWithReference(Path testFile) throws IOException {
        String fileName = testFile.getFileName().toString();

        assumeFalse(Utils.SKIPPED_FILES.contains(fileName),
                "Skipping " + fileName + " (in skip list)");

        List<GenericRecord> reference = Utils.readWithParquetJava(testFile);

        int rowIndex = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             MultiFileParquetReader mfReader = new MultiFileParquetReader(
                     List.of(InputFile.of(testFile)), context);
             MultiFileRowReader rowReader = mfReader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                Utils.compareRow(rowIndex, reference.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        assertThat(rowIndex).isEqualTo(reference.size());
    }

    @Test
    void compareMultiFileWithReference() throws IOException {
        Path dataDir = repoDir.resolve("data");

        Path fileA = dataDir.resolve("alltypes_plain.parquet");
        Path fileB = dataDir.resolve("alltypes_plain.snappy.parquet");

        // Reference: parquet-java, one file at a time, concatenated
        List<GenericRecord> reference = new ArrayList<>();
        reference.addAll(Utils.readWithParquetJava(fileA));
        reference.addAll(Utils.readWithParquetJava(fileB));

        // Hardwood: multi-file reader over same files in same order
        List<InputFile> inputs = List.of(InputFile.of(fileA), InputFile.of(fileB));

        int rowIndex = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             MultiFileParquetReader mfReader = new MultiFileParquetReader(inputs, context);
             MultiFileRowReader rowReader = mfReader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                Utils.compareRow(rowIndex, reference.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        assertThat(rowIndex).isEqualTo(reference.size());
    }

    @Test
    void emptyFileListThrowsException() {
        assertThatThrownBy(() ->
                new MultiFileParquetReader(List.of(), HardwoodContextImpl.create()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one file must be provided");
    }

    @Test
    void singleFileBehaviourIsSameAsSingleFileReader() throws IOException {
        Path file = repoDir.resolve("data/alltypes_plain.parquet");

        List<GenericRecord> reference = Utils.readWithParquetJava(file);

        int rowIndex = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             MultiFileParquetReader mfReader = new MultiFileParquetReader(
                     List.of(InputFile.of(file)), context);
             MultiFileRowReader rowReader = mfReader.createRowReader()) {

            while (rowReader.hasNext()) {
                rowReader.next();
                Utils.compareRow(rowIndex, reference.get(rowIndex), rowReader);
                rowIndex++;
            }
        }

        assertThat(rowIndex).isEqualTo(reference.size());
    }

    @Test
    void rejectsBadFileWhenEncountered() throws IOException {
        Path good = repoDir.resolve("data/alltypes_plain.parquet");
        Path bad  = repoDir.resolve("bad_data/PARQUET-1481.parquet");
        Utils.assertBadDataRejected("PARQUET-1481.parquet",
                "Invalid or corrupt physical type value: -7",
                multiFileReadAction(good, bad));
    }

    @Test
    void rejectDictheader() throws IOException {
        Path good = repoDir.resolve("data/alltypes_plain.parquet");
        Path bad  = repoDir.resolve("bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet");
        Utils.assertBadDataRejected("ARROW-RS-GH-6229-DICTHEADER.parquet",
                multiFileReadAction(good, bad));
    }

    @Test
    void rejectLevels() throws IOException {
        Path good = repoDir.resolve("data/good_c.parquet");
        Path bad  = repoDir.resolve("bad_data/ARROW-RS-GH-6229-LEVELS.parquet");
        Utils.assertBadDataRejected("ARROW-RS-GH-6229-LEVELS.parquet",
                "Value count mismatch for column 'c': metadata declares 1 values but pages contain 21",
                multiFileReadAction(good, bad));
    }

    private ThrowableAssert.ThrowingCallable multiFileReadAction(Path good, Path bad) {
        return () -> {
            try (HardwoodContextImpl context = HardwoodContextImpl.create();
                 MultiFileParquetReader mfReader = new MultiFileParquetReader(
                         List.of(InputFile.of(good), InputFile.of(bad)), context);
                 MultiFileRowReader rowReader = mfReader.createRowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                }
            }
        };
    }
}